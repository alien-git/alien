package AliEn::Service::Broker::Job;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Database::TaskQueue;

use AliEn::Service::Broker;
use strict;
use Data::Dumper;

use AliEn::Util;

use vars qw (@ISA);

push @ISA,"AliEn::Service::Broker";
use base qw(JSON::RPC::Legacy::Procedure);


use AlienClassad;

my $self = {};

sub initialize {
	$self = shift;
	my $options = (shift or {});

	$self->debug(1, "In initialize initializing service TransferBroker");

	$self->{SERVICE} = "Job";

	$self->{DB_MODULE} = "AliEn::Database::TaskQueue";

	$self->forkCheckProcInfo() or return;

	$self->SUPER::initialize($options) or return;

	#srand();
}

#
# This function is called when a jobAgent starts, and tries to get a task
#

sub getJobAgent {
	my $this           = shift;
  if ($_[0] and ref $_[0] eq "ARRAY"){
    my $ref=shift;
    @_=@$ref;
  }
	my $user           = shift;
	my $host           = shift;
	my $site_jdl       = shift;
	my $site_stage_jdl = shift;

	my $date = time;

	#DO NOT PUT ANY print STATEMENTS!!! Otherwise, it doesn't work with an httpd container

	$self->redirectOutput("JobBroker/$host");
	$self->info("In findjob finding a job for $host");

  $self->info("Before the update");
	$self->{DB}->updateHost($host, {status => 'ACTIVE', date => $date})
		or $self->{LOGGER}->error("JobBroker", "In findjob error updating status of host $host")
		and return;
  $self->info("Ready to extract params");
   my ($ok, @info)= $self->getWaitingAgent($site_jdl);
   my ( $agentid, $queueName, $fileBroker, $remote)= @info;
#  my ($ok, $agentid, $queueName, $fileBroker, $remote)= $self->getWaitingAgent($site_jdl);
  $self->info("HELLO $ok, $agentid");
  if ($ok< 1) {
    $self->info("We didn't get an agent (@info)");
    return {execute => [ $ok, @info ]};
  }
	my ($queueid, $jdl, $jobUser) = $self->{DB}->getWaitingJobForAgentId($agentid, $queueName, $host);
	$queueid
		or $self->info("There were no jobs waiting for agentid!")
		and return {execute => [ -2, "No jobs waiting in the queue" ]};

  if ($remote){
    $self->info("This job will be executed on a remote site");
	  $self->putlog($queueid, "info", "The job will read data remotely");
  }

	if ($fileBroker) {
		my $split = $self->{DB}->queryValue("select split from QUEUE where queueid=?", undef, {bind_values => [$queueid]});
		$split
			or $self->info("Error getting the masterjob of $queueid, and doing split per file")
			and return {execute => [ -2, "No jobs waiting in the queue" ]};
		$self->info("****AND FOR THIS JOB WE HAVE TO CALCULATE THE INPUTDATA");
		$jdl = $self->findFilesForFileBroker($split, $queueid, $jdl, $queueName);
		
		$self->checkMoreFilesForAgent($split);
		if (!$jdl) {
			$self->info("In fact, there were no files for this job. Kill it");
			$self->putlog($queueid, "error", "There were no more files to analyze. Killing the job");
			$self->{DB}->killProcessInt($queueid, 'admin');
			return {execute => [ -2, "No jobs waiting in the queue (after fileBroker)" ]};
		}
	}
	$self->putlog($queueid, "state", "Job state transition from WAITING to ASSIGNED ");

	$self->info("Getting the token");
	my $token = $self->getJobToken($queueid, $jobUser);

	$self->info("I got as token $token");
	if ((!$token) || ($token eq "-1")) {
		$self->{DB}->updateStatus($queueid, "%", "ERROR_A");
		$self->putlog($queueid, "state", "Job state transition from ASSIGNED to ERRROR_A");
		$self->info("In requestCommand error getting the token");
		return -1, "getting the token of the job $queueid";
	}

	$self->info("Command $queueid sent !");
	$self->{DB}->setSiteQueueStatus($queueName, "jobagent-match", $site_jdl);
	return {execute => [ {queueid => $queueid, token => $token, jdl => $jdl, user => $jobUser} ]};
}
sub getWaitingAgent {
  my $self= shift;
  my $site_jdl=shift;

	my ($queueName, $params) = $self->extractClassadParams($site_jdl);
	$self->info("The extract params worked");
	$queueName eq '-1' and return (0, $params);
	
	$self->info("We have the parameters:" . Dumper($params));

	$params->{returnId} = 1;
	my $entry = $self->{DB}->getNumberWaitingForSite($params);
  $self->info("AND THE ENTRY IS");
  $self->info(Dumper($entry));	
	$entry and $entry->{entryId} and 
	  return 1, $entry->{entryId}, $queueName, $entry->{fileBroker}, 0;

	my $installedPackages=$params->{installedpackages};
	$self->info("Let's check if we need a package");
	delete $params->{installedpackages};
	delete $params->{returnId};
	$params->{returnPackages} = 1;
	my $packages = $self->{DB}->getNumberWaitingForSite($params);
	if ($packages) {
		$self->info("Telling the site to install packages '$packages'");
		my @packs = grep (!/\%/, split(",", $packages));
		$self->info("After removing, we have to install @packs ");
		$self->{DB}->setSiteQueueStatus($queueName, "jobagent-install-pack", $site_jdl);
		return  -3, @packs;
	}
	$self->info("Now, let's check with remote access");
	$params->{returnId} = 1;
	delete $params->{returnPackages};
	delete $params->{site};
	$params->{installedpackages}=$installedPackages;
	$self->info(Dumper($params));
	$entry = $self->{DB}->getNumberWaitingForSite($params);
	$self->info(Dumper($entry));
	($entry) and 
	 return 1, $entry->{entryId}, $queueName, $entry->{fileBroker}, 1;
	
	$self->info("Finally, let's check packages for remote access");
	delete $params->{installedpackages};
	delete $params->{returnId};
	$params->{returnPackages} = 1;
	 $packages = $self->{DB}->getNumberWaitingForSite($params);
	if ($packages) {
		$self->info("Telling the site to install packages '$packages'");
		my @packs = grep (!/\%/, split(",", $packages));
		$self->info("After removing, we have to install @packs ");
		$self->{DB}->setSiteQueueStatus($queueName, "jobagent-install-pack", $site_jdl);
		return  -3, @packs;
	}
	
	$self->info("In findjob no job to match");
	$self->{DB}->setSiteQueueStatus($queueName, "jobagent-no-match", $site_jdl);
	return  -2, "No jobs waiting in the queue" ;
}


sub checkMoreFilesForAgent {
	my $self  = shift;
	my $split = shift;
	$self->info("Checking if all the files have been assigned");
	my $v = $self->{DB}->queryValue("select count(1) from FILES_BROKER where split=? and queueid is null",
		undef, {bind_values => [$split]});
	$v and return 1;
	$self->info("There are no more files to be processed!");
  my $jobs=$self->{DB}->queryColumn("select queueid from QUEUE where statusId=5 and split=?",undef, {bind_values => [$split]});
  foreach my $job (@$jobs){
    $self->{DB}->killProcessInt($job, 'admin');
  }

	return 1;
}

sub findFilesForFileBroker {
	my $self    = shift;
	my $split   = shift;
	my $queueid = shift;
	my $jdl     = shift;
	my $queueName =shift;
	
  my @info =split(/\:\:/, $queueName);
	my $site    = $info[1];
	
  my $limit;

  my $father_jdl=$self->{DB}->queryValue("select origjdl from QUEUEJDL where queueid=?", undef, {bind_values=>[$split]});

  $father_jdl or $self->info("Error getting the jdl of $split while doing the file broker") and return;
  eval { 
      my $ca=AlienClassad::AlienClassad->new($father_jdl) or die("Erorr creating the classad");
      (my $ok, $limit)=$ca->evaluateAttributeString("SplitMaxInputFileNumber");
  };
  if ($@){
     $self->info("THERE WAS AN ERROR GETTING THE FILE BROKER NUMBER! $@");
  }

        	
  $limit or $limit=10; 



  $self->info("The limit is $limit");

	
	$site = '%,$site,%';

	$self->info(
"UPDATE FILES_BROKER set queueid=? where queueid is null and split=? and sites like ? limit $limit and $queueid,$split, $site"
	);
	my $done =
		$self->{DB}->do("UPDATE FILES_BROKER set queueid=? where queueid is null and split=? and sites like ? limit $limit",
		{bind_values => [ $queueid, $split, $site ]});

	$self->info("WE HAVE $done files");

	if ($done < $limit) {
		$self->info("We didn't get enough files. Asking to read remotely");
		my $done2 =
			$self->{DB}->do("UPDATE FILES_BROKER set queueid=? where queueid is null and split=? limit " . ($limit - $done),
			{bind_values => [ $queueid, $split ]});
		$self->info("And now we have $done2");
		if (!$done2) {
			$self->info("There are no more files to process");
			return;
		}
	}

	my $files =
		$self->{DB}->queryColumn("SELECT lfn from FILES_BROKER where queueid=?", undef, {bind_values => [$queueid]});
	$files or $self->info("Error retrieving the list of files") and return;
	my $inputdata = 'inputdata= {"' . join('","', @$files) . '"};';

	$jdl =~ s/;/;$inputdata/;

	$self->info("AND THE JDL is $jdl");

	return $jdl;

}

sub checkQueueOpen {
	my $self       = shift;
	my $site_ca    = shift;
	my $queue_name = shift;
	if (!$queue_name) {
		(my $ok, $queue_name) = $site_ca->evaluateAttributeString("CE");
		if (!$queue_name) {
			$self->info("Error getting the queue name from the classad");
			return ("", "Error getting the queue name from the classad");
		}
	}
	my $open = $self->{DB}->queryValue("select count(*) from SITEQUEUES where blocked='open' and site='$queue_name'");
	if (!$open) {
		$self->{DB}->setSiteQueueStatus($queue_name, "closed-blocked", $site_ca->asJDL());
		return ("", "The queue is locked ");
	}
	return (1, "");
}

# ***************************************************************
# Creates a new token randomly. Alway 32 caracters long.
# ***************************************************************
my $createToken = sub {
	srand();
	my $token = "";
	my @Array = (
		'X', 'Q', 't', '2', '!', '^', '9', '5', '3', '4', '5', 'o', 'r', 't', '{', ')', '}', '[',
		']', 'h', '9', '|', 'm', 'n', 'b', 'v', 'c', 'x', 'z', 'a', 's', 'd', 'f', 'g', 'h', 'j',
		'k', 'l', ':', 'p', 'o', 'i', 'u', 'y', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
		'A', 'S', 'D', 'F', 'G', 'H', 'J', 'Z', 'X', 'C', 'V', 'B', 'N', 'M'
	);
	my $i;
	for ($i = 0 ; $i < 32 ; $i++) {
		$token .= $Array[ rand(@Array) ];
	}
	return $token;
};

sub getJobToken {
	my $self   = shift;
	my $procid = shift;
	my $user   = shift;

	$self->info("Getting  job $procid (and $user)");

	($procid)
		or $self->info("Error: In getJobToken not enough arguments") and return;

	$self->{DB}->queryValue("select count(*) from JOBTOKEN where jobId=?", undef, {bind_values => [$procid]}) 
       and $self->info("Job $procid already given..") and return;

	my $token = $createToken->();

	$self->{DB}->insertJobToken($procid, $user, $token)
		or $self->{LOGGER}->warning("CatalogDaemon", "Error updating jobToken for user $user") and return (-1, "error setting the job token");

	$self->info("Sending job $procid to $user");
	return $token;
}

# Checks if there are any agents needed that fulfill the requirements
# It returns an array of arrays of jobagents and requirements.
#

sub extractClassadParams {
	my $self    = shift;
	my $ca_text = shift;
	my $params  = {};

	$self->debug(1, "Creating the classad");
	my $classad = AlienClassad::AlienClassad->new($ca_text);
	$self->debug(1, "Classad created");

	my ($ok, $queueName) = $classad->evaluateAttributeString("CE");
	my @jobAgents;
	($ok, my $msg) = $self->checkQueueOpen($classad, $queueName);
	$ok or return (-1, $msg);
	$params->{site} = "";
	$queueName =~ /::(.*)::/ and $params->{site} = $1;
	($ok, my @closese) = $classad->evaluateAttributeVectorString("CloseSE");
	$ok and $params->{extrasites}="";
	foreach my $se (@closese) {
		$se =~ /::(.*)::/ and $params->{site}!~$1 and $params->{extrasites}.="$1,";
	}
	$params->{extrasites}=~s/,$//;
	($ok, my $ttl) = $classad->evaluateAttributeInt("TTL");
	$params->{ttl} = $ttl || 84000;
	($ok, $params->{disk}) = $classad->evaluateExpression("LocalDiskSpace");
	($ok, my @pack) = $classad->evaluateAttributeVectorString("Packages");
	$params->{packages} = "," . join(",,", sort @pack) . ",";
	($ok, @pack) = $classad->evaluateAttributeVectorString("InstalledPackages");
	$params->{installedpackages} = "," . join(",,", sort @pack) . ",";
	($ok, @pack) = $classad->evaluateAttributeVectorString("GridPartitions");
	$params->{partition} = "," . join(",", sort @pack) . ",";
	$params->{ce}        = $queueName;
  ($ok, $params->{user}) = $classad->evaluateAttributeString("User");
	($ok, $params->{splitFiles})= $classad->evaluateAttributeString("SplitMaxInputFileNumber");

	return ($queueName, $params);
}

sub offerAgent : Public{
	shift;
	if ($_[0] and ref $_[0] eq "ARRAY"){
    my $ref=shift;
    @_=@$ref;
  }
	my $user       = shift;
	my $host       = shift;
	my $ca_text    = shift;
	my $free_slots = (shift or 0);

	$self->redirectOutput("JobBroker/$host");
	$self->info(
		"And now Checking if there are any agents that can be started in the machine $host (up to a maximum of $free_slots)"
	);

	$free_slots
		or $self->info("Not enough resources")
		and return (-1, "Not enough resources");

	my ($queueName, $params) = $self->extractClassadParams($ca_text);
	$queueName eq '-1' and return $queueName, $params;

	delete $params->{installedpackages};
	my $waiting = $self->{DB}->getNumberWaitingForSite($params);

	$self->info("We could run $waiting jobs there");
	$waiting > $free_slots and $waiting = $free_slots;
	if ($waiting) {
		$self->info("Telling the site to start $waiting job agents");
		$self->{DB}->setSiteQueueStatus($queueName, "open-matching", $ca_text);
		return [ $waiting, '[Type="Job";Requirements = other.Type == "machine" ]' ];
	}
	return -2;
}

sub putlog {
	my $self    = shift;
	my $queueId = shift;
	my $status  = shift;
	my $message = shift;
	return $self->{DB}->insertJobMessage($queueId, $status, $message, 0);
}

sub invoke {
	my $other = shift;
	my $op    = shift;

	$self->info("$$ Ready to do a task operation (and $op '@_')");

	my $mydebug = $self->{LOGGER}->getDebugLevel();
	my $params  = [];

	(my $debug, $params) = AliEn::Util::getDebugLevelFromParameters(@_);
	$debug and $self->{LOGGER}->debugOn($debug);
	$self->{LOGGER}->keepAllMessages();

	#  $op = "$self->{TASK_DB}->".$op;
	my @info = $self->{DB}->$op(@_);

	my @loglist = @{$self->{LOGGER}->getMessages()};

	$debug and $self->{LOGGER}->debugOn($mydebug);
	$self->{LOGGER}->displayMessages();
	$self->info("$$ invoke DONE with OP: $op (and @_)");    #, rc = $rc");
	$self->info("$$ invoke result: @info" . scalar(@info));
	return {                                                #rc=>$rc,
		rcvalues   => \@info,
		rcmessages => \@loglist
	};
}

1;
