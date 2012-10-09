package AliEn::Service::Broker::Job;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Database::TaskQueue;

#use AliEn::TokenManager;

use AliEn::Service::Broker;
use strict;


use AliEn::Util;

use vars qw (@ISA);

push @ISA,"AliEn::Service::Broker";
use base qw(JSON::RPC::Procedure);


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

	srand();
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
	my ($queueName, $params) = $self->extractClassadParams($site_jdl);
	$self->info("The extract params worked");
	$queueName eq '-1' and return $queueName, $params;
	use Data::Dumper;
	$self->info("We have the parameters:" . Dumper($params));

	$params->{returnId} = 1;
	my $entry = $self->{DB}->getNumberWaitingForSite($params);
	my $agentid;
	$entry and $agentid = $entry->{entryId};

	if (!$agentid) {
		$self->info("Let's check if we need a package");
		delete $params->{packages};
		delete $params->{returnId};
		$params->{returnPackages} = 1;
		my $packages = $self->{DB}->getNumberWaitingForSite($params);
		if (not $packages) {
			$self->info("In findjob no job to match");
			$self->{DB}->setSiteQueueStatus($queueName, "jobagent-no-match", $site_jdl);
			return {execute => [ -2, "No jobs waiting in the queue" ]};
		} else {
			$self->info("Telling the site to install packages '$packages'");
			my @packs = grep (!/\%/, split(",", $packages));
			$self->info("After removing, we have to install @packs ");
			$self->{DB}->setSiteQueueStatus($queueName, "jobagent-install-pack", $site_jdl);
			return {execute => [ -3, @packs ]};
		}
	}
	my ($queueid, $jdl, $jobUser) = $self->{DB}->getWaitingJobForAgentId($agentid, $queueName, $host);
	$queueid
		or $self->info("There were no jobs waiting for agentid!")
		and return {execute => [ -2, "No jobs waiting in the queue" ]};

	if ($entry->{fileBroker}) {
		my $split = $self->{DB}->queryValue("select split from QUEUE where queueid=?", undef, {bind_values => [$queueid]});
		$split
			or $self->info("Error getting the masterjob of $queueid, and doing split per file")
			and return {execute => [ -2, "No jobs waiting in the queue" ]};
		$self->info("****AND FOR THIS JOB WE HAVE TO CALCULATE THE INPUTDATA");
		$jdl = $self->findFilesForFileBroker($split, $queueid, $jdl, $params->{site}, $params->{splitFiles});
		$self->checkMoreFilesForAgent($split);
		if (!$jdl) {
			$self->info("In fact, there were no files for this job. Kill it");
			$self->putlog($queueid, "error", "There were no more files to analyze. Killing the job");
			$self->{DB}->updateStatus($queueid, '%', 'KILLED');
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

sub checkMoreFilesForAgent {
	my $self  = shift;
	my $split = shift;
	$self->info("Checking if all the files have been assigned");
	my $v = $self->{DB}->queryValue("select count(1) from FILES_BROKER where split=? and queueid is null",
		undef, {bind_values => [$split]});
	$v and return 1;
	$self->info("There are no more files to be processed!");

	$self->{DB}->do("UPDATE QUEUE set statusId=-14 where statusId=5 and split=?", {bind_values => [$split]}); # KILLED TO WAITING
	$self->{DB}->do("UPDATE ACTIONS set todo=1 where action=='KILLED'");

	return 1;
}

sub findFilesForFileBroker {
	my $self    = shift;
	my $split   = shift;
	my $queueid = shift;
	my $jdl     = shift;
	my $site    = shift || "";
	my $limit   = shift || 10;
	
	
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
	($ok, my $ttl) = $classad->evaluateAttributeInt("TTL");
	$params->{ttl} = $ttl || 84000;
	($ok, $params->{disk}) = $classad->evaluateExpression("LocalDiskSpace");
	($ok, my @pack) = $classad->evaluateAttributeVectorString("Packages");
	$params->{packages} = "," . join(",,", sort @pack) . ",";
	($ok, @pack) = $classad->evaluateAttributeVectorString("GridPartition");
	$params->{partition} = "," . join(",", sort @pack) . ",";
	$params->{ce}        = $queueName;

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

	delete $params->{packages};
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
