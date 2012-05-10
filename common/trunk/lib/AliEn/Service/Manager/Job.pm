package AliEn::Service::Manager::Job;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;

use AliEn::Database::TaskQueue;
use AliEn::Service::Manager;
use AliEn::JOBLOG;
use AliEn::Util;
use Classad;


#use AliEn::Service::Optimizer::Job::Splitting;
#use AliEn::Database::TaskPriority;
use Data::Dumper;

use vars qw (@ISA $DEBUG);
@ISA = ("AliEn::Service::Manager");

$DEBUG = 0;

my $self = {};

sub initialize {
  $self = shift;
  my $options = (shift or {});

  $DEBUG and $self->debug(1, "In initialize initializing service JobManager");
  $self->{SERVICE} = "Job";

  $self->{DB_MODULE} = "AliEn::Database::TaskQueue";
  $self->SUPER::initialize($options);

  if ($ENV{'ALIEN_N_MANAGER_JOB_SERVER'}) {
    $self->{PREFORK} = $ENV{'ALIEN_N_MANAGER_JOB_SERVER'};
  }

  $self->{LOGGER}->notice("JobManager", "In initialize altering tables....");
  $self->{DB}->resyncSiteQueueTable()
    or $self->{LOGGER}->error("JobManager", "Cannot resync the SiteQueue Table!")
    and return;

  $self->{JOBLOG}  = new AliEn::JOBLOG();

#  # Initialize TaskPriority table
   $self->{CONFIG} = new AliEn::Config() or return;

  return $self;
}

##############################################################################
# Public functions
##############################################################################
sub alive {
  my $this       = shift;
  my $host       = shift;
  my $port       = shift;
  my $cename     = (shift or "");
  my $version    = (shift or "");
  my $free_slots = (shift or "");

  my $date = time;

  $self->info("Host $host (version $version $cename) is alive");

  my ($error) = $self->{DB}->getFieldFromHosts($host, "hostId");

  if (!$error) {
    $self->InsertHost($host, $port)
      or return (-1, $self->{LOGGER}->error_msg);
  }

  $self->info("Updating host $host");

  if (
    !$self->{DB}->updateHost(
      $host,
      { status    => 'CONNECTED',
        connected => 1,
        hostPort  => $port,
        date      => $date,
        version   => $version,
        cename    => $cename
      }
    )
    ) {
    $DEBUG and $self->debug(1, "In alive unable to update host $host");
    return;
  }
  if ($cename ne "") {
    my $blocking = $self->_getSiteQueueBlocked($cename);

    if ($blocking ne "open") {
      $self->info("The queue $cename is blocked in the master queue!");
      $self->setSiteQueueStatus($cename, "closed-blocked");
      return "-2";
    }
  }

  $DEBUG and $self->debug(1, "In alive finished updating host $host");

  my %queue = $self->GetNumberJobs($host, $cename, $free_slots);

  $self->info("Maximum number of jobs $queue{maxjobs} ($queue{maxqueuedjobs} queued)");

  $self->setAlive();
  return {%queue};
}

sub GetNumberJobs {
  my $this       = shift;
  my $host       = shift;
  my $site       = shift;
  my $free_slots = shift;

  $DEBUG and $self->debug(1, "In GetNumberJobs fetching maxJobs, maxQueued, queues for host $host");

  my $data = $self->{DB}->getFieldsFromHosts($host, "maxJobs, maxQueued, queues")
    or $self->info("There is no data for host $host")
    and return;

  $DEBUG and $self->debug(1, "In GetNumberJobs got $data->{maxJobs},$data->{maxQueued},$data->{queues}");

  my %queue = split(/[=;]/, $data->{queues});
  $queue{"maxjobs"}       = $data->{maxJobs};
  $queue{'maxqueuedjobs'} = $data->{maxQueued};

  if (($data->{maxJobs} eq "-1") && $free_slots) {
    $queue{maxjobs} = $queue{maxqueuedjobs} = $free_slots;
  }

  if ($site ne "") {
    my $queuestat;
    $DEBUG and $self->debug(1, "Getting site statistics for $site ...");
    $queuestat = $self->_getSiteQueueStatistics($site);
    $DEBUG and $self->debug(1, "Got site statistics for $site...");

    # copy the additional information into the queue hash
    my $qhash;
    foreach $qhash (@$queuestat) {
      $DEBUG and $self->debug(1, "Processing Queue hash...");
      foreach (keys %$qhash) {
        $DEBUG and $self->debug(1, "Looping Queue hash $_ ... $qhash->{$_}");
        $queue{$_} = $qhash->{$_};
        $DEBUG and $self->debug(1, "Status $site: $_ = $queue{$_}");
      }
    }
  }

  return %queue;
}

sub InsertHost {
  my $this = shift;
  my $host = shift;
  my $port = shift;
  my $domain;

  $self->info("Inserting new host $host");

  ($host =~ /^[^\.]*\.(.*)$/) and $domain = $1;

  ($domain)
    or $self->{LOGGER}->error("JobManager", "In InsertHost domain of $host not known")
    and return;

  $self->info("Domain is '$domain'");

  my $domainId = $self->{DB}->getSitesByDomain($domain, "siteId");

  defined $domainId
    or $self->{LOGGER}->warning("JobManager", "In InsertHost error during execution of database query");

  if (!(defined $domainId) || !(@$domainId)) {
    my $domainSt = $self->{CONFIG}->getInfoDomain($domain);
    $domainSt
      or $self->{LOGGER}->error("JobManager", "In InsertHost domain $domain not known in the LDAP server")
      and return;

    $self->info("Domain: $domainSt->{DOMAIN}; domain name: $domainSt->{OU}");

    $DEBUG and $self->debug(1, "In InsertHost inserting new site");
    $self->{DB}->insertSite(
      { siteName     => $domainSt->{OU},
        siteId       => '',
        masterHostId => '',
        adminName    => $domainSt->{ADMINISTRATOR} || "",
        location     => $domainSt->{LOCATION} || "",
        domain       => $domain,
        longitude    => $domainSt->{LONGITUDE} || "",
        latitude     => $domainSt->{LATITUDE} || "",
        record       => $domainSt->{RECORD} || "",
        url          => $domainSt->{URL} || ""
      }
      )
      or $self->{LOGGER}
      ->error("JobManager", "In InsertHost error inserting the domain $domainSt->{DOMAIN} in the database")
      and return;

    $domainId = $self->{DB}->getSitesByDomain($domain, "siteId");

    defined $domainId
      or $self->{LOGGER}->warning("JobManager", "In InsertHost error during execution of database query")
      and return;

    @$domainId
      or $self->{LOGGER}->error("JobManager", "In InsertHost insertion of the domain $domainSt->{DOMAIN} did not work")
      and return;

  }
  $domainId = $domainId->[0]->{"siteId"};

  $DEBUG and $self->debug(1, "Inserting a new host");

  $self->{DB}->insertHostSiteId($host, $domainId)
    or $self->{LOGGER}->error("JobManager", "In InsertHost insertion of the host $host did not work")
    and return;

  $self->info("Host $host inserted");

  return 1;
}

sub enterCommand {
  my $this = shift;
  $self->{LOGGER} or $this->info("We are entering the command directly") and $self = $this;
  $DEBUG and $self->debug(1, "In enterCommand with @_");
  my $host       = shift;
  my $jobca_text = shift;

  my $priority = (shift or 0);
  my $splitjob = (shift or "0");
  my $oldjob   = (shift or "0");
  my $options = shift || {};

  my ($user) = split '@', $host;

  ($jobca_text)
    or $self->{LOGGER}->error("JobManager", "In enterCommand jdl is missing")
    and return (-1, "jdl is missing");

  $options->{silent} or $self->info("Entering a new command ");
  $jobca_text =~ s/\\/\\\\/gs;
  $jobca_text =~ s/&amp;/&/g;
  $jobca_text =~ s/&amp;/&/g;

  $DEBUG and $self->debug(1, "In enterCommand JDL: $jobca_text");
  my $job_ca = Classad::Classad->new($jobca_text);
  if (!$job_ca->isOK()) {
    $self->info("In enterCommand incorrect JDL input\n $jobca_text");
    return (-1, "incorrect JDL input");
  }

  $jobca_text = $job_ca->asJDL;

  my ($ok,  @inputdata)       = $job_ca->evaluateAttributeVectorString("InputData");
  my ($ok2, @inputcollection) = $job_ca->evaluateAttributeVectorString("InputDataCollection");

  my $direct = 1;
  ($ok  && $inputdata[0])       and $direct = 0;
  ($ok2 && $inputcollection[0]) and $direct = 0;
  $direct = 0;

  my $nbJobsToSubmit = 1;
  if ($jobca_text =~ / split =/i) {
  	#
  	$self->info("Let's assume that we can submit at least 10 subjobs");
  	$nbJobsToSubmit =10;
    #$DEBUG and $self->debug(1, "Master Job! Compute the number of sub-jobs");
    #$self->{DATASET} or $self->{DATASET} = AliEn::Dataset->new();
    #$self->{DATASET} or $self->info("Error creating the dataset parser") and return;
    #push @ISA, "AliEn::Service::Optimizer::Job::Splitting";
    #require AliEn::Service::Optimizer::Job::Splitting;
    #$nbJobsToSubmit = $self->_getNbSubJobs($job_ca);
    #pop @ISA;
    ##$nbJobsToSubmit
    #  or $self->info("Error getting the number of subjobs")
    #  and return (-1, "Error getting the number of subjobs");
    $direct = 0;
  }

  my $set = {
    jdl        => $jobca_text,
    status     => 'INSERTING',
    submitHost => $host,
    priority   => $priority,
    split      => $splitjob
  };


  # MaxWaitingTime
  ($ok,  my $maxwaitingtime) = $job_ca->evaluateAttributeString("MaxWaitingTime");

  
  if ($maxwaitingtime && $maxwaitingtime =~ /^(\d+)\s*([smh])?$/i ){
  	my $value =$1;
  	my $unit = $2 || "s";

  	my $max = 2*7*24*3600;
  	
  	$unit =~ /m/i and $value*=60;
  	$unit =~ /h/i and $value*=3600;
  	
  	if($max > $value){	
  		$set->{expires}=$value;
  		$self->info("The job will expire if it stays $value $unit waiting");
  	} else {
  		$self->info("The given expiration time is too big (bigger than $max)");
  	}	  	
  } 
  
  if ($direct) {
    $self->info("The job should go directly to WAITING");
    $set->{status} = 'WAITING';
    my ($ok, $req) = $job_ca->evaluateExpression("Requirements");
    my $agentReq = $self->getJobAgentRequirements($req, $job_ca);
    $set->{agentId} = $self->{DB}->insertJobAgent($agentReq);
  } else {
    $options->{silent} or $self->info("Checking your job quota...");
    my ($ok, $error) = $self->{DB}->checkJobQuota($user, $nbJobsToSubmit);
    ($ok > 0) or return (-1, $error);
    $options->{silent} or $self->info("OK");
  }

  ($ok, my $email) = $job_ca->evaluateAttributeString("Email");
  if ($email) {
    $self->info("This job will send an email to $email");
    $set->{notify} = $email;
  }

  my $procid = $self->{DB}->insertJobLocked($set, $oldjob)
    or $self->{LOGGER}->alert("JobManager", "In enterCommand error inserting job")
    and return (-1, "inserting job");

  $email and $self->putJobLog($procid, "trace", "The job will send an email to '$email'");

  my $msg = "Job $procid inserted from $host ($set->{status})";
  ($splitjob) and $msg .= " [Master Job is $splitjob]";

  $self->putJobLog($procid, "state", $msg);

  return $procid;
}

sub getJobAgentRequirements {
  my $self   = shift;
  my $req    = shift;
  my $job_ca = shift;
  $req = "Requirements = $req ;\n";
  foreach my $entry ("user", "memory", "swap", "localdisk", "packages") {
    my ($ok, $info) = $job_ca->evaluateExpression($entry);
    ($ok and $info) or next;
    $req .= " $entry =$info;\n";
  }

  return $req;
}
sub SetProcInfoBunchFromDB {
  my ($this)=shift;
  $self->info("And we have to retrieve the entries from the databse");
  my $messages=$self->{DB}->retrieveJobMessages();
  $self->SetProcInfoBunch("db_direct", $messages);
  return 1;
}

sub SetProcInfoBunch {
  my ($this, $host, $info) = (shift, shift, shift);
  $self->info("$host is sending the procinfo $#$info messages");
  foreach my $entry (@$info) {

    #This if statement is here only because in some old version, the
    #clustermonitor sends the information of 'proc' and 'procinfo' reversed
    if ($entry->{procinfo} eq "proc") {
      $entry->{procinfo} = $entry->{tag};
      $entry->{tag}      = "proc";
    }
    if (!$entry->{tag} or $entry->{tag} eq "proc") {
      $self->SetProcInfo($entry->{jobId}, $entry->{procinfo}, "silent");
    } else {
      $self->putJobLog($entry->{jobId}, $entry->{tag}, $entry->{procinfo});
    }
  }
  return 1;
}

sub SetProcInfo {
  my ($this, $queueId, $procinfo, $silent) = @_;

#runtime char(20), runtimes int, cpu float, mem float, cputime int, rsize int, vsize int, ncpu int, cpufamily int, cpuspeed int, cost float"
  $silent or $self->info("New Procinfo for $queueId:|$procinfo|");
  my $now = time;
  if ($procinfo) {
    my @values = split " ", $procinfo;
    $values[13] or $values[13] = '-1';    # si2k consumed by the job

    # SHLEE: should be removed
    #$values[13]='1.3'; #si2k
    #$values[4]='3'; #cputime

    my ($status) = $self->{DB}->getFieldsFromQueue($queueId, "status");

    my $updateRef = {
      runtime      => $values[0],
      runtimes     => $values[1],
      cpu          => $values[2],
      mem          => $values[3],
      cputime      => $values[4],
      rsize        => $values[5],
      vsize        => $values[6],
      ncpu         => $values[7],
      cpufamily    => $values[8],
      cpuspeed     => $values[9],
      cost         => $values[10],
      maxrsize     => $values[11],
      maxvsize     => $values[12],
      procinfotime => "$now",
      si2k         => $values[13]
    };

    if ($status->{status} eq "ZOMBIE") {

      # in case a zombie comes back ....
      $self->changeStatusCommand($queueId, 'token', $status->{status}, "RUNNING")
        or $self->{LOGGER}
        ->error("JobManager", "In SetProcInfo could not change job $queueId from $status->{status} to RUNNING");
      $updateRef->{status} = "RUNNING";
    }

    my ($ok) = $self->{DB}->updateJobStats($queueId, $updateRef);

    $self->putJobLog($queueId, "proc", $procinfo);

    ($ok)
      or $self->{LOGGER}->error("JobManager", "In SetProcInfo error updating job $queueId")
      and return;
  } else {
    my ($ok) = $self->{DB}->updateJobStats(
      $queueId,
      { runtime   => '00',
        runtimes  => '00',
        cpu       => '0',
        mem       => '0',
        cputime   => '0',
        rsize     => '0',
        vsize     => '0',
        ncpu      => '0',
        cpufamily => '0',
        cpuspeed  => '0',
        cost      => '0',
        maxrsize  => '0',
        maxvsize  => '0',
        si2k      => '-1'
      }
    );
    ($ok)
      or $self->{LOGGER}->error("JobManager", "In SetProcInfo error updating job $queueId")
      and return;
  }

  1;
}

sub changeStatusCommand {
  my $this      = shift;
  my $queueId   = shift;
  my $token     = shift ||"";
  my $oldStatus = shift;
  my $status    = shift;
  my $site      = (shift or "");
  my $error  = (shift or "");
  my $node   = (shift or "");
  my $spyurl = (shift or "");

  ($queueId)
    or $self->{LOGGER}->error("JobManager", "In changeStatusCommand queueId not specified")
    and return (-1, " queueId not specified");
  my $date = time;

  $self->info("Command $queueId [$site/$node/$spyurl] changed to $status from $oldStatus");
  if ($status!= /KILLED/){
    $self->{DB}->getUsername($queueId, $token) or
      return (-1, "Error validating the token of job $queueId");
  }
  my $set = {};

  ($spyurl) and $set->{spyurl} = $spyurl;
  ($site)   and $set->{site}   = $site;
  ($node)   and $set->{node}   = $node;
  $set->{procinfotime} = time;

  if ($status eq "WAITING") {
    $self->info("\tASSIGNED HOST $error");
    $set->{exechost} = $error;
  } elsif ($status eq "RUNNING") {
    $set->{started} = $date;
  } elsif ($status eq "STARTED") {
    $set->{started} = $date;
    $error and $set->{batchid} = $error;
  } elsif ($status eq "SAVING") {
    $self->info("Setting the return code of the job as $error");
    $set->{error} = $error;
  } elsif (($status =~ /SAVED.*/ && $error)
    || ($status =~ /(ERROR_V)|(STAGING)/)) {
    $self->info("Updating the jdl of the job");
    $set->{resultsjdl} = $error;
    } elsif ($status eq "DONE") {
    $set->{finished} = $date;

    #check if the job has to be validated...
    my $data = $self->{DB}->getFieldsFromQueue($queueId, "jdl,exechost,submithost");

    defined $data
      or $self->{LOGGER}->error("JobManager", "In changeStatusCommand error during execution of database query")
      and return (-1, "error getting the jdl of the job");

    %$data
      or $self->{LOGGER}->error("JobManager", "In changeStatusCommand there is no data for job $queueId")
      and return (-1, "there is no data for the job $queueId");

    $data->{host} =~ s/^.*\@//;
    my $validate = 0;
    $data->{jdl} =~ /validate\s*=\s*1/i and $validate = 1;

    my $port = $self->{CONFIG}->{CLUSTERMONITOR_PORT};
    if ($validate) {
      $self->info("Submitting the validation job");

      my $executable = "";
      $data->{jdl} =~ /executable\s*=\s*"?(\S+)"?\s*;/i and $executable = $1;
      $executable =~ s/\"//g;
      my $validatejdl = "[
Executable=\"$executable.validate\";
Arguments=\"$queueId $data->{host} $port\";
Requirements= member(other.GridPartition,\"Validation\");
Type=\"Job\";
			]";
      $DEBUG and $self->debug(1, "In changeStatusCommand sending the command to validate the result of $queueId...");
      $self->enterCommand("$data->{submithost}", "$validatejdl");
    }
  }

  if ($status =~ /^(ERROR.*)|(SAVED_WARN)|(SAVED)|(KILLED)|(FAILED)|(EXPIRED)$/) {
    $set->{spyurl} = "";
    $self->{DB}->deleteJobToken($queueId);
    $set->{finished} = $date;
  }

  my $putlog = "";
  foreach (keys %$set) {
    $putlog .= "$_: $set->{$_} ";
  }

  my $from = "";
  $oldStatus !~ /^%$/ and $from = sprintf " from %-10s", $oldStatus;

  my $message = sprintf "Job state transition$from to %-10s |=| ", $status;

  my ($ok) = $self->{DB}->updateStatus($queueId, $oldStatus, $status, $set, $self);

  ($ok) or $message = "FAILED $message";

  $self->putJobLog($queueId, "state", $message, $putlog);

  if (!$ok) {
    my $error = ($AliEn::Logger::ERROR_MSG || "updating job $queueId from $oldStatus to $status");
    $self->info("In changeStatusCommand $error");
    return (-1, $error);
  }

  $self->info("Command $queueId updated!");

  return 1;
}

sub getExecHost {
  my $this    = shift;
  my $queueId = shift;

  ($queueId)
    or $self->{LOGGER}->error("JobManager", "In getExecHost queueId not specified")
    and return (-1, "No queueid");

  $self->info("Getting exechost of $queueId");

  my $date = time;

  $DEBUG and $self->debug(1, "In getExecHost asking for job $queueId");

  my ($host) = $self->{DB}->getFieldFromQueue($queueId, "execHost");

  ($host)
    or $self->info("Error getting the host of $queueId")
    and return (-1, "no host");

  $host =~ s/^.*\@//;

  my ($port) = $self->{DB}->getFieldFromHosts($host, "hostPort")
    or $self->info("Unable to fetch hostport for host $host")
    and return (-1, "unable to fetch hostport for host $host");

  $self->info("Done $host and $port");
  return "$host###$port";
}

sub getTop {
  my $this = shift;
  my $args = join(" ", @_);
  my $date = time;

  my $usage =
"\n\tUsage: top [-status <status>] [-user <user>] [-host <exechost>] [-command <commandName>] [-id <queueId>] [-split <origJobId>] [-all] [-all_status] [-site <siteName>]";

  $self->info("Asking for top...");

  if ($args =~ /-?-h(elp)/) {
    $self->info("Returning the help message of top");
    return ("Top: Gets the list of jobs from the queue$usage");
  }
  my $where      = " WHERE 1=1";
  my $columns    = "queueId, status, name, execHost, submitHost ";
  my $all_status = 0;
  my $error      = "";
  my $data;

  my @columns = (
    { name    => "user",
      pattern => "u(ser)?",
      start   => 'submithost like \'',
      end     => "\@\%'"
    },
    { name    => "host",
      pattern => "h(ost)?",
      start   => 'exechost like \'%\@',
      end     => "'"
    },
    { name    => "submithost",
      pattern => "submit(host)?",
      start   => 'submithost like \'%\@',
      end     => "'"
    },
    { name    => "id",
      pattern => "i(d)?",
      start   => "queueid='",
      end     => "'"
    },
    { name    => "split",
      pattern => "s(plit)?",
      start   => "split='",
      end     => "'"
    },
    { name    => "status",
      pattern => "s(tatus)?",
      start   => "status='",
      end     => "'"
    },
    { name    => "command",
      pattern => "c(ommand)?",
      start   => "name='",
      end     => "'"
    },
    { name    => "site",
      pattern => "site",
      start   => "site='",
      end     => '\''
    }
  );

  while (@_) {
    my $argv = shift;

    ($argv =~ /^-?-all_status=?/) and $all_status = 1 and next;
    ($argv =~ /^-?-a(ll)?=?/)
      and $columns .= ", received, started, finished,split"
      and next;
    my $found;
    foreach my $column (@columns) {
      if ($argv =~ /^-?-$column->{pattern}$/) {
        $found = $column;
        last;
      }
    }
    $found or $error = "argument '$argv' not understood" and last;
    my $type = $found->{name};

    my $value = shift or $error = "--$type requires a value" and last;
    $data->{$type} or $data->{$type} = [];

    push @{$data->{$type}}, "$found->{start}$value$found->{end}";
  }
  if ($error) {
    my $message = "Error in top: $error\n$usage";
    $self->{LOGGER}->error("JobManager", $message);
    return (-1, $message);
  }

  foreach my $column (@columns) {
    $data->{$column->{name}} or next;
    $where .= " and (" . join(" or ", @{$data->{$column->{name}}}) . ")";
  }
       $all_status
    or $data->{status}
    or $data->{id}
    or $where .=
" and ( status='RUNNING' or status='WAITING' or status='OVER_WAITING' or status='ASSIGNED' or status='QUEUED' or status='INSERTING' or status='STARTED' or status='SAVING' or status='SAVED' or status='SAVED_WARN' or status='TO_STAGE' or status='STAGGING' or status='A_STAGED' or status='STAGING')";

  $where .= " ORDER by queueId";

  $self->info("In getTop, doing query $columns, $where");

  my $rresult = $self->{DB}->getFieldsFromQueueEx($columns, $where)
    or $self->{LOGGER}->error("JobManager", "In getTop error getting data from database")
    and return (-1, "error getting data from database");

  my @entries = @$rresult;
  $self->info("Top done with $#entries +1");

  return $rresult;
}

sub getJobInfo {
  my $this     = shift;
  my $username = shift;
  my @jobids   = @_;
  my $date     = time;
  my $result   = my $jobtag;

  my $cnt = 0;
  foreach (@jobids) {
    if ($cnt) {
      $jobtag .= " or (queueId = $_) or (split = $_) ";
    } else {
      $jobtag .= " (queueId = $_) or (split = $_) ";
    }
    $cnt++;
  }

  $self->info("Asking for Jobinfo by $username and jobid's @jobids ...");
  my $allparts =
    $self->{DB}->getFieldsFromQueueEx("count(*) as count, min(started) as started, max(finished) as finished, status",
    " WHERE $jobtag GROUP BY status");

  for (@$allparts) {
    $result->{$_->{status}} = $_->{count};
  }
  return $result;
}

sub getSystem {
  my $this     = shift;
  my $username = shift;
  my @jobtag   = @_;
  my $date     = time;

  $self->info("Asking for Systeminfo by $username and jobtags @jobtag...");
  my $jdljobtag;
  my $joinjdljobtag;
  $joinjdljobtag = join '%', @jobtag;

  if ($#jobtag >= 0) {
    $jdljobtag = "JDL like '%Jobtag = %{%$joinjdljobtag%};%'";
  } else {
    $jdljobtag = "JDL like '%'";
  }

  $self->info("Query does $#jobtag $jdljobtag ...");
  my $allparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, status", "WHERE $jdljobtag GROUP BY status");

  my $userparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, status",
    "WHERE submitHost like '$username\@%' and $jdljobtag GROUP BY status");

  my $allsites = $self->{DB}->getFieldsFromQueueEx("count(*) as count, site", " WHERE $jdljobtag Group by site");

  my $sitejobs =
    $self->{DB}
    ->getFieldsFromQueueEx("count(*) as count, site, status", "WHERE $jdljobtag GROUP BY concat(site, status)");

  my $totalcost = $self->{DB}->queryRow("SELECT sum(cost) as cost FROM QUEUE WHERE $jdljobtag");

  my $totalusercost =
    $self->{DB}->queryRow("SELECT sum(cost) as cost FROM QUEUE WHERE submitHost like '$username\@%' and $jdljobtag");

  my $totalUsage =
    $self->{DB}->queryRow(
"SELECT sum(cpu*cpuspeed/100.0) as cpu,sum(rsize) as rmem,sum(vsize) as vmem FROM QUEUE WHERE status='RUNNING' and $jdljobtag"
    );

  my $totaluserUsage =
    $self->{DB}->queryRow(
"SELECT sum(cpu*cpuspeed/100.0) as cpu,sum(rsize) as rmem,sum(vsize) as vmem FROM QUEUE WHERE submitHost like '$username\@%' and status='RUNNING' and $jdljobtag"
    );

  my $resultreturn = {};

  $resultreturn->{'totcpu'}      = ($totalUsage->{cpu}      or 0);
  $resultreturn->{'totrmem'}     = ($totalUsage->{rmem}     or 0);
  $resultreturn->{'totvmem'}     = ($totalUsage->{vmem}     or 0);
  $resultreturn->{'totcost'}     = ($totalcost->{cost}      or 0);
  $resultreturn->{'totusercost'} = ($totalusercost->{cost}  or 0);
  $resultreturn->{'totusercpu'}  = ($totaluserUsage->{cpu}  or 0);
  $resultreturn->{'totuserrmem'} = ($totaluserUsage->{rmem} or 0);
  $resultreturn->{'totuservmem'} = ($totaluserUsage->{vmem} or 0);

  foreach my $status (@{AliEn::Util::JobStatus()}) {
    $resultreturn->{"nuser" . lc($status)} = 0;
    $resultreturn->{"n" . lc($status)}     = 0;
  }

  for (@$allparts) {
    my $type = lc($_->{status});
    $resultreturn->{"n$type"} = $_->{count};
  }

  for my $info (@$userparts) {
    foreach my $status (@{AliEn::Util::JobStatus()}) {
      if ($info->{status} eq lc($status)) {
        $resultreturn->{"nuser$info->{status}"} = $info->{count};
        last;
      }

    }
  }

  my @sitestatistic = ();
  my $arrayhash;

  # create the headers
  push @sitestatistic, [ ("Site", "Done", "Run", "Save", "Zomb", "Queu", "Start", "Error", "Idle", "Iact") ];

  foreach $arrayhash (@$allsites) {
    my @sitearray = ();
    if ((!($arrayhash->{site})) or ($arrayhash->{site} eq '0')) {
      next;
    }
    $DEBUG and $self->debug(1, "Cheking site $arrayhash->{site}");
    push @sitearray, $arrayhash->{site};
    my $site = {};
    foreach (@$sitejobs) {
      if ($arrayhash->{site} eq $_->{site}) {
        $site->{$site->{status}} = $_->{count};
      }
    }
    push @sitearray, ($site->{DONE}    or "0");
    push @sitearray, ($site->{RUNNING} or "0");
    push @sitearray, ($site->{SAVING}  or "0");
    push @sitearray, ($site->{ZOMBIE}  or "0");
    push @sitearray, ($site->{QUEUED}  or "0");
    push @sitearray, ($site->{STARTED} or "0");
    my $totalError = 0;

    foreach (grep (/^ERROR_/, keys %{$site})) {
      $totalError += $site->{$_};
    }
    push @sitearray, $totalError;
    push @sitearray, ($site->{IDLE}        or "0");
    push @sitearray, ($site->{INTERACTIVE} or "0");

    push @sitestatistic, [@sitearray];
  }

  foreach (@{AliEn::Util::JobStatus()}) {
    my $var = lc($_);
    $resultreturn->{"frac$var"} = 100.0;
    if ($resultreturn->{"n$var"}) {
      $resultreturn->{"frac$var"} = 100.0 * $resultreturn->{"nuser$var"} / $resultreturn->{"n$var"};
    }
  }

  $resultreturn->{'efficiency'}                 = 100.0;
  $resultreturn->{'userefficiency'}             = 100.0;
  $resultreturn->{'assigninefficiency'}         = 0.0;
  $resultreturn->{'userassigninefficiency'}     = 0.0;
  $resultreturn->{'executioninefficiency'}      = 0.0;
  $resultreturn->{'userexecutioninefficiency'}  = 0.0;
  $resultreturn->{'submissioninefficiency'}     = 0.0;
  $resultreturn->{'usersubmissioninefficiency'} = 0.0;
  $resultreturn->{'expiredinefficiency'}        = 0.0;
  $resultreturn->{'userexpiredinefficiency'}    = 0.0;
  $resultreturn->{'validationinefficiency'}     = 0.0;
  $resultreturn->{'uservalidationinefficiency'} = 0.0;

  $resultreturn->{'nbaseefficiency'} =
    $resultreturn->{'ndone'} + $resultreturn->{'nerror_a'} + $resultreturn->{'nerror_e'} + $resultreturn->{'nerror_s'} +
    $resultreturn->{'nerror_r'} + $resultreturn->{'nexpired'} + $resultreturn->{'nzombie'};
  $resultreturn->{'nuserbaseefficiency'} =
    $resultreturn->{'nuserdone'} + $resultreturn->{'nusererror_a'} + $resultreturn->{'nusererror_e'} +
    $resultreturn->{'nusererror_s'} + $resultreturn->{'nusererror_r'} + $resultreturn->{'nuserexpired'} +
    $resultreturn->{'nzombie'};

  if ($resultreturn->{'nbaseefficiency'}) {
    my $d = 100.0 / $resultreturn->{'nbaseefficiency'};
    $resultreturn->{'efficiency'}             = $d * $resultreturn->{'ndone'};
    $resultreturn->{'assigninefficiency'}     = $d * $resultreturn->{'nerror_a'};
    $resultreturn->{'executioninefficiency'}  = $d * $resultreturn->{'nerror_e'};
    $resultreturn->{'submissioninefficiency'} = $d * $resultreturn->{'nerror_s'};
    $resultreturn->{'expiredinefficiency'}    = $d * $resultreturn->{'nexpired'};
    $resultreturn->{'validationinefficiency'} = $d * ($resultreturn->{'nerror_v'} + $resultreturn->{'nerror_vt'});
  }
  if ($resultreturn->{'nuserbaseefficiency'}) {
    my $d = 100.0 / $resultreturn->{'nuserbaseefficiency'};
    $resultreturn->{'userefficiency'}             = $d * $resultreturn->{'nuserdone'};
    $resultreturn->{'userassigninefficiency'}     = $d * $resultreturn->{'nusererror_a'};
    $resultreturn->{'userexecutioninefficiency'}  = $d * $resultreturn->{'nusererror_e'};
    $resultreturn->{'usersubmissioninefficiency'} = $d * $resultreturn->{'nusererror_s'};
    $resultreturn->{'userexpiredinefficiency'}    = $d * $resultreturn->{'nuserexpired'};
    $resultreturn->{'uservalidationinefficiency'} =
      $d * ($resultreturn->{'nusererror_v'} + $resultreturn->{'nusererror_vt'});
  }

  $resultreturn->{'sitestat'} = "";

  for my $i (0 .. $#sitestatistic) {
    my $aref = $sitestatistic[$i];
    my $n    = @$aref - 1;
    for my $j (0 .. $n) {
      $resultreturn->{'sitestat'} .= $sitestatistic[$i][$j];
      $resultreturn->{'sitestat'} .= "#";
    }
    $resultreturn->{'sitestat'} .= "###";
  }

  return ($resultreturn);
}

=item GetJobJDL

returns the jdl of the job received as input

=cut

sub GetJobJDL {
  my $this = shift;
  my $id   = shift;

  $self->debug(1, "Asking for the jdl of $id");
  $id or $self->info("No id to check in GetJOBJDL", 11) and return (-1, "No id to check");
  my $rc = $self->{DB}->queryValue("select jdl from QUEUE where queueId=$id");
  $self->info("Giving back the jdl of $id\n");
  return $rc;

}

sub getTrace {
  my $this = shift;
  $self->info("Asking for trace @_ $#_ ...");
  my $jobid = shift;
  $jobid and $jobid =~ /^trace$/ and $jobid = shift;
  $jobid or return (-1, "You have to specify a job id!");

  $self->info("... for job $jobid");

  my @results = {};
  if ($_[0] eq "") {
    @results = $self->{JOBLOG}->getlog($jobid, "state");
  } else {
    @results = $self->{JOBLOG}->getlog($jobid, @_);
  }
  return join("", @results);
}

=item getJobRc

Gives the return code of a job

=cut

sub getJobRc {
  my $this = shift;
  my $id   = shift;

  $id or $self->info("No id to check in getJobRc", 11) and return (-1, "No id to check");
  my $rc = $self->{DB}->queryValue("select error from QUEUE where queueId=$id");
  $self->info("The return code of $id is $rc\n");
  return $rc;
}

=item getPs

Gets the list of jobs from the queue

Possible flags:

=cut 

sub getPs {
  my $this  = shift;
  my $flags = shift;
  my $args  = join(" ", @_);
  my $date  = time;
  my $i;

  my $status =
"status='RUNNING' or status='WAITING' or status='OVER_WAITING' or status='ASSIGNED' or status='QUEUED' or status='INSERTING' or status='SPLIT' or status='SPLITTING' or status='STARTED' or status='SAVING'";
  my $site = "";

  $self->info("Asking for ps (@_)...");

  my $user = "";
  my @userStatus;
  if ($flags =~ s/d//g) {
    push @userStatus, "status='DONE'";
  }
  if ($flags =~ s/f//g) {
    push @userStatus, "status='EXPIRED'", "status like 'ERROR_\%'", "status='KILLED'", "status='FAILED'",
      "status='ZOMBIE'";
  }
  if ($flags =~ s/r//g) {
    push @userStatus, "status='RUNNING'", "status='SAVING'", "status='WAITING'", "status='OVER_WAITING'",
      "status='ASSIGNED'", "status='QUEUED'", "status='INSERTING'", "status='SPLITTING'", "status='STARTED'",
      "status='SPLIT'";
  }

  if ($flags =~ s/A//g) {
    push @userStatus, 1;
  }

  if ($flags =~ s/I//g) {
    push @userStatus, "status='IDLE'", "status='INTERACTIV'", "status='FAULTY'";
  }
  if ($flags =~ s/z//g) {
    push @userStatus, "status='ZOMBIE'";
  }

  while ($args =~ s/-?-s(tatus)? (\S+)//) {
    push @userStatus, "status='$1'";
  }
  @userStatus and $status = join(" or ", @userStatus);

  my @userSite;
  while ($args =~ s/-?-s(ite)? (\S+)//) {
    push @userSite, "site='$1'";
  }
  @userSite and $site = "and ( " . join(" or ", @userSite) . ")";

#    }
#my $query="SELECT queueId, status, jdl, execHost, submitHost, runtime, cpu, mem, cputime, rsize, vsize, ncpu, cpufamily, cpuspeed, cost, maxrsize, maxvsize,received,started,finished  FROM QUEUE WHERE ( status=$status ) ";
  my $where = "WHERE ( $status ) $site ";

  $args =~ s/-?-u(ser)?=?\s+(\S+)// and $where .= " and submithost like '$2\@\%'";

  #    $args =~ s/-?-e(xec)?=?\s+(\S+)// and $where.=" and exechost like '\%$2'";
  #    $args =~ s/-?-c(ommand)?=?\s+(\S+)// and $where.=" and jdl like '\%Executable\%$2\%'";
  $args =~ s/-?-i(d)?=?\s*(\S+)// and $where .= " and ( p.queueid='$2' or split='$2')";

  if ($flags =~ s/s//) {
    $where .= " and (upper(origJdl) like '\%SPLIT\%' or split>0 ) ";
  } elsif ($flags !~ s/S//) {
    $where .= " and ((split is NULL) or (split=0))";
  }

  if ($flags !~ /^\s*$/) {
    $self->info("Error: I don't know what to do with '-$flags'");
    return (-1, "wrong syntax (don't know '-$flags')");

  }
  if ($args !~ /^\s*$/) {
    $self->info("Error: I don't know what to do with '$args'");
    return (-1, "wrong syntax (don't know '$args')");
  }

  #    if ($args !~ /^\s*$/ ){
  #      my $message="argument '$args' in ps not known";
  #      $self->{LOGGER}->error("JobManager", "Error: $message");
  #      return(-1, "$message");
  #    }

#    my $query="SELECT queueId, status, jdl, execHost FROM QUEUE WHERE ( status=$status ) $user $exechost order by queueId";

  $where .= " and p.queueid=q.queueid ORDER BY q.queueId";

  $self->info("In getPs getting data from database \n $where");

  #my (@ok) = $self->{DB}->query($query);
  my $rresult = $self->{DB}->getFieldsFromQueueEx(
"q.queueId, status, origJdl as jdl, execHost, submitHost, runtime, cpu, mem, cputime, rsize, vsize, ncpu, cpufamily, cpuspeed, cost, maxrsize, maxvsize, site, node, split, procinfotime,received,started,finished",
    "q, QUEUEPROC p,QUEUEJDL pq $where"
    )
    or $self->{LOGGER}->error("JobManager", "In getPs error getting data from database")
    and return (-1, "error getting data from database");

  $DEBUG and $self->debug(1, "In getPs getting ps done");

  my @jobs;
  for (@$rresult) {
    $DEBUG and $self->debug(1, "Found jobid $_->{queueId}");
    my ($executable) = $_->{jdl} =~ /.*Executable\s*=\s*"([^"]*)"/i;
    my ($split)      = $_->{jdl} =~ /.*Split\s*=.*"(.*)".*/i;
    $_->{cost} = int($_->{cost});
    push @jobs,
      join("###",
      $_->{queueId},  $_->{status},       $executable,     $_->{execHost}, $_->{submitHost},
      $_->{runtime},  $_->{cpu},          $_->{mem},       $_->{cputime},  $_->{rsize},
      $_->{vsize},    $_->{ncpu},         $_->{cpufamily}, $_->{cpuspeed}, $_->{cost},
      $_->{maxrsize}, $_->{maxvsize},     $_->{site},      $_->{node},     $split,
      $_->{split},    $_->{procinfotime}, $_->{received},  $_->{started},  $_->{finished});
  }

  (@jobs) or (push @jobs, "\n");

  $self->info("ps done with $#jobs entries");

  return join("\n", @jobs);
}

sub getSpyUrl {
  my $this = shift;
  my $queueId = shift or return;
  $self->info("Get Spy Url for $queueId");
  my ($url) = $self->{DB}->getFieldFromQueue($queueId, "spyurl");
  $url or $self->info("In spy cannot get the spyurl for job $queueId");
  $self->info("Returning Spy Url for $queueId $url");
  return $url;
}

sub queueinfo {
  my $this = shift;
  my $jdl  = "";
  grep (/^-jdl$/, @_) and $jdl = "jdl,";
  @_ = grep (!/^-jdl$/, @_);
  my $site = shift;
  my $sql = "site,blocked, status, statustime,$jdl " . join(", ", @{AliEn::Util::JobStatus()});
  $self->info("Quering  $sql");
  my $array = $self->{DB}->getFieldsFromSiteQueueEx($sql, "where site like '$site' ORDER by site");
  (@$array) or return;

  return $array;
}

sub jobinfo {
  my $this   = shift;
  my $site   = shift or return;
  my $status = shift or return;
  my $delay  = shift or return;
  my $now    = time;

  my $array = $self->{DB}->getFieldsFromQueueEx("q.queueId",
"q, QUEUEPROC p where site like '$site' and status='$status' and ( ($now - procinfotime) > $delay) and q.queueid=p.queueid"
  );

  if (@$array) {
    return $array;
  } else {
    my @array;
    my $emptyjob = {};
    $emptyjob->{queueId} = 0;
    push @array, $emptyjob;
    return \@array;
  }
}

sub spy {
  my $this    = shift;
  my $queueId = shift;
  my $file    = shift;

  my ($site) = $self->{DB}->getFieldsFromQueue($queueId, "site");

  $self->info(
"In spy contacting the IS at http://$self->{CONFIG}->{IS_HOST}:$self->{CONFIG}->{IS_PORT} for $queueId at $site->{'site'}..."
  );

  my $result = $self->{SOAP}->CallSOAP("IS", "getService", $site->{'site'}, "ClusterMonitor")
    or $self->{LOGGER}->error("JobManager", "In spy error contacting the information service")
    and return (-1, "Error contacting the IS");

  $result = $result->result;

  $self->info("In spy got http://$result->{'HOST'}:$result->{'PORT'}  ...");

  my $url = $self->getSpyUrl($queueId);
  $url or return (-1, "The job $queueId is no longer in the queue");

  my $result2 =
    SOAP::Lite->uri('AliEn/Service/ClusterMonitor')->proxy("http://$result->{'HOST'}:$result->{'PORT'}")
    ->getSpyFile($queueId, $file, $url, @_);

  $result2
    or $self->info("In spy could not contact the clustermonitor at http://$result->{'HOST'}:$result->{'PORT'}")
    and return (-1, "Error contacting the clustermonitor at http://$result->{'HOST'}:$result->{'PORT'}");

  return $result2->result;
}



sub validateProcess {
  my $this    = shift;
  my $queueId = shift;

  my $date = time;

  ($queueId)
    or $self->{LOGGER}->error("JobManager", "In validateProcess queueId is missing")
    and return (-1, "No queueId");

  $self->info("Validating process $queueId...");

  my ($data) = $self->{DB}->getFieldsFromQueue($queueId, "jdl, exechost, submithost");

  defined $data
    or $self->{LOGGER}->error("JobManager", "In validateProcess error during execution of database query")
    and return (-1, "during execution of database query");

  %$data
    or $self->{LOGGER}->error("JobManager", "In validateProcess process $queueId does not exist")
    and return (-1, "process $queueId does not exist");

  #my ( $oldJdl, $exechost, $submithost ) = split "###", $data;
  my $hostname = $data->{exechost};
  $hostname =~ s/^.*\@//;

  my ($port) = $self->{DB}->getFieldFromHosts($hostname, "hostport")
    or $self->info("Unable to fetch hostport for host $hostname")
    and return (-1, "unable to fetch hostport for host $hostname");

  $DEBUG and $self->debug(1, "In validateProcess sending a signal to $hostname $port to kill the process... \n");

  my $executable = "";
  $data->{jdl} =~ /executable\s*=\s*"?(\S+)"?\s*;/i and $executable = $1;
  $executable =~ s/\"//g;
  $executable
    or $self->info("Error getting the name of the executable!")
    and return (-1, "error getting the name of the executable");
  my $jdl = "[
  Executable=\"$executable.validate\";
  Arguments=\"$queueId $hostname $port\";
  Requirements= member(other.GridPartition,\"Validation\");
  Type=\"Job\";
]\n";

  $self->enterCommand($data->{submithost}, $jdl,0);

  $self->info("Validation done");
  return 1;
}

sub _getSiteQueueBlocked {
  my $self     = shift;
  my $site     = shift;
  my $blocking = $self->{DB}->getFieldsFromSiteQueueEx("blocked", "where site='$site'");
  @$blocking and return @$blocking[0]->{'blocked'};
  return;
}

sub _getSiteQueueStatistics {
  my $self = shift;
  my $site = shift;
  return $self->{DB}->getFieldsFromSiteQueueEx(join(", ", @{AliEn::Util::JobStatus()}), "where site='$site'");
}

sub _setSiteQueueBlocked {
  my $self = shift;
  my $site = shift;
  my $set  = {};
  $set->{'blocked'} = 'locked-error-sub';
  if ($ENV{ALIEN_IGNORE_BLOCK}) {
    $self->info("IGNORING THE BLOCKING");
    return 1;
  }
  return $self->{DB}->updateSiteQueue($set, "site='$site'");
}

sub setSiteQueueBlocked {
  my $this = shift;
  my $site = shift;
  return $self->_setSiteQueueBlocked($site);
}

sub getSiteQueueStatistics {
  my $this = shift;
  my $site = shift;
  return _getSiteQueueStatistics($site);
}

sub setSiteQueueStatus {
  my $this = shift;
  return $self->{DB}->setSiteQueueStatus(@_);
}

sub putJobLog {
  my $this    = shift;
  my $procid  = shift or return (-1, "no process id specified");
  my $tag     = shift or return (-1, "no tag specified");
  my $message = shift or return (-1, "no message specified");
  $self->{JOBLOG}->putlog($procid, $tag, "$message", @_);
}

#_______________________________________________________________________________________________________________________

#sub checkJobQuota {
#  my $self = shift;
#  my $user = shift
#    or $self->info("In checkJobQuota user is missing\n")
#    and return (-1, "user is missing");
#  my $nbJobsToSubmit = shift;
#  (defined $nbJobsToSubmit)
#    or $self->info("In checkJobQuota nbJobsToSubmit is missing\n")
#    and return (-1, "nbJobsToSubmit is missing");
#
#  $DEBUG and $self->debug(1, "In checkJobQuota user:$user, nbJobs:$nbJobsToSubmit");
#
#  my $array = $self->{PRIORITY_DB}->getFieldsFromPriorityEx(
#"unfinishedJobsLast24h, maxUnfinishedJobs, totalRunningTimeLast24h, maxTotalRunningTime, totalCpuCostLast24h, maxTotalCpuCost",
#    "where " . $self->{PRIORITY_DB}->reservedWord("user") . " like '$user'"
#    )
#    or $self->info("Failed to getting data from PRIORITY table")
#    and return (-1, "Failed to getting data from PRIORITY table");
#  $array->[0]
#    or $self->{LOGGER}->error("User $user not exist")
#    and return (-1, "User $user not exist in PRIORITY table");
#
#  my $unfinishedJobsLast24h   = $array->[0]->{'unfinishedJobsLast24h'};
#  my $maxUnfinishedJobs       = $array->[0]->{'maxUnfinishedJobs'};
#  my $totalRunningTimeLast24h = $array->[0]->{'totalRunningTimeLast24h'};
#  my $maxTotalRunningTime     = $array->[0]->{'maxTotalRunningTime'};
#  my $totalCpuCostLast24h     = $array->[0]->{'totalCpuCostLast24h'};
#  my $maxTotalCpuCost         = $array->[0]->{'maxTotalCpuCost'};
#
#  $DEBUG and $self->debug(1, "nbJobs: $nbJobsToSubmit, unfinishedJobs: $unfinishedJobsLast24h/$maxUnfinishedJobs");
#  $DEBUG and $self->debug(1, "totalRunningTime: $totalRunningTimeLast24h/$maxTotalRunningTime");
#  $DEBUG and $self->debug(1, "totalCpuCostLast24h: $totalCpuCostLast24h/$maxTotalCpuCost");
#
#  if ($nbJobsToSubmit + $unfinishedJobsLast24h > $maxUnfinishedJobs) {
#    $self->info("In checkJobQuota $user: Not allowed for nbJobs overflow");
#    return (-1,
#"DENIED: You're trying to submit $nbJobsToSubmit jobs. That exceeds your limit (at the moment,  $unfinishedJobsLast24h/$maxUnfinishedJobs)."
#    );
#  }
#
#  if ($totalRunningTimeLast24h >= $maxTotalRunningTime) {
#    $self->info("In checkJobQuota $user: Not allowed for totalRunningTime overflow");
#    return (-1, "DENIED: You've already executed your jobs for enough time.");
#  }
#
#  if ($totalCpuCostLast24h >= $maxTotalCpuCost) {
#    $self->info("In checkJobQuota $user: Not allowed for totalCpuCost overflow");
#    return (-1, "DENIED: You've already used enough CPU.");
#  }
#
#  $self->info("In checkJobQuota $user: Allowed");
#  return (1, undef);
#}

sub getJobQuotaList {
  my $this = shift;
  my $user = shift
    or $self->{LOGGER}->error("In getJobQuotaList user is missing\n")
    and return (-1, "user is missing");

  my $array = $self->{DB}->getFieldsFromPriorityEx(
"user, unfinishedJobsLast24h, maxUnfinishedJobs, totalRunningTimeLast24h, maxTotalRunningTime, totalCpuCostLast24h, maxTotalCpuCost",
    "where user like '$user'"
    )
    or $self->{LOGGER}->error("Failed to getting data from PRIORITY table")
    and return (-1, "Failed to getting data from PRIORITY table");
  $array->[0]
    or $self->{LOGGER}->error("User $user not exist")
    and return (-1, "User $user not exist in PRIORITY table");

  return $array;
}

sub setJobQuotaInfo {
  my $this = shift;
  my $user = shift
    or $self->{LOGGER}->error("In setJobQuotaInfo user is missing\n")
    and return (-1, "user is missing");
  my $field = shift
    or $self->{LOGGER}->error("In setJobQuotaInfo field is missing\n")
    and return (-1, "field is missing");
  my $value = shift;
  (defined $value)
    or $self->{LOGGER}->error("In setJobQuotaInfo value is missing\n")
    and return (-1, "value is missing");

  my $set = {};
  $set->{$field} = $value;
  my $done = $self->{DB}->updatePrioritySet($user, $set);
  $done or return (-1, "Failed to set the value in PRIORITY table");

  if ($done eq '0E0') {
    ($user ne "%") and return (-1, "User '$user' not exist.");
  }

  return 1;
}

sub calculateJobQuota {
  my $this   = shift;
  my $silent = shift;
  $self->{CATALOGUE}->execute("calculateJobQuota", $silent);

  return 1;
}

#
#sub getFileQuotaList {
#  my $this = shift;
#  my $user = shift
#    or $self->{LOGGER}->error("In getFileQuotaList user is missing\n")
#    and return (-1, "user is missing");
#
#  my $array = $self->{PRIORITY_DB}->getFieldsFromPriorityEx("user, nbFiles, maxNbFiles, totalSize, maxTotalSize, tmpIncreasedNbFiles, tmpIncreasedTotalSize", "where user like '$user'")
#    or $self->{LOGGER}->error("Failed to getting data from PRIORITY table")
#    and return (-1, "Failed to getting data from PRIORITY table");
#  $array->[0] or $self->{LOGGER}->error("User $user not exist")
#    and return (-1, "User $user not exist in PRIORITY table");
#
#  return $array;
#}
#
#sub setFileQuotaInfo {
#  my $this = shift;
#  my $user = shift
#    or $self->{LOGGER}->error("In setFileQuotaInfo user is missing\n")
#    and return (-1, "user is missing");
#  my $field = shift
#    or $self->{LOGGER}->error("In setFileQuotaInfo field is missing\n")
#    and return (-1, "field is missing");
#  my $value = shift;
#  (defined $value) or $self->{LOGGER}->error("In setFileQuotaInfo value is missing\n")
#    and return (-1, "value is missing");
#
#  my $set = {};
#  $set->{$field} = $value;
#  my $done = $self->{PRIORITY_DB}->updatePrioritySet($user, $set);
#  $done or return (-1, "Failed to set the value in PRIORITY table");
#
#  if ($done eq '0E0') {
#    ($user ne "%") and return (-1, "User '$user' not exist.");
#  }
#
#  return 1;
#}
#
#sub calculateFileQuota {
#  my $this = shift;
#  my $silent = shift;
#  $self->{CATALOGUE}->execute("calculateFileQuota", $silent);
#
#  return 1;
#}
#

#_______________________________________________________________________________________________________________________

1;
