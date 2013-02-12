package AliEn::Service::ClusterMonitor;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;
use AliEn::Service;
use AliEn::Service::FTD;


# Neede to do no blocing wait (Sounds weird heh?)
use POSIX ":sys_wait_h";

use AliEn::Config;

use AliEn::Database::TaskQueue;
use AliEn::Database::TXT::ClusterMonitor;
use AliEn::Database::CE;
use AliEn::TMPFile;
use AliEn::RPC;

use AliEn::CE;
#use AliEn::X509;
use vars qw (@ISA $DEBUG);
push @ISA, "AliEn::Service";
use base qw(JSON::RPC::Legacy::Procedure);


$DEBUG=0;

use Compress::Zlib;

#$SIG{CHLD}="IGNORE";

$SIG{INT} = \&catch_zap;    # best strategy

#First, let's make sure that the CPUServer is running

my %commands = (
    "STARTCLIENT"   => "StartRemoteQueue",
    "STARTSE"   => "StartSE",
    "STARTFTD"   => "StartFTD",
    "STOPCLIENT"    => "StopRemoteQueue",
    "UPDATEJOBS"    => "UpdateDistribution",
    "RESTARTCLIENT" => "RestartClient",
    "UPDATE"        => "UpdateDistribution",
    "KILLPROCESS"   => "KillProcessBatch",
    "CLEAR"         => "ClearError",
    "RELOAD"        => "ReloadConfiguration",
    "CHECKJOBS" => "CheckRunningJobs",
);
my $self = {};


sub initialize {
  $self = shift;

  my $options = (shift or {});


  $options->{PACKINSTALL} = 1;
  $options->{PACKCONFIG} = 1;
  $options->{force} =1;

  $self->{RPC}=new AliEn::RPC;

  $self->{PORT} = $self->{CONFIG}->{CLUSTERMONITOR_PORT};

  $self->{SERVICE}="ClusterMonitor";
  $self->{SERVICENAME}=$self->{CONFIG}->{CE_FULLNAME};
  $self->{CONFIG} = $self->{CONFIG}->Reload($options);
#  $self->{CATALOGUE}= AliEn::UI::Catalogue::LCM->new();
  

  ( $self->{CONFIG} )
    or print STDERR "Error: Initial configuration not found!!\n"
      and return;

  $self->{HOST} = $self->{CONFIG}->{HOST};

  #$self->{user}=($options->{user} or $self->{CONFIG}->{CLUSTER_MONITOR_USER});

  $self->{MESSAGES_LASTACK}=0;
  $self->{TXTDB}  = new AliEn::Database::TXT::ClusterMonitor();
  
  ( $self->{TXTDB} ) or return;
  
  $self->info("Contacting the Manager/Job" );
  ( $self->checkConnection() ) or return;
  $self->{RPC}->CallRPC("IS", "markAlive", "ClusterMonitor", "ClusterMonitor_$self->{HOST}", $self->{HOST}, $self->{PORT}, {VERSION=>$self->{CONFIG}->{VERSION}} );


  my ($done) =$self->{RPC}->CallRPC("Manager/Job", "alive",  $self->{HOST}, $self->{PORT}, "", $self->{CONFIG}->{VERSION}) or return;
  
  use Data::Dumper;
  $self->info(Dumper($done));


    $self->{MAXJOBS} = ($done->{maxjobs} or "");

    ( $self->{MAXJOBS} )
      or $self->{LOGGER}->warning( "ClusterMonitor",
        "Cluster monitor is not allowed to execute any jobs" );

    $self->info("Starting the Cluster monitoring (max $self->{MAXJOBS} jobs)" );

    ($done)
      or $self->{LOGGER}->error( "ClusterMonitor", "Manager/Job not contacted" )
      and return;

  $self->info(
"Manager/Job contacted"
    );

  my $batch = "AliEn::LQ";
  ( $self->{CONFIG}->{CE} )
    and $batch = "AliEn::LQ::$self->{CONFIG}->{CE_TYPE}";
  $self->info( "Batch system: $batch" );
  eval "require $batch";

  $options->{"CM_HOST"} = $self->{HOST};
  $options->{"CM_PORT"} = $self->{PORT};

  #$self->{BATCH} = $batch->new($options);
  my @list = ();
  $self->{CONFIG}->{CEs} and @list = @{ $self->{CONFIG}->{CEs} };
  $self->info( "Batch systems: @list" );


  #$self->{X509}=new AliEn::X509 or return;
  #$self->{X509}->checkProxy() or
  #  $self->info("Error checking the proxy") and return;
  #############################################################################
  $self->{LOCALJOBDB}=new AliEn::Database::CE or return;

#  $self->forkCheckProcInfo() or return;
  return $self;
}


sub checkConnection {
    my $self = shift;
    my $service=(shift or "JOB_MANAGER");

    $self->debug(1, "In checkConnection");
    my $return;#=$self->{SOAP}->checkService("SOAPProxy");
    my $done=$self->{RPC}->checkService("IS");
    ($service eq "IS") and $return=$done;
    $self->{RPC}->checkService("Broker/Job");
    $done=($service eq "JOB_BROKER") and $return=$done;
    $done=$self->{RPC}->checkService("Manager/Job");
    ($service eq "JOB_MANAGER") and $return=$done;

    return $return; 
}
my $my_getFile = sub {
    my $file = shift;

    my ( $buffer, $zbuffer );
    my $maxlength = 1024 * 1024 * 10;
    open( FILE, "$file" );
    my $aread = read( FILE, $buffer, $maxlength, 0 );
    close(FILE);

    return $buffer;
};


##############################################################################
# Public functions
##############################################################################

sub spy {
    my $this = shift;

    my ($done) =$self->{RPC}->CallRPC("Manager/Job", "spy",@_) or return; 
    return $done;
}

sub getQueueInfo {
    my $this = shift;
    $self->info( "Sending Queue information for @_");
    my $queueName = shift or return;

    my ($done) =$self->{RPC}->CallRPC("Manager/Job", "queueinfo",$queueName) or return;
    my $returninfo="";

    foreach ( keys %$done ) {
	my $tmpreturn = sprintf "$_ %s\n",@$done[0]->{$_};
	$returninfo .= $tmpreturn;
    }
    $self->info( "Sending Queue information $returninfo");
    return $returninfo;
}

sub getExcludedHosts {
    my $now   = time;
#    my @hosts =
#      $self->{TXTDB}
#      ->query("SELECT name from EXCLUDEDHOSTS WHERE excludetime > $now");
#    $self->debug(1, "Excluded hosts are: @hosts" );
#    return @hosts;
    return;
}

sub addExcludedHost {
    my $this   = shift;
    my $host   = shift;
#    my $period = shift || 86400;
#    $self->{LOGGER}
#      ->info( "ClusterMonitor", "Excluding $host for $period seconds" );
#    $period = time + $period;
#    $self->{TXTDB}
#      ->insert("INSERT INTO EXCLUDEDHOSTS VALUES ('$host',$period)");
#    return 1;
    return 0;
}

sub alive {
    $self->{ALIVE_COUNTS}++;
    if ( ( $self->{ALIVE_COUNTS} == 12 ) ) {
        $self->info( "Clustermonitor contacted" );
        $self->{ALIVE_COUNTS} = 0;
    }
    return {
        "currentjobs" => $self->{CURRENTJOBS},
        "VERSION"     => $self->{CONFIG}->{VERSION},
	"Name"        => $self->{CONFIG}->{CE_FULLNAME},
    };
}

sub offerAgent {
  my ( $this, $user, $port, $queueName, $silent, $ca, $free_slots ) = @_;
  ($silent) or ( $silent = 0 );

  my $date = time;

  ($silent) or 
    $self->info("Command requested from $user, $port and $queueName ($free_slots)" );

  ( $self->checkConnection("JOB_BROKER") ) or return  (-1, "Broker is down");

  $self->debug(1, "Connection is up");

  #in this subroutine, we check that we are not running more than the 
  #number of jobs that we have been assigned
  $self->checkCurrentJobs($silent, $queueName, $free_slots);

  $self->debug(1, "Asking the Broker for a new job" );

  #########################################################################
  ## ask the broker for a new job

  my ($done) =$self->{RPC}->CallRPC("Broker/Job", "offerAgent",  $user, 
				    $self->{HOST}, $ca, $free_slots);

  if (! $done) {
    $self->info( "Error " .$self->{LOGGER}->error_msg() );
    ($done) =$self->{RPC}->CallRPC("Manager/Job", "setSiteQueueStatus",$queueName,"open-broker-error");
    return (-1, $self->{LOGGER}->error_msg);
  }

  my @jobs =$self->{SOAP}->GetOutput($done);
  if ($jobs[0] eq "-2") {	
    $self->info( "No more jobs in the queue");
    return (-2,  "No job matched your ClassAd");
  }

  $self->info("The clustermonitor got $#jobs entries!!");
  return @jobs;
}
#
sub getNumberJobs {
  my $this=shift;
  
  my $ref=shift;
  @_=@$ref;
  
  my $queueName=shift;
  my $free_slots=shift;
  $self->info("Asking the Manager how many jobs we can run");

  my ($done) =$self->{RPC}->CallRPC("Manager/Job", "alive",  $self->{HOST},
				    $self->{PORT}, $queueName, $self->{CONFIG}->{VERSION}, $free_slots);
  ($done) or return {rcmessages=>['The ClusterMonitor could not talk to the ManagerJob']  ,rcvalues=>[]};

  if ($done == -2) {
    $self->info("The queue is locked!");
    return {rcmessages=>['The master has blocked the queue for us!']  ,rcvalues=>[]};
  }
  $self->info("We can run $done->{maxqueuedjobs} and $done->{maxjobs};");

  return $done;
}

# Private function
# 
sub checkCurrentJobs {
  my $self=shift;
  my $silent=shift;
  my $queueName=shift;
  my $free_slots=shift;
  my ($done) =$self->{RPC}->CallRPC("Manager/Job", "alive",  $self->{HOST},
				    $self->{PORT}, $queueName, $self->{CONFIG}->{VERSION}, $free_slots);
  ($done) or die("Error contacting the Job Manager: ".$self->{LOGGER}->error_msg);

  ($done == -2) and die ("The master has blocked the queue for us!\n" );


  ##########################################################################
  ## info from the Job Manger
  $self->{MAXJOBS} = $done->{maxjobs};
  $self->{MAXQUEUEDJOBS} =$done->{maxqueuedjobs};

  $self->{RUNNING} = $done->{RUNNING};
  $self->{QUEUED}  = $done->{QUEUED};
  $self->{ASSIGNED}= $done->{ASSIGNED};
  $self->{ZOMBIE}  = $done->{ZOMBIE};
  $self->{IDLE}    = $done->{IDLE};
  $self->{INTERACTIVE} = $done->{INSERTING};
  $self->{SAVING}      = $done->{SAVING};
  $self->{STARTED}     = $done->{STARTED};


  #########################################################################
  ## calculate the queued and running jobs

  my $currentjobs =$self->{CURJOBS}= $self->{RUNNING} + $self->{QUEUED} + $self->{ASSIGNED} + $self->{IDLE} + $self->{INTERACTIVE} + $self->{SAVING} + $self->{STARTED};
  my $queuedjobs = $self->{QUEUED} + $self->{ASSIGNED};    

  $self->info( "currentjobs: $currentjobs     queuedjobs: $queuedjobs");    
  my $sql = "";
  my $name;
  $queueName =~ /::([^:]*)$/ and $name = $1;
  
  if ( defined $done->{$name} ) {
    ($silent)
      or $self->info("Limit for queue $name $done->{$name}" );
    ($self->{MAXJOBS}, $self->{MAXQUEUEDJOBS}) = split ":", $done->{$name};
    $sql = " and queue='$queueName'";
  }
  
  #########################################################################
  ## check for the maximum jobs
  
  if ( $currentjobs >= $self->{MAXJOBS} && !$free_slots) {
    ($silent)
      or $self->{LOGGER}->notice( "ClusterMonitor",
				  "Already executing $self->{MAXJOBS} jobs" );
    
    
    ($done) =$self->{RPC}->CallRPC("Manager/Job", "setSiteQueueStatus",$queueName,"closed-maxrunning");
    die (  "executing maximum number of jobs\n" );
  }
  
  #########################################################################
  ## check for the maximum queued jobs
  
  if ( $queuedjobs >= $self->{MAXQUEUEDJOBS} ) {
    ($silent)
      or $self->{LOGGER}->notice( "ClusterMonitor",
				  "There are $self->{MAXQUEUEDJOBS} jobs queued" );
    
    ($done) =$self->{RPC}->CallRPC("Manager/Job", "setSiteQueueStatus",$queueName,"closed-maxqueued");
    
    die( "maximum number of queued jobs ($self->{MAXQUEUEDJOBS})\n" );
  }
  return 1;
}
#
#
#
sub getJobAgent {
  my $this=shift;
  my $agentId=shift;
  my $wn=shift;
  my $user=shift;
  $self->info( "Getting a job to be executed (by $user in $wn, agentId is $agentId)" );

  my ($done) =$self->{RPC}->CallRPC("Broker/Job", "getJobAgent",$user, $self->{CONFIG}->{HOST},  @_);
  ($done) or return (-1, $self->{LOGGER}->error_msg);


  $self->info("Getting the job done");
  return @$done;
  
}


sub GetProcInfo {
    my ( $this, $queueId ) = @_;

  $self->info( "Get Procinfo for $queueId: !!" );
    my @get = $self->{TXTDB}->query("SELECT runtime,runtimes,cpu,mem,cputime,rsize,vsize,ncpu,cpufamily,cpuspeed,cost from PROCESSES where queueId = ?", undef, {bind_values=>[$queueId]});
    $self->info( "Get Procinfo for $queueId: @get" );
    return $get[0];
}


sub SetProcInfo {
  my $this=shift;
  my $ref=shift;
  @_=@$ref;
  my (  $queueId, $procinfo) = @_;

  #runtime char(20), runtimes int, cpu float, mem float, cputime int, rsize int, vsize int, ncpu int, cpufamily int, cpuspeed int, cost float"
  $self->info( "Set Procinfo for $queueId: $procinfo!!" );

  return $self->{LOCALJOBDB}->insertMessage($queueId, "proc", $procinfo,0);
}

########### to be reimplemented
sub getIdleProcess() {
    my $this = shift;
    my $executable = shift;
#    my ($data) =
#	$self->{TXTDB}
#    ->query("SELECT nodeName,queueId from PROCESSES where status='IDLE' and command='$executable' LIMIT 1");
#    $self->info( "SELECT nodeName,queueId from PROCESSES where status='IDLE' and command='$executable' LIMIT 1\n$data");
    
#    if (defined $data) {
#	if ($data) {
#	    my ( $nodeName,$queueId ) = split "###", $data;
#	    if ( (defined $nodeName) && (defined $queueId) ) {
#		$self->changeStatusCommand($queueId,"%","INTERACTIV");
#		return $data;
#	    }
#	    return;
#	}
#    }
    return;
}


sub killZombie {
    my $this = shift;
    my $queueId = shift;
    $self->debug(1, "Killing Zombie Process $queueId" );
    my @queues = @{ $self->{CONFIG}->{CEs_TYPE} };
    if (!@queues) {

      return "No batch system";
    }

    #my $status = $self->{BATCH}->kill($queueId);

    #return $status;
}

sub getStatus {
    my $this = shift;
    my $queueId = shift;

    $self->debug(1, "Getting Status of Job $queueId" );
    my @queues = @{ $self->{CONFIG}->{CEs_TYPE} };
    if (! @queues){

      return "No batch system";
    }
    #my $status = $self->{BATCH}->getStatus($queueId);

    #return $status;
}

sub getQueueStatus {
    my $this = shift;
    $self->debug(1, "Sending Queue status" );

    my @queues = @{ $self->{CONFIG}->{CEs_TYPE} };
    if (!@queues) {
      return "No batch system";
    }
    my $queue;
    my $message;

#    my @m = $self->{BATCH}->getQueueStatus();
#    $message .= join "", @m;

    return $message;
}


sub putJobLog : Public{
  my $this=shift;
  my $ref=shift;
  @_=@$ref;
  my ($queueId, $tag, $message)=(shift,shift,shift);
  return $self->{LOCALJOBDB}->insertMessage($queueId, $tag,$message,0);
}

###################################################
#
#  Bank functions  
# 
###################################################

sub bank {
    my $this=shift;
    return $self->_CallBank("bank", @_);
}

sub _CallBank {
	my $self = shift;
        my $function = shift;
  
  $self->info("Getting $function from the LBSG ");
  
  my ($done) = $self->{RPC}->CallRPC("LBSG",$function,@_);

  ($done) or return (-1, $self->{LOGGER}->error_msg);

  $self->info ("Done $function");
  
  return $done;

}

sub getOutput {
  my $self    = shift;
  
  if ($_[0] and ref $_[0] eq "ARRAY"){
    my $ref=shift;
    @_=@$ref;    
  }
  
  my $queueId = shift;
  my $output  = shift || "stdout";
  my $url     =shift;
  my @options =shift;
  ($queueId) or $self->info("Error: no queueId specified!!") and return;

  $self->info( "Getting the $output of $queueId" );

  if (!defined $url){

    ($url) = $self->{RPC}->CallRPC("Manager/Job","getSpyUrl", $queueId);
    $url or 
      $self->info( "Error job $queueId is not executed here" ) and return; 
    $self->info("And we have to contact '$url'");
  }

  if ( $url eq "" ) {
    $self->info( "Getting the local file");

    return $my_getFile->("$self->{CONFIG}->{LOG_DIR}/proc/$queueId/$output", @options);
  }

  $self->info("Contacting the jobagent at $url");

  $self->{RPC}->Connect($url, $url, "JobAgent");
  $self->info("And now, doing the call");
  $self->{LOGGER}->debugOn();
  my ($done) =$self->{RPC}->CallRPC($url, "getFile", $output, @options);

  $self->info("Finished Contacting the jobagent at $url"); ###############


  if(!$done) {
      $self->info("Could not get file via SOAP, trying to get it via LRMS");
      $done = $self->{BATCH}->getOutputFile($queueId,$output);
  }

  $self->debug(1, "Got $done" );
  $done or $self->info("No output") and return "No output\n";

  return $done;
}

sub getStdout {
    my $this    = shift;
    my $queueId = shift;
    return($self->getOutput($queueId,"stdout"));
}

sub getStderr {
    my $this    = shift;
    my $queueId = shift;
    return($self->getOutput($queueId,"stderr"));
}

sub getSpyFile {
  my $this = shift;
  my $queueId = shift;
  my $file    = shift;
  my $url     = shift;
  $self->info( "Getting the Spy File $file for job $queueId (url $url)");
  return ($self->getOutput($queueId,$file, $url, @_));
}

=item KillProcessBatch($queueId)

This subroutine is called whenever a job has to be cancelled. 
It will try to contact the JobAgent that started the service, and tell it
to die gracefully

=cut

sub KillProcessBatch {
  my $this    = shift;
  my $queueId = shift;

  $self->info("Removing process $queueId from the queue" );

  my $data=$self->{LOCALJOBDB}->queryRow("SELECT workernode FROM JOBAGENT where jobId = ?", undef, {bind_values=>[$queueId]});
  
  ($data and $data->{workernode}) or 
    $self->info( "Error getting the address of job $queueId (maybe the job already finished??)") and return 1;

  $self->{RPC}->Connect($data->{workernode}, "http://$data->{workernode}", 'JobAgent');
  $self->{RPC}->CallRPC($data->{workernode}, "dieGracefully", $queueId)
    or $self->info( "The job didn't want to die")
      and return;
#  $self->{BATCH}->kill($data->{batchId})
#    or $self->info( "Error killing $data->{batchId}" )
#      and return;

  $self->info( "Process removed" );

  return 1;
}

sub getFileSOAP {

    #This just opens the file from whereever and returns it
    # When dispatched via SOAP, do *NOT* use $self->{LOGGER}

    my $this = shift;
    my $file = shift;
    my $dir  = ( shift or undef );
    my $options= shift || {};
    my $buffer;
    my $maxlength = 1024 * 10000;


    if ($dir) {
        $file = $self->{CONFIG}->{$dir} . "/" . $file;
    }

    if (   ( $file =~ /^$self->{CONFIG}->{CACHE_DIR}.*/ )
        or ( $file =~ /^$self->{CONFIG}->{LOG_DIR}.*/ )
        or ( $file =~ /^$self->{CONFIG}->{TMP_DIR}.*/ )
        or ( $file =~ /^$ENV{ALIEN_ROOT}.*/ ) )
    {

        ( $file =~ /\.\./ ) and $self->{LOGGER}->warning( "ClusterMonitor",
            "User requests a file from a non authorized directory. File $file" )
          and return;

	my $open="$file";
	$options->{grep} and $open="grep '$options->{grep}' $file|" and
	  $self->info("Returning only the entries that match $options->{grep}");
	$options->{head} and $open="head -$options->{head} $file|" and
	  $self->info("Returning the first $options->{head} lines of $file");
	$options->{tail} and $open="tail -$options->{tail} $file|" and
	  $self->info("Returning the last $options->{tail} lines of $file");
        if ( open( FILE, $open ) ) {
	  my $aread = read( FILE, $buffer, $maxlength, 0 );
	  close(FILE);
	  ( $aread < $maxlength ) or return;
	  $self->info( "$file" );
        }
        else {
	  $self->info("$file does not exist" );
	  return;
        }

        $buffer or return "";
        my $var = SOAP::Data->type( base64 => $buffer );

        return $var;
    }
    else {

        # The directory we wish to get from is now autirized
        $self->{LOGGER}->warning( "ClusterMonitor",
            "User requests a file from a non authorized directory" );

        return;
    }
}


sub putFILE : Public {
    my $this    = shift;
    my $ref=shift;
    @_=@$ref;
    my $QUEUEID = shift;
    my $buffer  = shift;

    #my $directory=shift;
    my $file      = shift;
    my $maxlength = 1024 * 10000;
    $self->info( "Getting a request to put a file");

    my $fileName=AliEn::TMPFile->new({filename=>"$file", 
				      base_dir=>"$self->{CONFIG}->{LOG_DIR}/proc/"});
    ( -d "$self->{CONFIG}->{LOG_DIR}/proc" )
      or mkdir "$self->{CONFIG}->{LOG_DIR}/proc", 0777;

    if (-f "$fileName"){
      $self->info("Trying to overwrite the file $fileName");
      #return (-1, "The file $fileName already exists");
    }


    if (! open( FILE, ">$fileName" ) ) {
      $self->{LOGGER}->error("ClusterMonitor", "Error opening the file $fileName");
      return (-1, "Can't open '$fileName' (request for $file)");

    }

    syswrite( FILE, $buffer, $maxlength, 0 );
    close(FILE);
    $self->info("File $file saved in $fileName
File copied successfully!");

    return "$fileName";
}

sub getMessages() {
    my $this     = shift;
    return ;
}


sub RestartClient {
    my $this = shift;

    $self->info( "Restarting all queues" );
    $this->StopRemoteQueue();
    $this->StartRemoteQueue();

    return 1;
}

sub ClearError {
    my $this = shift;

    $self->info("Setting all ERROR_S and QUEUED to EXPIRED" );
    my (@queueId) =
      $self->{TXTDB}->insert(
"UPDATE PROCESSES set status='EXPIRED' WHERE status='ERROR_E' or status='ERROR_V' or status='ERROR_S' or status='ASSIGNED'"
      );

    return 1;
}

sub UpdateDistribution {
    my $this = shift;

    $self->info("Request to update the distribution" );
    my $error;
    $error =
      system(
"cd ~/AliEn;cvs -d:pserver:cvs\@alisoft.cern.ch:/soft/cvsroot -nQ update -AdP"
      );

    if ($error) {
      $self->{LOGGER}->warning( "ClusterMonitor", "Conflict in CVS-Update" );

      return;
    }

    
    $self->info( "Updating the distribution" );
    
    $error =
      system(
	     "cd ~/AliEn;cvs -d:pserver:cvs\@alisoft.cern.ch:/soft/cvsroot -Q update -AdP;$ENV{ALIEN_ROOT}/bin/alien-perl Makefile.PL;make;make install"
	    );
    $self->info("Update done with return $error" );
    $this->StopRemoteQueue();

    exec("$ENV{ALIEN_ROOT}/bin/alien StartMonitor");
    exit;
}


sub checkMessages {  
  my $self=shift;
  my $silent=shift ||0;

  my $method="info";
  $silent and $method="debug";

  my $time = time;
  $self->info("Ready to get the messages");
  my ($res)=$self->{RPC}->CallRPC('MessagesMaster', "getMessages", 'ClusterMonitor', $self->{HOST}, $self->{MESSAGES_LASTACK}) or 
    $self->info("Error getting the messages") and return;
  use Data::Dumper;
  print Dumper($res);

  $self->info("Got the messages");

  my $ref;

  my $UpperCaseName;

  (@$res)
    or $self->debug(1, "Still alive: No messages to execute" );

  foreach my $data ( @$res) {
    $self->info( "Message is for me!!" );
    $UpperCaseName = "\U$data->{Message}";
    $self->debug(2, "Command: $UpperCaseName" );
    $data->{ID}>$self->{MESSAGES_LASTACK} and $self->{MESSAGES_LASTACK}=$data->{ID};
    my $status = 'SUCCESS';

    if ( $commands{$UpperCaseName} ) {
      my $a = $commands{$UpperCaseName};
      if ( !( $self->$a( $data->{MessageArgs} ) ) ) {
	$status = 'FAILED';
      } 
    }
    else {
      $self->{LOGGER}->error( "ClusterMonitor", "Command $data->{Message} not known" );
      #Do not know command
      $status = "UNKNOWN";
    }
    $self->info("Message done with $status");
  }

  return 1;
}

sub checkQueuedJobs {
   my $self=shift;
   my $silent=shift;
   my $queueName = shift;
   my $method="info";
   $silent and $method="debug";
   my @debugLevel=();
   $silent and @debugLevel=1;
   ############################################################################
   # here we check the queue: if processes claimed to be queued, but they are not, they become a ERROR_E
   ############################################################################
     
   my $this = shift;
   
   $self->$method(@debugLevel, "In checkQueuedJobs .... for $queueName");
   
   $queueName or return;
   my ($done) =$self->{RPC}->CallRPC("Manager/Job", "jobinfo",$queueName,"QUEUED","600");
   my $returninfo="";
   ($done) or return;
   $self->$method(@debugLevel,"In checkQueuedJobs .... got return");
   foreach (@$done) {
     $self->$method( @debugLevel, "Checking Job Id $_->{queueId} ");
     if ($_->{queueId} > 0) {
       if ($self->{CONFIG}->{CE_TYPE} eq "LCG" ) {
	 
#	 if ( $self->{BATCH}->getStatus($_->{queueId}) eq "DEQUEUED" ) {
	   # change the status to ERROR_E
#	   $self->info( "Job Id $_->{queueId} is not queued anymore! Changing to ERROR_E");
#	   $self->changeStatusCommand($_->{queueId},"QUEUED", "ERROR_E","","");
#	 }
       }
     }
   }
   
   $self->$method(@debugLevel, "Finished checkQueuedJobs!");
 }

sub checkWakesUp {
  my $self   = shift;
  my $silent =(shift || 0);
  my $method ="info";

  $silent and $method="debug";
  my @debugLevel=();
  $silent and push @debugLevel,1;
  $self->$method( @debugLevel,  "Still alive and checking messages" );
#  my $done =
#      $self->{RPC}->CallRPC("IS", "alive", $self->{HOST}, $self->{PORT}, "",
#			      $self->{CONFIG}->{VERSION} );

  $self->checkQueuedJobs($silent, $self->{SERVICENAME});
  $self->checkMessages($silent);
#  $self->checkExpired($silent);
  $self->checkJobAgents($silent);
#  $self->{BATCH}->cleanUp();
#  $self->checkZombies($silent);
  return; 
}


# Forward the call from JobAgent to AliEn::Service::IS in central services
sub getCpuSI2k {
  my $this = shift;
  if ($_[0] and ref $_[0] eq  'ARRAY'){
    my $ref=shift;
    @_=@$ref;
  }
  my $cpu_type = shift;

  $self->info("Someone asked us the cpusi2k of $cpu_type");
  my $cache = AliEn::Util::returnCacheValue($self, "CPU_$cpu_type");
  $cache and return $cache;
  $self->info("Let's ask IS");
  my ($done) = $self->{RPC}->CallRPC("IS", "getCpuSI2k",  $cpu_type, $self->{HOST}) or return (-1, $self->{LOGGER}->error_msg());
  
  AliEn::Util::setCacheValue($self, "CPU_$cpu_type", $done);
  
  
  return $done;
}

sub checkJobAgents {
  my $self=shift;

  my $silent =(shift || 0);
  my $method ="info";
  my @data; 
  $silent  and $method="debug" and push @data, 1;

  $self->$method(@data, "Checking the queued agents");
  my @inBatch=();#$self->{BATCH}->getAllBatchIds();
  my $before=time();
  $before=$before-900;
  my $info=$self->{LOCALJOBDB}->query("SELECT * from JOBAGENT where timestamp < ?", undef, {bind_values=>[$before]});

  foreach my $job (@$info){
    $self->$method(@data, "Checking if the agent $job->{batchId} is still there...");
    if (!grep (/^$job->{batchId}$/, @inBatch)) {
      $self->info("Agent $job->{batchId} is dead!!\n");
      #$self->{LOCALJOBDB}->removeJobAgent( $self->{BATCH}->needsCleaningUp(), { batchId => $job->{batchId} });
    }
    @inBatch=grep (! /^$job$/, @inBatch);    
  }
  @inBatch and $self->$method(@data, "According to the batch, there are still @inBatch");
  return 1;
}

sub catch_zap {
    my $signame = shift;
    print STDERR "Cluster Monitor recievec Signal\n";
    die;
}

sub ReloadConfiguration{
  my $this=shift;

  $self->info( "Reloading the configuration");
  $self->{CONFIG}=$self->{CONFIG}->Reload({"force", 1});

  $self->info( "Done reloading\n");

  return 1;
}


sub DESTROY {
    my $this = shift;
}


sub CheckRunningJobs{
  my $this=shift;

  return 1;
}
# These funtions should be declared private

sub StartRemoteQueue {
  my $this = shift;
  return $self->StartService("StartCE", @_);
}

sub StopRemoteQueue {
  my $this     = shift;
  return $self->StartService("StopCE", @_);
}


sub StartSE {
  my $this=shift;
  return $self->StartService("StartSE", @_);
}
sub StartFTD {
  my $this=shift;

  return  $self->StartService("StartFTD", @_);
}

sub StartService{
  my $this=shift;
  my $service=(shift or return);

  $self->info("Starting the service $service" );
  my $done=system("$ENV{ALIEN_ROOT}/bin/alien", $service, @_);

  $self->info("Done with $done" );

  return 1;

}



###############
sub packmanOperations {
 my $this=shift;

 my $ref=shift;
 @_=@$ref;


 $self->info("CALLING A PACKMAN OPERATION");
 use AliEn::UI::Catalogue::LCM;  

 $self->{UI} or $self->{UI}=AliEn::UI::Catalogue::LCM->new();
 $self->info("GOT TTHE UI");
 $self->{UI} or $self->info("WE DIDN'T GET THE UI") and return (-1, "Error getting theuI");
 $self->info("Calling f_packman, with @_");
 my (@info, @loglist);
 eval {
   $self->{LOGGER}->keepAllMessages();
   @info=$self->{UI}->execute("packman", @_);
   @loglist = @{$self->{LOGGER}->getMessages()};
   $self->{LOGGER}->displayMessages();
 };
 if ($@){
   $self->info("THERE WAS AN ERROR: $@");
 }
 $self->info("DONE! result");
 
 return { rcvalues=>\@info, rcmessages=>\@loglist };
  
}
################

1;

