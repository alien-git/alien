# the processing scheme of the Process Monitor is changed as follows:
# First a Monitoring Check process is forked, then the usual 
# SOAP server is forked. After this, the main process spawns a child
# to execute the user program.
# The Monitoring Process, accumulates with a 1 sec. sleep information
# about the CPU/MEM etc. resource usage of all child processes
# After 10 accumulations, the information is passed to the 
# ClusterMonitor.
# The resource usage of the user program is measured with the GNU
# <time> command and written into a resource file /proc/<JOBID>/resources
# When the user program terminates, the ProcessMonitor <stopService> stops
# all the SOAP servers, while the Monitoring Checks runs, until it does not
# find any used resources and sends the last summary of the averages resource
# usage to the ClusterMonitor and terminates.

# - added support for interactive jobs:
#   - jobs with an Interactive Attribute in the JDL,
#   - change to status "IDLE", when they are started
#   - if they get a forward connection from the TcpRouter,
#   - they change to "INTERACTIVE", if the connection closes,
#   - they go back to IDLE


package AliEn::Service::JobAgent;

use strict;

use AliEn::UI::Catalogue::LCM;
use AliEn::Config;
use IO::Handle;
use POSIX;
use Compress::Zlib;
use AliEn::MSS;
use Archive::Zip;
use Filesys::DiskUsage qw /du/;
use AliEn::Service;
use Classad;
use LWP::UserAgent;
use Socket;
use Carp;
use vars qw(@ISA);
use AliEn::Classad::Host;
use AliEn::Util;
use AliEn::X509;
use AliEn::MD5;
use Filesys::DiskFree;
use AliEn::PackMan;
use AliEn::TMPFile;


@ISA=qw(AliEn::Service);

my $self = {};

$SIG{INT} = \&catch_zap;

=head1 NAME AliEn::Service::JobAgent


=head1 DESCRIPTION

This it the Job wrapper that gets executed on the worker node. First of all, it requests a job to execute from the Job Broker. Once it has a job, it downloads the input that it needs, and it executes it.

Each job has also a TTL (Time To Live), and the JobAgent checks that the process does not exceed it. The JobAgent itself also has a TTL

=head1 SYNOPSIS

 alien RunAgent


=head1 METHODS



=over


=item




=cut

sub dieGracefully {
  my $this=shift;
  my $queueId=shift;
  my $parent=getppid();
  $self->info("Someone sent me a message to die. I'm $$ (of $parent) and  $self->{CHILDPID}");

  my $workdir= $self->getWorkingDir();

  (  $workdir =~ m{/alien-job-$queueId$} ) or 
    $self->info("Error: someone is trying to kill $queueId, but we are $workdir") and return;
  
  $self->info("First, let's kill the child");
  $self->stopService($self->{CHILDPID});
  $self->info("Let's remove the working directory");
  system("rm", "-rf", $workdir);
  $self->stopService($parent);

  return 1;
}



#############PRIVATE METHODS

sub initialize {
  $self = shift;
  $self->{JOBAGENTSTARTS}=time();
  my $options =(shift or {});
  $self->debug(1, "Initializing the JobAgent in debug mode");
  $self->{SERVICEPID}=$$;
  $self->{WORKDIRFILE}="$self->{CONFIG}->{TMP_DIR}/jobagent.$$.options";

  $self->{FORKCHECKPROCESS} = 1;
  $self->{LISTEN}=1;
  $self->{PREFORK}=1;

  $self->{PROCESSID} = 0;
  $self->{STARTTIME} = '0';
  $self->{STOPTIME} = '0';
  $self->{OUTPUTFILES} = "";
  $self->{TTL}=($self->{CONFIG}->{CE_TTL} || 12*3600);
  $self->{TTL} and $self->info("This jobagent is going to live for $self->{TTL} seconds");
  ($self->{HOSTNAME},$self->{HOSTPORT}) =
    split ":" , $ENV{ALIEN_CM_AS_LDAP_PROXY};

  #$self->{HOST} = $ENV{'ALIEN_HOSTNAME'}.".".$ENV{'ALIEN_DOMAIN'};
  $self->{HOST} = $self->{CONFIG}->{HOST};

  $ENV{'ALIEN_SITE'} = $self->{CONFIG}->{SITE};
  $self->{CONFIG}->{SITE_HOST} and $ENV{'ALIEN_SITE_HOST'} = $self->{CONFIG}->{SITE_HOST};
  print "Executing in $self->{HOST}\n";
  $self->{PID}=$$;
  print "PID = $self->{PID}\n";

  my $packConfig=1;
  $options->{disablePack} and $packConfig=0;
  $self->{SOAP}=new AliEn::SOAP;

  $self->{SOAP}->{CLUSTERMONITOR}=SOAP::Lite
    ->uri("AliEn/Service/ClusterMonitor")
      ->proxy("http://$self->{HOSTNAME}:$self->{HOSTPORT}");

  $self->{CONFIG} = new AliEn::Config() or return;

  $self->{PORT} = $self->getPort();
  $self->{PORT} or return;

  $self->{SERVICE}="JobAgent";
  $self->{SERVICENAME}="JobAgent";
  $self->debug(1, "The initialization finished successfully!!");

  if ($self->{CONFIG}->{AGENT_API_PROXY}) {
      # configure the api service endpoint list
      $self->{CONFIG}->ConfigureApiClient();
      $self->info("Using Api Service as proxy - URL-Endpoints: $ENV{'GCLIENT_SERVER_LIST'}");
  } else {
      $self->info("Using Proxy Service as proxy");
  }

  $self->{JOBLOADED}=0;
  $self->{X509}= new AliEn::X509 or return;
  $self->{PACKMAN}=AliEn::PackMan->new({PACKMAN_METHOD=>"Local"}) or 
    $self->info("Error getting the packman") and return ;


  $self->{WORKDIR} = $ENV{HOME};
  # If specified, this directory is used. REMARK If $ENV{WORKDIR} is set, this is used!!
  $self->{CONFIG}->{WORK_DIR} and $self->{WORKDIR} = $self->{CONFIG}->{WORK_DIR};
    # If the batch-system defined this
  ( defined $ENV{WORKDIR} ) and $self->{WORKDIR} = $ENV{WORKDIR};

  
  ( defined $ENV{ALIEN_WORKDIR} ) and $self->{WORKDIR} = $ENV{ALIEN_WORKDIR};
  ( defined $ENV{TMPBATCH} ) and $self->{WORKDIR} = $ENV{TMPBATCH};
  $ENV{ALIEN_WORKDIR}=$self->{WORKDIR};

  return $self;
}

sub requestJob {
  my $self=shift;

  $self->{REGISTER_LOGS_DONE}=0;
  $self->{FORKCHECKPROCESS} = 0;
  $self->{CPU_CONSUMED}={VALUE=>0, TIME=>time};

  $self->GetJDL() or return;
  $self->info("Got the jdl");



  $self->{SOAP}->CallSOAP("CLUSTERMONITOR","jobStarts", $ENV{ALIEN_PROC_ID}, $ENV{ALIEN_JOBAGENT_ID});



#  $self->{LOGFILE}=AliEn::TMPFile->new({filename=>"proc.$ENV{ALIEN_PROC_ID}.out"});
  $self->{LOGFILE}="$self->{CONFIG}->{TMP_DIR}/proc.$ENV{ALIEN_PROC_ID}.out";
  if ($self->{LOGFILE}){
    $self->info("Let's redirect the output to $self->{LOGFILE}");
    $self->{LOGGER}->redirect($self->{LOGFILE});
  } else{
    $self->info("We couldn't redirect the output...");
  }
  $self->checkJobJDL() or $self->sendJAStatus('ERROR_JDL') and return;

  $self->info("Contacting VO: $self->{VOs}");

  $self->CreateDirs or $self->sendJAStatus('ERROR_DIRS') and return;

  #let's put the workdir in the file
  open (FILE, ">$self->{WORKDIRFILE}") or print "Error opening the file $self->{WORKDIRFILE}\n" and return;
  print FILE "WORKDIR=$self->{WORKDIR}\n";
  close FILE;

  #This subroutine has a fork. The father will do the rest, while the child returns and starts the JobAgent
  ( $self->startMonitor() ) or $self->sendJAStatus('ERROR_START') and return;

  # resource tracking
  $self->{MAXVSIZE} = 0;
  $self->{MAXRSIZE} = 0;
  $self->{SUMVSIZE} = 0;
  $self->{SUMRSIZE} = 0;
  $self->{SUMCOUNT} = 0;
  $self->{AVRSIZE}  = 0;
  $self->{AVVSIZE}  = 0;
  $self->{SUMCPU}   = 0; 
  $self->{AVCPU}    = 0;
  $self->{MAXRESOURCECOST} = 0;
  $self->{PROCINFO} = "";
  $self->{JOBLOADED}=1;

  if ($self->{MONITOR}) {
    my $cpu_type = $self->{MONITOR}->getCpuType();
    if($cpu_type){
      $cpu_type->{host} = $self->{HOST};
      my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR","getCpuSI2k", $cpu_type);
      if($done && $done->result){
        $self->info("SpecINT2k for this machine is ".$done->result);
	$self->{MONITOR}->setCpuSI2k($done->result);
      }else{
        $self->info("Got invalid SI2k estimation for this machine. Not reporting consumed ksi2k for this job.");
      }
    }else{
      $self->info("ApMon cannot determine cpu_type for this machine. Not reporting consumed ksi2k for this job.");
    }		      
    $self->{MONITOR}->addJobToMonitor($self->{PROCESSID}, $self->{WORKDIR}, $self->{CONFIG}->{CE_FULLNAME}.'_Jobs', $ENV{ALIEN_PROC_ID});
  }
  $self->sendJAStatus('JOB_STARTED');
  return 1;
}


sub changeStatus {
  my $self=shift;
  my @print=@_;
  map {defined $_ or $_=""} @print;
  $self->debug(1, "We have to contact $self->{VOs}");
  my $done;
  foreach my $data (split (/\s+/, $self->{VOs})) {
    my ($org, $cm, $id, $token)=split("#", $data);
    $self->info("Contacting $org");

    $self->info("Putting the status of $id to @print");
    $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR_$org", "changeStatusCommand", $id, @_ );
    if ($self->{MONITOR}) {
      #$self->info("Writting status to parent process $id=$_[1]");
      $self->writePipeMessage($self->{JOB_STATUS_WTR}, "$id=$_[1]\n");
      $self->{MONITOR}->sendParameters($self->{CONFIG}->{CE_FULLNAME}.'_Jobs', $id, { 'status' => AliEn::Util::statusForML($_[1]), 'host' => $self->{HOST} });
      #$self->info("Finished writting status.");
    }

    $done and $done=$done ->result;
    if (!$done){
      print STDERR "Error contacting the ClusterMonitor\nGoing to the Manager/Job";
      my @arguments=@_;
      $done =    $self->{SOAP}->CallSOAP("Manager_Job_$org", "changeStatusCommand",  $id, shift @arguments, shift @arguments, $self->{CONFIG}->{CE_FULLNAME}, @arguments );
    }
    $done and $self->{STATUS}=$_[1];

  }
  $self->info("Status changed to @print");

  return $done;
}

sub putJobLog {
  my $self =shift;
  my $id=$ENV{ALIEN_PROC_ID};
  $id or $self->info("Job id not defined... ignoring the putJobLog") and  return 1;
  $self->info("Putting in the joblog: $id, @_");
  my $joblog = $self->{SOAP}->CallSOAP("CLUSTERMONITOR","putJobLog", $id,@_) or return;
  return 1;
}

sub getHostClassad{
  my $self=shift;
  my $ca=AliEn::Classad::Host->new({PACKMAN=>$self->{PACKMAN}}) or return;
  if ($self->{TTL}){
    $self->info("We have some time to live...");
#    my ($ok, $requirements)=$ca->evaluateExpression("Requirements");
#    $ok or $self->info("Error getting the requirements of this classad ". $ca->asJDL()) and return;
    my $timeleft=$self->{TTL} - ( time()-$self->{JOBAGENTSTARTS});
    $self->info("We still have $timeleft seconds to live");
    my $proxy=$self->{X509}->getRemainingProxyTime();
    $self->info("The proxy is valid for $proxy seconds");

    if (($proxy > 0 && $proxy < $timeleft)) {
#      $self->info("Let's try to extend the life of the proxy");
#      $self->{X509}->extendProxyTime($timeleft) or 
      $timeleft=$proxy;
    }
    #let's get 5 minutes to register the output
    $timeleft-=300;
#    $requirements .= " && (other.TTL<$timeleft) ";
    $ca->set_expression("TTL", $timeleft);
    $self->{TTL}=$timeleft;
#    $ca->set_expression( "Requirements", $requirements ) or return;

  }
  $self->info("We are using". $ca->asJDL);

  return $ca->asJDL();
}
sub checkStageJob {
  my $self=shift;
  my $info=shift;
  my $catalog=shift;
  ($info and $info->{stage} and $info->{stage}->{queueid}) or return 1;
  my $queueid= $info->{stage}->{queueid};
  my $jdl= $info->{stage}->{jdl};
  $self->info("We have to stage the files for job $queueid");
  
  my $pid=fork();
  defined $pid or $self->info("ERROR FORKING THE STAGE PROCESS") and return;
  $pid and return 1;
  $self->info("Ok, let's start staging the files");
  my $ca=Classad::Classad->new($jdl);
  my $status="STAGING";
  if (!$ca->isOK() ){
    $self->info("The jdl of the stage job is not correct!! '$jdl'");
    $status="ERROR_STG";
  } else {
    my ($ok, @files)=$ca->evaluateAttributeVectorString("inputdata");
    if ($ok and @files){
      map {s/,nodownload$//} @files;
      $self->info("Staging the files @files");
      $catalog->execute("stage", @files);
    }
  }
  $ENV{ALIEN_PROC_ID}=$queueid;
  $self->{CA}=$ca;
  $self->{QUEUEID}=$queueid;
  $self->checkJobJDL();
  $jdl=~ s/\[/\[StageCE="$self->{CONFIG}->{CE_FULLNAME}";/;
  $self->putJobLog("trace", "The jobagent finished issuing the staging commands(with $status");
  $self->changeStatus("A_STAGED", "STAGING", $jdl);
  exit(0);
}

sub GetJDL {
  my $self = shift;

  $self->info("The job agent asks for a job to do:");

  my $jdl;
  my $i=$ENV{ALIEN_JOBAGENT_RETRY} || 5;

  my $result;
  if ($ENV{ALIEN_PROC_ID}){
    $self->info("ASKING FOR ANOTHER JOB");
    $self->putJobLog("trace","Asking for a new job");
  }

  while(1) {
    $self->info("Getting the jdl from the clusterMonitor, agentId is $ENV{ALIEN_JOBAGENT_ID}...");

    my $hostca=$self->getHostClassad();
    if (!$hostca){
      $self->sendJAStatus('ERROR_HC');
#      $catalog and  $catalog->close();
      return;
    }
    my $hostca_stage;
 #   if ($catalog){
 #     $self->info("We have a catalog (we can stage)");
 #     $hostca_stage=$hostca;
 #     $hostca_stage=~ s/\[/\[TO_STAGE=1;/;
 #   }

    $self->sendJAStatus(undef, {TTL=>$self->{TTL}});

    my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR","getJobAgent", $ENV{ALIEN_JOBAGENT_ID}, "$self->{HOST}:$self->{PORT}", $self->{CONFIG}->{ROLE}, $hostca, $hostca_stage);
    my $info;
    $done and $info=$done->result;
    if ($info){
      $self->info("Got something from the ClusterMonitor");
#      use Data::Dumper;
#      print Dumper($info);
#      $self->checkStageJob($info, $catalog);
      if (!$info->{execute}){
	$self->info("We didn't get anything to execute");
      }	else{
	my @execute=@{$info->{execute}};
	$result=shift @execute;
	if ($result eq "-3") {
	  $self->sendJAStatus('INSTALLING_PKGS');
	  $self->{SOAP}->CallSOAP("Manager/Job", "setSiteQueueStatus",$self->{CONFIG}->{CE_FULLNAME},"jobagent-install-pack");
	  $self->info("We have to install some packages (@execute)");
	  foreach (@execute) {
	    my ($ok, $source)=$self->installPackage( $_);
	    if (! $ok){
	      $self->info("Error installing the package $_");
	      $self->sendJAStatus('ERROR_IP');
#	      $catalog and $catalog->close();
	      return;
	    }
	  }
	  $i++; #this iteration doesn't count
	}elsif ( $result eq "-2"){
	  $self->info("No jobs waiting in the queue");
	} else {
	  $self->{SOAP}->CallSOAP("Manager/Job", "setSiteQueueStatus",$self->{CONFIG}->{CE_FULLNAME},"jobagent-matched");
	  last;
	}
	
      }
    } else{
	$self->info("The clusterMonitor didn't return anything");
    }
    --$i or  last;
    print "We didn't get the jdl... let's sleep and try again\n";
    $self->{SOAP}->CallSOAP("Manager/Job", "setSiteQueueStatus",$self->{CONFIG}->{CE_FULLNAME},"jobagent-no-match", $hostca);
    
    sleep (30);
    if($self->{MONITOR}){
      $self->{MONITOR}->sendBgMonitoring();
    }
    $self->sendJAStatus('REQUESTING_JOB');
  }
#  $catalog and  $catalog->close();

  $result or $self->info("Error getting a jdl to execute");
  ( UNIVERSAL::isa( $result, "HASH" )) and $jdl=$result->{jdl};
  if (!$jdl) {
    $self->info("Could not download any  jdl!");
    $self->sendJAStatus('ERROR_GET_JDL');
    return;
  }

  my $queueid=$ENV{ALIEN_PROC_ID}=$self->{QUEUEID}=$result->{queueid};
  my $token=$ENV{ALIEN_JOB_TOKEN}=$result->{token};
  $self->{JOB_USER} = $result->{user};


  my $message="The job has been taken by the jobagent $ENV{ALIEN_JOBAGENT_ID}";
  $ENV{EDG_WL_JOBID} and $message.="(  $ENV{EDG_WL_JOBID} )";
  if (  $ENV{LSB_JOBID} ){
    $message.=" (LSF ID $ENV{LSB_JOBID} )";
     $self->sendJAStatus(undef, {LSF_ID=>$ENV{LSB_JOBID}});
  }
  $self->putJobLog("trace",$message);


  $self->info("ok\nTrying with $jdl");

  $self->{CA} = Classad::Classad->new("$jdl");
  ( $self->{CA}->isOK() ) and return 1;

  $jdl =~ s/&amp;/&/g;
  $jdl =~ s/&amp;/&/g;
  $self->info("Trying again... ($jdl)");
  $self->{CA} = Classad::Classad->new("$jdl");
  ( $self->{CA}->isOK() ) and return 1;

  $self->sendJAStatus('ERROR_JDL');
  return;
}

sub checkJobJDL {
  my $self=shift;
  my $ok;

  ($ok, $self->{INTERACTIVE}) = $self->{CA}->evaluateAttributeString("Interactive");
  ($ok, $self->{COMMAND} ) = $self->{CA}->evaluateAttributeString("Executable");
  ($ok, $self->{VALIDATIONSCRIPT} ) = $self->{CA}->evaluateAttributeString("Validationcommand");
  print "AFTER CHECKING THE JDL, we have $self->{VALIDATIONSCRIPT}\n";
  ($ok, $self->{OUTPUTDIR})=$self->{CA}->evaluateAttributeString("OutputDir");
  ($ok, my @args ) = $self->{CA}->evaluateAttributeVectorString("Arguments");
  $self->{ARG}="";
  $ok and $self->{ARG}=" ".(join (" ", @args));
  ($ok, $self->{VOs} ) = $self->{CA}->evaluateAttributeString("AliEn_Master_VO");
  ($ok, my $jobttl) =$self->{CA}->evaluateExpression("TTL");
  $self->info("The job needs $jobttl seconds to execute");
  ($ok, my $masterid) =$self->{CA}->evaluateAttributeString("MasterJobId");
  if ($ok) {
    $self->info("Setting the MasterJobId to $masterid");
    $ENV{ALIEN_MASTERJOBID}=$masterid;
  }
  $self->{JOBEXPECTEDEND}=time()+$jobttl+600;
  $self->putJobLog("trace","The job needs $jobttl seconds");


  $self->{VOs}="$self->{CONFIG}->{ORG_NAME}#$ENV{ALIEN_CM_AS_LDAP_PROXY}#$self->{QUEUEID}#$ENV{ALIEN_JOB_TOKEN}  $self->{VOs}";

  $ENV{ALIEN_VOs} = "$self->{VOs}";	  

  my $oldOrg=$self->{CONFIG}->{ORG_NAME};
  my $oldCM=$ENV{ALIEN_CM_AS_LDAP_PROXY};
  foreach my $data (split (/\s+/, $self->{VOs})){
    my ($org, $cm, $id, $token)=split ("#", $data);
    $self->info("Connecting to services for $org");
    $self->{SOAP}->{"CLUSTERMONITOR_$org"}=SOAP::Lite
      ->uri("AliEn/Service/ClusterMonitor")
	->proxy("http://$cm");

    $ENV{ALIEN_CM_AS_LDAP_PROXY}=$cm;
    $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $org});

    $self->{SOAP}->{"Manager_Job_$org"}=SOAP::Lite
      ->uri("AliEn/Service/Manager/Job")
	->proxy("http://$self->{CONFIG}->{JOB_MANAGER_ADDRESS}");
  }
  $ENV{ALIEN_CM_AS_LDAP_PROXY}=$oldCM;
  $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});

  ($ok, my @packages ) = 
    $self->{CA}->evaluateAttributeVectorString("Packages");
  $ENV{ALIEN_PACKAGES}=join (" ", @packages);

  $self->info("PACKAGES REQUIRED: $ENV{ALIEN_PACKAGES}");


  ($ok, my @env_variables)=
    $self->{CA}->evaluateAttributeVectorString("JDLVARIABLES");
  $self->info("We have to define @env_variables");
  foreach my $var (@env_variables) {
    ($ok, my @values)=
      $self->{CA}->evaluateAttributeVectorString($var);
    if (!$ok) {
      $self->putJobLog( "warning", "The JobAgent was supposed to set '$var', but that's not defined in the jdl");
      next;
    }
    $var=uc("ALIEN_JDL_$var");
    my $value=join("##", @values);
    $self->putJobLog("trace", "Defining the environment variable $var=$value");
    $ENV{$var}=$value;
    
  }
  return 1;

}

sub CreateDirs {
  my $self=shift;
  my $done=1;

  $self->{WORKDIR} =~ s{(/alien-job-\d+)?\/?$}{/alien-job-$ENV{ALIEN_PROC_ID}};
  $ENV{ALIEN_WORKDIR} = $self->{WORKDIR};

  my @dirs=($self->{CONFIG}->{LOG_DIR},
	    "$self->{CONFIG}->{TMP_DIR}/PORTS", $self->{WORKDIR},
	    "$self->{CONFIG}->{TMP_DIR}/proc/");


  foreach my $fullDir (@dirs){
    my $dir = "";
    (-d  $fullDir) and next;
    foreach ( split ( "/", $fullDir ) ) {
      $dir .= "/$_";
      mkdir $dir, 0777;
    }
  }

  $self->putJobLog("trace","Creating the working directory $self->{WORKDIR}");

  if ( !( -d $self->{WORKDIR} ) ) {
    $self->putJobLog("error","Could not create the working directory $self->{WORKDIR} on $self->{HOST}");
  }

  # remove old workdirs from former jobs while are not touched longer since 1 week!
  open WORKDIRLIST ,"ls -d $self->{WORKDIR}/../alien-job-* |";
  my $now = time;
  while (<WORKDIRLIST>) {
    chomp($_);
    my   ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
	  $atime,$mtime,$ctime,$blksize,$blocks)= stat($_);
    if ( (defined $mtime) && ($mtime >0) ) {
      if ( ($now - $mtime) > (60*60*24*7) ) {
	# these directories are old and can be deleted
	if ( $_ =~/.*alien\-job\-.*/ ) {
	  system("rm -rf $_");
	  $self->info("Removed old AliEn working directory $_");
	}
      }
    }
  }

  close WORKDIRLIST;
  # check the space in our workind directory
  my $handle=Filesys::DiskFree->new();
  $handle->df_dir($self->{WORKDIR});
  my $space=$handle->avail($self->{WORKDIR});
  my $freemegabytes=int($space/(1024*1024));
  $self->info("Workdir has $freemegabytes MB free space");

  my ( $okwork, @workspace ) =
      $self->{CA}->evaluateAttributeVectorString("Workdirectorysize");
  $self->{WORKSPACE}=0;
  if ($okwork) {
    if (defined $workspace[0]) {
      my $unit=1;
      ($workspace[0] =~ s/KB//g) and $unit = 1./1024.;
      ($workspace[0] =~ s/MB//g) and $unit = 1;
      ($workspace[0] =~ s/GB//g) and $unit = 1024;

      if (($workspace[0]*$unit) > $freemegabytes) {
	# not enough space
	$self->putJobLog("error","Request $workspace[0] * $unit MB, but only $freemegabytes MB free in $self->{WORKDIR}!");
	$self->registerLogs(0);
	$self->changeStatus("%", "ERROR_IB");
	$done=0;
      } else {
	# enough space
	$self->putJobLog("trace","Request $workspace[0] * $unit MB, found $freemegabytes MB free!");
	$self->{WORKSPACE}=$workspace[0]*$unit;
      }
    }
  }

#    $self->{LOCALDIR} = "$self->{CONFIG}->{TMP_DIR}/proc$self->{QUEUEID}";
#    if ( !( -d "$self->{LOCALDIR}" ) ) {
#        mkdir "$self->{LOCALDIR}", 0777;
#    }

  chdir $self->{WORKDIR};
  return $done;
}

sub getStatus {
  $self->info("Asking the status of process $self->{QUEUEID}");

  return $self->{QUEUEID};

}

my $my_getFile = sub {
    my $file = shift;
    my $options=(shift || {});
    if ($options->{grep} || $options->{tail} || $options->{head}) {
      my $open="$file";
      $options->{grep} and 
	$self->info("Returning only the entries that match $options->{grep}");
      $options->{head} and $open="head -$options->{head} $file|" and
	$self->info("Returning the first $options->{head} lines of $file");
      $options->{tail} and $open="tail -$options->{tail} $file|" and
	$self->info("Returning the last $options->{tail} lines of $file");
      open (FILE, $open) or return;
      my $buffer=join("", grep (/$options->{grep}/, <FILE>));
      close FILE;
      return $buffer;
    }
    my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
        $atime,$mtime,$ctime,$blksize,$blocks) = stat($file);
    my $buffer;
    my $maxlength = 1024 * 1024 * 10;
    my $bsize = ($size < $maxlength ? $size : $maxlength ); 
    open( FILE, "$file" );
    my $aread = read( FILE, $buffer, $size, 0 );
    close(FILE);

    return $buffer;
};

sub getWorkDir {
    my $self   = shift;
    my $workdir=$self->getWorkingDir();
    my $buffer = `ls -la $workdir/`;
    $self->info("Getting the working directory ($workdir)");
    my $var = SOAP::Data->type( base64 => $buffer );
    $self->info("Getting the workdir done");
    return ($var);
}

sub getNodeInfo {
    my $self   = shift;
    my $buffer = `echo ------------------------------;date; echo ------------------------------;echo $self->{CONFIG}->{HOST}; echo ------------------------------;cat /proc/cpuinfo; echo ------------------------------;vmstat -n 1 1; echo ------------------------------;ps -ax ; `;
    $self->info("Getting the working directory");
    my $var = SOAP::Data->type( base64 => $buffer );
    return ($var);
}

sub getOutput {
    my $this   = shift;
    my $output = shift || "stdout";
    if ($output eq "workdir") {
	return $self->getWorkDir();
    }
    if ($output eq "nodeinfo") {
	return $self->getNodeInfo();
    }
    my $workdir= $self->getWorkingDir();

    my $file = "$workdir/$output";
    $self->info("Getting the file $file");
    my $buffer=$my_getFile->($file, @_);
    my $var = SOAP::Data->type( base64 => $buffer );
    return ($var);
}

sub getWorkingDir {
  my $self=shift;
  #We have to know which file we have to return 
  #We don't know the workdir
  open (FILE, "<$self->{WORKDIRFILE}") or print "Error opening the file $self->{WORKDIRFILE}\n" and return;
  my @line=<FILE>;
  close FILE;
  my $workdir= join("", grep (s/workdir\s*=\s*//i, @line));
  chomp $workdir;

#  print "WE HAVE THE FILE @line and $workdir\n";
  $self->info("Working directory is $workdir");
  return $workdir;
}
sub getFile {
  my $this=shift;
  my $file =(shift or "stdout");
  $self->info("Trying to get the file $file");
  return($self->getOutput($file, @_));
}


sub zgetStdout {
    my $file = "$self->{WORKDIR}/stdout";

    #my $buffer=$my_getFile->($file);
    my $zbuffer = compress( $my_getFile->($file) );
    ($zbuffer) or return "Error\n";
    my $var = SOAP::Data->type( base64 => $zbuffer );
    return ( $var, 1 );
}

sub zgetStderr {
    my $file    = "$self->{CONFIG}->{TMP_DIR}/proc$self->{QUEUEID}/stderr";
    my $zbuffer = compress( $my_getFile->($file) );
    ($zbuffer) or return "Error\n";
    my $var = SOAP::Data->type( base64 => $zbuffer );
    return ( $var, 1 );
}

sub startMonitor {
  my $self=shift;

  # create a pipe that will be use as a communication channel between the forked process and the parent
  # so that the clild will communicate to the parent the current status of the job
  $self->info("Creating pipe to send status from child to parent");
  pipe($self->{JOB_STATUS_RDR}, $self->{JOB_STATUS_WTR});
  $self->{JOB_STATUS_WTR}->autoflush(1);
  $self->info("Forking child to execute the job...");
  my $error = fork();
  $self->info("Fork done $error");
  ( defined $error ) or print STDERR "Error forking the process\n" and return;

  waitpid( $error, &WNOHANG );

  #The parent returns
  $self->{PROCESSID} = $error;
  $self->{PROCESSID} and return 1;

  $self->debug(1, "The father locks the port");

  POSIX::setpgid($$, 0);
  if (! $self->executeCommand() ) {
    $self->registerLogs(0);
    $self->changeStatus("%", "ERROR_E");
  }
  AliEn::Util::kill_really_all($$);
  
  $self->info("Command executed, with status $self->{STATUS}");
  my $status=$self->{STATUS};
  ($status eq "SAVING") and $status="DONE";

  exit;
}

sub stopService {
  my $s=shift;

  my $pid=shift;

#  if ( $self->{STATUS} eq "STARTED" ) {
#     $self->changeStatus("%", "ERROR_P");	
#  }
  $self->info("Killing JobAgent\n");

  $self->SUPER::stopService($pid);
  return 1;
}


sub setAlive {
  my $s=shift;
  $self->info("In JobAgent, Setting alive");
  # the setAlive is sent to ML from checkWakesUp
  return 1;
}

sub getCatalogue {
  my $self=shift;
  my $options={no_catalog=>1};
  my $catalog;

  eval{ 
    $options->{silent} or $options->{silent}=0;
    $options->{packman_method} or $options->{packman_method}="Local";
    $options->{role} or $options->{role}=$self->{CONFIG}->{CLUSTER_MONITOR_USER};
#    my $options={silent=>0, packman_method=>'Local', 'role'=>$self->{CONFIG}->{CLUSTER_MONITOR_USER}};
    $self->{CONFIG}->{AGENT_API_PROXY} and 
      $options->{gapi_catalog}=$self->{CONFIG}->{AGENT_API_PROXY};
    $self->info("Trying to get a catalogue");
    $catalog = AliEn::UI::Catalogue::LCM::->new($options);
  };
  if ($@) {print "ERROR GETTING THE CATALOGUE $@\n";}
  if (!$catalog) {
    $self->putJobLog("error","The job couldn't authenticate to the catalogue");

    print STDERR "Error getting the catalog!\n";
    return;
  }
  $self->info("Got the catalogue");
  return $catalog;
}

sub getBatchId{
  my $self=shift;
  $self->info("Finding out the batch id");

  my $queuename = "AliEn::LQ";
  ( $self->{CONFIG}->{CE} ) 
    and $queuename .= "::$self->{CONFIG}->{CE_TYPE}";

  eval "require $queuename"
    or $self->info("Error requiring '$queuename': $@")
      and return;
  my $b = $queuename->new();
  $b or $self->info("Error creating a $queuename") and return;

   #my $id="";
   my $id=0;
   eval { $id=$b->getBatchId()};
   if ($@){
       $self->info("Error getting the id of the batch system");
   }
   return $id 

}


sub executeCommand {
  my $this = shift;
  
  
  my $batchid=$self->getBatchId();
  $self->changeStatus("%",  "STARTED", $batchid,$self->{HOST}, $self->{PROCESSPORT} );

  $ENV{ALIEN_PROC_ID} = $self->{QUEUEID};
  my $catalog=$self->getCatalogue() or return;

  $self->debug(1, "Getting input files and command");
  if ( !( $self->getFiles($catalog) ) ) {
    print STDERR "Error getting the files\n";
    $catalog->close();
    $self->registerLogs(0);

    $self->changeStatus("%",  "ERROR_IB");
    return ;
  }
  $catalog->close();

  #    my $localDir="$self->{CONFIG}->{TMP_DIR}/proc$self->{QUEUEID}";
  
  my $timecommand="";
  my $hasgnutime = 0;

  if (open (TESTIT,"$ENV{ALIEN_ROOT}/bin/time --version 2>&1 |")){
    if ( grep ( /^GNU/ , <TESTIT> )) {
      $timecommand = "$ENV{ALIEN_ROOT}/bin/time -o $self->{WORKDIR}/resources.dat -f \"%E %S %U %P %M %K %D %X %F %R %W %c %w %I %O %r %s %k\" ";
      $hasgnutime = 1;
    }
    close TESTIT;
  }
  my @list = "$timecommand$self->{WORKDIR}/command$self->{ARG}";

#  my ( $ok, @outputFiles ) =
#    $self->{CA}->evaluateAttributeVectorString("OutputFile");

#  if (($ok) and @outputFiles) {
#      $self->{OUTPUTFILES} = join(" $self->{WORKDIR}/",@outputFiles);
#      @list=(@list, " --output ". join(",,",@outputFiles) . " ");
#  }

#  my ( $ok2, @inputData ) =  
#    $self->{CA}->evaluateAttributeVectorString("InputData");

#  if (($ok2) and @inputData) {
#      $self->{INPUTDATA} = join(" ",@inputData);
#      @list=(@list, " --inputdata ". join(",,",@inputData) . " " );
#  }

#  my ( $ok3, @inputFiles ) =
#      $self->{CA}->evaluateAttributeVectorString("InputFile");

#  if (($ok3) and @inputFiles) {
#      $self->{INPUTFILES} = join(" $self->{WORKDIR}/",@inputFiles);
#      @list=(@list, " --input ". join(",,",@inputFiles) . " ");
#  }
  my ($ok,  @packages)=$self->{CA}->evaluateAttributeVectorString("Packages");
  my $user=$self->{CA}->evaluateAttributeString("User");
  if ($ok) {
    my @packInst;
    foreach (@packages) {
      my ($ok, $source)=$self->installPackage( $_, $user);
       if (!$ok){
	 $self->registerLogs(0);
	 $self->changeStatus("%",  "ERROR_E");
#	 $catalog->close();
	 return;
       }
      if ($source){
	push @packInst, $source;
      }
    }
    @list=(@packInst, @list);
  }
  my $s=join (" ", @list);
#  $catalog->close();
  $self->{STATUS}="RUNNING";
  ($self->{INTERACTIVE}) and  $self->{STATUS}="IDLE";

  $self->changeStatus("STARTED",$self->{STATUS},0,$self->{HOST},$self->{PORT});

  $s=~ s/^\s*//;
  $self->info("Ready to do the system call '$s'");
  my $oldEnv=$ENV{ALIEN_CM_AS_LDAP_PROXY};
  $ENV{ALIEN_CM_AS_LDAP_PROXY}="$self->{HOST}:$self->{PORT}/JobAgent";
  $self->info("Setting the LDAP PROXY to  $ENV{ALIEN_CM_AS_LDAP_PROXY}");
  $ENV{LD_LIBRARY_PATH}="/lib:/usr/lib:$ENV{LD_LIBRARY_PATH}";
  open SAVEOUT,  ">&STDOUT";
  open SAVEOUT2, ">&STDERR";

  open SAVEOUT,  ">&STDOUT";
  open SAVEOUT2, ">&STDERR";

  if ( !open STDOUT, ">$self->{WORKDIR}/stdout" ) {
    open STDOUT, ">&SAVEOUT";
    die "stdout not opened!!";
  }

  if ( !open( STDERR, ">$self->{WORKDIR}/stderr" ) ) {
    open STDOUT, ">&SAVEOUT";
    open STDERR, ">&SAVEOUT2";
    print STDERR "Could not open stderr file\n";
    die;
  }

  print "Test: ClusterMonitor is at $self->{HOSTNAME}:$self->{HOSTPORT}\n";
  print "Execution machine:  $self->{HOST}\n";

  chdir $self->{WORKDIR};
  my $error = system($s);
  $ENV{LD_LIBRARY_PATH} =~ s{^/lib:/usr/lib:}{};
 $ENV{ALIEN_CM_AS_LDAP_PROXY}=$oldEnv;
  ##################################################################
  # now process resources.dat in a human readable format
  ##################################################################
  
  open (POUT,">$self->{WORKDIR}/resources");
  printf POUT "Executed: @list	
============================================================
AliEn Job-Id                              : $self->{QUEUEID}
============================================================\n";
  if ($hasgnutime) {
    ##################################################################
    open (PD,"$self->{WORKDIR}/resources.dat");	
    
    ##################################################################
    while (<PD>) {
      my @tags = split " ",$_;
      
      print POUT  "Elapsed real time             [[h]:min:s] : $tags[0]
CPU in kernel mode                  [sec] : $tags[1]
CPU in user   mode                  [sec] : $tags[2]
CPU perc. of this job               [\%  ] : $tags[3]\n";
      #	print POUT  "Max. resident MEM size              [kb ] : $tags[4]\n";
      #	print POUT  "Av.  total    MEM use               [kb ] : $tags[5]\n";
      #	my $memcost = $tags[5];
      #	print POUT  "Av.  MEM size of unshared data      [kb ] : $tags[6]\n";
      #	print POUT  "Av.  MEM size of shared text space  [kb ] : $tags[7]\n";
      print POUT  "Major page faults                   [#  ] : $tags[8]
Minor page faults                   [#  ] : $tags[9]\n";
      #	print POUT  "times, process swapped out of MEM   [#  ] : $tags[10]\n";
      #	print POUT  "Involuntarily context switching     [#  ] : $tags[11]\n";
      #	print POUT  "Waits for voluntaily context sw.    [#  ] : $tags[12]\n";
#	print POUT  "File System Inputs                  [#  ] : $tags[13]\n";
#	print POUT  "File System Outputs                 [#  ] : $tags[14]\n";
#	print POUT  "Socket Messages received            [#  ] : $tags[15]\n";
#	print POUT  "Socket Messages sent                [#  ] : $tags[16]\n";
#	my $iocost = $tags[13]+$tags[14]+$tags[15]+$tags[16];
#	print POUT  "Delivered signals to the process    [#  ] : $tags[17]\n";
      print POUT  "------------------------------------------------------------\n";
    }
    close PD;
  }
  close POUT;


  close STDOUT;
  close STDERR;

  open STDOUT, ">&SAVEOUT";
  open STDERR, ">&SAVEOUT2";
  $error=$error/256;
  $self->changeStatus($self->{STATUS}, "SAVING",$error);	

  $self->info("Command executed with $error.");

  return 1;
}
sub installPackage {
  my $self=shift;
  my $package=shift;
  my $user=shift;
#  my $user=$self->{INSTALL_USER};
  $self->info("Installing Package $_");

  my ($version);

  $package =~ s/::(.*)$// and $version=$1;
  $package =~ s/^(.*)\@// and $user=$1;

  #The first time, we get the user from the catalogue
  $user or $user=$self->{CONFIG}->{ROLE};
#    if ($self->{INSTALL_USER}){
#      $user=$self->{INSTALL_USER};
#    }else{
#      ($user)=$catalogue->execute("whoami", "-silent");
#      if (!$user) {print "ERROR GETTING THE CATALOGUE $@\n";}
#      $user and $self->{INSTALL_USER}=$user;
#    }
#  }
  $self->info("Getting the package $package (version $version) as $user");

  $ENV{ALIEN_PROC_ID} and
    $self->putJobLog("trace","Installing package $_");


  my ($ok, $source);

  $self->info("First, let's try to do it ourselves");
  eval {
    if ($self->{CONFIG}->{PACKMAN_FORBIDWNINSTALL}){
      $self->info("This site doesn't allow installation of software in the worker node");
      $ok=-1;
      die("This site doesn't allow installation of software in the worker node");
    }
    ($ok, $source)=$self->{PACKMAN}->installPackage($user, $package, $version, undef, {NO_FORK=>1});
    
  };
  my $error=$@;
  if ($ok eq  '-1') {
    $self->info("It didn't work :( Asking the packman to install the package");
    ($ok, $source)=AliEn::PackMan::installPackage($self->{PACKMAN},$user, $package, $version);
    $self->info("$$ The packman called returned $ok and $source");
    if ($ok eq '-1'){
      $self->info("There were some problems installing the package");
      $ENV{ALIEN_PROC_ID} and
	$self->putJobLog("error","Package $_ not installed ");
      return;
    }
  }
  $self->info("Package $package installed successfully ($ok)!!");
  ($source) and   $self->info("For the package we have to do $source");
  return ($ok, $source);
}

sub mergeXMLfile{
  my $self=shift;
  my $catalog=shift;
  my $output=shift;
  my $input=shift;
  $self->putJobLog("trace","We have to merge $input with $output");

  $self->_getInputFile($catalog, $input, "$output.orig") or return;

  my $d=AliEn::Dataset->new();

  my $info=$d->readxml("$output.orig") or 
    $self->putJobLog("error","Error reading the xml file $output.orig")  and return;

  my ($ok, @lfn)=$self->{CA}->evaluateAttributeVectorString("InputData");

  map {s{^lf:}{alien://}i} @lfn;
  map {s{,.*$}{}i} @lfn;

  foreach my $event (keys %{$info->{collection}->{event}}){
    my $delete=1;
    foreach my $entry (keys %{$info->{collection}->{event}->{$event}->{file}}){

      if (!grep (/^$info->{collection}->{event}->{$event}->{file}->{$entry}->{turl}$/i, @lfn)){
	$self->info("Let's remove $entry");
	delete $info->{collection}->{event}->{$event}->{file}->{$entry};
	next;
      }
      $delete=0;
    }
    $delete and delete $info->{collection}->{event}->{$event};
  }

  open (FILE, ">$output");
  print FILE $d->writexml($info);
  close FILE;
  $self->putJobLog( "trace", "XML file $output created merging $input (and @lfn)");

  return 1;
}

sub dumpInputDataList {
  my $self=shift;
  my $catalog=shift;
  my $xml=0;
  my ($ok, $dumplist)=$self->{CA}->evaluateAttributeString("InputDataList");
  ($dumplist)  or return 1;
  ($ok, my $format)=$self->{CA}->evaluateAttributeString("InputDataListFormat");
  if ($format){
    if ($format =~ /^xml-single/i){
      $xml="single";
    } elsif ($format =~ /^xml-group/i) {
      $xml="group";
    } elsif ($format =~ /^merge:(.*)/i) {
      return $self->mergeXMLfile($catalog, $dumplist, $1);
      $xml="group";
    } else {
      $self->putJobLog("error","The inputdatalistType was $format, but I don't understand it :(. Ignoring it");
    }
  }
  $self->putJobLog("trace","Putting the list of files in the file '$dumplist'");
  $self->info("Putting the inputfiles in the file '$dumplist'");
  if (!open (FILE, ">$dumplist") ){
    $self->info("Error putting the list of files in $dumplist");
    $self->putJobLog("error","Error putting the list of files in the file $dumplist");
    return;
  }

  $xml and print FILE "<\?xml version=\"1.0\"\?>
<alien>
<collection name=\"jobinputdata\">
<event name=\"0\">\n";

  ($ok, my @lfns)=$self->{CA}->evaluateAttributeVectorString("InputData");
  my $filehash={};
  my $event=0;
  my $eventsum=0;
  foreach my $file (@lfns){
    $file =~ s/LF://;
    $file =~ s/,nodownload//i;
    if ($xml) {
      my $basefilename =$file;
      $basefilename=~ s{^.*/([^/]*)$}{$1};
      if ($xml eq "single") {
	if ($eventsum){
	  printf FILE "    </event>
    <event name=\"%d\">\n", $eventsum;
	}
      } elsif (defined $filehash->{$basefilename}) {
	if ($event != $filehash->{$basefilename}){
	  printf FILE "    </event>
      <event name=\"%d\">\n", $event;
	}
      }
      print FILE "      <file name=\"$basefilename\" lfn=\"$file\" 
turl=\"alien://$file\" />\n";
      (defined $filehash->{$basefilename}) or  $filehash->{$basefilename}=0;
      $filehash->{$basefilename}++;
      $event = $filehash->{$basefilename};
      $eventsum++;
    } else {
      print FILE "$file\n";
    }
  }
  $xml and print FILE "    </event>
</collection>
</alien>\n";
  
  
  close FILE;
  return 1;
}

sub getInputZip {
  my $self=shift;
  my $catalog=shift;
  my ($ok, @files)=$self->{CA}->evaluateAttributeVectorString("InputZip");
  $ok or return 1;
  $self->info("There are some input archives....");
  foreach my $file (@files){
    $self->putJobLog("trace","Getting InputZip $file");
    if (!$catalog->execute("unzip", $file)){
    $self->putJobLog("error","Error getting the inputzip $file");
      return
    }
  }
  return 1;
}
sub _getInputFile {
  my $self=shift;
  my $catalog=shift;
  my $lfnName=shift;
  my $pfn=shift;

  $self->putJobLog("trace","Downloading input file: $lfnName");
  $self->info( "Getting $lfnName");

  my $options="-silent";
  for (my $i=0;$i<2;$i++) {
    $catalog->execute("get", "-l", $lfnName,$pfn, $options ) and return 1;

    $options="";
    $self->putJobLog("trace","Error downloading input file: $lfnName (trying again)");

  }
  $self->putJobLog("error","Could not download the input file: $lfnName (into $pfn)");

  return;
}

sub getFiles {
  my $self    = shift;
  my $catalog = shift;
  #print "In getFiles\n";
  $self->info("Getting the files");
  my $oldmode=$self->{LOGGER}->getMode();
  $self->info("Got mode $oldmode");
  $self->dumpInputDataList($catalog);

  $self->getInputZip($catalog) or return;

  my @files=$self->getListInputFiles();

  foreach my $file (@files) {
    $self->_getInputFile($catalog, $file->{cat},$file->{real}) or return;
  }

  my $procDir = AliEn::Util::getProcDir($self->{JOB_USER}, undef, $self->{QUEUEID});

#  if (!( $catalog->execute("mkdir","$procDir/job-output","-ps"))) {
#    print STDERR "ERROR Creating the job-output directory!\n";
#    $self->putJobLog("error","Could not create the output directory in the catalogue: $procDir/job-output");
#    return;
#  }

  $self->info("Let's check if there are any files to stage");
  
  my ($ok, @stage)=$self->{CA}->evaluateAttributeVectorString("InputData");
  if ($ok) {
    my @lfns=();
    foreach my $lfn (@stage) {
      $lfn =~ /,nodownload/ and next;
      print "The lfn $lfn has to be staged!!\n";
      push @lfns, $lfn;
    }
    if (@lfns){
      $catalog->execute("stage", @lfns);
    }
  }

  chmod 0750, "$self->{WORKDIR}/command";
  $self->{LOGGER}->setMinimum(split(" ",$oldmode));

  return 1;

}

sub getListInputFiles {
  my $self=shift;

  
  my $dir = AliEn::Util::getProcDir($self->{JOB_USER}, undef, $self->{QUEUEID}) . "/";

  my @files=({cat=>$self->{COMMAND}, real=>"$self->{WORKDIR}/command"});
  if ($self->{VALIDATIONSCRIPT}) {
    my $validation=$self->{VALIDATIONSCRIPT};
    $validation=~ s{^.*/([^/]*)$}{$self->{WORKDIR}/$1};
    push @files, {cat=>$self->{VALIDATIONSCRIPT}, 
		  real=>$validation};
  }else {
    $self->info("There is no validation script");
  }
  my ( $ok,  @inputFiles)=$self->{CA}->evaluateAttributeVectorString("InputDownload");
  foreach (@inputFiles){
    my ($proc, $lfn)=split /->/;
    $self->debug(1, "Adding '$lfn' (dir '$dir')");
    $proc =~ s{^$dir}{$self->{WORKDIR}/};
    push @files, {cat=> $lfn, real=>$proc};
    if ($proc =~ /^($self->{WORKDIR}\/.*\/)[^\/]*$/ ) {
      $self->info("Checking if $1 exists");
      if (! -d $1) {
	mkdir $1 or print "Error making the directory $1 ($!)\n";
      }
    }

  }
 
  return @files
}

sub getUserDefinedGUIDS{
  my $self=shift;

  my ($ok, $guidFile)=$self->{CA}->evaluateAttributeString("GUIDFile");
  my %guids;

  if ($guidFile){
    $self->putJobLog("trace","Using the guids from $guidFile");
    if (!open (FILE, "<$guidFile")){
      $self->putJobLog("error","The job was supposed to create '$guidFile' with the guids, but it didn't... I will generate the guids");
    }else{
      %guids=split (/\s+/, <FILE>);
      use Data::Dumper;
      $self->info("Using the guids". Dumper(%guids));
      close FILE;
    }

  }
  return %guids;
}




sub processJDL_split_Output_Filenames_From_Options_And_Initialize_fileTable{
    my $self=shift;
    my $jdlstrings=shift;
    my $fileTable;
    my $filestring;
    my $options="";
    foreach my $jdlelement (@$jdlstrings){
        ($filestring, $options)=split(/\@/, $jdlelement,2);
        my @files = split (/,/, $filestring);
        @files=$self->_findFilesLike(@files);
        foreach my $filename (@files) {
             $fileTable->{$filename}={
                               name=>$filename,
                               options=>$options};
        }
    }
    return $fileTable;
}


sub processJDL_get_Output_Archivename_And_Included_Files_And_Initialize_archiveTable{
    my $self=shift;
    my $jdlstrings=shift;
    my $archiveTable;
    my $name;
    my $filestring;
    my $options="";
    foreach  my $jdlelement (@$jdlstrings){
        ($filestring, $options)=split (/\@/, $jdlelement,2);
        ($name, my @files)=split(/[\:,]/, $filestring);
        @files=$self->_findFilesLike(@files);
        (scalar(@files) < 1) and next;  # for false JDLs, with archive definition and missing file definition
        $archiveTable->{$name}={name=>$name,
                               includedFiles=>\@files,
                               options=>$options};
    }
    return $archiveTable;
}


sub processJDL_get_SEnames_And_Real_Options{
    my $self=shift;
    my $jdlstring=shift;
    my @senames;
    my $sename;
    my @seweights;
    my $seweight;
    my $copies=0;
    my @tags;
    ($jdlstring eq "NONE") and ($jdlstring="");

    my (@options)=split (/,/, $jdlstring);
    foreach my $option (@options){
       if($option =~ /::/){
          if($option =~ /;/){
             my ($sename, $seweight)=split (/;/, $option,2);
             push (@senames, uc($sename));
             push (@seweights, $seweight);
          } else{
             push (@senames, uc($option));
             push (@seweights, "1");
          }
       } elsif ($option =~ /copies/){
             $option =~ s/copies\=//;
             if(isdigit $option) {
               if($option > 9){    # we don't allow more than 9 copies
                 $copies=9;
               } elsif ($option > 1){  #if not a natural number, we use default
                    $copies=$option;
               }
             }
       } else {
            push @tags, $option;
       }   
    }
    if(scalar(@senames) > 0){
      ($copies eq 0) and $copies=scalar(@senames);
    }else{  # if the use didn't supply any SEs, we add the config one
      push @senames, uc($self->{CONFIG}->{SE_FULLNAME});
      push (@seweights, "1");
    }
    ($copies eq 0) and $copies=2;
    (scalar(@tags) < 1) and @tags = ("");
    return (\@senames,\@seweights,$copies,\@tags);
}


sub analyseJDL_And_Move_By_Default_Files_To_Archives{
  my $self=shift;
  my $archives=shift;
  my $files=shift;
  my $defaultArchiveName=shift;
  my $defaultFiles=shift;

  my $fileTable;
  for my $j(0..$#{$files}) {
     if(! grep(/\@/, $$files[$j])){
           $self->info("Filestring has no options, so we put it in the Default Archive.");
           my @toDefFiles =  split (/,/, $$files[$j]);
           push @$defaultFiles, @toDefFiles;
           delete $$files[$j];
     } else {
           my ($filestring, $optionstring)=split (/\@/, $$files[$j],2);
           if(!(grep( /^no_archive$/, split (/,/, $optionstring)))){
              if($fileTable->{$optionstring}) {
                   $self->info("A file with already known options, will be united in one archive");
                   push @{$fileTable->{$optionstring}}, $filestring;
              } else {
                   my @filetag=($filestring);
                   $fileTable->{$optionstring}=\@filetag;
              }
              delete $$files[$j];
           }
     }
  }
  my $j=0;
  foreach my $optionstring (keys(%$fileTable)) {
           my $filestring="";
           foreach my $filename (@{$fileTable->{$optionstring}}) {
              $filestring .= $filename.",";
           } 
           $filestring =~ s/,$//;

           push @$archives, $defaultArchiveName.time()."_".$j++.".zip:".$filestring."@".$optionstring;
           $self->info("Filestring $filestring will be moved from files to archives");
  }
  return ($archives, $files, $defaultFiles);
}


sub check_On_Default_Output_Files_And_Put_In_Archive_If_Not_Exist{
  my $self=shift;
  my $archives=shift;
  my $files=shift;
  my $defaultArchiveName=shift;
  my $defaultOutputFiles=shift;
  my $localDefaultSEs=shift;
  my $defaultTags=shift;
  my @toBeAddedDefaultOutputFiles=();
  my @allfiles = (); 
  foreach my $archive (@$archives) {
      my ($archivestring, $options)=split(/\@/, $archive,2);
      my ($archivename, $filestring)=split(/:/, $archivestring,2);
      push @allfiles , split (/,/, $filestring);
  }
  foreach my $file (@$files) {
      my ($filestring, $options)=split (/\@/, $file,2);
      push @allfiles , split (/,/, $filestring);
  }
  for my $deffile (@$defaultOutputFiles) {
     if (! grep( /$deffile/ , @allfiles)){
        push @toBeAddedDefaultOutputFiles, $deffile;
     }
  }  
  (scalar(@toBeAddedDefaultOutputFiles) > 0) and push @$archives, $defaultArchiveName
		.":".join(",",@toBeAddedDefaultOutputFiles)."@".$localDefaultSEs.",".$defaultTags;
  return ($archives, $files);
}

sub processJDL_Check_on_Tag{
  my $self=shift;
  my $tags=shift;
  my $pattern=shift;
  for my $j(0..$#{$tags}) {
     if($$tags[$j] =~ /^$pattern$/i) { 
        delete $$tags[$j]; 
        return (1, $tags);
     }
  }
  return (0, $tags);
}

sub create_Default_Output_Archive_Entry{
   my $self=shift;
   my $jdlstring=shift;
   my $defaultOutputFiles=shift;
   my $defaultses=shift;
   my $defaulttags=shift;
   $jdlstring .= ":";
   foreach (@$defaultOutputFiles){
      $jdlstring .= $_.",";
   } 
   $jdlstring =~ s/,$//;
   $jdlstring .= "@".$defaultses.",".$defaulttags;
   return $jdlstring;
}


sub putJDLerrorInJobLog{
  my $self=shift;
  my $message=shift;

  $self->putJobLog("error", "We encountered an error in the supplied JDL.");
  $self->putJobLog("error", "The error is: $message");
  return 0;
}

sub prepare_File_And_Archives_From_JDL_And_Upload_Files{
  my $self=shift;
  my $archiveTable;
  my $fileTable;
  my $archives;
  my $files;
  my $ArchiveFailedFiles;
  my $archivesSpecified = 0;
  my $filesSpecified = 0;
  #######################################################################################################
  #######################################################################################################
  #######
  ## configuration parameters for the submit putFiles
  #my $monALISA_URL = "http://pcalimonitor.cern.ch/services/getBestSE.jsp";
  my $monALISA_URL = "";
  $self->{CONFIG}->{SEDETECTMONALISAURL} and  $monALISA_URL=$self->{CONFIG}->{SEDETECTMONALISAURL};


  my $defaultArchiveName= ".alien_archive.$ENV{ALIEN_PROC_ID}.".uc($self->{CONFIG}->{SE_FULLNAME}.".");

  my @localDefaultSEs = ();
  push @localDefaultSEs , $self->{CONFIG}->{SE_FULLNAME};  # we could have done without an array, but maybe we would like to have more than one SE in the future


  my @defaultOutputFilesList = ();   # This variable is maybe never again used, but was planned to force certain files not to be lost
                                     # like the @defaultOutputArchiveFilesList with "stdout","stderr","resources".
  my @defaultOutputArchiveFilesList = ("stdout","stderr","resources");
  my $defaultOutputFiles = \@defaultOutputFilesList;
  my $defaultOutputArchiveFiles = \@defaultOutputArchiveFilesList;

  my @defaultTags = ();
  #######################################################################################################
  #######################################################################################################
  my $defaultSEsString = join(";1,",@localDefaultSEs).";1";
  my $defaultTagString = join(",",@defaultTags);
  $defaultArchiveName=~ s/\:\://g; # if :: exists in the filename, the later processing will fail !
  #######

  my ( $ok, @files ) =
    $self->{CA}->evaluateAttributeVectorString("OutputFile");
  ( $ok, my @archives ) =
    $self->{CA}->evaluateAttributeVectorString("OutputArchive");
  ($ok, my $username ) = $self->{CA}->evaluateAttributeString("User");

  ## create a default archive if nothing is specified
  ##
  if((scalar(@archives) < 1) and (scalar(@files) < 1)) {
      $self->putJobLog("trace", "The JDL didn't contain any output specification. Creating default Archive.");
      push @archives, $self->create_Default_Output_Archive_Entry($defaultArchiveName.time().".zip", $defaultOutputArchiveFiles, 
              $defaultSEsString, $defaultTagString);
      $archives = \@archives;
      $files = \@files;

  } else {


      ## move all files from Files to Archives if they are non tagged with no_archive
      ##
      ($archives, $files, $defaultOutputFiles) = $self->analyseJDL_And_Move_By_Default_Files_To_Archives(\@archives, \@files, $defaultArchiveName, $defaultOutputFiles);
      
      ($archives, $files) = $self->check_On_Default_Output_Files_And_Put_In_Archive_If_Not_Exist(\@archives, \@files, $defaultArchiveName.time().".zip",
				$defaultOutputFiles, $defaultSEsString, $defaultTagString);
      
  }


  $archiveTable = $self->processJDL_get_Output_Archivename_And_Included_Files_And_Initialize_archiveTable(\@$archives);


  ($archiveTable, $ArchiveFailedFiles) = $self->createZipArchive($archiveTable) or
       print "Error creating the Archives\n" and return;

  push @files, @$ArchiveFailedFiles;

  (scalar(@files) > 0) and
                 $fileTable = $self->processJDL_split_Output_Filenames_From_Options_And_Initialize_fileTable(\@$files);



  my @filesAndArchivesAndFiles = each(%$archiveTable);
  push @filesAndArchivesAndFiles, each(%$fileTable);

  my $overallFileTable;
  %$overallFileTable= (%$archiveTable, %$fileTable);

 
  #foreach my $entry (keys(%$overallFileTable)) {
  #   $self->putJobLog("trace", "We will call putFiles with filesAndArchivessAndFiles elements: ".$overallFileTable->{$entry}->{name});
  #}
  
  
  $self->putJobLog("trace", "Finally, processing archives: @archives");

  $self->putJobLog("trace", "Finally, processing files: @files");


  if(scalar(keys(%$overallFileTable)) > 0){
      return $self->putFiles($overallFileTable, $username, $monALISA_URL);
  }
  

 
  return 0;
}


sub putFiles {
  my $self=shift;
  my $filesAndArchives=shift;
  my $username=shift;
  my $monALISA_URL=shift;
  my $filesUploaded=1;
  system ("ls -al $self->{WORKDIR}");
  my $oldOrg=$self->{CONFIG}->{ORG_NAME};
  my $jdl;
  my $no_links=0;
  my %guids=$self->getUserDefinedGUIDS();
  my $optionStore;
  my $incompleteUploades=0;
  my $successCounter=0;
  my $failedSEs;

  foreach my $data (split (/\s+/, $self->{VOs})){
    my ($org, $cm,$id, $token)=split ("#", $data);
    $self->info("Connecting to services for $org ($data)");
    $ENV{ALIEN_PROC_ID}=$id;
    $ENV{ALIEN_JOB_TOKEN}=$token;
    $ENV{ALIEN_ORGANISATION}=$org;
    $ENV{ALIEN_CM_AS_LDAP_PROXY}=$cm;
    $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $org});
    my @uploadedFiles=();
    my $remoteDir = "$self->{CONFIG}->{LOG_DIR}/proc$id";
    my $ui=AliEn::UI::Catalogue::LCM->new({no_catalog=>1});
    if (!$ui) {
      $self->info("Error getting an instance of the catalog");
      $self->putJobLog("error","Could not get an instance of the LCM");
      return;
    }

    #this hash will contain all the files that have already been submitted,
    #so that we can know if we are registering a new file or a replica
    my $submitted={};
    my $localdir= $self->{WORKDIR};




    foreach my $fileOrArchiveEntry (keys(%$filesAndArchives)) {

      $self->info("Processing  file  ".$filesAndArchives->{$fileOrArchiveEntry}->{name});
      $self->info("File has options  ".$filesAndArchives->{$fileOrArchiveEntry}->{options});

      $filesAndArchives->{$fileOrArchiveEntry}->{options} or $filesAndArchives->{$fileOrArchiveEntry}->{options}="NONE";

      

      my $lastShot=0;  # will trigger not to stick in the failedSEs while loop, in case of MonALISA is not answering or available.

      ##
      ## If we didn't already process exactly this options string
      ##
      if (!exists($optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}})) {    # if optionstore was not initialized before

          my ($ses, $seweights, $copies, $tags)=$self->processJDL_get_SEnames_And_Real_Options($filesAndArchives->{$fileOrArchiveEntry}->{options});
    
          ($no_links, $tags)  = $self->processJDL_Check_on_Tag($tags, "no_links_registration"); 
          
          $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}={
                                 ses=>$ses,
                                 seweights=>$seweights,
                                 copies=>$copies,
                                 tags=>$tags,
                                 username=>$username
                                 };
    
    
          if(($monALISA_URL ne "") and (my $monSes = $self->askMonALISAForNPrioritizedSEs($monALISA_URL, $username, $copies, $ses, $seweights, $tags))){
                (scalar(@$monSes)  > 0) and $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}  = $monSes;
                $self->putJobLog("trace","SE list after asking MonALISA: @{$optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}}");
          } else {
                $self->putJobLog("trace","MonALISA wasn't available, so we use alternative SE list generation.");
                $lastShot=1; 
                my ($ISreturn, $ISses) = $self->getAlternateSEInfoFromDB($username, $copies, $ses, $seweights, $tags);
                if($ISreturn){
                     (scalar(@$ISses)  > 0) and $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}  = $ISses;
                     $self->putJobLog("trace","We got an SE list from the IS DB");
                } else {
                     $self->putJobLog("trace","The IS DB request failed, we take the local SE specification and ignore the weights from the JDL.");
                     $ses=$self->complementSEListWithLocalConfigSEList($ses); 
                     $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses} = $ses; 
                     if(scalar(@$ses) < $copies) {  
                        $self->putJobLog("trace","The local SE specification has not enough SEs in order to fullfill the requirement of $copies.");
                     }
                }
          }
       
      } 
      $self->putJobLog("trace","Effective SE list for the current file will be @{$optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}}");

      (exists($guids{$filesAndArchives->{$fileOrArchiveEntry}->{name}})) or 
             $guids{$filesAndArchives->{$fileOrArchiveEntry}->{name}} = "";
          
      my ($uploadStatus, $failedSEs) = $self->uploadFile($ui,$filesAndArchives->{$fileOrArchiveEntry}->{name},
                                 $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{copies},
                                 $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses},
                                 $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{tags},
		                 $guids{$filesAndArchives->{$fileOrArchiveEntry}->{name}}, $submitted);

      $uploadStatus and $successCounter++;
      ($uploadStatus eq -1) and $incompleteUploades=1;
      $lastShot and %$failedSEs=();

      $lastShot=0;  # will trigger not to stick in the while loop, in case of MonALISA is not answering or available.

      while (keys(%$failedSEs) ne 0) {      

              $self->putJobLog("trace","We have failed SEs");
              $self->putJobLog("trace","Gonna prepare the failed SE list for a request to MonALISA for an adapted SE list.");
                   $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{seweights}
                   = $self->mark_Failed_Attempted_SEs($optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}, $failedSEs);

              my @selist = @{$optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}};

              if(($monALISA_URL ne "") and ($optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}
                         = $self->askMonALISAForNPrioritizedSEs($monALISA_URL, $username, 
                                       $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{copies}, 
                                       $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}, 
                                       $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{seweights},
                                       $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{tags}))){
                     $self->putJobLog("trace","MonALISA has delivered a fail adapted SE list, we will use it.");
              } else {
                     $self->putJobLog("trace","MonALiSA wasn't available, so we use alternative SE list generation.");
                     $lastShot=1; 
                     
                     my ($ISreturn, $ISses) = $self->getAlternateSEInfoFromDB(
                                             $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{username},
                                             $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{copies},
                                             $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses},
                                             $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{seweights},
                                             $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{tags});
                     $self->info("DB IS SE LIST IS: @$ISses");
                     if($ISreturn){
                          (scalar(@$ISses)  > 0) and $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}  = $ISses;
                          $self->putJobLog("trace","We got an adapted SE list from the IS DB");
                     } else {
                       $self->putJobLog("trace","The IS DB request failed, we take the local SE specification and ignore the weights from the JDL.");
                       $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}
                          =$self->complementSEListWithLocalConfigSEList($optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses});
                       if(scalar(@{$optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}}) 
                                 < $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{copies}) {
                          $self->putJobLog("trace","The local SE specification has not enough SEs in order to fullfill the requirement of"
                                   .$optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{copies});
                       }
                    }
                }
                     

                ($uploadStatus, my $newFailedSEs) = $self->uploadFile($ui,$filesAndArchives->{$fileOrArchiveEntry}->{name},
                                 $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{copies},
                                 $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses},
                                 $optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{tags}, 
                                 $guids{$filesAndArchives->{$fileOrArchiveEntry}->{name}}, $submitted);
                if($uploadStatus) {
                    $failedSEs= ();
                } else {
                    push @selist, @{$optionStore->{$filesAndArchives->{$fileOrArchiveEntry}->{options}}->{ses}};
                    if($lastShot) {
                        %$failedSEs=();
                    } else {
                        %$failedSEs= (%$failedSEs, %$newFailedSEs);
                    }
                }

              $uploadStatus and $successCounter++;
              ($uploadStatus eq -1) and $incompleteUploades=1;
      }

      $no_links and next;
      my @list;
      foreach my $file( keys %{$filesAndArchives->{$fileOrArchiveEntry}->{entries}}) {
         my $guid=$guids{$file} || "";
         $self->info("Checking if $file has a guid ($guid)");
         push @list, join("###", $file, $filesAndArchives->{$fileOrArchiveEntry}->{entries}->{$file}->{size},
         $filesAndArchives->{$fileOrArchiveEntry}->{entries}->{$file}->{md5},$guid );
      }
      $submitted->{$filesAndArchives->{$fileOrArchiveEntry}->{name}}->{links}=\@list;
    }


    my @list=();
    foreach my $key (keys %$submitted){
      my $links="";
      my $entry=$submitted->{$key};
      if ($entry->{links} ) {
	$links.=";;".join(";;",@{$entry->{links}});
      }
      push @list, "\"".join ("###", $key, $entry->{guid}, $entry->{size}, 
			     $entry->{md5},  join("###",@{$entry->{PFNS}}), 
			     $links) ."\"";
    }
    if (@list) {
      $self->{CA}->set_expression("RegisteredOutput", "{".join(",",@list)."}");
      $self->{JDL_CHANGED}=1;
    }
    $self->info("Closing the catalogue");
    $ui->close();
  }
  $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});

  $self->putJobLog("trace", "we had ".scalar(keys(%$filesAndArchives))
          ." files and archives to store, we successfully stored $successCounter");

  $incompleteUploades and $self->putJobLog("warning", "yet not all files and archives were stored as many times as wanted.");
  $incompleteUploades and return -1;

  if (scalar(keys(%$filesAndArchives)) eq $successCounter) {
      $self->putJobLog("trace","OK, SUCCESS. All files for this submit were sucessfully uploaded.");
      return 1;
  }
  return 0;
}


sub uploadFile {
  my $self=shift;
  my $ui=shift;;
  my $file=shift;
  my $seWishCount=shift;
  my $seList=shift;
  my $options=shift;
  my $guid=shift;
  my $submitted=shift;
  my $failedSEs;
  
  $self->info("Submitting the file $file");
  if (! -f "$self->{WORKDIR}/$file")  {
    $self->info("The job was supposed to create $file, but it doesn't exist!!",1);
    $self->putJobLog("error", "The job didn't create $file");
    return; 
  }
  $guid and $self->putJobLog("trace", "The file $file has the guid $guid");
  my $info;
  my $sereplicacount = 0;
  my $seselecter = 0;
  while($sereplicacount < $seWishCount && $seselecter < scalar(@$seList)) {
    $self->putJobLog("trace","Registering $file in @$seList[$seselecter] (guid $guid)");
    my $silent="-silent";
    my $statusOfExecuteUpload;
     for my $j(0..5) {  # this 5 times try was just taken from the old version
      ($statusOfExecuteUpload)=$ui->execute("upload", "$self->{WORKDIR}/$file", @$seList[$seselecter], $guid, $silent);
      $self->info("After the upload, we have". $self->{LOGGER}->error_msg());
      if($statusOfExecuteUpload){
          $sereplicacount++;
          last;
      }
      my $error="(no error message)";
      ($self->{LOGGER}->error_msg()) and $error="(error: ".$self->{LOGGER}->error_msg().")";
      $self->putJobLog( "warning", "File upload failed... sleeping  and retrying $error");
      sleep(10);
      $silent="";
    }
    if($statusOfExecuteUpload){
       if ($info) {
         push @{$info->{PFN_LIST}}, "$statusOfExecuteUpload->{selist}/$statusOfExecuteUpload->{pfn}";
       }else{
         $info=$statusOfExecuteUpload;
         $guid=$info->{guid};
         $info->{PFN_LIST}=["$info->{selist}/$info->{pfn}"];
       }
       if ($submitted->{$file}){
         push @{$submitted->{$file}->{PFNS}}, "$statusOfExecuteUpload->{selist}/$statusOfExecuteUpload->{pfn}";
       }else{
         $submitted->{$file}=$info;
         $submitted->{$file}->{PFNS}=["$statusOfExecuteUpload->{selist}/$statusOfExecuteUpload->{pfn}"];
       }
    } else {
       $failedSEs->{@$seList[$seselecter]} = 1;
    }
    $seselecter++;
  }
  ($info) or $self->putJobLog("error","Error registering the file $self->{WORKDIR}/$file");
  if ($sereplicacount != $seWishCount) {
       if($sereplicacount eq 0) {
             $self->putJobLog("error","Could not store the file $file on any of the $seWishCount wished SEs");
             return (0, $failedSEs);
       }
       $self->putJobLog("warning","Could store the file $file only on $sereplicacount of the $seWishCount wished SEs");
       return (-1, $failedSEs);
  } else {
       $self->putJobLog("trace","Successfully stored the file $file on $sereplicacount of the $seWishCount wished SEs");
       return (1, $failedSEs);
  }
  return 0;
}

#This subroutine receives a list of local files 
#that might include patterns, and it returns all
#the local files that match the pattern
sub _findFilesLike {
  my $self=shift;
  my @noPattern;
  foreach my $file (@_){
    $file !~ /\*/ and push(@noPattern, $file) and next;
    my $options="";
    $file =~ s/(\@.*)$// and $options=$1;
    #Ok, it is a pattern. We should check all the local files with that name
    open (FILE, "ls $file|") or print "Error looking for $file\n" and next;
    my @localfiles=<FILE>;
    close FILE;
    map {chomp($_); $_="$_$options"} @localfiles;
    push @noPattern, @localfiles;
  }
  return @noPattern;
}



sub createZipArchive{
  my $self=shift;
  my $archiveTable=shift;
  my @files=();
  foreach my $name (keys(%$archiveTable)) {
       my $filename = $name;
       my @includedFiles =  @{$archiveTable->{$name}->{includedFiles}};
       if (! @includedFiles) {
         $self->info("There are no files for the archive $filename!!");
         $self->putJobLog("error","The files ".@includedFiles." weren't produced!! (ignoring the zip file $filename)");
         next;
       }
       $archiveTable->{$name}={zip=>Archive::Zip->new() ,
                                entries=>{},
                                name=>"$name",
                                options=>$archiveTable->{$name}->{options},
                                nonarchivedFiles=>{}
                                };
       foreach my $file (@includedFiles) { 
          my $size=-s $file;
          if (!defined $size) {
            $self->putJobLog("error","The file $file doesn't exist");
            next;
          }
          $archiveTable->{$name}->{zip}->addFile($file);
          $archiveTable->{$name}->{entries}->{$file}={size=> $size,
                                                       md5=>AliEn::MD5->new($file)};
       }
       if ($archiveTable->{$name}->{zip}->numberOfMembers()<1){
         $self->putJobLog("error","The archive '$filename' doesn't have any files inside. Ignoring it...");
         delete $archiveTable->{$name};
         next;
       }
       if (grep(/.root$/ , $archiveTable->{$name}->{zip}->memberNames())) {
         $self->info("There is a root file. Do not compress the files");
         foreach my $member ($archiveTable->{$name}->{zip}->members()){
   	$member->desiredCompressionLevel(0);
         }
       }
       $archiveTable->{$name}->{zip}->writeToFileNamed($filename);
     }
  return ($archiveTable,\@files);
}

###################################
##
## Ask MonALISA for a priorized list of SEs, with respect to
## a list of known (and weighted) list of SE, a count of desired return entries,
## and a open list of options that are altogether send over HTTP 
##
## Example of the HTTP Get Request
## http://pcalimonitor.cern.ch/services/getBestSE.jsp?se=ALICE::Catania::DPM;1&se=ALICE::IPNO::DPM;-1&count=3&tag=something&tag=somethingelse 
##
## the weights for the send to MonALISA are
## 	SENAME		# nothing means the SE should be considered as a wish
##	SENAME;-1 	# minus one means either the user did specify as not to use
##	SENAME;-1	#    or we want to do a request while having already used that SE, so we need more 
##	SENAME;-2	# is supposed to be set due to a failed uploadFiles request, after receiving this SE from MonALISA request before
##	
##	
##################################
sub askMonALISAForNPrioritizedSEs{
   my $self=shift;
   my $url=shift;
   my $username=shift;
   my $secount=shift;
   my $ses=shift;
   my $seweights=shift;
   my $options=shift;
  
   $url .= "?username=$username&"; 
   for my $j(0..$#{$ses}) {
   
      if( @$ses[$j] ne ""){
          $url .= "se=@$ses[$j];@$seweights[$j]&";
      }
   }
   
   if($secount > 0) {
      $url .= "count=$secount&";
   }else{
      return;
   }
   my @nonMonALISATags = ("no_links_registration","no_archive");
   foreach (@$options){
      if(($_ ne "") and (grep(!/$_/ , @nonMonALISATags))){
         $url .= "tag=$_&";
      }
   }
   $url =~ s/&$//;
   $self->putJobLog("trace","MonALISA will be asked: ".$url);
   my $monua = LWP::UserAgent->new();
   $monua->timeout(25);
   $monua->agent( "AgentName/0.1 " . $monua->agent );
   my $monreq = HTTP::Request->new("GET" => $url);
   $monreq->header("Accept" => "text/html");
   my $monres = $monua->request($monreq);
   my $monoutput = $monres->content;
   my @selist;
   ( $monres->is_success() ) and @selist =  split (/\n/, $monoutput);
   
   return (\@selist);
}


############################
## Suppose we hat failing SEs in a list delivered before by MonALISA.
## Now we supply new weights for the list, telling MonALISA by '-1'
## that we don't want to have it again, because we already used it,
## and by '-2' that we tried to use it, but it failed.
##
##
sub mark_Failed_Attempted_SEs {
   my $self=shift;
   my $OrigSElist=shift;
   my $failedSEs=shift;
   my @newSEweights;
   
   foreach (@$OrigSElist){
      if($failedSEs->{$_}) {
         push @newSEweights,"-2";   
      } else {
         push @newSEweights,"-1";   
      }
   }
   return \@newSEweights;
}



sub getAlternateSEInfoFromDB {
   my $self=shift;
   my $username=shift;
   my $secount=shift;
   my $ses=shift;
   my $seweights=shift;
   my $options=shift;
   my $custodial=0;


   # get out the custiodial info if available
   for my $option (@$options) {
      if ($option =~ /custodial/){
         $option =~ s/custodial\=//;
         if(isdigit $option) {
              if ($option > $secount) { $custodial=$secount;}
              else {  $custodial=$option }
         }
      }
   }
#   $self->info("getAlternateSEInfoFromDB, custodial=$custodial");
   # some more additional information 
   # build a weighted SE table
   my $weightTable;
   for my $j(0..$#{$ses}) {
      $weightTable->{$$ses[$j]}=$$seweights[$j];
   }
#   $self->info("getAlternateSEInfoFromDB, past weightTable generation.");
  
   $self->{SOAP} or $self->{SOAP}=new AliEn::SOAP;
   my $dbsetable=$self->{SOAP}->CallSOAP("Authen", "getListOfSEoutOfDB");
   $dbsetable or return 0;
   $dbsetable and $dbsetable=$dbsetable->result;

   my $spliceoffset=0;
   for my $j(0..$#{$dbsetable}) {
         if(!$$dbsetable[$j]->{sename}){
              splice(@$dbsetable,$j-$spliceoffset,1);
                        $spliceoffset++;
          }elsif(!$$dbsetable[$j]->{protocols}){
              $$dbsetable[$j]->{protocols}  = "none";
          }
   }
  
  
#   $self->info("getAlternateSEInfoFromDB, SOAP called is performed successfully.");

   for my $j(0..$#{$dbsetable}) {
       $$dbsetable[$j]->{sename} = uc( $$dbsetable[$j]->{sename} );
   }


   my @selist=();
   my $resc = 0;
 
   # consider the weights as a priority
   
   while((scalar(@selist) < $custodial) and ($#$dbsetable >= $resc)) {
       if(($$dbsetable[$resc]->{protocols} eq "custodial") &&
                         (($weightTable->{$$dbsetable[$resc]->{sename}}) && ($weightTable->{$$dbsetable[$resc]->{sename}} > 0 ))) {
           push @selist, $$dbsetable[$resc]->{sename};
       }
       $resc++;
   }

   $resc=0;

   while((scalar(@selist) < $secount) and ($#$dbsetable >= $resc)) {
       if(($$dbsetable[$resc]->{protocols} ne "custodial") && 
                         (($weightTable->{$$dbsetable[$resc]->{sename}}) && ($weightTable->{$$dbsetable[$resc]->{sename}} > 0 ))) {
           push @selist, $$dbsetable[$resc]->{sename};
       }
       $resc++;
   }

   # go beyong weights   
   $resc=0;
   while((scalar(@selist) < $custodial) and ($#$dbsetable >= $resc)) {
       if(($$dbsetable[$resc]->{protocols} eq "custodial") && (
                         (($weightTable->{$$dbsetable[$resc]->{sename}}) && ($weightTable->{$$dbsetable[$resc]->{sename}} eq 0))
                         or (! $weightTable->{$$dbsetable[$resc]->{sename}}))) {
           push @selist, $$dbsetable[$resc]->{sename};
       }
       $resc++;
   }

   $resc=0;
   while((scalar(@selist) < $secount) and ($#$dbsetable >= $resc)) {
       if(($$dbsetable[$resc]->{protocols} ne "custodial") && (
                         (($weightTable->{$$dbsetable[$resc]->{sename}}) && ($weightTable->{$$dbsetable[$resc]->{sename}} eq 0))
                         or (! $weightTable->{$$dbsetable[$resc]->{sename}}))) {
           push @selist, $$dbsetable[$resc]->{sename};
       }
       $resc++;
   }

   (scalar(@selist) eq $secount) and return (1, \@selist);
   return (0,\@selist);
}


sub complementSEListWithLocalConfigSEList {
  my $self=shift;
  my $ses=shift;
  my @localDefaultSEs=@{$self->{CONFIG}->{SEs_FULLNAME}};

  for my $entry (@localDefaultSEs) {
    (!grep (/^$entry$/i, @$ses)) and push @$ses, $entry;
  }
  return $ses;
}

#sub copyInMSS {
#  my $self=shift;
#  my $catalog=shift;
#  my $lfn=shift;
#  my $localfile=shift;

#  my $size=(-s $localfile);
#  self->info("Trying to save directly in the MSS");
#  my $name = $self->{CONFIG}->{SE_MSS};
#  $name
#    or $self->{LOGGER}->warning( "SE", "Error: no mass storage system" )
#      and return;
#  if ($name eq "file"  ) {
#    if ($self->{HOST} ne  $self->{CONFIG}->{SE_HOST}) {
#      $self->info("Using the file method, and we are not in the right machine ($self->{CONFIG}->{SE_HOST})... let's exist just in case");
#      return;
#    }
#  }
#  $name = "AliEn::MSS::$name";
#  eval "require $name"
#    or $self->{LOGGER}->warning( "SE", "Error: $name does not exist $! and $@" )
#      and return;
#  my $mss = $name->new($self);

#  $mss or return;
#  $self->info("Got the mss");
#  my ($target,$guid)=$mss->newFileName();
#  $target or return;
#  $target="$self->{CONFIG}->{SE_SAVEDIR}/$target";
#  $self->info("Writing to $target");
#  my $pfn;
#  eval {
#    $pfn=$mss->save($localfile,$target);
#  };
#  if ($@) {
#    $self->info("Error copying the file: $@");
#    return;
#  }
#  $pfn or $self->info("The save didn't work :(") and return;
#  $self->info("The save worked ($pfn)");
#  return $catalog->execute("register", $lfn, $pfn, $size);
#}
#sub submitLocalFile {
#  my $self            = shift;
#  my $catalog         = shift;
#  my $localdirectory  = shift;
#  my $remotedirectory = shift;
#  my $filename        = shift;
#  my $sename          = shift || "";

#  my $id=$ENV{ALIEN_PROC_ID};
#  my $lfn= AliEn::Util::getProcDir($self->{JOB_USER}, undef, $id) . "/job-output/$filename";

#  my $localfile="$localdirectory/$filename";
#  #Let's see if the file exists...
#  if (! -f $localfile) {
#    $self->info("The job was supposed to create $filename, but the file $filename doesn't exist!!",1);
#    return ;
#  }

#  #First, let's try to put the file directly in the SE
#  if (!$catalog->execute("add", $lfn, "file://$self->{HOST}$localdirectory/$filename", $sename)) {  
#    #Now, let's try registering the file.
#    if (! $self->copyInMSS($catalog,$lfn,$localfile)) {
#      $self->info("The registration didn't work...");
#      $self->submitFileToClusterMonitor($localdirectory, $filename, $lfn, $catalog) or return;
#    }
#  }
#  #Ok The file is registered. Checking if it has to be registered in more than
#  # one place
#  if ($self->{OUTPUTDIR} and ($self->{STATUS} eq "DONE") ) {
#    #copying the output to the right place
#    $catalog->execute("cp", $lfn, $self->{OUTPUTDIR}) or
#      $self->putJobLog("error","Error copying $lfn to $self->{OUTPUTDIR}");
#  }
#  return $lfn;
#}

=item submitFileToClusterMonitor($localdirectory, $filename)

This function uploads a file from the WN into the machine running the clustermonitor

=cut

sub submitFileToClusterMonitor{
  my $self=shift;
  my $localdirectory=shift;
  my $filename=shift;
  my $lfn=shift;
  my $catalog=shift;
  my $options=shift || {};
  my $maxlength = 1024 * 10000;

  my $fullName="$localdirectory/$filename";
  my $buffer;
  my $size;
  if ( open( FILE, $fullName ) ) {
    $size= read( FILE, $buffer, $maxlength, 0 );
    close(FILE);
    ( $size < $maxlength ) or $self->info("The file is bigger than $maxlength)") and return;
  }
  else {
    print STDERR "File $fullName could not be opened\n";
    return;
  }
  my $md5=AliEn::MD5->new($fullName);
  my $id=$ENV{ALIEN_PROC_ID};
  my $var = SOAP::Data->type( base64 => $buffer );
  my $org=$self->{CONFIG}->{ORG_NAME};
  # Modified so that putFILE return local (to ClusterMonitor) path to file
  my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR_$org","putFILE", $id, $var, $filename );
  
  ($done) and ( $done = $done->result );
  ($done)
    or print STDERR
      "Error contacting ClusterMonitor 	$ENV{ALIEN_CM_AS_LDAP_PROXY}\n"
	and return;
	
  $self->info("Inserting $done in catalog...");

    #    print STDERR "$localdirectory/$filename has size $size\n";
  # To make sure we are in the right database.
  
  my $return=$lfn;

  if ($catalog){
#! $options->{no_register}){
    my $dir= AliEn::Util::getProcDir($self->{JOB_USER}, undef, $ENV{ALIEN_PROC_ID}) . "/job-log";
    my $host="$ENV{ALIEN_CM_AS_LDAP_PROXY}";
    $catalog->execute("mkdir", $dir);

    $catalog->execute( "register",  "$dir/$lfn",
		       "soap://$ENV{ALIEN_CM_AS_LDAP_PROXY}$done?URI=ClusterMonitor",$size, "no_se", "-md5", $md5) 
      or print STDERR "ERROR Adding the entry $done to the catalog!!\n"
	and return;
  }

  return {size=>$size, md5=>$md5, se=>'no_se', 
	  pfn=>"soap://$ENV{ALIEN_CM_AS_LDAP_PROXY}$done?URI=ClusterMonitor"};
}

sub catch_zap {
    my $signame = shift;
    print STDERR
      "Somebody sent me a $signame signal.Registering the output\n";
    #$self->putFiles();  old entry, redirected trough new funtion:
    $self->prepare_File_And_Archives_From_JDL_And_Upload_Files();

    $self->registerLogs(0);
    $self->changeStatus("%", "INTERRUPTED");
    $self->{STATUS}="INTERRUPTED";

    finishMonitor();
    exit;
}

sub getChildProcs {
  my $this = shift;
  my $pid = shift;
  my $results = shift;
  my $sallps = shift;

  my $first ;
  my @allps;

  if ($sallps) {
    @allps= @{$sallps};
  }

  my @all;

  if ( $#allps == -1 )  {
    open (A, "ps -eo \"pid ppid\"|");
    my @lines=<A>;
    close (A);
    shift @lines;
    push @allps, @lines;

  }

  foreach (@allps) {
    my ($newpid,$newppid) = split " ", $_;
    ($newpid) and chomp $newpid;
    ($newppid) and chomp $newppid;

    if ( ($newpid == $pid) || ($newppid == $pid) ) {
      if ( ($newpid != $self->{PROCESSID}) ) {
	push @all, $_;
      }
    }
  }
  
  foreach  (@all) {
    my ($newpid,$newppid) = split " ", $_;
    chomp $newpid;
    if ($newpid  != $pid) {
      $self->getChildProcs($newpid,$results,\@allps);
    } else {
      #           print $newpid,"\n";
      push @{$results}, $newpid;
    }
  }
}


sub getProcInfo {
  my $this = shift;
  my $pid = shift;
  my @allprocs;
  my $cmd=0;
  my $runtime="00:00:00";
  my $start=0;
  my $cpu=0;
  my $mem=0;
  my $cputime=0;
  my $rsz=0;
  my $vsize=0;
  my @procids;
  my @proccpu;

  $self->debug(1, "Getting the procInfo of $pid");

  $self->getChildProcs($pid,\@allprocs);
  $self->debug(1, "Got children @allprocs");
  @allprocs or return "";
  $self->debug(1, "Getting info");
  if($self->{MONITOR}){
    my $res = $self->{MONITOR}->getJobMonInfo($pid, "cpu_ksi2k");
    if(defined($res->{cpu_ksi2k})){
      $self->{CPU_KSI2K} = $res->{cpu_ksi2k};
    }else{
      delete $self->{CPU_KSI2K};
    }
  }
  
  # check if we have a new ps
  # new ps has a bigger default for the 'command' column size and alien expects 16 
  # characters only but old 'ps' doesn't understand the new format with :size so we have 
  # to check first what kind of ps we have.
  my $ps_format = "command start";
  if(open(FILE, "ps -p 1 -o \"command:16 start:8 \%cpu\" |")){
    my $line = <FILE>; # ignore header
    $line = <FILE>;
    $ps_format = "command:16 start:8" if $line !~ /^command:16 start:8/;
    close FILE;
  }else{
    print "getProcInfo: cannot determine the ps behaviour\n";
  }
  for (@allprocs) {
    my $npid = $_;
    chomp $npid;
    #    print "ps --no-headers --pid $npid -o \"cmd start %cpu %mem cputime rsz vsize\"\n";
    #the --no-headers and --pid do not exist in mac
    open (FILE, "ps -p $npid -o \"$ps_format \%cpu \%mem cputime rsz vsize\"|") or print "getProcInfo: error checking ps\n" and next;
    my @psInfo=<FILE>;
    close FILE;
    shift @psInfo; #get rid of the headers

    my $all=shift @psInfo;
    $all or next;

    # look for the pid in the proclist
    my $checkpid;
    my $position=0;
    my $atposition=-1;
    for $checkpid (@procids) {
      if ($checkpid == $npid) {
	$atposition=$position;
	last;
      }
      $position++;
    }
    
    # add, if it does not exist yet ....
    if ($atposition == -1) {
      my $cpu0=0;
      # add to the list;
      push @procids, $npid;
      push @proccpu, $cpu0;
      $atposition = $#procids-1;
    }

    $self->debug(1, "Processing ps output: $all");
    
    $all =~ /(.{16})\s+(.{8})\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)/;
    #    print "-> $1,$2,$3,$4,$5,$6,$7\n";
    my $a1 = $1;
    my $a2 = $2;
    my $a3 = $3;
    my $a4 = $4;
    my $a5 = $5;
    my $a6 = $6;
    my $a7 = $7;
    
    my $timestart = `date +\%s -d \" $a2 \"`;
    chomp $timestart;
    my $timenow   = `date +%s`;
    chomp $timenow;
    my $timerun   = $timenow - $timestart;
    
    if ($timerun<0) {
      $timerun = (24*3600)+$timerun;
      }
    if ($timerun > $start) {
      $start = $timerun;
    }
    if (! ($cmd)) {
      $cmd = $a1;
    }
    $cpu += $a3;
    $mem += $a4;
    
    my ($cpuh,$cpum,$cpus) = split ":",$a5;
    my $cpusec = $cpuh*3600 + $cpum*60 + $cpus;
    
    $cputime += $cpusec;
    $rsz += $a6;
    $vsize += $a7;
    #    print $npid,": $a1, $a2, $a3, $a4, $a5, $a7,  Running $timestart $timerun seconds\n";
    
    if ($cpusec > $proccpu[$atposition]) {
      $proccpu[$atposition] = $cpusec;
    }
    
    if ($start < 60 ) {
      $runtime = sprintf "00:00:%02d", $start; 
    } else {
      if ($start <3600) {
	$runtime = sprintf "00:%02d:%02d", ($start/60), ($start%60);
      } else {
	$runtime = sprintf "%02d:%02d:%02d", ($start/3600), ($start-int($start/3600)*3600)/60, ($start-int($start/3600)*3600)%60;
      }
    }
  }
  my ($ncpu, $cpuspeed, $cpufamily)= $self->getSystemInfo();

  my $sumcpu=0;
  my $accpu;
  for $accpu (@proccpu) {  
    $sumcpu+=$accpu;
  }
  
#  my $resourcecost = sprintf "%.02f",$cputime * $cpuspeed/1000;

  # work aroung, because this values are not available on mac
  defined $cpuspeed
      or $cpuspeed = 0;
  defined $cpufamily
      or $cpufamily = "unknown";

  my $resourcecost = sprintf "%.02f",$sumcpu * $cpuspeed/1000;

#  if (@allprocs) {
  $self->debug(1, "Returning: $runtime $start $cpu $mem $cputime $rsz $vsize $ncpu $cpufamily $cpuspeed $resourcecost");
  return "$runtime $start $cpu $mem $cputime $rsz $vsize $ncpu $cpufamily $cpuspeed $resourcecost"
      #
#  }
}

sub getSystemInfo {
  my $self=shift;
  # get the system information
  my $ncpu=0;
  my $cpuspeed;
  my $cpufamily;
  $self->{SYSTEM_INFO} and return @{$self->{SYSTEM_INFO}};
  open (A, "/proc/cpuinfo");
  while (<A>) {
    my $a;
    $a = $_;
    chomp $a;
    my @b = split ":", $a;
    if ($b[0]) {
      if ($b[0] =~ /(cpu )?family.*/) {
	$ncpu++;
	$cpufamily = $b[1];
      }
      if ($b[0] =~ /cpu MHz.*/) {
	$cpuspeed = $b[1];
      }
      #    print $_;
    }
  }
  close (A);
  $self->{SYSTEM_INFO}=[$ncpu, $cpuspeed, $cpufamily];
  return ($ncpu, $cpuspeed, $cpufamily);

}


sub firstExecution {
  my $self=shift;
  my $method=shift;
  $self->debug(1, "First time that we check the process");
  
  my $date =localtime;
  $date =~ s/^\S+\s(.*):[^:]*$/$1/	;
  $self->{STARTTIME} = $date;
  my $counter=0;
  my $procinfo;
  do {
    $procinfo = $self->getProcInfo($self->{PROCESSID});

    $counter++;
    ($procinfo) or 	sleep (1);
    $self->info("Waiting for Execution");
  } while ( ($counter<10) &&  (!$procinfo) );

  my @ap	= split " ",$procinfo;
  if ( ($procinfo) && ($procinfo ne "") ) { 
    $self->{SLEEP_PERIOD}=60;

    $procinfo = "$ap[0] $ap[1] $ap[2] $ap[3] $ap[4] $ap[5] $ap[6] $ap[7] $ap[8] $ap[9] $ap[10] $self->{AVRSIZE} $self->{AVVSIZE}";
    $procinfo .= " $self->{CPU_KSI2K}" if(defined $self->{CPU_KSI2K});
    $self->info("Process started:  $procinfo");

    $self->{PROCINFO} = $procinfo;
    #send it to the ClusterMonitor
    my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR","SetProcInfo",
				       $self->{QUEUEID}, "$procinfo");
    $self->{SOAP}->checkSOAPreturn($done);
    return 1;
  }
  $self->{SLEEP_PERIOD}=10;
  $self->{LOGGER}->$method("JobAgent", "Job hasn't started yet...");

  $self->checkProcess($self->{PROCESSID}) or
    $self->info("But the job is no longer there...")
      and return 1;
  $self->{LOGGER}->$method("JobAgent", "And it is still there...");
  $self->{STARTTIME}="0";
  return 0;

}
sub checkProcess{
  my $self=shift;
  my $id=shift;

  $self->debug(1, "Checking if $id is still alive");
#  system("ps -ef |grep $id");
  kill(0,$id) or return;
  
  my @defunct = `ps -p $id -o state`;
  $self->debug(1, "Defunct check. Got: " . join(" ", @defunct));
  shift @defunct; # remove header
  $defunct[0] 
    and ($defunct[0] =~ /Z/)
    and $self->info("The process is defunct") 
    and return;

  $self->debug(1, "Checking if the proccess still has time");
  my $killMessage;

  my $time=time;
  ($time > $self->{JOBEXPECTEDEND}) and 
    $killMessage="it was running for longer than its TTL";

  if ($time-1200 >$self->{CPU_CONSUMED}->{TIME}){
    my $consumed=AliEn::Util::get_pid_jiffies($id);
    if ($consumed and ($consumed eq $self->{CPU_CONSUMED}->{VALUE})) {
      $killMessage="due to zero CPU consumption in the last 20 minutes!";
    }
    $self->{CPU_CONSUMED}={TIME=>$time, VALUE=>$consumed};
    
  }
  if ($self->{WORKSPACE} ){
    my $space=du($self->{WORKDIR} ) /1024 /1024;
    $self->info( "Checking the disk space usage of $self->{WORKDIR} (now $space, out of $self->{WORKSPACE} MB ");
    $space <$self->{WORKSPACE} or 
      $killMessage="using more than $self->{WORKSPACE} MB of diskspace (right now we were using $space MB)";
  }

  if ($killMessage){
    AliEn::Util::kill_really_all($self->{PROCESSID});
    $self->info("Killing the job ($killMessage)");
    $self->putJobLog("error","Killing the job ($killMessage)");
    $self->changeStatus("%", "ERROR_E");
    return;
  }

  return 1 ;
}
sub lastExecution {
  my $self=shift;

  $self->{PROCINFO} or $self->{PROCINFO}="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0";
  my @ap	= split " ",$self->{PROCINFO};
  # if the job is over, do a last average and submit it to the ClusterMonitor
  $self->{AVRSIZE}=$self->{AVVSIZE}=$self->{AVCPU}=0;
  
  if ($self->{SUMCOUNT}) {
    $self->{AVRSIZE} = int ($self->{SUMRSIZE}/$self->{SUMCOUNT});
    $self->{AVVSIZE} = int ($self->{SUMVSIZE}/$self->{SUMCOUNT});
    $self->{AVCPU}   = sprintf "%.02f",($self->{SUMCPU}/$self->{SUMCOUNT});
  }     
  
  $self->{MAXRSIZE} = int ($self->{MAXRSIZE});
  $self->{MAXVSIZE} = int ($self->{MAXVSIZE});

  my $procinfo = "$ap[0] $ap[1] $self->{AVCPU} $ap[3] $ap[4] $self->{MAXRSIZE} $self->{MAXVSIZE} $ap[7] $ap[8] $ap[9] $self->{MAXRESOURCECOST} $self->{AVRSIZE} $self->{AVVSIZE}";
  $procinfo .= " $self->{CPU_KSI2K}" if(defined $self->{CPU_KSI2K});
  $self->info("Last ProcInfo: $procinfo");
  
  #submit the last Proc Info
  my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR","SetProcInfo", $self->{QUEUEID}, "$procinfo");
#	$self->{SOAP}->checkSOAPreturn($done);

  # add some output to the process resource file
  my ($ProcRuntime,$ProcStart,$ProcCpu,$ProcMem, $ProcCputime,$ProcRsz,$ProcVsize,$ProcNcpu, $ProcCpufamily,$ProcCpuspeed,$ProcResourceCost,$cpuKsi2k);
  ($ProcRuntime,$ProcStart,$ProcCpu,$ProcMem, $ProcCputime,$ProcRsz,$ProcVsize,$ProcNcpu, $ProcCpufamily,$ProcCpuspeed,$ProcResourceCost)= split ' ',$self->{PROCINFO};
  my $date =localtime;
  $date =~ s/^\S+\s(.*):[^:]*$/$1/	;
  $self->{STOPTIME} = $date;
	
  my $DuSize = `du -Lsc $self->{WORKDIR}/| tail -1|awk '{print \$1}'`; 
  my $DuOutSize =0;
  if ($self->{OUTPUTFILES}) {
    $DuOutSize =`du -Lsc $self->{OUTPUTFILES}| tail -1|awk '{print \$1}'`;
  }
  $cpuKsi2k = defined($self->{CPU_KSI2K}) || "?";
	
  chomp $DuSize;
  chomp $DuOutSize;
	
  open (POUT,">>$self->{WORKDIR}/resources");
  print POUT  "Execution Host                            : $self->{HOST}
Command                                   : $self->{COMMAND}\n";	    
  if ($self->{ARG}) {
    print POUT  "Args                                      : $self->{ARG}\n";
  }
  print POUT  "Start Time                                : $self->{STARTTIME}
Stop  Time                                : $self->{STOPTIME}
Input Sandbox File Size             [kb ] : $DuSize
Output Sandbox File Size            [kb ] : $DuOutSize
Elapsed real time                   [sec] : $ProcRuntime
CPU perc. of this job               [\%  ] : $ProcCpu
MEM perc. of this job               [\%  ] : $ProcMem
CPU time                            [sec] : $ProcCputime
CPU KSI2K			    [#  ] : $cpuKsi2k
Max. res. MEM size                  [kb ] : $ProcRsz
Max. vir. MEM size                  [kb ] : $ProcVsize
CPUs                                [#  ] : $ProcNcpu
CPU family                                : $ProcCpufamily
CPU Speed                           [MHz] : $ProcCpuspeed
------------------------------------------------------------\n";
  my $cpucost = $ProcCputime * $ProcCpuspeed/1000 ;
  print POUT  "CPU      Cost                             : $cpucost
============================================================\n";
  close POUT;
	
  # put all output files into AliEn
  #	    

  $self->{STATUS}="SAVED";

  if ( $self->{VALIDATIONSCRIPT} ) {
    $self->putJobLog("trace","Validating the output");
    my $validation=$self->{VALIDATIONSCRIPT};
    $validation=~ s{^.*/([^/]*)$}{$self->{WORKDIR}/$1};

    if ( -r $validation ) {	
      chmod 0750, $validation;
      my $validatepid = fork();
      if (! $validatepid ) {
	# execute the validation script
	$self->info("Executing the validation script: $validation");
	unlink "$self->{WORKDIR}/.validated";
	if (! system($validation) ){
	  $self->info("The validation finished successfully!!");
	  system("touch $self->{WORKDIR}/.validated" ) ;
	}
	$self->info("Validation finished!!");
	exit 0;
      }
      my $waitstart = time;
      my $waitstop  = time;
      while ( ($waitstop-300) < ($waitstart) ) {
	sleep 5;
	$self->info("Checking $validatepid");
	kill (0,$validatepid) or last;

	my @defunct = `ps -p $validatepid -o state`;
	$self->debug(1, "Defunct check. Got: " . join(" ", @defunct));
	shift @defunct; # remove header
	$defunct[0]
	    and ($defunct[0] =~ /Z/)
	    and $self->info("The process is defunct")
	    and last;

	$waitstop = time;
      }
      if ( ($waitstop-300) > ($waitstart) ) {
	$self->putJobLog("trace","The validation script din't finish");
	$self->{STATUS} = "ERROR_VT";
      } else {
	( -e "$self->{WORKDIR}/.validated" ) or  $self->{STATUS} = "ERROR_V";
      }
    } else {
      $self->putJobLog("error","The validation script '$validation' didn't exist");
      $self->{STATUS} = "ERROR_VN";
    }
    $self->putJobLog("trace","After the validation $self->{STATUS}");
  }

  # store the files
  #$self->putFiles() or $self->{STATUS}="ERROR_SV";  old entry, redirected trough new funtion:
  my $uploadFilesState = $self->prepare_File_And_Archives_From_JDL_And_Upload_Files() ;

  ($uploadFilesState eq -1) and $self->{STATUS}="SAVED_WARNING";
 
  ($uploadFilesState eq 0) and $self->{STATUS}="ERROR_SV";

  $self->registerLogs();

  my $jdl;
  $self->{JDL_CHANGED} and $jdl=$self->{CA}->asJDL();
  
  my $success=$self->changeStatus("%",$self->{STATUS}, $jdl);
  # don't send data about this job anymore
  if($self->{MONITOR}){
    $self->{MONITOR}->removeJobToMonitor($self->{PROCESSID});
  }
  chdir;
  system("rm", "-rf", $self->{WORKDIR});
  $self->putJobLog("state", "The job finished on the worker node with status $self->{STATUS}");
  $self->{JOBLOADED}=0;
  $self->{SOAP}->CallSOAP("CLUSTERMONITOR", "jobExits", $ENV{ALIEN_PROC_ID});
  delete $ENV{ALIEN_JOB_TOKEN};
  delete $ENV{ALIEN_PROC_ID};
  if (!$success){
    $self->sendJAStatus('DONE');
    $self->info("The job did not finish properly... we don't ask for more jobs");
    $self->stopService(getppid());
    kill (9, getppid());
    exit(0);
  }
  return 1;
}

sub checkWakesUp {
  # we calculate the resource usage of the running process and submit it to the ClusterMonitor
  my $this=shift;
  my $silent=shift;
  my $method="info";
  my @loggingData;
  $silent and $method="debug" and push @loggingData, 1;;

  $self->$method(@loggingData, "Calculating the resource Usage");
  
  if($self->{MONITOR}){ 
    $self->{MONITOR}->sendBgMonitoring();
  }
  my $procinfo;
  my $i;
  if (! $self->{JOBLOADED}) {
    $self->sendJAStatus('REQUESTING_JOB');
    $self->info("Asking for a new job");
    if (! $self->requestJob()) {
      $self->sendJAStatus('DONE');
      $self->info("There are no jobs to execute");
      #Tell the CM that we are done"

      $self->{SOAP}->CallSOAP("CLUSTERMONITOR", "agentExits", $ENV{ALIEN_JOBAGENT_ID});
      # killeverything connected
      $self->info("We have to  kill $self->{SERVICEPID} or ".getppid());
#      system("ps -ef |grep JOB");
      unlink $self->{WORKDIRFILE};
      system ("rm", "-rf", $self->{WORKDIRFILE});
      $self->stopService(getppid());
      kill (9, getppid());
      exit(0);
    }
  }

  if ($self->{STARTTIME} eq '0') {
    $procinfo =$self->firstExecution($method);
    $procinfo or return; 
  }

  # if ApMon available, send my status
  if($self->{MONITOR}){
    $self->sendJAStatus('RUNNING_JOB');
    #$self->info("Trying to read message from pipe from child...");
    my $status = $self->readPipeMessage($self->{JOB_STATUS_RDR});
    if($status){
      #$self->info("Parsing status from child process:[$status]");
      my ($jid, $stat) = (undef, undef);
      # find last status, if it exists
      for my $line (split(/\n/, $status)){
        if($line =~ /(\S+)=(\S+)/){
          ($jid, $stat) = ($1, $2);
        }
      }
      # update the status for the current job
      #$self->info("Updating status for ALIEN_PROC_ID=$ENV{ALIEN_PROC_ID}, jid=$jid, stat=$stat");
      $self->{STATUS} = $stat if($jid && $stat && ($ENV{ALIEN_PROC_ID} eq $jid));
    }
    # send data about current job
    if($self->{STATUS}){
      #$self->info("Sending status info for $self->{CONFIG}->{CE_FULLNAME}_Jobs/$ENV{ALIEN_PROC_ID} = $self->{STATUS}");
      $self->{MONITOR}->sendParameters($self->{CONFIG}->{CE_FULLNAME}.'_Jobs', $ENV{ALIEN_PROC_ID}, {'status' => AliEn::Util::statusForML($self->{STATUS}), 'host' => $self->{HOST}, 'job_user' => $self->{JOB_USER} });
    }else{
      $self->info("Status info not avialble for $self->{CONFIG}->{CE_FULLNAME}_Jobs/$self->{PROCESSID}...");
    }
  }
	
  my @all;		
  for $i ( 1 .. 10 ) {
    $procinfo = $this->getProcInfo($self->{PROCESSID});

    if (!($procinfo)) {
      next;
      #			$self->lastExecution();
    } else {
      $self->{PROCINFO} = $procinfo;
      @all = split " ",$procinfo;
      if ( $all[6] > $self->{MAXVSIZE} ) {
	$self->{MAXVSIZE} = $all[6];
      }
      if ( $all[5] > $self->{MAXRSIZE} ) {
	$self->{MAXRSIZE} = $all[5];
      }

      $self->{SUMRSIZE} += $all[5];
      $self->{SUMVSIZE} += $all[6];
      $self->{SUMCPU} += $all[2];
      if ( $all[10] > $self->{MAXRESOURCECOST} ) {
	$self->{MAXRESOURCECOST} = $all[10];
      }
      $self->{SUMCOUNT} ++;
      $self->{AVRSIZE} = int ($self->{SUMRSIZE}/$self->{SUMCOUNT});
      $self->{AVVSIZE} = int ($self->{SUMVSIZE}/$self->{SUMCOUNT});
      $self->{AVCPU}   = sprintf "%.02f",($self->{SUMCPU}/$self->{SUMCOUNT});	
      $self->{PROCINFO} = "$all[0] $all[1] $self->{AVCPU} $all[3] $all[4] $all[5] $all[6] $all[7] $all[8] $all[9] $self->{MAXRESOURCECOST} $self->{AVRSIZE} $self->{AVVSIZE}";
      $self->{PROCINFO} .= " $self->{CPU_KSI2K}" if defined($self->{CPU_KSI2K});
    }

    #	$self->info("ProcInfo: $procinfo");
    sleep(1);

  }

  #we are going to send the procinfo only one every ten times
  $self->{ALIEN_PROC_INFO} or  $self->{ALIEN_PROC_INFO}=0;
  $self->{ALIEN_PROC_INFO}++;
  if ($self->{ALIEN_PROC_INFO} eq "10") {
    $self->{SOAP}->CallSOAP("CLUSTERMONITOR","SetProcInfo",
			    $self->{QUEUEID}, $self->{PROCINFO});
    $self->{ALIEN_PROC_INFO}=0;
  }
  

  $self->checkProcess($self->{PROCESSID}) and return;

  $self->info("Process $self->{PROCESSID} has finished");
  waitpid(-1, &WNOHANG);

  $self->lastExecution();
  $self->{LOGGER}->redirect();
  $self->info("Back to the normal log file");
  return 1;
}

sub alive{
  my $this=shift;
  my $jobid=shift; 
  print "Process Monitor contacted to see if it's alive (and running $jobid)\n";
  ( "$jobid" eq "$ENV{ALIEN_PROC_ID}") or print "Running the wrong job ($jobid instead of $ENV{ALIEN_PROC_ID}!!\n" and return;

  print "Checking if the process $self->{PID} is still alive...\n";

  my $jobs=`ps -eo pid |grep $self->{PID}`;

  print "GOT $jobs\n";
  $jobs or print "Process died!!!\n" and return;

  return 1;
}
sub registerLogs {
  my $this=shift;
  my $skip_register=shift;
  defined $skip_register or $skip_register=1;
  $self->{REGISTER_LOGS_DONE} and return 1;

  $self->info("Let's try to put the log files in the jdl ");
  my $func=sub {
#    my $catalog=shift;
  #    my $dir= AliEn::Util::getProcDir($self->{JOB_USER}, undef, $ENV{ALIEN_PROC_ID}) . "/job-log";
#    my $localfile="$self->{CONFIG}->{LOG_DIR}/$ENV{ALIEN_LOG}";
    my $host="$ENV{ALIEN_CM_AS_LDAP_PROXY}";
#    $catalog->execute("mkdir", $dir);
#    $self->info("Putting the log files in $dir");
    print "I'm looking into $self->{CONFIG}->{TMP_DIR}/proc/\n";
    my $dir=$self->{LOGFILE};
    $dir=~ s{/([^/]*)$}{/};
    my $basename=$1;
    my $data=$self->submitFileToClusterMonitor($dir,$basename, "execution.out");
    $data or $self->info("Error submitting the log file") and return;

    $self->info("And now, let's update the jdl");
    $self->{CA}->set_expression("RegisteredLog", "\"execution.out######$data->{size}###$data->{md5}###$data->{se}/$data->{pfn}###\"");
    $self->{JDL_CHANGED}=1;
  };
  $self->doInAllVO({},$func);
  $self->{REGISTER_LOGS_DONE}=1;


  return 1;
}

sub doInAllVO{
  my $self=shift;
  my $options=shift ||{};
  my $func=shift;
  foreach my $data (split (/\s+/, $self->{VOs})){
    my ($org, $cm,$id, $token)=split ("#", $data);
    $self->info("Connecting to services for $org ($data)");
    $ENV{ALIEN_PROC_ID}=$id;
    $ENV{ALIEN_JOB_TOKEN}=$token;
    $ENV{ALIEN_ORGANISATION}=$org;
    $ENV{ALIEN_CM_AS_LDAP_PROXY}=$cm;
    $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $org});
    $func->();
  }
}

# This is called by child processes to read messages (if they exist) from the parent.
sub readPipeMessage {
  my $self = shift;
  my $PIPE = shift;

  my ($rin, $win, $ein, $rout, $wout, $eout) = ('', '', '');
  my $retMsg = "";
  vec($rin,fileno($PIPE),1) = 1;
  $ein = $rin | $win;
  my ($nfound,$timeleft) = select($rout=$rin, $wout=$win, $eout=$ein, 0);
  if($nfound){
    sysread($PIPE, $retMsg, 1024);
  }
  return $retMsg;
}

# This is called by main process to send a message to a child that reads form the given pipe
sub writePipeMessage {
  my $self = shift;
  my ($PIPE, $msg) = @_;

  if(defined $PIPE){
    syswrite($PIPE, $msg, length($msg));
  }
}

# Send the given status of the JobAgent to MonaLisa
sub sendJAStatus {
  my $self = shift;
  my $status = shift;
  my $params = shift || {};
  return if ! $self->{MONITOR};

  # add the given parameters

  defined  $status and $params->{ja_status} = AliEn::Util::jaStatusForML($status);
  if($ENV{ALIEN_JOBAGENT_ID} && $ENV{ALIEN_JOBAGENT_ID} =~ /(\d+)\.(\d+)/){
    $params->{ja_id_maj} = $1;
    $params->{ja_id_min} = $2;
  }
  $ENV{SITE_NAME} and $params->{siteName}=$ENV{SITE_NAME};
  $params->{job_id} = $ENV{ALIEN_PROC_ID} || 0;
  $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_".$self->{SERVICENAME}, "$self->{HOST}:$self->{PORT}", $params);
  return 1;
}

return 1;

