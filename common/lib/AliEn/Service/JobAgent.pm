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

BEGIN{ $Devel::Trace::TRACE = 0 }

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

use Fcntl qw/O_WRONLY O_CREAT O_EXCL/;


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

  $self->{TOTALJOBS}=0;
  $self->{PROCESSID} = 0;
  $self->{STARTTIME} = '0';
  $self->{STOPTIME} = '0';
  $self->{OUTPUTFILES} = "";
  $self->{TTL}=($self->{CONFIG}->{CE_TTL} || 12*3600);
  $self->{TTL} and $self->info("This jobagent is going to live for $self->{TTL} seconds");
  $self->{ORIG_TTL}=$self->{TTL};
  ($self->{HOSTNAME},$self->{HOSTPORT}) =
    split ":" , $ENV{ALIEN_CM_AS_LDAP_PROXY};

  #$self->{HOST} = $ENV{'ALIEN_HOSTNAME'}.".".$ENV{'ALIEN_DOMAIN'};
  $self->{HOST} = $self->{CONFIG}->{HOST};

  $ENV{'ALIEN_SITE'} = $self->{CONFIG}->{SITE};
  $self->{CONFIG}->{SITE_HOST} and $ENV{'ALIEN_SITE_HOST'} = $self->{CONFIG}->{SITE_HOST};
  print "Executing in $self->{HOST}\n";
  $self->{PID}=$$;
  print "PID = $self->{PID}\n";
  $ENV{ALIEN_JOBAGENT_ID} and $ENV{ALIEN_JOBAGENT_ID}.="_$self->{PID}";

  my $packConfig=1;
  $options->{disablePack} and $packConfig=0;
  $self->{SOAP}=new AliEn::SOAP;

  $self->{SOAP}->{CLUSTERMONITOR}=SOAP::Lite
    ->uri("AliEn/Service/ClusterMonitor")
      ->proxy("http://$self->{HOSTNAME}:$self->{HOSTPORT}");

  $self->{CONFIG} = new AliEn::Config() or return;

  $self->{UI} = 0;

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
  if ( $self->{CONFIG}->{CE_INSTALLMETHOD} and $self->{CONFIG}->{CE_INSTALLMETHOD}=~"CVMFS" ) {
     $self->{PACKMAN}=AliEn::PackMan->new({PACKMAN_METHOD=>"CVMFS"});
   } else {   
     $self->{PACKMAN}=AliEn::PackMan->new({PACKMAN_METHOD=>"Local"});
   }

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
  $self->{TOTALJOBS}=$self->{TOTALJOBS}+1;


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
  $self->sendJAStatus('JOB_STARTED', {totaljobs=>$self->{TOTALJOBS}});
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

sub putAgentLog {
  my $self=shift;
  my $message=shift;
  my $id="$self->{CONFIG}->{CE_FULLNAME}_$ENV{ALIEN_JOBAGENT_ID}";
  $self->{agentlog_counter} or $self->{agentlog_counter}=0;  
  $self->{agentlog_counter}+=1;
  
  if($self->{last_agent_message} eq $message){
    $self->info("This is the same message");
    $self->{last_agent_counter}+=1;
    return 1;
  }


  if ($self->{last_agent_counter}){
    $self->{agentlog_counter}+=1;

    $self->{SOAP}->CallSOAP("CLUSTERMONITOR","putJobLog", $id,"agent",
        sprintf("%03d Last message repeated %d time(s)", $self->{agentlog_counter}, $self->{last_agent_counter}),@_) 
  }
  my $joblog = $self->{SOAP}->CallSOAP("CLUSTERMONITOR","putJobLog", $id,"agent", 
        sprintf("%03d %s", $self->{agentlog_counter}, $message),@_) or return;
  $self->{last_agent_message}=$message;
  $self->{last_agent_counter}=0;
  return 1;
}


sub getHostClassad{
  my $self=shift;
  my $ca=AliEn::Classad::Host->new({PACKMAN=>$self->{PACKMAN}}) or return;
  if ($self->{TTL}){
    $self->info("We have some time to live...");
    my $time = time();
    my $time_subs = $time-$self->{JOBAGENTSTARTS};
    my $timeleft=$self->{ORIG_TTL} - $time_subs;
    $self->info("We still have $timeleft seconds to live (".$time." - $self->{JOBAGENTSTARTS} = ".$time_subs.")");
    my $proxy=$self->{X509}->getRemainingProxyTime();
    $self->info("The proxy is valid for $proxy seconds");

    if (($proxy > 0 && $proxy < $timeleft)) {
#      $self->info("Let's try to extend the life of the proxy");
#      $self->{X509}->extendProxyTime($timeleft) or 
      $timeleft=$proxy;
    }
    #let's get 5 minutes to register the output
    $timeleft-=300;
    $ca->set_expression("TTL", $timeleft);
    $self->{TTL}=$timeleft;
    if ($timeleft<0){
      $self->info("We don't have any time left to execute jobs!");
      return;
    }

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
      return;
    }
    my $hostca_stage;

    $self->sendJAStatus(undef, {TTL=>$self->{TTL}});
 
    my $host=$self->{CONFIG}->{HOST};
    if ($ENV{ALIEN_CM_AS_LDAP_PROXY}){
       $host=$ENV{ALIEN_CM_AS_LDAP_PROXY};
       $host=~ s/^(https?:\/\/)?([^:]*)(:\d+)?/$2/;
       $self->info("The host is $host");
    }
    my $done = $self->{SOAP}->CallSOAP("Broker/Job", "getJobAgent", $self->{CONFIG}->{ROLE}, $host, $hostca, $hostca_stage);
    my $info;
    $done and $info=$done->result;
    if ($info){
	      $self->info("Got something from the ClusterMonitor");
	      if (!$info->{execute}){
		$self->info("We didn't get anything to execute");
	      }	else{
		my @execute=@{$info->{execute}};
		$result=shift @execute;
		if ($result eq "-3") {
		  $self->sendJAStatus('INSTALLING_PKGS', {packages=>join("", @execute)});
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
    } 
    else{
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


  my $message="The Job has been taken by Jobagent $ENV{ALIEN_JOBAGENT_ID}, AliEn Version: $self->{CONFIG}->{VERSION}";
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

#--- memory requirement
  ($ok, my @memrequest) = $self->{CA}->evaluateAttributeVectorString("Memorysize");
  if($ok){
      if (defined $memrequest[0]) {
	  my $munit = 1024; # default user input is in MB, mem query yields it in KB
         ($memrequest[0] =~ s/KB//g) and $munit = 1;
         ($memrequest[0] =~ s/MB//g) and $munit = 1024;
         ($memrequest[0] =~ s/GB//g) and $munit = 1024*1024;          
         $self->{MEMORY} = $memrequest[0] * $munit;
         $self->info("The job needs a maximum of $self->{MEMORY} KB of memory");
      }
  }

  ($ok, my $masterid) =$self->{CA}->evaluateAttributeString("MasterJobId");
  if ($ok) {
    $self->info("Setting the MasterJobId to $masterid");
    $ENV{ALIEN_MASTERJOBID}=$masterid;
  }
  
  my $proxytime;
  $self->{JOBEXPECTEDEND}=time()+$jobttl+600;
  ($ok, my $ttlproxy) = $self->{CA}->evaluateAttributeString("ProxyTTL");
  $ok and $ttlproxy and $self->{JOBEXPECTEDEND}=time()+$self->{TTL}-600;
  $self->putJobLog("trace",
  "The job needs $jobttl seconds, allowed until $self->{JOBEXPECTEDEND} (".localtime($self->{JOBEXPECTEDEND}).") ".( $ttlproxy ? "ProxyTTL=1 (Using $self->{TTL})" : "" ) );

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

  if (-d $self->{WORKDIR}) {
    $self->putJobLog("error","Working directory ($self->{WORKDIR}) of job $ENV{ALIEN_PROC_ID} already exists");
	$self->registerLogs(0);
	$self->changeStatus("%", "ERROR_IB");
	return 0;
  }

  $self->putJobLog("trace","Creating the working directory $self->{WORKDIR}");

  foreach my $fullDir (@dirs){
    my $dir = "";
    (-d  $fullDir) and next;
    foreach ( split ( "/", $fullDir ) ) {
      $dir .= "/$_";
      mkdir $dir, 0777;
      if (! (-d $dir) ) {
        $self->putJobLog("error","Directory $dir of job $ENV{ALIEN_PROC_ID} could not be created");
	    $self->registerLogs(0);
	    $self->changeStatus("%", "ERROR_IB");
	    return 0;
      }
    }
  }
  
  if(!chdir $self->{WORKDIR}){
  	$self->putJobLog("error","Could not chdir to working directory $self->{WORKDIR}) in job $ENV{ALIEN_PROC_ID}");
	$self->registerLogs(0);
	$self->changeStatus("%", "ERROR_IB");
	return 0;
  }

#  if ( !( -d $self->{WORKDIR} ) ) {
#    $self->putJobLog("error","Could not create the working directory $self->{WORKDIR} on $self->{HOST}");
#  }

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
  if (! $space){
     $self->info("Probably '$self->{WORKDIR}' is a link... getting the size in a different way");
     $handle->df();
     $space=$handle->avail($self->{WORKDIR});
  }
  
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
  my $notrace = defined $self->{CONFIG}->{CE_ENVIRONMENT_LIST} && grep( /NOTRACE/i,  @{$self->{CONFIG}->{CE_ENVIRONMENT_LIST}} );
  
  eval{ 
    $options->{silent} or $options->{silent}=0;

  if ( $self->{CONFIG}->{CE_INSTALLMETHOD} and $self->{CONFIG}->{CE_INSTALLMETHOD}=~"CVMFS" ) {
    $options->{packman_method} or $options->{packman_method}="CVMFS";
   } else {   
    $options->{packman_method} or $options->{packman_method}="Local";
   }

    $options->{role} or $options->{role}=$self->{CONFIG}->{CLUSTER_MONITOR_USER};
#    my $options={silent=>0, packman_method=>'Local', 'role'=>$self->{CONFIG}->{CLUSTER_MONITOR_USER}};
    $self->{CONFIG}->{AGENT_API_PROXY} and 
      $options->{gapi_catalog}=$self->{CONFIG}->{AGENT_API_PROXY};
    $self->info("Trying to get a catalogue");
    
    # copy STDERR to another filehandle
    open (my $STDOLD, '>&', STDERR);
    # redirect STDERR to log.txt
    open (STDERR, '>>', 'develTrace_'.$ENV{ALIEN_PROC_ID}.'');
    $Devel::Trace::TRACE = 1-$notrace;
    
    $catalog = AliEn::UI::Catalogue::LCM::->new($options);
    
    $Devel::Trace::TRACE = 0;
    open (STDERR, '>&', $STDOLD);    

  };
  if ($@) {print "ERROR GETTING THE CATALOGUE $@\n";}
  if (!$catalog) {
    $self->putJobLog("error","The job couldn't authenticate to the catalogue (no_trace $notrace)");

    if (1-$notrace){
	    open(FI, 'develTrace_'.$ENV{ALIEN_PROC_ID}.'') or $self->putJobLog("trace","Can't open develTrace") and return;
	    my $c = 0;
	    my $trace = "";  
	    while (<FI>){
	      #($_ =~ /Logger/i or $_ =~ /ISA/i or $_ =~ /Log\/Agent/i or $_ =~ /Class\/Struct/i or $_ =~ /Rotate/i) or 
	      $c++;
	      $trace .= "T$c: $_ -";
	      $c%1000==0 and $self->putJobLog("trace","$trace") and $trace="";
	    }   
	    close (FI);
	    $self->putJobLog("trace","$trace");
    }
    
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
    if ( $self->{CONFIG}->{CE_INSTALLMETHOD} and $self->{CONFIG}->{CE_INSTALLMETHOD}=~"CVMFS" ) {
         my ($ok, $source)=$self->{PACKMAN}->installPackage($user, join(",", @packages), undef, {NO_FORK=>1});
#        my ($ok, $source)=$self->installPackage( join(",", @packages), $user);
         if ($source){  
	   push @packInst, $source;
         }
    } else {   
      foreach (@packages) {
        my ($ok, $source)=$self->installPackage( $_, $user);
        if (!$ok){
	   $self->registerLogs(0);
	   $self->changeStatus("%",  "ERROR_E");
	   return;
         }
        if ($source){  
	  push @packInst, $source;
        }
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

  chdir $self->{WORKDIR} or return;
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
    if ($source) {
       $self->info("Checking if the packman installation can be accessed");
       $source =~ s/^\s*//;
       my ($file, $rest)=split(/ /, $source);
       $self->info("The file is '$file'");
       if (! -f "$file"){
          $self->info("We can't access the packman installation in $file");
          $ENV{ALIEN_PROC_ID} and
             $self->putJobLog("error","Package $_ not installed (can't access $file) ");
          return;
       }
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
  my $curdir = `pwd`;
  chomp($curdir);
  $self->putJobLog("trace","Putting the list of files in the file '".$curdir."/$dumplist'");
  $self->info("Putting the inputfiles in the file '$dumplist'");
  if (! sysopen(FILE, $dumplist, O_WRONLY | O_CREAT | O_EXCL) ){
    $self->info("Error putting the list of files in $dumplist");
    $self->putJobLog("error","Error putting the list of files in the file '".$curdir."/$dumplist");
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
  my $done={};

  foreach my $file (@lfns){
    my $remote=0;

    $file =~ s/LF://;
    $file =~ s/,nodownload//i and $remote=1;
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
      my $turl="alien://$file";
      if (! $remote){
        #How do I get the name of the local file??
        my $real= $self->findProcName($file, $done);
        $turl="file:///$self->{WORKDIR}/$real";
      }

      print FILE "      <file name=\"$basefilename\" lfn=\"$file\" 
turl=\"$turl\" />\n";
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

  my $options="-silent";
  if($lfnName =~ /###/){
    (my $tt,$lfnName,my @tmp) = split(/###/,$lfnName);
  }
  
  $self->info( "Getting inputfile $lfnName going to $pfn");
  
  for (my $i=0;$i<2;$i++) {
    $catalog->execute("get",$lfnName,"$pfn", $options ) and return 1;
    $options="";
    $self->putJobLog("trace","Error downloading input file: $lfnName (trying again). Message: ". $self->{LOGGER}->error_msg());
  }
  $self->putJobLog("error","Could not download the input file: $lfnName (into $pfn). Message: ". $self->{LOGGER}->error_msg());

  return;
}

sub getFiles {
  my $self    = shift;
  my $catalog = shift;
  #print "In getFiles\n";
  $self->info("Getting the files");
  my $oldmode=$self->{LOGGER}->getMode();
  $self->info("Got mode $oldmode");
  $self->dumpInputDataList($catalog) or return;

  $self->getInputZip($catalog) or return;

  my @files=$self->getListInputFiles($catalog);

  foreach my $file (@files) {
    $self->_getInputFile($catalog, $file->{cat},$file->{real}) or return;
  }

  #my $procDir = AliEn::Util::getProcDir($self->{JOB_USER}, undef, $self->{QUEUEID});

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

sub getFilesFromInputCollection{
  my $self = shift;
  my $job_ca = shift;
  my $catalogue=shift;
  
  my @files;
  my ($ok, @inputData) =
    $job_ca->evaluateAttributeVectorString("InputDataCollection");

	foreach my $file (@inputData) {
    $self->putJobLog("trace", "Using the inputcollection $file");

    my ($file2, $options) = split(',', $file, 2);
    $options and $options = ",$options";
    $options or $options = "";
    $file2 =~ s/^LF://;
    my ($type) = $catalogue->execute("type", $file2);
    $self->info("IT IS A $type");
    if ($type =~ /^collection$/) {
    	my ($files)=$catalogue->execute("listFilesFromCollection", $file2);
    	if ($files) {
    	  foreach my $entry (@$files) {
          if ($entry->{origLFN}) {
            push @files, "LF:$entry->{origLFN}$options";
          } else {
            push @files, "GUID:$entry->{guid}";
          }
    	  }
    	}
    }  else {
    	my ($localFile) = $catalogue->execute("get", $file2);
    	$localFile or $self->info("Error getting $file2") and return;
    	my $ds=AliEn::Dataset->new(); 
    	my $dataset = $ds->readxml($localFile);
    	my $lfnRef = $ds->getAllLFN()
       or $self->info("Error getting the LFNS from the dataset")
        and return;

      map { $_ = "LF:$_$options" } @{ $lfnRef->{lfns} };
      $self->info("Adding the files " . @{ $lfnRef->{lfns} });
      push @files, @{ $lfnRef->{lfns} };
    	
    } 
	}
  return 1, @files;
}


sub getListInputFiles {
  my $self=shift;
  my $catalogue=shift;

  #my $dir = AliEn::Util::getProcDir($self->{JOB_USER}, undef, $self->{QUEUEID}) . "/";

  my @files=({cat=>$self->{COMMAND}, real=>"$self->{WORKDIR}/command"});
  if ($self->{VALIDATIONSCRIPT}) {
    my $validation=$self->{VALIDATIONSCRIPT};
    $validation=~ s{^.*/([^/]*)$}{$self->{WORKDIR}/$1};
    push @files, {cat=>$self->{VALIDATIONSCRIPT},real=>$validation};
  }else {
    $self->info("There is no validation script");
  }
  my ( $ok,  @inputFiles)=$self->{CA}->evaluateAttributeVectorString("InputFile");
  ($ok, my @inputData)=$self->{CA}->evaluateAttributeVectorString("InputData");

   ($ok, my @moreFiles)=$self->getFilesFromInputCollection($self->{CA}, $catalogue);

  my $done={};
  foreach my $lfn (@inputFiles , @inputData, @moreFiles ){
    $lfn=~ s/^LF://;
    $lfn =~ /,nodownload/ and $self->info("Ignoring $lfn") and next;    
    $self->debug(1, "Adding '$lfn' ");
    my $real= $self->findProcName($lfn, $done);
    if ($real =~ /^(.*\/)[^\/]*$/ ) {
      $self->info("Checking if $self->{WORKDIR}/$1 exists");
      if (! -d "$self->{WORKDIR}/$1") {
        mkdir "$self->{WORKDIR}/$1" or print "Error making the directory $self->{WORKDIR}/$1 ($!)\n";
      }
    }
    push @files, {cat=> $lfn, real=>"$self->{WORKDIR}/$real"};
  }
  return @files
}

sub findProcName {
  my $self     = shift;
  my $origname = shift;
  my $done     = (shift or {});

  $done->{files}
    or $done->{files} = { stdout => 0, resources => 0, stderr => 0 };
  $done->{dir} or $done->{dir} = -1;
  $self->debug(1, "In findProcName finding a procname for $origname");

  $origname =~ /\/([^\/]*)$/ and $origname = $1;
  $self->debug(1, "In findProcName finding a name for $origname");
  my $i = $done->{files}->{$origname};
  my $name;
  if (!defined $i) {
    $done->{files}->{$origname} = 1;
    $name = $origname;
  } else {
    $name = "$i/$origname";
    $done->{files}->{$origname}++;
  }
  return $name;

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
    foreach my $jdlelement (@$jdlstrings){
        my ($filestring, $options)=split(/\@/, $jdlelement,2);
        $options or $options = "";
        my @files = $self->_findFilesLike(split (/,/, $filestring));
#        @files=$self->_findFilesLike(@files);
        $self->info("Found Files: @files, options: $options");
        foreach my $filename (@files) {
             $fileTable->{$filename}={ name=>$filename, options=>$options};
        }
    }
    return $fileTable;
}


sub processJDL_get_Output_Archivename_And_Included_Files_And_Initialize_archiveTable{
    my $self=shift;
    my $jdlstrings=shift;
    my $defaultArchiveName=shift;
    my $archiveTable;
    my $archiveCounter=0;

    foreach  my $jdlelement (@$jdlstrings){
        my ($filestring, $options)=split (/\@/, $jdlelement,2);
        $options or $options = "";
        my ($name, @files)=split(/[\:,]/, $filestring);
        ($name eq "") and $name = "$defaultArchiveName.$archiveCounter.zip";
        $archiveCounter++;
        @files=$self->_findFilesLike(@files);
        $self->info("Found Archive: $name, incl. Files: @files, options: $options");
        (scalar(@files) < 1) and next;  # for false JDLs, with archive definition and missing file definition
        $archiveTable->{$name}={name=>$name, includedFiles=>\@files, options=>$options};
    }
    return $archiveTable;
}


# DROPPED IN v2-18
#
#sub analyseJDL_And_Move_By_Default_Files_To_Archives{
#  my $self=shift;
#  my $archives=shift;
#  my $files=shift;
#  my $defaultArchiveName=shift;
#  my $defaultOptionString=shift;
#
#  my $fileTable;
#  for my $j(0..$#{$files}) {
#           my ($filestring, $optionstring)=split (/\@/, $$files[$j],2);
#           $optionstring or $optionstring = "<NONE>";
#           (my $no_archive, $optionstring)  = $self->processJDL_Check_on_Tag($optionstring, "no_archive");
#           if(!$no_archive){
#              if($fileTable->{$optionstring}) {
#                   $self->info("A file with already known options, will be united in one archive");
#                   push @{$fileTable->{$optionstring}}, $filestring;
#              } else {
#                   my @filetag=($filestring);
#                   $fileTable->{$optionstring}=\@filetag;
#              }
#              delete $$files[$j];
#           }
#  }
#  my $j=0;
#  foreach my $optionstring (keys(%$fileTable)) {
#           my $filestring="";
#           foreach my $filename (@{$fileTable->{$optionstring}}) {
#              $filestring .= $filename.",";
#           } 
#           $filestring =~ s/,$//;
#           $optionstring eq "<NONE>" and $optionstring = $defaultOptionString;
#           push @$archives, $defaultArchiveName.time()."_".$j++.".zip:".$filestring."@".$optionstring;
#           $self->info("Filestring $filestring will be moved from files to archives");
#  }
#  return ($archives, $files);
#}

sub analyseOutputTag_getArchivesAndFiles{
  my $self=shift;
  my $archives=shift;
  my $files=shift;
  my $outputtags=shift;

  foreach my $outputtag (@$outputtags) {
      my ($filestring, $optionstring)=split (/\@/, $outputtag,2);
      ($filestring =~ /:/) and push @$archives, $outputtag and next;
      push @$files, $outputtag;
  }
  return ($archives, $files);
}


sub delete__no_archive__tagforbackwardcompabilitytorlessV218{
  my $self=shift;
  my $files=(shift || return ());
  
  foreach (@$files) {
     my ($filestring, $optionstring)=split (/\@/, $_,2);
     (my $uselessBool, $optionstring)  = $self->processJDL_Check_on_Tag($optionstring, "no_archive");
     $_ = $filestring."@".$optionstring;
  }
  return $files;
}

sub processJDL_Check_on_Tag{
  my $self=shift;
  my $tagstring=(shift || return (0,""));
  my $pattern=(shift || return (0,""));
  my @tags = split (/,/, $tagstring);
  $tagstring = "";
  my $back=0;
  foreach (@tags){
     ($_ =~ /^$pattern$/i) and $back=1 and next;
     $tagstring .= $_.","; 
  }
  $tagstring =~ s/,$//;
  return ($back, $tagstring);
}

sub create_Default_Output_Archive_Entry{
   my $self=shift;
   my $jdlstring=(shift || return "");
   my $defaultOutputFiles=shift;
   my $defaulttags=shift;
   $jdlstring .= ":";
   foreach (@$defaultOutputFiles){
      $jdlstring .= $_.",";
   } 
   $jdlstring =~ s/,$//;
   ($defaulttags ne "") and $jdlstring .= "@".$defaulttags;
   return $jdlstring;
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



sub createZipArchives{
  my $self=shift;
  my $archiveTable=shift;
  my @files=();
  foreach my $name (keys(%$archiveTable)) {
       my $filename = $name;
       my @includedFiles =  @{$archiveTable->{$name}->{includedFiles}};
       if (! @includedFiles) {
         $self->info("There are no files for the archive $filename.");
         $self->putJobLog("error","The files ".@includedFiles." weren't produced/present. (ignoring the zip file $filename)");
         next;
       }
       $archiveTable->{$name}={zip=>Archive::Zip->new() ,
                                entries=>{},
                                name=>"$name",
                                options=>$archiveTable->{$name}->{options},
                                nonarchivedFiles=>{}
                                };
       my $total_size=0;
       foreach my $file (@includedFiles) { 
          my $size=-s $file;
          if (!defined $size) {
            $self->putJobLog("error","The file $file doesn't exist");
            next;
          }
          $archiveTable->{$name}->{zip}->addFile($file);
          $archiveTable->{$name}->{entries}->{$file}={size=> $size,
                                                       md5=>AliEn::MD5->new($file)};
          $total_size+=$size; 
       }
       if ($archiveTable->{$name}->{zip}->numberOfMembers()<1){
         $self->putJobLog("error","The archive '$filename' doesn't have any files inside. Ignoring it...");
         delete $archiveTable->{$name};
         next;
       }
       if ($total_size >= 4*1024*1024*1024) { #4GB zip limit 
         $self->putJobLog("error","The archive '$filename' is over the limit of 4GB ($total_size bytes)");
         return;
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

sub prepare_Error_Files {
    my $self = shift;

    my ($ok, @outputEntriesError ) = $self->{CA}->evaluateAttributeVectorString("OutputErrorE");
    $ok and scalar(@outputEntriesError) or return 1;

    $self->{UI} = AliEn::UI::Catalogue::LCM->new({no_catalog=>1,role=>$self->{JOB_USER}});
    if (!$self->{UI}) {
      $self->info("Error getting an instance of the catalog saving ERROR_E output");
      $self->putJobLog("error","Could not get an instance of the LCM saving ERROR_E output");
      $self->registerLogs();
      $self->putJobLog("trace","Registered the JobLogOnClusterMonitor.");
      return;
    }
    else{
      my ($okoo , @origOutput) = $self->{CA}->evaluateAttributeVectorString("Output");
      $self->{CA}->set_expression("Output", "{\"" . join("\",\"", @outputEntriesError) . "\"}");

      my $uploadFilesState = $self->prepare_File_And_Archives_From_JDL_And_Upload_Files() ;

      $okoo and @origOutput and $self->{CA}->set_expression("Output", "{\"" . join("\",\"", @origOutput) . "\"}");

      ($uploadFilesState eq -1) or ($uploadFilesState eq 0) and $self->putJobLog("trace","Error $uploadFilesState uploading error logs");

      $self->registerLogs();
      $self->{UI}->close();
    }

    return 1;
}



sub prepare_File_And_Archives_From_JDL_And_Upload_Files{
  my $self=shift;
  my $archiveTable;
  my $fileTable = {};
  my $archives;
  my $files;
  my $ArchiveFailedFiles;

  my $defaultArchNoSpec = "alien_defarchNOSPEC.$ENV{ALIEN_PROC_ID}";
  my $defaultArchiveName= "alien_defarch.$ENV{ALIEN_PROC_ID}";
  my @defaultOutputArchiveFiles = ("stdout","stderr","resources");
  my $defaultOptionString = ""; # could be SE,!SE,qos=N,select=N,guid etc. , equal to a valid continuation of file@


  my ( $ok, @fileEntries ) = $self->{CA}->evaluateAttributeVectorString("OutputFile");
  ( $ok, my @archiveEntries ) = $self->{CA}->evaluateAttributeVectorString("OutputArchive");
  ($ok, my @outputEntries ) = $self->{CA}->evaluateAttributeVectorString("Output");
  $archives = \@archiveEntries;
  $files = \@fileEntries;
  ($archives, $files) = $self->analyseOutputTag_getArchivesAndFiles($archives, $files,\@outputEntries);

  ## create a default archive if nothing is specified
  ##
  ((scalar(@$archives) < 1) and (scalar(@$files) < 1)) 
     and  $self->putJobLog("trace", "The JDL didn't contain any output specification. Creating default Archive.")
     and  push @$archives, $self->create_Default_Output_Archive_Entry($defaultArchNoSpec,
            \@defaultOutputArchiveFiles, $defaultOptionString);
  #} else {
  #    ($archives, $files) = $self->analyseJDL_And_Move_By_Default_Files_To_Archives(\@archiveEntries, \@fileEntries,
  #            $defaultArchiveName.time().".zip", $defaultOptionString);
  #}
  ($files) = $self->delete__no_archive__tagforbackwardcompabilitytorlessV218($files);

  $archiveTable = $self->processJDL_get_Output_Archivename_And_Included_Files_And_Initialize_archiveTable($archives,$defaultArchiveName);

  ($archiveTable, $ArchiveFailedFiles) = $self->createZipArchives($archiveTable) or
       print "Error creating the Archives\n" and return 0;

  push @$files, @$ArchiveFailedFiles;

  (scalar(@$files) > 0) and
                 $fileTable = $self->processJDL_split_Output_Filenames_From_Options_And_Initialize_fileTable($files);

  my @overallFileTable;
  #%$overallFileTable= (%$archiveTable, %$fileTable);
  @overallFileTable= ($fileTable, $archiveTable);
  
  
  if ( (scalar(@$archives)> 0 ) or (scalar(@$files)> 0 ) ) {
    (scalar(@$archives)> 0 ) and $self->putJobLog("trace", "We marked the following archives to be uploaded: @$archives");
    (scalar(@$files)> 0 ) and $self->putJobLog("trace", "We marked the following files to be uploaded: @$files");

    #(scalar(keys(%$overallFileTable)) > 0) and
      return $self->putFiles(@overallFileTable);
  } 
  return 0;
}


sub putFiles {
  my $self=shift;
  #my $fs_table=shift;
  my @filesAndArchives=@_;
  my $filesAdded=1;
  system ("ls -al $self->{WORKDIR}");
  my $oldOrg=$self->{CONFIG}->{ORG_NAME};
  my $jdl;
  my %guids=$self->getUserDefinedGUIDS();
  my $incompleteAddes=0;
  my $fileRegError=0;
  my $successCounter=0;
  my $failedSEs;
  my $JDLOutputCount=0;

  foreach my $data (split (/\s+/, $self->{VOs})){
    my ($org, $cm,$id, $token)=split ("#", $data);
    $self->info("Connecting to services for $org ($data)");
    $ENV{ALIEN_PROC_ID}=$id;
    $ENV{ALIEN_JOB_TOKEN}=$token;
    $ENV{ALIEN_ORGANISATION}=$org;
    $ENV{ALIEN_CM_AS_LDAP_PROXY}=$cm;
    $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $org});
    my @addedFiles=();
    my $remoteDir = "$self->{CONFIG}->{LOG_DIR}/proc$id";

    #so that we can know if we are registering a new file or a replica
    my @registerInJDL=();
    
    $self->{PROCDIR} = $self->{OUTPUTDIR} || "~/alien-job-$ENV{ALIEN_PROC_ID}";
    my $user=$self->{CA}->evaluateAttributeString("User");

    ($self->{STATUS} =~ /^ERROR_V/)
        and  $self->{PROCDIR} = "$self->{CONFIG}->{USER_DIR}/".substr($user, 0, 1)."/$user/recycle/alien-job-$ENV{ALIEN_PROC_ID}"; 

    $self->{UI}->execute("mkdir","-p",$self->{PROCDIR});
    
    foreach my $fs_table (@filesAndArchives) {
      $JDLOutputCount += scalar(keys(%$fs_table)); 
    foreach my $fileOrArch (keys(%$fs_table)) {
      
      my $size=-s $self->{WORKDIR}."/".$fs_table->{$fileOrArch}->{name};
      ($size gt 0) 
         or   $self->putJobLog("trace", "WARNING: You specified to add -- $fs_table->{$fileOrArch}->{name} --, yet the FILE HAS SIZE ZERO after job execution, therefore we will ")
         and  $self->putJobLog("trace", "WARNING: not add the file. This warning is the only one and simply for your information, the job will proceed without further intervention.")
         and $successCounter++
         and next;

      $fs_table->{$fileOrArch}->{options} or $fs_table->{$fileOrArch}->{options}="";
      $self->info("Processing  file  ".$fs_table->{$fileOrArch}->{name});
      $self->info("File has options  ".$fs_table->{$fileOrArch}->{options});
      
      (my $no_links, $fs_table->{$fileOrArch}->{options})  = $self->processJDL_Check_on_Tag($fs_table->{$fileOrArch}->{options}, "no_links_registration");
      
      my $guid=0;
      if (exists($guids{$fs_table->{$fileOrArch}->{name}})){
        $guid="$guids{$fs_table->{$fileOrArch}->{name}}";
        $self->putJobLog("trace", "The file $fs_table->{$fileOrArch}->{name} has the guid $guids{$fs_table->{$fileOrArch}->{name}}");
      }
      
#      if($self->{STATUS} =~ /^ERROR_V/) {
#        # just upload the files ...
#        my @addEnvs = $self->addFile("$self->{WORKDIR}/$fs_table->{$fileOrArch}->{name}","$recyclebin/$fs_table->{$fileOrArch}->{name}", "$fs_table->{$fileOrArch}->{options}",$guid,1);
#        my $success = shift @addEnvs;
#        $success or $self->putJobLog("error","The job went to ERROR_V, but we can't upload the output files for later registration") and next;
#        my $env1 = AliEn::Util::deserializeSignedEnvelope(shift @addEnvs);
#        my @pfns = ("$env1->{se}/$env1->{turl}");
#        foreach my $env (@addEnvs) {
#           push @pfns, AliEn::Util::getValFromEnvelope($env,"turl");
#        }
#        my @list = ();
#        foreach my $file( keys %{$fs_table->{$fileOrArch}->{entries}}) {  # if it is a file, there are just no entries
#            push @list, join("###", $file, $fs_table->{$fileOrArch}->{entries}->{$file}->{size},
#            $fs_table->{$fileOrArch}->{entries}->{$file}->{md5});
#        }
#         my $links="";
#        (scalar(@list) gt 0) and $links.=";;".join(";;",@list);
#        
#        push @registerInJDL, "\"".join ("###", $env1->{lfn}, $env1->{guid}, $env1->{size},
#                               $env1->{md5},  join("###",@pfns),
#                               $links) ."\"";
#  
#        $success and  $successCounter++;
#        ($success eq -1) and $incompleteAddes=1;
#  
#        next;  
#      }

      my $links="";
      if(!$no_links) {
      my @list = ();
        foreach my $file( keys %{$fs_table->{$fileOrArch}->{entries}}) {  # if it is a file, there are just no entries
            ($fs_table->{$fileOrArch}->{entries}->{$file}->{size} gt 0)
              or   $self->putJobLog("trace", "WARNING: You specified to add -- $file --, yet the FILE HAS SIZE ZERO after job execution, therefore we will ")
              and  $self->putJobLog("trace", "WARNING: not add the file. This warning is the only one and simply for your information, the job will proceed without further intervention.")
              and next;
            push @list, join("###", "$self->{PROCDIR}/$file", $fs_table->{$fileOrArch}->{entries}->{$file}->{size},
            $fs_table->{$fileOrArch}->{entries}->{$file}->{md5},($guids{$file} || 0));
        }
      (scalar(@list) gt 0) and $links= join(";;",@list);
      }

      my @addEnvs = $self->addFile("$self->{WORKDIR}/$fs_table->{$fileOrArch}->{name}","$self->{PROCDIR}/$fs_table->{$fileOrArch}->{name}", "$fs_table->{$fileOrArch}->{options}",$guid,1,$links);
     
      my $success = shift @addEnvs;
      $success  or next;
      $success and $successCounter++;
      ($success eq -1) and $incompleteAddes=1;
      my @pfns=();
      foreach my $env (@addEnvs) {
        my $proxy =  AliEn::Util::getValFromEnvelope($env,"proxy");
        my $turl =  AliEn::Util::getValFromEnvelope($env,"turl");
        $proxy and $turl =~ s/$proxy//;
        push @pfns, $turl;
        $guids{$fs_table->{$fileOrArch}->{name}} = AliEn::Util::getValFromEnvelope($env,"guid");
      }
      push @addedFiles, join("\",\"",@pfns);

      #$no_links and next;
      #my $signedEnvs = shift @addEnvs;
      #foreach my $file( keys %{$fs_table->{$fileOrArch}->{entries}}) {  # if it is a file, there are just no entries
      #   $self->registerFile($file, $fs_table->{$fileOrArch}->{name}, $signedEnvs, $fs_table->{$fileOrArch}->{entries}->{$file}->{size},$fs_table->{$fileOrArch}->{entries}->{$file}->{md5})
      #    and (push @lfnTracker, "$self->{PROCDIR}/$file") or $fileRegError=1;
      #    
      #}
   
    }
    }
    

    ($self->{STATUS} =~ /^ERROR_V/) and $self->{UI}->execute("rmdir","$self->{PROCDIR}");
    my $regPFNS = join("\",\"",@addedFiles);
    $self->{CA}->set_expression("SuccessfullyBookedPFNS", "{\"".$regPFNS."\"}");
    $self->{JDL_CHANGED}=1;
    $self->registerLogs(0);
  }

  $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});

#  if (scalar(keys(%$fs_table)) ne $successCounter) {
  if ($JDLOutputCount ne $successCounter) {
     $self->putJobLog("error","THERE WAS AT LEAST ONE FILE, THAT WE COULDN'T STORE ON ANY SE.");
     return 0;
  }
  #if ($fileRegError) {
  #   $self->putJobLog("error","THERE WAS AT LEAST ONE FILE LINK REGISTRATION THAT WAS NOT SUCCESSFULL.");
  #   return 0;
  #}


  if($incompleteAddes) {
     #$self->putJobLog("trace", "WARNING: We had  ".scalar(keys(%$fs_table))
     #        ." files and archives to store.");
     $self->putJobLog("trace", "WARNING: We could store all files at least one time, but not all files were stored as many times as specified.");
     return -1;
  }

  $self->putJobLog("trace","OK. All files and archives for this job where added as specified. Superb!");
  return 1;
}



sub addFile {
  my $self=shift;
  my $pfn=shift;
  my $lfn=shift;
  my $storeTags=shift;
  my $guid=shift;
  my $uploadOnly=(shift || 0); 
  my $links=(shift || 0); 
  my @addResult;

  (! -f "$pfn") and $self->putJobLog("error", "The job didn't create $pfn") and return 0; 
  my $size=-s $pfn;
  $size or $self->putJobLog("error", "The file $pfn has size 0.") and return 0;
  my $md5 = AliEn::MD5->new($pfn);

  my $options = " -feedback ";
  $guid and $options .= " -guid=$guid";

  if($links){ $links = " -links=".$links } else { $links = ""; } 

  $self->putJobLog("trace","adding file: size $size md5 $md5, links: $links, options: $options, lfn: $lfn, local file: $pfn, storage tags: $storeTags");

  #my $mydebug = $self->{LOGGER}->getDebugLevel();
  #$self->{LOGGER}->debugOn(1);
  $self->{LOGGER}->keepAllMessages();


  if($uploadOnly) {
    @addResult=$self->{UI}->execute("add", "-upload -size=$size -md5=$md5 $links", $options, "$lfn", "$pfn", $storeTags);
  #} else {
  #  @addResult=$self->{UI}->execute("add", " -size $size -md5 $md5 ", $options, "$lfn", "$pfn", $storeTags);
  }

  my $success = shift @addResult;
  defined($success) or $success =0;

  ($success ne 1) 
     and $self->highVerboseTransactionLog(@{$self->{LOGGER}->getMessages()});

  #$self->{LOGGER}->debugOn($mydebug);
  $self->{LOGGER}->displayMessages();

  if($success eq 1) {
     $self->putJobLog("trace","Successfully stored the file $lfn.");
  }elsif($success eq -1) {
     $self->putJobLog("trace","Could store the file $lfn only on ".scalar(@addResult));
  } else {
     $self->putJobLog("error","Could not store the file $lfn on any SE. This file is lost!");
     return (0);
  }
  return ($success, @addResult);
}


#sub registerFile {
#  my $self=shift;
#  my $file=shift;
#  my $archive=shift;
#  my $signedEnvelope=shift;
#  my $size=shift;
#  my $md5=shift;
#
#  my $env = AliEn::Util::deserializeSignedEnvelope($signedEnvelope);
#
#  $size or $size = $env->{size};
#  $md5 or $md5 = $env->{md5};
#
#  #my $mydebug = $self->{LOGGER}->getDebugLevel();
#  #$self->{LOGGER}->debugOn(5);
#  $self->{LOGGER}->keepAllMessages();
#
#  my $addResult=0;
#  my $maxTry= 3;
#  $self->putJobLog("trace", "Trying to register file with: add -r -size $size -md5 $md5  $file guid:///$env->{guid}?ZIP=$file");
#  for (my $tries = 0; $tries < $maxTry; $tries++) { 
#    ($addResult)=$self->{UI}->execute("add", "-r", "-feedback", "-size $size", "-md5 $md5", "$self->{PROCDIR}/$file", "guid:///$env->{guid}?ZIP=$file");
#    $addResult and last;
#  }
#
#  ($addResult ne 1) 
#    and $self->highVerboseTransactionLog(@{$self->{LOGGER}->getMessages()});
#  
#  #$self->{LOGGER}->debugOn($mydebug);
#  $self->{LOGGER}->displayMessages();
#
#  if($addResult eq 1) { 
#     $self->putJobLog("trace","Successfully registered the file link $file in archive $archive.");
#     return 1;
#  } 
#  $self->putJobLog("error","Error while registering file link $file in archive $archive");
#  return 0;
#}


sub highVerboseTransactionLog {
  my $self=shift;
  my $logit = join("####",@_);
  $logit =~ s/\n//g;
  $self->putJobLog("trace", "--------- Error in file upload: HIGH VERBOSITY IO TRANSACTION LOG ---------:");
  $self->putJobLog("trace", "$logit");
  $self->putJobLog("trace", "--------- END OF HIGH VERBOSITY IO TRANSACTION LOG ---------.");
}

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

#  if ($catalog){
##! $options->{no_register}){
#    my $dir= AliEn::Util::getProcDir($self->{JOB_USER}, undef, $ENV{ALIEN_PROC_ID}) . "/job-log";
#    my $host="$ENV{ALIEN_CM_AS_LDAP_PROXY}";
#    $catalog->execute("mkdir", $dir);
#
#    $catalog->execute( "register",  "$dir/$lfn",
#		       "soap://$ENV{ALIEN_CM_AS_LDAP_PROXY}$done?URI=ClusterMonitor",$size, "no_se", "-md5", $md5) 
#      or print STDERR "ERROR Adding the entry $done to the catalog!!\n"
#	and return;
#  }

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
  if ($self->{MEMORY}){
    $self->info("Checking the memory requirements");
    my $memory=AliEn::Util::find_memory_consumption($self->{MONITOR},$self->{PROCESSID});
    if($memory){
      $self->info("Process Memory measured at = $memory");
      $memory > $self->{MEMORY}
        and $killMessage="using more than $self->{MEMORY} memory (right now, $memory)";
    } else {
      $self->info("Failed to read process memory from monitor");
    }
  }

  if ($killMessage){
    AliEn::Util::kill_really_all($self->{PROCESSID});
    $self->info("Killing the job ($killMessage)");
    $self->putJobLog("error","Killing the job ($killMessage)");
    $self->prepare_Error_Files();
    my $jdl = ($self->{JDL_CHANGED} ? $self->{CA}->asJDL() : undef);
    $self->changeStatus("%", "ERROR_E", $jdl);
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
	$self->putJobLog("trace","The validation script didn't finish");
	$self->{STATUS} = "ERROR_VT";
      } else {
	( -e "$self->{WORKDIR}/.validated" ) or  $self->{STATUS} = "ERROR_V";
        $self->putJobLog("trace","The validation created some trace");
        if ( open(my $f, "<", "$self->{WORKDIR}/.alienValidation.trace") ){
         my $traceContent=join("", <$f>);
         close $f;
         $self->putJobLog("trace",$traceContent);
        }
      }
    } else {
      $self->putJobLog("error","The validation script '$validation' didn't exist");
      $self->{STATUS} = "ERROR_VN";
    }
    # following out, since STATUS is not always true as SAVED, see above.
    #$self->putJobLog("trace","After the validation preliminary Job status: $self->{STATUS}");
  }

  # store the files
  #$self->putFiles() or $self->{STATUS}="ERROR_SV";  old entry, redirected trough new funtion:
  $self->{UI} = AliEn::UI::Catalogue::LCM->new({no_catalog=>1,role=>$self->{JOB_USER}});
  if (!$self->{UI}) {
      $self->info("Error getting an instance of the catalog");
      $self->putJobLog("error","Could not get an instance of the LCM");
      $self->registerLogs();
      $self->putJobLog("trace","Registered the JobLogOnClusterMonitor.");
  } else {
    #this hash will contain all the files that have already been submitted,
    my $uploadFilesState = $self->prepare_File_And_Archives_From_JDL_And_Upload_Files() ;

    if ($self->{STATUS}=~ /SAVED/){
      ($uploadFilesState eq -1) and $self->{STATUS}="SAVED_WARN";
      ($uploadFilesState eq 0) and $self->{STATUS}="ERROR_SV";
    }

    $self->registerLogs();
    $self->{UI}->close();
  }

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
    $self->sendJAStatus('DONE', {totaljobs=>$self->{TOTALJOBS}, error=>1});
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
  $silent and $method="debug" and push @loggingData, 1;

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
      $self->sendJAStatus('DONE',  {totaljobs=>$self->{TOTALJOBS}});
      $self->info("There are no jobs to execute. We have executed $self->{TOTALJOBS}");
      #Tell the CM that we are done"
      $self->{MONITOR} and 
        $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_".$self->{SERVICENAME}, "$self->{HOST}:$self->{PORT}", 
                                           { 'numjobs' => $self->{TOTALJOBS} });
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
      $self->{MONITOR}->sendParameters($self->{CONFIG}->{CE_FULLNAME}.'_Jobs', $ENV{ALIEN_PROC_ID}, {'status' => AliEn::Util::statusForML($self->{STATUS}), 'host' => $self->{HOST}, 'job_user' => $self->{JOB_USER}, 'masterjob_id' => $ENV{ALIEN_MASTERJOBID}, 'host_pid' => $self->{PROCESSID} });
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
      last;
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
  
  # Check for tracelog messages (we are supposed to be in $self->{WORKDIR})
  $self->checkTraceLog();  

  $self->checkProcess($self->{PROCESSID}) and return;

  $self->info("Process $self->{PROCESSID} has finished");
  waitpid(-1, &WNOHANG);

  $self->lastExecution();
  
  # unset jdl environment variables
  my ($ok, @env_variables)= 
    $self->{CA}->evaluateAttributeVectorString("JDLVARIABLES");
  $self->info("We have to undefine @env_variables");
  foreach my $var (@env_variables) {
    $var=uc("ALIEN_JDL_$var");
    delete $ENV{$var};  
  }
    
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
    my $registerLogString="";
    if($data) {
      $registerLogString = "\"".join ("###", "execution.out", 0, ($data->{size} || 0), ($data->{md5}|| 0),  $data->{pfn}) ."\"";
    } else {
      $self->putJobLog("error", "Error submitting the execution.out log file. The file will not be there!");
    }

#    ($self->{STATUS} =~ /^ERROR_V/)  and
#       $registerLogString = join(",", $self->{JDL_REGISTERFILES}, $registerLogString);
    
    $self->{CA}->set_expression("JobLogOnClusterMonitor", "{".$registerLogString."}");
    $self->info("We set the JobLogOnClusterMonitor in the JDL");
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
  my $msg="The jobagent is in $status";
  $params->{job_id} = $ENV{ALIEN_PROC_ID} || 0;
  foreach my $key (keys %$params){
     $params->{$key} and $msg .=" $key=$params->{$key}";
    
   }
  
  $self->putAgentLog($msg);

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

sub checkTraceLog {
  my $self = shift;
  
  my @files = glob(".traceLog.*");
  foreach my $file (@files){
  	# .traceLog.1234567890 $1 is the timestamp
  	$file =~ /^\.traceLog\.(\d+)$/ or next;
  	open (FILEH, "<$file") or $self->info("Couldn't open $file") and next;
    while (<FILEH>) { 
      chomp $_;	
      $self->putJobLog("trace", $_, $1);
    }
    close FILEH;  	
  }
  system("rm -f .traceLog.*");
  
  return 1;
}

return 1;

