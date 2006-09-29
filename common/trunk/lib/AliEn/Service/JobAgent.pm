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
use POSIX ":sys_wait_h";
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

  return 1;
}

sub requestJob {
  my $self=shift;

  $self->{REGISTER_LOGS_DONE}=0;
  $self->{FORKCHECKPROCESS} = 0;

  $self->GetJDL() or return;

  my $redirect="$self->{CONFIG}->{TMP_DIR}/proc/$ENV{ALIEN_PROC_ID}.out";
  $self->info("Let's redirect the output to $redirect");
  $self->{LOGGER}->redirect($redirect);
 
  $self->checkJobJDL() or $self->sendJAStatus('ERROR_JDL') and return;

  print "Contacting VO: $self->{VOs}\n";

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

  $self->info("Putting in the joblog: @_");
  my $joblog = $self->{SOAP}->CallSOAP("CLUSTERMONITOR","putJobLog", @_) or return;
  return 1;
}

sub getHostClassad{
  my $self=shift;
  my $ca=new AliEn::Classad::Host or return;
  if ($self->{TTL}){
    $self->info("We have some time to live...");
    my ($ok, $requirements)=$ca->evaluateExpression("Requirements");
    $ok or $self->info("Error getting the requirements of this classad ". $ca->asJDL()) and return;
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
    $requirements .= " && (other.TTL<$timeleft) ";
    $ca->set_expression("TTL", $timeleft);
    $self->{TTL}=$timeleft;
    $ca->set_expression( "Requirements", $requirements ) or return;

  }
  $self->info("We are using". $ca->asJDL);

  return $ca->asJDL();
}

sub GetJDL {
  my $self = shift;

  $self->info("The job agent asks for a job to do:");

  my $jdl;
  my $i=$ENV{ALIEN_JOBAGENT_RETRY} || 5;

  my $result;
  while(1) {
    print "Getting the jdl from the clusterMonitor, agentId is $ENV{ALIEN_JOBAGENT_ID}...\n";
    my $hostca=$self->getHostClassad() or $self->sendJAStatus('ERROR_HC') and return;
    $self->sendJAStatus(undef, {TTL=>$self->{TTL}});

    my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR","getJobAgent", $ENV{ALIEN_JOBAGENT_ID}, "$self->{HOST}:$self->{PORT}", $self->{CONFIG}->{ROLE}, $hostca);
    if ($done) {
      $self->info("Got something from the ClusterMonitor");
      $result=$done->result;
      if ($result) {
	if ($result eq "-3") {
	  $self->sendJAStatus('INSTALLING_PKGS');
	  $self->{SOAP}->CallSOAP("Manager/Job", "setSiteQueueStatus",$self->{CONFIG}->{CE_FULLNAME},"jobagent-install-pack");
	  my @packages=$done->paramsout();
	  $self->info("We have to install some packages");
	  foreach (@packages) {
	    my ($ok, $source)=$self->installPackage($_);
	    $ok or $self->info("Error insalling the package $_") and $self->sendJAStatus('ERROR_IP') and return;
	  }
	  $i++; #this iteration doesn't count
	}else {
	  $self->{SOAP}->CallSOAP("Manager/Job", "setSiteQueueStatus",$self->{CONFIG}->{CE_FULLNAME},"jobagent-matched");
	  last;
	}
       }
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
  $self->putJobLog($ENV{ALIEN_PROC_ID},"trace",$message);


  print "ok\nTrying with $jdl\n";

  $self->{CA} = Classad::Classad->new("$jdl");
  ( $self->{CA}->isOK() ) and return 1;

  $jdl =~ s/&amp;/&/g;
  $jdl =~ s/&amp;/&/g;
  print "Trying again... ($jdl)\n";
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
  $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","The job needs $jobttl seconds");


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

  print "PACKAGES REQUIRED: $ENV{ALIEN_PACKAGES}\n";


  ($ok, my @env_variables)=
    $self->{CA}->evaluateAttributeVectorString("JDLVARIABLES");
  $self->info("We have to define @env_variables");
  foreach my $var (@env_variables) {
    ($ok, my @values)=
      $self->{CA}->evaluateAttributeVectorString($var);
    if (!$ok) {
      $self->putJobLog($ENV{ALIEN_PROC_ID}, "warning", "The JobAgent was supposed to set '$var', but that's not defined in the jdl");
      next;
    }
    $var=uc("ALIEN_JDL_$var");
    my $value=join("##", @values);
    $self->putJobLog($ENV{ALIEN_PROC_ID},"trace", "Defining the environment variable $var=$value");
    $ENV{$var}=$value;
    
  }
  return 1;

}

sub CreateDirs {
  my $self=shift;
  my $done=1;

  $self->{WORKDIR} = $ENV{HOME};
  # If specified, this directory is used. REMARK If $ENV{WORKDIR} is set, this is used!!
  $self->{CONFIG}->{WORK_DIR} and $self->{WORKDIR} = $self->{CONFIG}->{WORK_DIR};
    # If the batch-system defined this
  ( defined $ENV{WORKDIR} ) and $self->{WORKDIR} = $ENV{WORKDIR};

  
  ( defined $ENV{ALIEN_WORKDIR} ) and $self->{WORKDIR} = $ENV{ALIEN_WORKDIR};
  ( defined $ENV{TMPBATCH} ) and $self->{WORKDIR} = $ENV{TMPBATCH};

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

  $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","Creating the working directory $self->{WORKDIR}");

  if ( !( -d $self->{WORKDIR} ) ) {
    $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Could not create the working directory $self->{WORKDIR} on $self->{HOST}");
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
  $handle->df();
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
	$self->putJobLog($ENV{ALIEN_PROC_ID},"error","Request $workspace[0] * $unit MB, but only $freemegabytes MB free!");
	$self->registerLogs(0);
	$self->changeStatus("%", "ERROR_IB");
	$done=0;
      } else {
	# enough space
	$self->putJobLog($ENV{ALIEN_PROC_ID},"trace","Request $workspace[0] * $unit MB, found $freemegabytes MB free!");
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
  print "Asking the status of process $self->{QUEUEID}\n";

  return $self->{QUEUEID};

}

my $my_getFile = sub {
    my $file = shift;
    my $options=(shift || {});
    if ($options->{grep} || $options->{tail} || $options->{head}) {
      my $open="$file";
      $options->{grep} and 
	print "Returning only the entries that match $options->{grep}\n";
      $options->{head} and $open="head -$options->{head} $file|" and
	print "Returning the first $options->{head} lines of $file\n";
      $options->{tail} and $open="tail -$options->{tail} $file|" and
	print "Returning the last $options->{tail} lines of $file\n";
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
  print "Fork done $error\n";
  ( defined $error ) or print STDERR "Error forking the process\n" and return;

  waitpid( $error, &WNOHANG );

  #The parent returns
  $self->{PROCESSID} = $error;
  $self->{PROCESSID} and return 1;

  $self->debug(1, "The father locks the port");


  if (! $self->executeCommand() ) {
    $self->registerLogs(0);
    $self->changeStatus("%", "ERROR_E");
  }

  $self->info("Command executed, with status $self->{STATUS}");
  my $status=$self->{STATUS};
  ($status eq "SAVING") and $status="DONE";
  $self->sendEmail($status);
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

sub executeCommand {
  my $this = shift;
  
  
  $self->changeStatus("%",  "STARTED", 0,$self->{HOST}, $self->{PROCESSPORT} );
  
  $ENV{ALIEN_PROC_ID} = $self->{QUEUEID};

  $self->debug(1, "Getting input files and command");
  if ( !( $self->getFiles() ) ) {
    print STDERR "Error getting the files\n";
    $self->registerLogs(0);

    $self->changeStatus("%",  "ERROR_IB");
    return ;
  }

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
  if ($ok) {
    my @packInst;
    foreach (@packages) {
      my ($ok, $source)=$self->installPackage($_);
       if (!$ok){
	 $self->registerLogs(0);
	 $self->changeStatus("%",  "ERROR_E");
	 return;
       }
      if ($source){
	push @packInst, $source;
      }
    }
    @list=(@packInst, @list);
  }
  my $s=join (" ", @list);

  $self->{STATUS}="RUNNING";
  ($self->{INTERACTIVE}) and  $self->{STATUS}="IDLE";

  $self->changeStatus("STARTED",$self->{STATUS},0,$self->{HOST},$self->{PORT});

  $self->info("Ready to do the system call $s");
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

  my $error = system($s);
  $ENV{LD_LIBRARY_PATH} =~ s{^/lib:/usr/lib:}{};

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
#  my $user=$self->{INSTALL_USER};
  print "Installing Package $_\n";

  my ($version, $user);

  $package =~ s/::(.*)$// and $version=$1;
  $package =~ s/^(.*)\@// and $user=$1;

  #The first time, we get the user from the catalogue
  if (! $user) {
    if ($self->{INSTALL_USER}){
      $user=$self->{INSTALL_USER};
    }else{
      my $catalog;
      eval{ 
	$catalog = AliEn::UI::Catalogue::LCM->new({"silent"=> "0" , "gapi_catalog"=>"$self->{CONFIG}->{AGENT_API_PROXY}"});
	($user)=$catalog->execute("whoami", "-silent");
	$catalog->close();
      };
      if ($@) {print "ERROR GETTING THE CATALOGUE $@\n";}
      $user and $self->{INSTALL_USER}=$user;
    }
    $self->{SOAP} or $self->{SOAP}=new AliEn::SOAP;
  }
  $self->info("Getting the package $package (version $version) as $user");

  $ENV{ALIEN_PROC_ID} and
    $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","Installing package $_");
  my $result;
  my $retry=5;
  while (1) {
    $self->info("Asking the package manager to install $package as $user");

    $result=$self->{SOAP}->CallSOAP("PackMan", "installPackage", $user, $package, $version) and last;
    my $message=$AliEn::Logger::ERROR_MSG;
    $self->info("The reason it wasn't installed was $message");
    $message =~ /Package is being installed/ or $retry--;
    $retry or last;
    $self->info("Let's sleep for some time and try again");
    sleep (30);
  }
  if (! $result){
    $self->info("The package has not been instaled!!");
    $ENV{ALIEN_PROC_ID} and
      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Package $_ not installed ");
    return;
  }

  my ($ok, $source)=$self->{SOAP}->GetOutput($result);

  ($source) and   $self->info("For the package we have to do $source");
  return ($ok, $source);
}

sub dumpInputDataList {
  my $self=shift;
  my $xml=0;
  my ($ok, $dumplist)=$self->{CA}->evaluateAttributeString("InputDataList");
  ($dumplist)  or return 1;
  ($ok, my $format)=$self->{CA}->evaluateAttributeString("InputDataListFormat");
  if ($format){
    if ($format =~ /^xml-single/i){
      $xml="single";
    } elsif ($format =~ /^xml-group/i) {
      $xml="group";
    } else {
      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","The inputdatalistType was $format, but I don't understand it :(. Ignoring it");
    }
  }
  $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","Putting the list of files in the file '$dumplist'");
  $self->info("Putting the inputfiles in the file '$dumplist'");
  if (!open (FILE, ">$dumplist") ){
    $self->info("Error putting the list of files in $dumplist");
    $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Error putting the list of files in the file $dumplist");
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
      print FILE "      <file name=\"$basefilename\" lfn=\"/$file\" 
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
    $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","Getting InputZip $file");
    if (!$catalog->execute("unzip", $file)){
    $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Error getting the inputzip $file");
      return
    }
  }
  return 1;
}

sub getFiles {
  my $self    = shift;
  #print "In getFiles\n";
  $self->info("Getting the files");
  my $oldmode=$self->{LOGGER}->getMode();
  $self->info("Got mode $oldmode");
  $self->dumpInputDataList();


  my $catalog;

  eval{ 
    my $options={silent=>0};
    $self->{CONFIG}->{AGENT_API_PROXY} and 
      $options->{gapi_catalog}=$self->{CONFIG}->{AGENT_API_PROXY};
    $catalog = AliEn::UI::Catalogue::LCM::->new($options);
  };
  if ($@) {print "ERROR GETTING THE CATALOGUE $@\n";}
  if (!$catalog) {
    $self->putJobLog($ENV{ALIEN_PROC_ID},"error","The job couldn't authenticate to the catalogue");

    print STDERR "Error getting the catalog!\n";
    return;
  }
  $self->info("Got the catalogue");

  if (!$self->getInputZip($catalog)){
    $catalog->close();
    return;
  }
  my @files=$self->getListInputFiles($catalog);

  foreach my $file (@files) {
    $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","Downloading input file: $file->{cat}");
    print "Getting $file->{cat}\n";
    my $done;
    my $options="-silent";
    for (my $i=0;$i<2;$i++) {
      $catalog->execute("get", "-l",$file->{cat},$file->{real}, $options ) 
	and $done=1 and last;
      $options="";
      $self->{LOGGER}->error("JobAgent","Getting the $file->{cat} for the job $self->{QUEUEID} from the catalog!! (trying again)" );
    }
    if (!$done) {
      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Could not download the input file: $file->{cat}");
      print STDERR
	"ERROR: Getting the $file->{cat} for the job $self->{QUEUEID} from the catalog!!\n";
      $catalog->close();
      return;
    }
  }

  my $procDir = AliEn::Util::getProcDir($self->{JOB_USER}, undef, $self->{QUEUEID});

  if (!( $catalog->execute("mkdir","$procDir/job-output","-ps"))) {
    print STDERR "ERROR Creating the job-output directory!\n";
    $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Could not create the output directory in the catalogue: $procDir/job-output");
    $catalog->close();
    return;
  }

  $self->info("Let's check if there are any files to stage");
  
  my ($ok, @stage)=$self->{CA}->evaluateAttributeVectorString("InputData");
  if ($ok) {
    my @lfns=();
    foreach my $lfn (@stage) {
      $lfn =~ /,nodownload/ or next;
      print "The lfn $lfn has to be staged!!\n";
      push @lfns, $lfn;
    }
    if (@lfns){
      $catalog->execute("stage", @lfns);
    }
  }

  $catalog->close();

  chmod 0755, "$self->{WORKDIR}/command";
  $self->{LOGGER}->setMinimum(split(" ",$oldmode));

  return 1;

}

sub getListInputFiles {
  my $self=shift;
  my $catalog=shift;
  
  my $dir = AliEn::Util::getProcDir($self->{JOB_USER}, undef, $self->{QUEUEID}) . "/";

  my @files=({cat=>$self->{COMMAND}, real=>"$self->{WORKDIR}/command"});
  if ($self->{VALIDATIONSCRIPT}) {
    my $validation=$self->{VALIDATIONSCRIPT};
    $validation=~ s{^.*/([^/]*)$}{$self->{WORKDIR}/$1};
    push @files, {cat=>$self->{VALIDATIONSCRIPT}, 
		  real=>$validation};
  }else {
    $self->info("THERE IS NO VALIDATION");
  }
  my ($ok,  $createLinks)=$self->{CA}->evaluateAttributeString("CreateLinks");
  if ($createLinks) {
    foreach my $file ($catalog->execute("find",$dir, "*")) {
      my $format="${dir}job-log/";
      $file=~ /^$format/ and next;
      $self->debug(1, "Adding '$file' (dir '$dir')");
      my $work=$file;
      $work =~ s{^$dir}{$self->{WORKDIR}/};
      push @files, {cat=> $file, real=>$work};
      if ($work =~ /^($self->{WORKDIR}\/.*\/)[^\/]*$/ ) {
	$self->info("Checking if $1 exists");
	if (! -d $1) {
	  mkdir $1 or print "Error making the directory $1 ($!)\n";
	}
      }
    }
  } else {
    my ( $ok, @inputFiles)=$self->{CA}->evaluateAttributeVectorString("InputDownload");
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
  }
  return @files
}

sub getUserDefinedGUIDS{
  my $self=shift;

  my ($ok, $guidFile)=$self->{CA}->evaluateAttributeString("GUIDFile");
  my %guids;
  if ($guidFile){
    $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","Using the guids from $guidFile");
    if (!open (FILE, "<$guidFile")){
      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","The job was supposed to create '$guidFile' with the guids, but it didn't... I will generate the guids");
    }else{
      %guids=split (/\s+/, <FILE>);
      use Data::Dumper;
      $self->info("Using the guids". Dumper(%guids));
      close FILE;
    }

  }
  return %guids;
}


sub putFiles {
  my $self      = shift;
  my $filesUploaded=1;
  print "Putting the stdout and stderr in the catalogue\n";
  system ("ls -al $self->{WORKDIR}");
  my $oldOrg=$self->{CONFIG}->{ORG_NAME};
  my ( $ok, @files ) =
    $self->{CA}->evaluateAttributeVectorString("OutputFile");

  ( $ok, my @archives ) =
    $self->{CA}->evaluateAttributeVectorString("OutputArchive");
  
  my ($zipArchives, $files)=$self->prepareZipArchives(\@archives,  @files) or 
    print "Error creating the zipArchives\n" and return;
  @files=@$files;
  my @zipArchive=@$zipArchives;
  my $jdl;

  my %guids=$self->getUserDefinedGUIDS();

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
      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Could not get an instance of the LCM");
      return;
    }

    $self->putJobLog($id,"trace","Saving the files in the SE");

    #this hash will contain all the files that have already been submitted,
    #so that we can now if we are registering a new file or a replica
    my $submitted={};
    my $localdir= $self->{WORKDIR};
    foreach my $fileName (@files){
      my ($file2, $options)=split (/\@/, $fileName,2);
      my @se=();
      if ($options) {
	$self->info("The file has the options: $options");
	my @options=split (/,/, $options);
	foreach (@options){
	  $self->info("Checking $_");
	  $_ =~ /^noarchive$/i and  next;
	  if ($_ =~ /^(replica)|(custodial)$/i){
	    $self->info("Finding a SE with QoS '$_'");
	    my $se=$ui->findCloseSE($_);
	    if (!$se){
	      $self->info("We didn't manage to find any SE that can hold this data");
	      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Could not find an SE of type '$_'");
	      return;
	    }
	    push @se, uc($se);
	  }else {
	    $self->info("Putting the output in the SE $_");
	    push @se,uc($_);
	  }
	}
      }
      $self->info("Submitting file $file2");
      if (! -f "$self->{WORKDIR}/$file2")  {
	$self->info("The job was supposed to create $file2, but it doesn't exist!!",1);
	$self->putJobLog($id, "error", "The job didn't create $file2");
	next;
      }
      my @options=("$self->{WORKDIR}/$file2", $se[0]);
      $guids{$file2} and $self->putJobLog($id,"trace", "The file $file2 has the guid $guids{$file2} ") and push @options, $guids{$file2};
      my ($info)=$ui->execute("upload", @options);
      if (!$info) {
	$self->info("Error registering the file $self->{WORKDIR}/$fileName");
	$self->putJobLog($id,"error","Error registering the file $self->{WORKDIR}/$fileName");
	next;
      }
      if ($submitted->{$file2}){
	$submitted->{$file2}->{selist}.=",$info->{selist}";
      }else{
	$submitted->{$file2}=$info;
      }
				 
    }
    foreach my $arch(@zipArchive) {
      my $message="Submitting the archive $arch->{name} with $arch->{se}";
      $arch->{options} and $message.=" and $arch->{options}";
      $self->info($message);
      my @ses;
      $arch->{options} and push @ses, split (/,/, $arch->{options});
      push @ses, $arch->{se};
      my $info;
      my $guid="";
      foreach my $se (@ses) {
	$self->info("Putting the file $arch->{name} in $se (guid $guid)");
	$self->putJobLog($id,"trace","Registering $arch->{name} in $se");
	my ($info2, $silent)=(undef, "-silent");
	for (my $j=0;$j<5;$j++){
	  ($info2)=$ui->execute("upload", "$self->{WORKDIR}/$arch->{name}",
				$se, $guid, $silent);
	  $info2 and last; 
	  $self->info("Error uploading the file... sleep and retry");
	  $self->putJobLog($id, "trace", "warning: file upload failed... sleeping  and retrying");
	  sleep(10);
	  $silent="";
	}
	if (!$info2) {
	  $self->info("Couldn't upload the file $arch->{name} to $se\n");
	  $self->putJobLog($id,"error","Error registering $arch->{name}");
	  next;
	}
	if ($info) {
	  $info->{selist}.=",$info2->{selist}";
	}else{
	  $info=$info2;
	  $guid=$info->{guid};
	}
      }
      if (!$info ) {
	$filesUploaded=0;
	next;
      }
      if ($submitted->{$arch->{name}}){
	$submitted->{$arch->{name}}->{selist}.=",$info->{selist}";
      }else{
	$submitted->{$arch->{name}}=$info;
      }
      my @list;
      foreach my $file( keys %{$arch->{entries}}) {
	my $guid=$guids{$file} || "";
	$self->info("Checking if $file has a guid ($guid)");
	push @list, join("###", $file, $arch->{entries}->{$file}->{size},
			 $arch->{entries}->{$file}->{md5},$guid );
      }
      $submitted->{$arch->{name}}->{links}=\@list;
    }

    my @list=();
    foreach my $key (keys %$submitted){
      my $links="";
      my $entry=$submitted->{$key};
      if ($entry->{links} ) {
	$links.=";;".join(";;",@{$entry->{links}});
      }
      push @list, "\"".join ("###", $key, $entry->{guid}, $entry->{size}, 
			     $entry->{md5}, $entry->{selist}, $links) ."\"";
    }
    if (@list) {
      
      $self->{CA}->set_expression("RegisteredOutput", "{".join(",",@list)."}");
      $self->{JDL_CHANGED}=1;
    }
    $self->info("Closing the catalogue");
    $ui->close();
  }
  $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});
  $self->info("Files uploaded");
  return $filesUploaded;
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
sub prepareZipArchives{
  my $self=shift;
  my $outArchivesRef=shift;
  my @outputArchives=@$outArchivesRef;
#  my $currentzip;

  #first, let's get rid of the patterns
  my @noPattern=$self->_findFilesLike(@_);

  my $archiveList;
  my @files=();

  if (! @outputArchives){
    ($archiveList, @files)=$self->createDefaultZipArchive(@noPattern, "stdout", "stderr", "resources");
  } else {
    ($archiveList, @files)=$self->createUserZipArchives(\@outputArchives, @noPattern);
  }


  my @archives=();
  foreach my $name (keys %$archiveList) {

    if (grep(/.root$/ , $archiveList->{$name}->{zip}->memberNames())) {
      print "There is a root file. Do not compress the files\n";
      foreach my $member ($archiveList->{$name}->{zip}->members()){
	$member->desiredCompressionLevel(0);
      }
    }
    my $myName=( $archiveList->{$name}->{filename} ||".alien_archive.$ENV{ALIEN_PROC_ID}.$name.".time().".zip");
    $archiveList->{$name}->{zip}->writeToFileNamed($myName);
    $archiveList->{$name}->{name}=$myName;
    push @archives, $archiveList->{$name};
  }

  $self->info("Checking in $#archives+1 archives and $#files+1 files");
  return (\@archives, \@files);
}

sub createUserZipArchives{
  my $self=shift;
  my $ref=shift;
  my @outputArchives=@$ref;
  my $archiveList;
  $self->info("The user specified how he wanted the archives\n");
  foreach my $archive (@outputArchives){
    $self->info("Putting the files into $archive");
    my ($name, $options)=split(/\@/, $archive);
    $options or $options="";
    ($name, my @files)=split(/[\:,]/, $name);
    $self->info("The archive has to be called: '$name', with files: @files, and with options: $options");
    my @noPattern=$self->_findFilesLike(@files);
    if (! @noPattern) {
      $self->info("There are no files for the archive $name!!");
      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","The files @files weren't produced!! (ignoring the tar file $name)");
      next;
    }

    $archiveList->{$name} or
      $archiveList->{$name}={zip=>Archive::Zip->new() ,
			     entries=>{},
			     filename=>"$name",
			     se=>uc($self->{CONFIG}->{SE_FULLNAME})};
    if ($options) {
      my ($first, @rest)=split (/,/ , $options);
      $archiveList->{$name}->{se}=$first;
      $archiveList->{$name}->{options}=join(",", @rest);
    }
    foreach my $file (@noPattern) {
      $self->info("Checking $file");
      my $size=-s $file;
      if (!defined $size) {
	$self->putJobLog($ENV{ALIEN_PROC_ID},"error","The file $file doesn't exist");
	next;
      }
      $archiveList->{$name}->{zip}->addFile($file);
      $archiveList->{$name}->{entries}->{$file}={size=> $size,
						 md5=>AliEn::MD5->new($file),
						}
    };
  }
  $self->info("Done splitting into the archives");
  return ($archiveList, @_);
}

sub createDefaultZipArchive{
  my $self=shift;
  my @files=();
  my $archiveList;
  my $MAXSIZE=2*1024*1204;
  $self->info("Using the default archiving");
  foreach my $file (@_) {
    $self->info("Checking $file");
    my ($file2, $options)=split (/\@/, $file,2);
    my $opt;
    my @se=();
    if ($options) {
      $self->info("The file has the options: $options");
      my @options=split (/,/, $options);
      foreach (@options){
	$self->info("Checking $_");
	$_ =~ /^noarchive$/i and $opt->{noarchive}=1 and next;
	$self->info("Putting the output in the SE $_");
	push @se,uc($_);
      }
    }
    #if the SE were not defined, do it in the current one
    @se or push @se,  uc($self->{CONFIG}->{SE_FULLNAME});
    my $size=-s $file2;
    if (!defined $size) {
      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","The file $file2 doesn't exist");
      next;
    }
    if (($size>$MAXSIZE) || $opt->{noarchive}) {
      push @files, $file;
      next;
    }
    foreach my $archive (@se){

      $archiveList->{$archive} or
	$archiveList->{$archive}={zip=>Archive::Zip->new()};
      $archiveList->{$archive}->{zip}->addFile($file2);
      my $md5=AliEn::MD5->new($file2);
      $archiveList->{$archive}->{entries} or 
	$archiveList->{$archive}->{entries}={};
      $archiveList->{$archive}->{entries}->{$file2}={size=> $size,
						     md5=>$md5};

      $archiveList->{$archive}->{se}=$archive;
    }
  }
  return ($archiveList,@files);
}


sub copyInMSS {
  my $self=shift;
  my $catalog=shift;
  my $lfn=shift;
  my $localfile=shift;

  my $size=(-s $localfile);
  $self->info("Trying to save directly in the MSS");
  my $name = $self->{CONFIG}->{SE_MSS};
  $name
    or $self->{LOGGER}->warning( "SE", "Error: no mass storage system" )
      and return;
  if ($name eq "file"  ) {
    if ($self->{HOST} ne  $self->{CONFIG}->{SE_HOST}) {
      $self->info("Using the file method, and we are not in the right machine ($self->{CONFIG}->{SE_HOST})... let's exist just in case");
      return;
    }
  }
  $name = "AliEn::MSS::$name";
  eval "require $name"
    or $self->{LOGGER}->warning( "SE", "Error: $name does not exist $! and $@" )
      and return;
  my $mss = $name->new($self);

  $mss or return;
  $self->info("Got the mss");
  my ($target,$guid)=$mss->newFileName();
  $target or return;
  $target="$self->{CONFIG}->{SE_SAVEDIR}/$target";
  $self->info("Writing to $target");
  my $pfn;
  eval {
    $pfn=$mss->save($localfile,$target);
  };
  if ($@) {
    $self->info("Error copying the file: $@");
    return;
  }
  $pfn or $self->info("The save didn't work :(") and return;
  $self->info("The save worked ($pfn)");
  return $catalog->execute("register", $lfn, $pfn, $size);
}
sub submitLocalFile {
  my $self            = shift;
  my $catalog         = shift;
  my $localdirectory  = shift;
  my $remotedirectory = shift;
  my $filename        = shift;
  my $sename          = shift || "";

  my $id=$ENV{ALIEN_PROC_ID};
  my $lfn= AliEn::Util::getProcDir($self->{JOB_USER}, undef, $id) . "/job-output/$filename";

  my $localfile="$localdirectory/$filename";
  #Let's see if the file exists...
  if (! -f $localfile) {
    $self->info("The job was supposed to create $filename, but the file $filename doesn't exist!!",1);
    return ;
  }

  #First, let's try to put the file directly in the SE
  if (!$catalog->execute("add", $lfn, "file://$self->{HOST}$localdirectory/$filename", $sename)) {  
    #Now, let's try registering the file.
    if (! $self->copyInMSS($catalog,$lfn,$localfile)) {
      $self->info("The registration didn't work...");
      $self->submitFileToClusterMonitor($localdirectory, $filename, $lfn, $catalog) or return;
    }
  }
  #Ok The file is registered. Checking if it has to be registered in more than
  # one place
  if ($self->{OUTPUTDIR} and ($self->{STATUS} eq "DONE") ) {
    #copying the output to the right place
    $catalog->execute("cp", $lfn, $self->{OUTPUTDIR}) or
      $self->putJobLog($id,"error","Error copying $lfn to $self->{OUTPUTDIR}");
  }
  return $lfn;
}

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
	
	print "Inserting $done in catalog...";

    #    print STDERR "$localdirectory/$filename has size $size\n";
  # To make sure we are in the right database.
  
  my $return=$lfn;
  if (! $options->{no_register}){
    my $dir= AliEn::Util::getProcDir($self->{JOB_USER}, undef, $ENV{ALIEN_PROC_ID}) . "/job-log";
    my $host="$ENV{ALIEN_CM_AS_LDAP_PROXY}";
    $catalog->execute("mkdir", $dir);
    ( $catalog->execute( "register",  "$dir/$lfn",
			 "soap://$ENV{ALIEN_CM_AS_LDAP_PROXY}$done?URI=ClusterMonitor",$size) )
      or print STDERR "ERROR Adding the entry $done to the catalog!!\n"
	and return;
  } else {
    print "Let's put the log file in the JDL\n";
    my $ui=AliEn::UI::Catalogue::LCM->new({no_catalog=>1});
    ($return)=$ui->execute("upload", "-u", "soap://$ENV{ALIEN_CM_AS_LDAP_PROXY}$done?URI=ClusterMonitor")
#    $return=
  }
  print "done!!\n";
  return $return;
}

sub catch_zap {
    my $signame = shift;
    print STDERR
      "Somebody sent me a $signame signal.Registering the output\n";
    $self->putFiles();
    $self->registerLogs(0);
    $self->changeStatus("%", "INTERRUPTED");
    $self->{STATUS}="INTERRUPTED";
    $self->sendEmail($self->{STATUS});

    finishMonitor();
    exit;
}

sub sendEmail {
    my $self   = shift;
    my $status = shift;

    my ( $ok, $user ) = $self->{CA}->evaluateAttributeString("Email");

    ($user) or print "Not sending any email\n" and return;

    print "Sending an email to $user (job $status)...\n";

    ( $ok, my @files ) =
      $self->{CA}->evaluateAttributeVectorString("OutputFile");
    @files = ( "stdout", "stderr", @files );
    my $output = join ", ", @files;
    my $ua = new LWP::UserAgent;

    my $procDir = AliEn::Util::getProcDir($self->{JOB_USER}, undef, $self->{QUEUEID});
    map { $_ = "\tget $procDir/job-output/$_" } @files;
    my $type = join "\n", @files;

    map { $_ =~ s/get/alien --exec get/ } @files;
    my $shell = join "\n", @files;

    $ua->agent( "AgentName/0.1 " . $ua->agent );
    
    my $message="The job produced the following files: $output\n
You can get the output from the AliEn prompt typing:
$type

You can also get the files from the shell prompt typing:
 
$shell";
    $status=~ /^ERROR_/ and $message="The job did not run properly. This is probably due to a site being misconfigured\n\nYou can see the execution log in the AliEn prompt in the directory $procDir/job-log/execution.out\n";

    # Create a request
    my $req = HTTP::Request->new( POST => "mailto:$user" );
    $req->header(
        Subject => "AliEn-Job $self->{QUEUEID} finished with status $status" );
    my $URL=($self->{CONFIG}->{PORTAL_URL} || "http://alien.cern.ch/Alien/main?task=job&");
    $req->content("AliEn-Job $self->{QUEUEID} finished with status $status\n
You can see the ouput produced by the job in ${URL}jobID=$self->{QUEUEID}

$message

Please, make sure to copy any file that you want, since those are temporary files, and will be deleted at some point.

If you have any problem, please contact us
"
    );

    # Pass request to the user agent and get a response back

    my $res = $ua->request($req);

    print "ok\n";

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
  if($self->{MONITOR} && $ENV{ALIEN_PROC_ID}){
    my $res = $self->{MONITOR}->getJobMonInfo($ENV{ALIEN_PROC_ID}, "cpu_ksi2k");
    $self->{CPU_KSI2K} = $res->{cpu_ksi2k} if $res->{cpu_ksi2k};
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

  (time() > $self->{JOBEXPECTEDEND}) and 
    $killMessage="it was running for longer than its TTL";

  if ($self->{WORKSPACE} ){
    my $space=du($self->{WORKDIR} ) /1024 /1024;
    $self->info( "Checking the disk space usage of $self->{WORKDIR} (now $space, out of $self->{WORKSPACE} MB ");
    $space <$self->{WORKSPACE} or 
      $killMessage="using more than $self->{WORKSPACE} MB of diskspace (right now we were using $space MB)";
  }

  if ($killMessage){
    kill(9, $self->{PROCESSID});
    $self->info("Killing the job ($killMessage)");
    $self->putJobLog($ENV{ALIEN_PROC_ID},"error","Killing the job ($killMessage)");
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
  $cpuKsi2k = $self->{CPU_KSI2K} || "?";
	
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
  #	    delete $ENV{ALIEN_JOB_TOKEN};

  $self->{STATUS}="SAVED";

  if ( $self->{VALIDATIONSCRIPT} ) {
    $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","Validating the output");
    my $validation=$self->{VALIDATIONSCRIPT};
    $validation=~ s{^.*/([^/]*)$}{$self->{WORKDIR}/$1};

    if ( -r $validation ) {	
      chmod 0755, $validation;
      my $validatepid = fork();
      if (! $validatepid ) {
	# execute the validation script
	print "Executing the validation script: $validation\n";
	unlink "$self->{WORKDIR}/.validated";
	if (! system($validation) ){
	  print "The validation finished successfully!!\n";
	  system("touch $self->{WORKDIR}/.validated" ) ;
	}
	print "Validation finished!!\n";
	exit 0;
      }
      my $waitstart = time;
      my $waitstop  = time;
      while ( ($waitstop-300) < ($waitstart) ) {
	sleep 5;
	print "Checking $validatepid\n";
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
	$self->putJobLog($ENV{ALIEN_PROC_ID},"trace","The validation script din't finish");
	$self->{STATUS} = "ERROR_VT";
      } else {
	( -e "$self->{WORKDIR}/.validated" ) or  $self->{STATUS} = "ERROR_V";
      }
    } else {
      $self->putJobLog($ENV{ALIEN_PROC_ID},"error","The validation script '$validation' didn't exist");
      $self->{STATUS} = "ERROR_VN";
    }
    $self->putJobLog($ENV{ALIEN_PROC_ID},"trace","After the validation $self->{STATUS}");
  }

  # store the files
  $self->putFiles() or $self->{STATUS}="ERROR_SV";
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
  $self->{JOBLOADED}=0;
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
  my $counter=0;
  if (! $self->{JOBLOADED}) {
    $self->sendJAStatus('REQUESTING_JOB');
    $self->info("Asking for a new job");
    if (! $self->requestJob()) {
      $self->sendJAStatus('DONE');
      $self->info("There are no jobs to execute");
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
      $self->{MONITOR}->sendParameters($self->{CONFIG}->{CE_FULLNAME}.'_Jobs', $ENV{ALIEN_PROC_ID}, {'status' => AliEn::Util::statusForML($self->{STATUS}), 'host' => $self->{HOST} });
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

  $self->info("Let's try to put the log files (registering them $skip_register)");
  my $func=sub {
    my $catalog=shift;
  #    my $dir= AliEn::Util::getProcDir($self->{JOB_USER}, undef, $ENV{ALIEN_PROC_ID}) . "/job-log";
#    my $localfile="$self->{CONFIG}->{LOG_DIR}/$ENV{ALIEN_LOG}";
    my $host="$ENV{ALIEN_CM_AS_LDAP_PROXY}";
#    $catalog->execute("mkdir", $dir);
#    $self->info("Putting the log files in $dir");
    print "I'm looking into $self->{CONFIG}->{TMP_DIR}/proc/\n";
    my $data=$self->submitFileToClusterMonitor("$self->{CONFIG}->{TMP_DIR}/proc/", 
				      "$ENV{ALIEN_PROC_ID}.out", 
				      "execution.out", $catalog, {no_register=>$skip_register});
    if ($skip_register) {
      $self->info("And now, let's update the jdl");
      $self->{CA}->set_expression("RegisteredLog", "\"execution.out###$data->{guid}###$data->{size}###$data->{md5}###$data->{selist}###\"");
      $self->{JDL_CHANGED}=1;
    }
  };
  $self->doInAllVO({no_catalog=>$skip_register}, $func);
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
    my $start={debug=>0,silent=>0};
    $options->{no_catalog} and $start->{no_catalog}=1;
    $start->{gapi_catalog} = $self->{CONFIG}->{AGENT_API_PROXY};

    my $catalog= AliEn::UI::Catalogue::LCM->new($start);
    ($catalog) or print STDERR "Error getting the catalog!\n" and next;
#    $catalog->execute("whoami");
    $func->($catalog);
    $catalog->close;
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
  $params->{job_id} = $ENV{ALIEN_PROC_ID} || 0;
  $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_".$self->{SERVICENAME}, "$self->{HOST}:$self->{PORT}", $params);
  return 1;
}

return 1;

