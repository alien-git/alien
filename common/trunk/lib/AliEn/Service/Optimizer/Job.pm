package AliEn::Service::Optimizer::Job;

use strict;

use vars qw (@ISA);

use AliEn::Database::TaskQueue;
use AliEn::Database::IS;
use AliEn::Service::Optimizer;
use AliEn::Catalogue;
use AliEn::UI::Catalogue::LCM::Computer;

use AliEn::Util;

@ISA=qw(AliEn::Service::Optimizer);

use Data::Dumper;

my $self;

sub initialize {
  $self=shift;
  my $options =(shift or {});

  $options->{ROLE}=$options->{role}="admin";

  $self->{SERVICE}="Job";

  $self->{DB_MODULE}="AliEn::Database::TaskQueue";

  $self->debug(1, "In initialize initializing service JobOptimizer" );

  $self->debug(1, "In initialize creating AliEn::UI::Catalogue::LCM::Computer instance" );


  $self->{SOAP}->checkService("Manager/Job", "JOB_MANAGER", "-retry")
    or $self->{LOGGER}->error( "JobOptimizer", "In initialize error checking Manager/Job service" )
      and return;

  $self->{CATALOGUE} = AliEn::UI::Catalogue::LCM::Computer->new($options);

  ( $self->{CATALOGUE} )
    or $self->{LOGGER}->error( "JobOptimizer", "In initialize error creating AliEn::UI::Catalogue::LCM::Computer instance" )
      and return;

  $self->{SOAP}->{Authen}= SOAP::Lite->uri('AliEn/Service/Authen')
    ->proxy("http://$self->{CONFIG}->{AUTH_HOST}:$self->{CONFIG}->{AUTH_PORT}");
  $self->{PARENT}=$$;

  $self->SUPER::initialize(@_) or return;

  $self->{DB}->setArchive();

#  $self->{JOBLOG} = new AliEn::JOBLOG();

  $self->StartChildren() or return;


  $self->{FORKCHECKPROCESS}=1;
  return $self;
}
sub checkWakesUp {
  my $self   = shift;
  my $silent =(shift || 0);
  my $method ="info";

  $silent and $method="debug";
  my @debugLevel=();
  $silent and push @debugLevel,1;
  $self->$method( @debugLevel,  "Still alive and checking messages" );

  my $messages=$self->{DB}->retrieveJobMessages();

  if ($messages and $#$messages>-1){
    $self->info("Sending $#$messages to the job manager");
    $self->{SOAP}->CallSOAP("Manager/Job", "SetProcInfoBunch", $self->{HOST}, $messages) or 
      $self->info("ERROR!!! we couldn't send the messages to the job manager");

  }
  return; 
}
sub StartChildren{

  my $self=shift;

  $self->info( "Let's start all the optimizers");
  my $i=0;

  my @optimizers=("Merging", "Inserting", "Splitting", "Zombies", "Hosts", "Expired", "HeartBeat", "Priority", "Resubmit", "Killed", "Saved");

  my $mlEnabled = ($self->{CONFIG}->{MONALISA_HOST} || $self->{CONFIG}->{MONALISA_APMONCONFIG});
  $mlEnabled and push @optimizers, "MonALISA";

  my $dir="$ENV{ALIEN_HOME}/var/log/AliEn/$self->{CONFIG}->{ORG_NAME}/JobOptimizer";

  mkdir $dir,  0755;
  foreach (@optimizers){
    my $name="AliEn::Service::Optimizer::Job::$_";

    $self->info( "Starting the $name");
    eval "require $name";
    if ($@) {
      $self->info( "Error requiring the optimizer $name $! $@");
      return;
    }
    my $d="CHILDPID$i";
    $self->{$d}=fork();
    $i++;
    defined $self->{$d} 
      or $self->info( "Error forking a process") and return;
    #the father goes on...
    $self->{$d} and next;

    $self->info( "Putting the output in $dir/$_.log");
    $self->{LOGGER}->redirect("$dir/$_.log");
    #The children should just initialize and start checking;
    #	@ISA=($name,@ISA);
    $self->info( "The optimizer $name starts");
    bless($self, $name);
    #let's give time to the father to prepair everything
    sleep(10);
    
    my $tmpServName = $self->{SERVICENAME};
    my $shortName = $_;
    $self->{SERVICENAME} = "Job_".$shortName."Optimizer";
    $self->setupApMon();
    if(($shortName eq "MonALISA") && (! $self->{MONITOR})){
      $self->{LOGGER}->error("MonALISA", "Error: Can not initialize ApMon");
      exit(-2);
    }
    $self->{SERVICENAME} = $tmpServName;
    
    $self->startChecking();
    $self->{LOGGER}->error("Job", "Error: the job optimizer $shortName died!!\n");
    exit(-2);

  }


  return $self;
}

sub copyInput {
  my $self=shift;
  my $procid=shift;
  my $job_ca=shift;
  my $user=shift;

  my ($ok, $split)=$job_ca->evaluateAttributeString("Split");
  $split and
    $self->info("The job is going to be split... don't need to copy the input")
      and return 1;

  ($ok, my @inputFile) = $job_ca->evaluateAttributeVectorString("InputBox");
  ($ok, my $createLinks)=$job_ca->evaluateAttributeString("CreateLinks");

  my $procDir = AliEn::Util::getProcDir($user, undef, $procid);

  my @filesToDownload=();
  my $file;
  my $done={};
  foreach $file (@inputFile) {
    my ( $pfn, $pfnSize, $pfnName, $pfnSE ) = split "###", $file;
    $pfnName and $file=$pfnName;
    $self->info("In copyInput adding file $file (from the InputBox $pfn)");
#    my $procname=$self->findProcName($procid, $file, $done, $user);
    my $procname=$self->findProcName($procDir, $file, $done,$createLinks);
    if ( defined $pfnSize ) {
      $self->info("Adding $procname with $pfn and $pfnSize");

      if (! $self->{CATALOGUE}->execute( "register", $procname, $pfn, $pfnSize, $pfnSE ) ) {
	print "The registration failed ($AliEn::Logger::ERROR_MSG) let's try again...\n";
	$self->{CATALOGUE}->execute( "register", $procname, $pfn, $pfnSize ) or 
	  print STDERR "ERROR Adding the entry $pfn to the catalog as $procname!!\n"
	  and return;
      }
      push @filesToDownload, "\"${procname}->$procname\"";
    }
    else {
      if ( $file=~ /,nodownload/) {
	  $self->info("Skipping file $file (from the InputBox) - nodownload option" );
	  next;
      }
      $self->info("Adding file $file (from the InputBox)" );
      if ($file=~ /\*/) {
	$self->info("Adding all the files with that pattern" );
	my ($name, $dir);
	if ($file=~ /^([^\*]*)\*(.*)$/) { $dir=$1; $name=$2};
	$name or $name="*";
	my @list=$self->{CATALOGUE}->execute( "find", "-silent", "$dir", "$name" );
	if (!@list) {
	  $self->info("Error: there are no files that match $file");
	  $self->putJobLog($procid,"error", "There are no files with the pattern '$file'");
	   return;
	}
	my $counter="";
	foreach my $pattern (@list) {
#	  my $procname2= $self->findProcName($procid, $pattern, $done, $user);
	  my $procname2= $self->findProcName($procDir, $pattern, $done, $createLinks);
	  $self->info("Copying $pattern as $procname2");
	  if ($createLinks) {
	    ( $self->{CATALOGUE}->execute( "cp",  $pattern, $procname2, "-silent") )
	      or print STDERR "JobOptimizer: in copyInput error copying the entry $pattern to $procname!!\n"
		and return;
	  } else {
	    push  @filesToDownload, "\"${procname}->$file\"";
	  }
	}
      } else {
	if ($createLinks) {
	  if (!$self->{CATALOGUE}->execute( "cp", $file, $procname, "-silent" )){
	    print "Copying failed!!! Let's try again\n";
	    $self->{CATALOGUE}->execute( "cp", $file, $procname)
	      or print STDERR "JobOptimizer: in copyInput error copying the entry $file to $procname to the catalog!!\n"
		and return;
	  }
	}else {
	  push @filesToDownload, "\"${procname}->$file\"";
	}
      }
    }
  }
  if ( ! $createLinks and @filesToDownload) {
    $self->info("Putting in the jdl the list of files that have to be downloaded");
    $job_ca->set_expression("InputDownload", "{". join(",", @filesToDownload)."}");
  }
  
  # change to the correct owner

  $self->{CATALOGUE}->execute("chown","$user","$procDir/", "-f");
  $self->{CATALOGUE}->execute("chmod","700","$procDir/");
  1;
}
# This subroutine finds the name in the proc directory where the file should
# be inserted
#

sub findProcName{
  my $self=shift;
  my $procDir = shift;
  my $origname=shift;
  my $done=(shift or {});
  my $createLinks=shift ||0;

  $done->{files} or $done->{files}={stdout=>0, resources=>0, stderr=>0};
  $done->{dir} or $done->{dir}=-1;
  $self->debug(1, "In findProcName finding a procname for $origname");

  $origname =~ /\/([^\/]*)$/ and $origname=$1;
  $self->debug(1, "In findProcName finding a name for $origname");
  my $i=$done->{files}->{$origname};
  my $name;
  if (!defined $i) {
    $done->{files}->{$origname}=1;
    $name="$procDir/$origname";
  } else {
    $name= "/$procDir/$i/$origname";
    $done->{files}->{$origname}++;
  }
  if ($createLinks && defined $i && ($done->{dir}<$i) ){
    $self->{CATALOGUE}->execute( "mkdir", "-p", "$procDir/$i","-silent" );
    $done->{dir}=$i;
  }
  return $name;

}

sub updateWaiting {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  $self->checkMirrorData($job_ca, $queueid) or return;
  $self->checkChangedReq($job_ca, $queueid) or return;

  return 1;
}

sub checkJobs {
  my $self=shift;
  my $silent=shift;
  my $status=shift;
  my $function=shift;
  
  my $method="info";
  $silent and $method="debug";
  
  $self->{LOGGER}->$method("Job", "Checking $status jobs ");
  my $continue=1;
  #We never want to get more tahn 15 jobs at the same time, just in case the jdls are too long
  while ($continue) {
    my $jobs=$self->{DB}->getJobsByStatus($status,"queueid limit 15");

    defined $jobs
      or $self->{LOGGER}->warning( "JobOptimizer", "In checkJobs error during execution of database query" )
	and return;
    
    @$jobs
      or $self->{LOGGER}->$method( "JobOptimizer", "There are no jobs $status" )
	and return;	#check if it's ok to return undef here!!

    $continue=0;
    $#{$jobs} eq 14 and $continue=1;
    $self->info("THERE ARE $#{$jobs} jobs, let's continue? $continue");


    foreach my $data (@$jobs){
      $self->{LOGGER}->$method("JobOptimizer", "Checking job $data->{queueid}");
      my $job_ca = Classad::Classad->new($data->{jdl});
      if ( !$job_ca->isOK() ) {
	print STDERR "JobOptimizer: in checkJobs incorrect JDL input\n" . $data->{jdl} . "\n";
	$self->{DB}->updateStatus($data->{queueid},"%","ERROR_I");
	
	next;
      }
      
      ############################################################################
      # Job Predecessor functionality
      ############################################################################
      my ( $ok, $jobPredecessors ) = $job_ca->evaluateExpression("JobPredecessor");
      $ok and $self->info("Found Job Predecessor $jobPredecessors");
      # here we have to replace the organisation name !!!
      $jobPredecessors=~ s/\"//g;
      $jobPredecessors=~ s/\{//g;
      $jobPredecessors=~ s/\}//g;
      $jobPredecessors=~ s/\s//g;
      my @predecessors = split ',', $jobPredecessors;
      my $checkpredecessor=0;
      foreach (@predecessors) {
	# check if the predecessor has status done
	my $state = $self->{DB}->getFieldsFromQueueEx("status","where queueId='$_'");
	defined $state
	  or $self->{LOGGER}->warning( "JobOptimizer", "In checkJobs error during execution of database query" ) and $self->{DB}->updateStatus($data->{queueid},"INSERTING","ERROR_C")
	    and next;
	$self->info("Status of predecessor $_ is @$state[0]->{status}");
	if (@$state[0]->{status} eq 'DONE') {
	  $checkpredecessor=1;
	} else {
	  $checkpredecessor=-1;
      }
      }
      if ($checkpredecessor<0) {
	$self->info("In checkJobs - the predecessor @predecessors of job $data->{queueid} are not yet finished");
	next;
      }
      
      $self->info("In checkJobs - calling $function");
      $self->$function($data->{queueid}, $job_ca, $status);
    }

  }
  return 1;
}

sub createInputBox {
    my $self=shift;
    my $job_ca=shift;
    my $inputdata=shift;
    $self->debug(1, "Creating the input box");
    my $inputbox={};
    #First, get the inputData

    my ($ok, @files)=$job_ca->evaluateAttributeVectorString("InputFile");

    push (@files, @{$inputdata});

    foreach my $file (@files) {
	$file=~ s/\"//g;
	$file=~ s/^LF://;
	$self->debug(1, "Adding $file");
	my $name = $file;
	$name =~ s/^.*\///;
	my $tempName=$name;
	my $i=1;
	while ($inputbox->{$name}){
	    $name="$tempName.$i";
	    $i++;
	}
	$inputbox->{$name} = $file;
    }

    $self->debug(1, "In createInputBox InputBox\n". Dumper $inputbox);
    return $inputbox;
}


sub checkMirrorData {
    my $self=shift;
    my $job_ca=shift;
    my $id=shift;

    my ( $ok, @input ) =  $job_ca->evaluateAttributeVectorString("InputData");
    $ok or return 1;
    $self->debug(1, "In checkMirrorData job $id has input data");
    map { $_=~ s/^LF://} @input;
    foreach my $input (@input) {
	my $se="Alice::CNAF::Castor";
	my @se=$self->{CATALOGUE}->execute("whereis", $input, "-silent");
#	grep ( /$se/, @se) 
#	    or print "mirror -fb $input $se\n";
    }
    return 1;
}
sub checkChangedReq{
    my $self=shift;
    my $job_ca=shift;
    my $id=shift;

    my ( $ok, $req ) = $job_ca->evaluateExpression("Requirements");
    ($ok and $req) or
	$self->info("Error getting the requirements of the job")
	    and return;
    $self->debug(1, "In checkChangedReq got $req");
    ( $ok, my $origreq ) = $job_ca->evaluateExpression("OrigRequirements");
    $origreq 
	or $self->info("No original req...") and return 1;
    
    $self->debug(1, "In checkChangedReq original requirements $origreq");
    $job_ca->set_expression("Requirements", $origreq);
    
    $self->{CATALOGUE}->{QUEUE}->checkRequirements($job_ca);
    ( $ok, my $newreq ) = $job_ca->evaluateExpression("Requirements");
    ($ok and  $req) or 
	$self->info("Error getting the requirements of the job")
	    and return;
    $newreq =~ s/&& 1//;
    $job_ca->set_expression("Requirements", $newreq);
    $self->debug(1, "In checkChangedReq got $newreq");
    if ($newreq ne $req) {
	$self->updateJobReq($job_ca, $id) or return;
    }
    return 1;
}

sub updateJobReq {
  my $self=shift;
  my $job_ca=shift;
  my $id=shift;
  $self->info("The job requirements of $id have changed!!");
  $self->{DB}->updateStatus($id, "WAITING", "UPDATING")
    or $self->info("Error updating status for job $id" )
      and return;

  $self->copyInputFiles($job_ca, $id) or return;
  
  #	$job_ca->set_expression("OrigRequirements", "member(other.GridPartition, \"Production\")")
#	    or $self->info( "Error putting the origreq") and return;
  my $new_jdl= $job_ca->asJDL();
  $self->info("Putting as jdl: $new_jdl");
    
  my $update=$self->{DB}->setJdl($id,$new_jdl);
  $update or $self->info("Error doing the update") and return;
  $self->{DB}->updateStatus($id, "UPDATING", "WAITING")
    or $self->info("Error updating status for job $id" )
      and return;


  $self->info("Job updated!!");
  return 1;
}
sub DESTROY {
  my $self=shift;
  my $now = `date`;
  $self or return;
  print "In Destroy. I'm $$ and the father was $self->{PARENT}\n";
  ($self->{PARENT} eq $$)  or return;
  print "$now: KILLING ALL THE CHILDREN\n";
  $self or return 1;

  foreach (grep (/PID/, keys %$self)){
    print "$now: KILLING $_ and $self->{$_}\n";
    kill 9, $self->{$_};
  }
  
}

sub copyInputFiles {
  my $self=shift;
  my $job_ca=shift;
  my $jobId=shift;

  my $name;
  my $inputBox={};
  my ( $ok, @inputData ) =
    $job_ca->evaluateAttributeVectorString("InputData");
  @inputData or
    $self->info("There is no inputData")
      and return 1;

  foreach my $lfn ( @inputData ) {
    $lfn =~ s/^LF://;
    $self->debug(1, "In copyInputFiles updating $lfn");
    my $name = "";
    $lfn =~ /([^\/]*)$/ and $name = $1;

    $inputBox->{$name} = "$lfn";
  }

  my $user = AliEn::Util::getJobUserByDB($self->{DB}, $jobId);
  my $procDir = AliEn::Util::getProcDir($user, undef, $jobId);

  foreach $name (keys %$inputBox) {
    $self->info("Deleting $name ($procDir)");
    $self->{CATALOGUE}->execute( "remove", "$procDir/$name" ) 
      or print STDERR "JobOptimizer: in copyInputFiles error copying the entry to the catalog\n"
	and return;
    $self->{CATALOGUE}->execute( "cp", $inputBox->{$name}, "$procDir/$name" ) 
      or print STDERR "JobOptimizer: in copyInputFiles error copying the entry to the catalog\n"
	and return;
  }
  return 1;
}
sub setAlive {
  my $self=shift;
  my $date=time;

  ($date<$self->{LASTALIVE})
     and return;
  if($self->{MONITOR}){
    # send the alive status also to ML
    $self->{MONITOR}->sendBgMonitoring();
    #$self->info("Job -> setAlive -> sent Bg Mon info to ML.");
  }
  $self->{LASTALIVE}=$date+3600;
  $self->info( "At the moment, we don't notify anyone that we are alive... although we should tell the optimizer");

}

sub putJobLog {
  my $self=shift;
  return $self->{DB}->insertJobMessage(@_);
}
return 1;

