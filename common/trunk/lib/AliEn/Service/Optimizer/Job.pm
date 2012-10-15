package AliEn::Service::Optimizer::Job;

use strict;

use vars qw (@ISA);

use AliEn::Database::TaskQueue;
use AliEn::Database::IS;
use AliEn::Service::Optimizer;
use AliEn::Catalogue;
use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Dataset;

use POSIX ":sys_wait_h";

use AliEn::Util;

@ISA = qw(AliEn::Service::Optimizer);

my $self;

sub initialize {
  $self = shift;
  my $options = (shift or {});

  $options->{ROLE} = $options->{role} = "admin";

  $self->{SERVICE} = "Job";

  $self->{DB_MODULE} = "AliEn::Database::TaskQueue";

  $self->debug(1, "In initialize initializing service JobOptimizer");

  $self->debug(1,
    "In initialize creating AliEn::UI::Catalogue::LCM::Computer instance");

  $self->{CATALOGUE} = AliEn::UI::Catalogue::LCM::Computer->new($options);

  ($self->{CATALOGUE})
    or $self->{LOGGER}->error("JobOptimizer",
    "In initialize error creating AliEn::UI::Catalogue::LCM::Computer instance")
    and return;

  $self->{PARENT} = $$;

  $self->SUPER::initialize(@_) or return;

  $self->{DB}->setArchive();
  $self->{DATASET} = AliEn::Dataset->new()
    or $self->info("Error creating the dataset")
    and return;

  #  $self->{JOBLOG} = new AliEn::JOBLOG();

  my @optimizers = (
    "Merging",  "Inserting", "Splitting", "Zombies",
    "Hosts",    "Expired",   "HeartBeat", "Priority",
    "Saved",     "Staging",
    "Quota", "WaitingTime", "ToStage",
  );    #,"ResolveReq");

  my $mlEnabled =
    (    $self->{CONFIG}->{MONALISA_HOST}
      || $self->{CONFIG}->{MONALISA_APMONCONFIG});
  $mlEnabled and push @optimizers, "MonALISA";

  my $chargeEnabled = $self->{CONFIG}->{LBSG_ADDRESS};
  $chargeEnabled and push @optimizers, "Charge";

  #  @optimizers=("Merging");

  $self->StartChildren(@optimizers) or return;

  $self->{FORKCHECKPROCESS} = 1;
  return $self;
}

sub checkWakesUp {
  my $self   = shift;
  my $silent = (shift || 0);
  my $method = "info";

  $silent and $method = "debug";
  my @debugLevel = ();
  $silent and push @debugLevel, 1;
  $self->$method(@debugLevel, "Still alive and checking messages");

  my $messages = $self->{DB}->queryValue("select count(1) from JOBMESSAGES");
  if ($messages ) {
    $self->info("Telling the JobManager  to process JOBMESSAGES ($messages messages)");
    $self->{RPC}
      ->CallRPC("Manager/Job", "SetProcInfoBunchFromDB", $self->{HOST}, $messages)
      or
      $self->info("ERROR!!! we couldn't send the messages to the job manager");
  }
  return;
}
sub checkForkProcess{
  my $self=shift;
  my $limit=shift;
  $self->info("Checking if there are already $limit processes");
  
  my $total=$self->checkChildren();
 
  if ($total > $limit) {
    $self->info("There are already too many processes... do not fork");
    return;
  }
  my $newLog = 0;
  while (1) {
    $self->{KIDS}->{$newLog} or last;
    $newLog++;
  }
  my $pid=fork();
  defined $pid or return;
  if ($pid){
    $self->info("The father has a new children: $pid (log  $newLog)");
    $self->{KIDS}->{$newLog}= $pid;
    return 1;
  }
  
  $self->{LOGFILE} =~ s/\.log$/.$newLog.log/;
  $self->info("i'm the kid $$ (to log $self->{LOGFILE})");
  $self->{LOGGER}->redirect($self->{LOGFILE});
  $self->info("i'm the kid $$");
  $self->{KID}=1;
  return ;
}

sub copyInput {
  my $self   = shift;
  my $procid = shift;
  my $job_ca = shift;
  my $user   = shift;
  my $ef_se_org = shift; # to check user SE reqs with inputdata SE
  my $checkSites = keys %$ef_se_org;
  my $no_se_org = shift; # to check user !SE reqs with inputdata SE (if undef $ef_se_org)
  my $ef_se;
  my $return;
  $self->debug(1, "At the beginnning of copyInput of $procid");
  my ($ok, $split) = $job_ca->evaluateAttributeString("Split");
  $self->debug(1, "Already evaluated the split");
  $split
    and
    $self->info("The job is going to be split... don't need to copy the input")
    and return
    {};

  $self->debug(1, "Already evaluated the inputbox");
  ($ok, my @inputData) = $job_ca->evaluateAttributeVectorString("InputData");
  $self->debug(1, "Before the copy of the inputcollection");
  $self->copyInputCollection($job_ca, $procid, \@inputData)
    or $self->info("Error checking the input collection")
    and return;
  my $file;

  my $size = 0;

  my $done = {};

  my @allreq;
  my @allreqPattern;
  $self->debug(1, "And the new eval");
  eval {
    foreach $file ( @inputData) {
      my $nodownload = 0;
      $file =~ s/,nodownload$// and $nodownload = 1;

      $self->debug(1, "In copyInput adding file $file");

      #    my $procname=$self->findProcName($procid, $file, $done, $user);
      $file =~ s/^LF://i;
      $self->debug(1, "Adding file $file (from the InputBox)");
      my ($fileInfo) =
        $self->{CATALOGUE}->execute("whereis", "-ri", $file, "-silent");
      if (!$fileInfo) {
        $self->putJobLog($procid, "error", "Error checking the file $file");
        die("The file $file doesn't exist");
      }

      my @sites = sort @{ $fileInfo->{REAL_SE} };
      if (!@sites) {
        $self->putJobLog($procid, "error", "Error checking the file $file");
        die("The file $file isn't in any SE");
      }

	  if($checkSites){
      	%$ef_se = %$ef_se_org;
      	foreach my $site (keys %$ef_se){
      		grep { /$site/i } @sites or delete $ef_se->{$site};
      	}
      } elsif (keys %$no_se_org){
      	for (my $a=0; $a<scalar(@sites); $a++) { 
      		$no_se_org->{uc($sites[$a])} and splice(@sites, $a, 1);
      	}
      }
            
      $checkSites and !keys %$ef_se and $return = { failed => 1 } and return;
      !$checkSites and keys %$no_se_org and !scalar(@sites) and $return = { failed => 1 } and return;
	    
      my $sePattern = join("_", @sites);

      if (!$checkSites && !grep (/^$sePattern$/, @allreqPattern)) {
        $self->putJobLog($procid, "trace",
          "Adding the requirement to '@sites' due to $file");

        map { $_ = " member(other.CloseSE,\"$_\") " } @sites;
        my $sereq = "(" . join(" || ", @sites) . ")";
        $self->info(
"Putting the requirement $sereq ($sePattern is not in @allreqPattern)" );
        push @allreq,        $sereq;
        push @allreqPattern, $sePattern;
      }
      $nodownload
        and $self->debug(1,
        "Skipping file $file (from the InputBox) - nodownload option")
        and next;
      $size += $fileInfo->{size};

    }
  };
  my $error = $@;

  if ($error) {
    $self->info("Something went wrong while copying the input: $@");
    return;
  }
  
  $return and return $return;
  $checkSites and $self->putJobLog($procid, "trace", "JDL requirements over SEs restricted by user (member.CloseSE)");
  
  if ($size) {
    #let's round up the size
    $size = (int($size / (1024 * 8192) + 1)) * 8192;

  }
  my ($okwork, @workspace) =
    $job_ca->evaluateAttributeVectorString("Workdirectorysize");
  if ($okwork && defined $workspace[0] && $workspace[0] > 0) {
    my $unit = 1;
    $workspace[0] =~ s/MB//i and $unit = 1024;
    $workspace[0] =~ s/GB//i and $unit = 1024 * 1024;
    my $space = $workspace[0] * $unit;
    if ($space > $size) {
      $self->info("The job requires some extra workspace: $workspace[0]");
      $size = $space;
    }
  }
  my $req = join(" && ", "( other.LocalDiskSpace > $size )", @allreq);
  $self->info("The requirements from input are $req");
  return { requirements => "$req" };
}

sub updateWaiting {
  my $self    = shift;
  my $queueid = shift;
  my $job_ca  = shift;

  $self->checkMirrorData($job_ca, $queueid) or return;
  $self->checkChangedReq($job_ca, $queueid) or return;

  return 1;
}
sub checkChildren {
  my $self=shift;

  my $newKids = {};
  $self->{KIDS} or $self->{KIDS}={};

  my $total   = 1;
  foreach my $p (keys %{$self->{KIDS}}) {
    (CORE::kill 0, $self->{KIDS}->{$p} and waitpid($self->{KIDS}->{$p} , WNOHANG) <= 0) or next;
    $newKids->{$p} = $self->{KIDS}->{$p};
    $total++;
  }
  $self->{KIDS}=$newKids;
  if ($total == 1){
    $self->info("There are no kids: min_id back to zero:");
    $self->{MIN_ID}=0;
  }
  $self->info("There are $total processes");
  return $total;
}
sub checkJobs {
  my $self     = shift;
  my $silent   = shift;
  my $status   = shift;
  my $function = shift;
  my $limit    = (shift or 15);
  my $prefork  = shift || 0;
  
  my $method = "info";
  $silent and $method = "debug";

  $self->{LOGGER}->$method("Job", "Checking status=$status jobs ");
  my $continue = 1;

#We never want to get more tahn 15 jobs at the same time, just in case the jdls are too long
  $self->{MIN_ID} or $self->{MIN_ID}=0;
  $self->checkChildren();
  while ($continue) {
  	$self->info("Checking the jobs in a particular status");
    my $jobs =
      $self->{DB}->getJobsByStatus($status, "queueid", "queueid", $limit,  $self->{MIN_ID});
      

    defined $jobs
      or $self->info("In checkJobs error during execution of database query")
      and return;

    if (not @$jobs){
      $self->info("There are no jobs status=$status and $self->{MIN_ID}");
      return;    #check if it's ok to return undef here!!
    }

    $continue = 0;
    $#{$jobs} eq $limit-1 and $continue = 1;
    $self->info("THERE ARE $#{$jobs} jobs, let's continue? $continue min id $self->{MIN_ID}");

    if ($self->checkForkProcess($prefork)) {
      #This is the father. We have forked a kid that will do these things. Let's just see the highest number
      foreach my $data (@$jobs) {
        $data->{queueid} > $self->{MIN_ID} and $self->{MIN_ID} = $data->{queueid};
      }
      next;
    }    

    foreach my $data (@$jobs) {
      $self->{LOGGER}->$method("JobOptimizer", "Checking job $data->{queueid}");
      my $job_ca = AlienClassad::AlienClassad->new($data->{jdl});

      $self->info("In checkJobs - calling $function");
      $self->$function($data->{queueid}, $job_ca, $status);
    }
    $self->{KID} and exit(0);
  }
  return 1;
}


sub checkMirrorData {
  my $self   = shift;
  my $job_ca = shift;
  my $id     = shift;

  my ($ok, @input) = $job_ca->evaluateAttributeVectorString("InputData");
  $ok or return 1;
  $self->debug(1, "In checkMirrorData job $id has input data");
  map { $_ =~ s/^LF:// } @input;
  foreach my $input (@input) {
    my $se = "Alice::CNAF::Castor";
    my @se = $self->{CATALOGUE}->execute("whereis", $input, "-silent");

    #	grep ( /$se/, @se)
    #	    or print "mirror -fb $input $se\n";
  }
  return 1;
}

sub checkChangedReq {
  my $self   = shift;
  my $job_ca = shift;
  my $id     = shift;

  my ($ok, $req) = $job_ca->evaluateExpression("Requirements");
  ($ok and $req)
    or $self->info("Error getting the requirements of the job")
    and return;
  $self->debug(1, "In checkChangedReq got $req");
  ($ok, my $origreq) = $job_ca->evaluateExpression("OrigRequirements");
  $origreq
    or $self->info("No original req...")
    and return 1;

  $self->debug(1, "In checkChangedReq original requirements $origreq");
  $job_ca->set_expression("Requirements", $origreq);

  $self->{CATALOGUE}->{QUEUE}->checkRequirements($job_ca);
  ($ok, my $newreq) = $job_ca->evaluateExpression("Requirements");
  ($ok and $req)
    or $self->info("Error getting the requirements of the job")
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
  my $self   = shift;
  my $job_ca = shift;
  my $id     = shift;
  $self->info("The job requirements of $id have changed!!");
  $self->{DB}->updateStatus($id, "WAITING", "UPDATING")
    or $self->info("Error updating status for job $id")
    and return;

  $self->copyInputFiles($job_ca, $id) or return;

#	$job_ca->set_expression("OrigRequirements", "member(other.GridPartition, \"Production\")")
#	    or $self->info( "Error putting the origreq") and return;
  my $new_jdl = $job_ca->asJDL();
  $self->info("Putting as jdl: $new_jdl");

  my $update = $self->{DB}->setJdl($id, $new_jdl);
  $update or $self->info("Error doing the update") and return;
  $self->{DB}->updateStatus($id, "UPDATING", "WAITING")
    or $self->info("Error updating status for job $id")
    and return;

  $self->info("Job updated!!");
  return 1;
}

sub DESTROY {
  my $self = shift;
  my $now  = `date`;
  $self or return;
  print "In Destroy. I'm $$ and the father was $self->{PARENT}\n";
  ($self->{PARENT} eq $$) or return;
  print "$now: KILLING ALL THE CHILDREN\n";
  $self or return 1;

  foreach (grep (/PID/, keys %$self)) {
    print "$now: KILLING $_ and $self->{$_}\n";
    kill 9, $self->{$_};
  }

}

sub copyInputCollection {
  my $self     = shift;
  my $job_ca   = shift;
  my $jobId    = shift;
  my $inputBox = shift;
  $self->debug(1, "Checking if the job defines the InputDataCollection");

  my ($ok, @inputData) =
    $job_ca->evaluateAttributeVectorString("InputDataCollection");
  @inputData
    or $self->debug(1, "There is no inputDataCollection")
    and return 1;
  ($ok, my $split) = $job_ca->evaluateAttributeString("Split");

  foreach my $file (@inputData) {
    $self->putJobLog($jobId, "trace", "Using the inputcollection $file");

    my ($file2, $options) = split(',', $file, 2);
    $options and $options = ",$options";
    $options or $options = "";
    $file2 =~ s/^LF://;
    
    my $ref_before = $#{$inputBox};
    
    my ($type) = $self->{CATALOGUE}->execute("type", $file2);
    $self->info("IT IS A $type");
    if ($type =~ /^collection$/) {
      $self->copyInputCollectionFromColl($jobId, $file2, $options, $inputBox)
        or return;
    } else {
      $self->copyInputCollectionFromXML($jobId, $file2, $options, $inputBox)
        or return;
    }
    my $lfnRef;
    if($type !~ /^collection$/) {
    	$lfnRef = $self->{DATASET}->getAllLFN()
          or $self->info("Error getting the LFNS from the dataset")
          and return;
        $lfnRef = $#{$lfnRef->{lfns}};
    } else {
    	$lfnRef = $#{$inputBox} - $ref_before;
    }
    if ($split and $#{$inputBox} > 3000) {
      $self->putJobLog($jobId, "error",
"There are $lfnRef files in the collection $file2 (split job). Putting the job to error"
      );
      return;
    }
    if (!$split and $#{$inputBox} > 1000) {
      $self->putJobLog($jobId, "error",
"There are $lfnRef files in the collection $file2. Putting the job to error"
      );
      return;
    }
  }
  return 1;
}

sub copyInputCollectionFromColl {
  my $self     = shift;
  my $jobId    = shift;
  my $lfn      = shift;
  my $options  = shift;
  my $inputBox = shift;

  my ($files) = $self->{CATALOGUE}->execute("listFilesFromCollection", $lfn);

  if (!$files) {
    $self->putJobLog($jobId, "error", "Error getting the inputcollection $lfn");
    return;
  }
  $self->info("Now we have to add the files");
  foreach my $entry (@$files) {
    if ($entry->{origLFN}) {
      push @$inputBox, "LF:$entry->{origLFN}$options";
    } else {
      push @$inputBox, "GUID:$entry->{guid}";
    }
  }
  return 1;
}

sub copyInputCollectionFromXML {
  my $self     = shift;
  my $jobId    = shift;
  my $lfn      = shift;
  my $options  = shift;
  my $inputBox = shift;
  my ($localFile) = $self->{CATALOGUE}->execute("get", $lfn);
  if (!$localFile) {
    $self->putJobLog($jobId, "error", "Error getting the inputcollection $lfn");
    return;
  }
  $self->info("Let's read the dataset");
  my $dataset = $self->{DATASET}->readxml($localFile);
  if (!$dataset) {
    $self->putJobLog($jobId, "error",
      "Error creating the dataset from the collection $lfn");
    return;
  }
  my $total = 0;
  $dataset->{collection}
    and $dataset->{collection}->{event}
    and $total = keys %{ $dataset->{collection}->{event} };

  $self->info("Getting the LFNS from the dataset");
  my $lfnRef = $self->{DATASET}->getAllLFN()
    or $self->info("Error getting the LFNS from the dataset")
    and return;

  map { $_ = "LF:$_$options" } @{ $lfnRef->{lfns} };
  $self->info("Adding the files " . @{ $lfnRef->{lfns} });
  push @$inputBox, @{ $lfnRef->{lfns} };
  return 1;
}

sub copyInputFiles {
  my $self   = shift;
  my $job_ca = shift;
  my $jobId  = shift;

  my $name;
  my $inputBox = {};
  $self->copyInputCollection($job_ca, $jobId, $inputBox)
    or $self->info("Error copying the inputCollection")
    and return;

  my ($ok, @inputData) = $job_ca->evaluateAttributeVectorString("InputData");
  @inputData
    or $self->info("There is no inputData")
    and return { requirements => "" };

  foreach my $lfn (@inputData) {
    ($lfn =~ /nodownload/) and next;
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
    $self->{CATALOGUE}->execute("remove", "$procDir/$name")
      or print STDERR
      "JobOptimizer: in copyInputFiles error copying the entry to the catalog\n"
      and return;
    $self->{CATALOGUE}->execute("cp", $inputBox->{$name}, "$procDir/$name")
      or print STDERR
      "JobOptimizer: in copyInputFiles error copying the entry to the catalog\n"
      and return;
  }
  return { requirements => "" };
}

sub setAlive {
  my $self = shift;
  my $date = time;

  ($date < $self->{LASTALIVE})
    and return;
  if ($self->{MONITOR}) {

    # send the alive status also to ML
    $self->{MONITOR}->sendBgMonitoring();

    #$self->info("Job -> setAlive -> sent Bg Mon info to ML.");
  }
  $self->{LASTALIVE} = $date + 3600;
  $self->info(
"At the moment, we don't notify anyone that we are alive... although we should tell the optimizer"
  );

}

sub putJobLog {
  my $self = shift;

  $self->info(join(" ", "Putting in the log: ", @_));
  return $self->{DB}->insertJobMessage(@_);
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

return 1;

