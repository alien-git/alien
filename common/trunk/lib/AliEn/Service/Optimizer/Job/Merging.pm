package AliEn::Service::Optimizer::Job::Merging;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::Service::Manager::Job;
use AliEn::Database::Admin;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";

  #We need the admin db, because this optimizer can enter jobs automatically
  $self->{ADMINDB} or  $self->{ADMINDB}= new AliEn::Database::Admin();

  $self->{LOGGER}->$method("Merging", "The merging optimizer starts");

  $self->{DB}->queryValue("SELECT todo from ACTIONS where action='MERGING'")
    or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='MERGING'");

  $self->info("There are some jobs to check!!");

    my $jobs=$self->{DB}->query("SELECT queueid, jdl, status from QUEUE q, JOBSTOMERGE j where q.queueid=j.masterid and status='SPLIT' union select queueid,jdl,status from QUEUE where status='FORCEMERGE'");
  foreach my $job (@$jobs){
    $self->{DB}->delete("JOBSTOMERGE", "masterId=?", {bind_values=>[$job->{queueid}]});
    my $job_ca=Classad::Classad->new($job->{jdl});
    if ( !$job_ca->isOK() ) {
      $self->info("JobOptimizer: in checkJobs incorrect JDL input\n" . $job->{jdl} );
      $self->{DB}->updateStatus($job->{queueid},"%","ERROR_I");
      
      next;
    }
    $self->updateMerging($job->{queueid}, $job_ca, $job->{status});

  }

  my $done3=$self->checkJobs($silent, "MERGING","checkMerging",100); 

  $self->{LOGGER}->$method("Merging", "The merging optimizer finished");

  return;
}


sub checkMerging {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;
  my $status =shift;

  my $newStatus="DONE";
  $self->info("Checking if the merging jobs of $queueid have finished");

  my ($olduser)=$self->{CATALOGUE}->execute("whoami");

  eval {
    my $info=$self->{DB}->getFieldsFromQueue($queueid, "merging,submitHost")
      or die("Error getting the merging jobs of $queueid");

    my @subjobs=split(",", $info->{merging});
    my $user;
    ( $info->{submitHost} =~ /^(.*)\@/ ) and ( $user = $1 );
    $self->{CATALOGUE}->execute("user", "-", "$user")
      or die("Error changing to user $user");
    my @finished;
    my @running;
    foreach my $subjob (@subjobs){
      $subjob or next;
      $self->info("Checking if the job $subjob has finished");
      my $status=$self->{DB}->getFieldFromQueue($subjob, "status")
	or die ("Error getting the status of $subjob");
      $status =~ /DONE/ and push (@finished, $subjob)  and  next;
      if ($status =~ /(ERROR_)|(KILLED)/){
	$self->info("Skipping job $subjob (in $status)");
	$self->putJobLog($queueid, "error", "One of the merging jobs ($subjob) failed or was killed");
	next;
      }
      push @running, $subjob;
    }
    my $procDir=AliEn::Util::getProcDir($user, undef, $queueid);;
    foreach my $job (@finished) {
      my $subProcDir=AliEn::Util::getProcDir($user, undef, $job);
      $self->info("Let's copy the output of $job");
      $self->putJobLog($queueid, "state", "The merging $job finished!!");
      $self->{CATALOGUE}->execute("mkdir", "-p", "$procDir/merged", "$procDir/merge-logs") 
	or die("error creating the directories");
      $self->{CATALOGUE}->execute("cp", "$subProcDir/job-log/execution.out",
				  "$procDir/merge-logs/execution.$job.out", "-user", $user );
      my @files=$self->{CATALOGUE}->execute("find", "$subProcDir/job-output", "*");
      foreach my $file (@files){
	$file=~ /\/((stdout)|(stderr)|(resources)|(alien_archive.*))$/ and next;
	$self->info("Copying $file");
	my $basename=$file;
	$basename=~ s{^.*/([^/]*)$}{$1};
	if ($self->{CATALOGUE}->{CATALOG}->isFile("$procDir/merged/$basename")){
	  $self->info("Removing the previous merge");
	  $self->{CATALOGUE}->execute("rm", "$procDir/merged/$basename");
	}
	if (! $self->{CATALOGUE}->execute("cp", $file, "$procDir/merged/")){
	  $self->info("Error copying $file");
	  $self->putJobLog($queueid, "error", "Error copying $file");
	}
      }

    }
    my $newMerging=join(",",@running);
    $self->info("Now we have to wait for '$newMerging'");
    $newStatus="DONE";
    $self->{DB}->update("QUEUE", { merging=>$newMerging}, "queueid=?", {bind_values=>[$queueid]});
    if (@running) {
      $self->info("The jobs @running are still running");
      $newStatus=undef;
    }

  };
  if ($@) {
    $self->info("Something failed: $@");
    $newStatus="ERROR_M";
  }
  $self->{CATALOGUE}->execute("user","-", $olduser);

  $newStatus or return;
  my $message="Job state transition from $status to $newStatus";
  $self->{DB}->updateStatus($queueid,$status,$newStatus) or 
    $message="Failed: $message";
  $self->putJobLog($queueid,"state", $message);

  return 1;
}

sub checkMasterResubmition {
  my $self=shift;
  my $user=shift;
  my $queueid=shift;
  my $job_ca=shift;
  my $rparts=shift;
  $self->info("Checking if the masterjob has some kind of resubmition"); 
  my ($ok, $number)=$job_ca->evaluateAttributeString("MasterResubmitThreshold");
  $ok or return 1;
  ($ok, my $type)=$job_ca->evaluateAttributeString("ResubmitType");
  $type or $type="system";

  my @status=();
  if ($type =~ /^system$/){
    push @status, 'EXPIRED','ERROR_IB','ERROR_E','FAILED','ERROR_SV', 'ERROR_A';
  }elsif ($type =~ /^all$/){
    push @status,  'EXPIRED','ERROR_IB','ERROR_E','FAILED','ERROR_SV', 'ERROR_A', 'ERROR_V';
  }else {
    push @status,  split(",", $type);

  }
  $self->info("Checking the jobs in @status (they have to be less than $number)");
  my $failed=0;
  my $total=0;

  foreach my $p (@$rparts){
    $total+=$p->{count};
    if (grep (/^$p->{status}$/i, @status)){
      $self->info("This is a job that will be resubmitted");
      $failed+=$p->{count};
    }
  }
  my $resubmit=0;
  if ($number =~ s/\%//){
    $self->info("Resubmitting if more than $number %");
    $failed*100.0/$total > $number and $resubmit=1;
  } else {
    $failed >$number and $resubmit=1;
  }
  $resubmit or $self->info("We don't resubmit any jobs. Only $failed failures, and we can have up to $number") and return 1;

  $self->info("We have to resubmit some of the jobs!!");
  my @newstatus;
  foreach my $s (@status){
    push @newstatus, "-status", $s;
  }
  $self->putJobLog($queueid, "info", "Automatic resubmition of jobs in $type (there were $failed)");
  eval {
    $self->info("At the moment, @ISA");
    push @ISA, "AliEn::Service::Manager::Job";
    my ($status, $error)=$self->getMasterJob($user, $queueid, "resubmit", @newstatus);
    $status eq "-1" and die("resubmition failed: $error\n");
  };
  if ($@){
    $self->info("The resubmition didn't work : $@");
  }
  @ISA=grep ( ! /AliEn::Service::Manager::Job/, @ISA);
  $self->info("Back to @ISA");
  return 1;
}


sub updateMerging {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;
  my $status =shift;

  my $newStatus="DONE";
  my $set={};
  my ($olduser)=$self->{CATALOGUE}->execute("whoami");

  eval {
    #my @part_jobs=$self->{DB}->query("SELECT count(*),status from QUEUE where split=$queueid group by status");
    my $rparts = $self->{DB}->getFieldsFromQueueEx("count(*) as count, status", "WHERE split=? GROUP BY status", {bind_values=>[$queueid]})
      or die("Could not get splitted jobs for $queueid");

    my $user = AliEn::Util::getJobUserByDB($self->{DB}, $queueid);
    $self->{CATALOGUE}->execute("user", "-", "$user")
      or die("Error changing to user $user");
    my $procDir = AliEn::Util::getProcDir($user, undef, $queueid);
    $self->{CATALOGUE}->execute("mkdir","-p", "$procDir/job-log");

    if (! $self->checkMaxFailed($job_ca, $rparts)){
      $self->putJobLog($queueid, "error","There were too many subjobs failing");
      $self->info("At the moment, @ISA");
      push @ISA, "AliEn::Service::Manager::Job";
      my ($status, $error)=$self->getMasterJob($user, $queueid, "kill", "-status", "WAITING", "-status", "INSERTING");

      @ISA=grep ( ! /AliEn::Service::Manager::Job/, @ISA);
      $self->info("Back to @ISA");
      return;
    }

    $self->checkMasterResubmition($user, $queueid, $job_ca, $rparts) or return;
    if ($#{$rparts} > -1) {
      $self->info("Jobs for $queueid");
      for (@$rparts) {
	$self->info("Checking Jobs  $_->{status}");
	# force the copy of a split job
	if ($status eq "TERMSPLIT" ) {
	  if ($_->{status} =~ /RUNNING/) {
	    $self->info("There are still jobs running");
	    $newStatus=undef;
	    return;
	  }
	}

	if ($status eq "SPLIT" ) { 
	  if ($_->{status} !~ /(DONE)|(FAILED)|(KILLED)|(EXPIRED)|(ERROR_)/) {
	    $newStatus=undef;
	    $self->info("There are still jobs running");
	    return;
	  }
	}
      }

      $self->info( "All the jobs finished. Checking best place for execution (user $user)");

      my ($info)=$self->{DB}->getFieldsFromQueue($queueid,"merging")
	or die ("Job $queueid doesn't exist");

      $self->copyOutputDirectories( $queueid, $job_ca, $procDir, $user) 
	or die ("error copying the output directories");
      
      $self->checkMergingCollection($job_ca, $queueid, $procDir);
      $self->checkMergingSubmission($job_ca, $queueid, $procDir, $user, $set, $info) 
	or die("Error doing the submission of the merging jobs");
      if ($set->{newStatus}){
	$status=$newStatus;
	delete $set->{newStatus};
      }
    }
  };
  if ($@){
    $newStatus="ERROR_M";
    $self->info("Error updating the job $queueid: $@");
  }
  $self->{CATALOGUE}->execute("user","-", $olduser);

  if ($newStatus) {
    my $message="Job state transition from $status to $newStatus";
    delete $set->{newStatus};
    $self->{DB}->updateStatus($queueid,$status,$newStatus, $set) or 
      $message="Failed: $message";
    $self->putJobLog($queueid,"state", $message);
  }
  return 1;
}


sub checkMaxFailed{
  my $self=shift;
  my $job_ca=shift;
  my $rparts=shift;

  $self->info("Checking if there are too many failures");
  my   ($ok,  $failed)=$job_ca->evaluateAttributeString("MaxFailed");
  $ok or ($ok,  $failed)=$job_ca->evaluateExpression("MaxFailed");
  ($ok,  my $initFailed)=$job_ca->evaluateAttributeString("MaxInitFailed");
  $ok or ($ok,  $initFailed)=$job_ca->evaluateExpression("MaxInitFailed");
  $self->info("We accept $failed and $initFailed");
  ($failed or $initFailed) or return 1;

  my $total=0;
  my $failure=0;
  my $success=0;
  foreach my $p (@$rparts){
    $total+=$p->{count};
    $p->{status}=~ /^ERROR/ and $failure+=$p->{count};
    $p->{status}=~ /^DONE/ and $success+=$p->{count};
  }
  $self->info("The job has $total ($failure/$success)");
  if ($failed){
    $self->info("Checking that there are no more than '$failed' jobs");
    if ($failed =~ s/\%//){
      $failed > ($failure/$total) and return;
    }else {
      $failed<$failure and return;
    }
  }

  if ($initFailed){
    $self->info("Checking that there are no more thatn '$initFailed' initial failures");
    if (! $success){
      if ($initFailed =~ s/\%//){
	$initFailed > ($failure/$total) and return;
      }else {
	$failed<$failure and return;
      }
    }
  }
  return 1;
}

sub copyOutputDirectories{
  my $self=shift;
  my $masterId=shift;
  my $job_ca=shift;
  my $procDir=shift;
  my $user=shift;


  my $subJobs = $self->{DB}->getFieldsFromQueueEx("queueId,status,submitHost", "WHERE split=?", {bind_values=>[$masterId]})
	or die ("Could not get splitted jobs for $masterId");

  # copy all the result files into the master job directory
  my $cnt=0;

  my $outputdir = "$procDir/subjobs";

  for (@$subJobs) {
    $_->{status} eq "DONE" or print "Skipping $_->{queueId}\n" and next;
    $cnt++;
    my $subId=$_->{queueId};
    #		    my $newdir = sprintf "%03d",$cnt;
    my $origdir=AliEn::Util::getProcDir(undef, $_->{submitHost}, $subId);
#    my $eventdir=$self->getOutputDir($subId, $jdlrun, $jdlevent) or next;
    my $destdir="$outputdir/$subId";
    $destdir =~ s{//}{/}g;
    $self->debug(1,"Copying from $origdir to $destdir");
    if (! $self->{CATALOGUE}->execute("cd", $origdir, "-silent")){
      if (! $self->{CATALOGUE}->execute("cd", $destdir)){
	$self->info("The directory doesn't exist any more (and it has not been copied to $destdir!!)");
	$self->putJobLog($masterId,"error", "error moving subjob output of $subId to $destdir");
      }
      next;
    }

    if (! $self->{CATALOGUE}->execute("mkdir",$destdir,"-p") ){
      $self->info("Error creating the destination directory $destdir");
      $self->putJobLog($masterId, "error", "Error creating the directory $destdir");
      next;
    }
    $self->debug(1, "And now, the cp $origdir/job-output $destdir");
    if ($self->{CATALOGUE}->execute("cp", "-silent", "$origdir/job-output", $destdir) ) {
      #Let's put the log files of the subjobs
      $self->{CATALOGUE}->execute("cp",  "-silent","$origdir/job-log/execution.out", "$procDir/job-log/execution.$subId.out");
      $self->{CATALOGUE}->execute("cp",  "-silent","$origdir/job-log/execution.err", "$procDir/job-log/execution.$subId.err");

      # delete the proc directory
      if ($subId > 0) {
	$self->putJobLog($masterId,"delete", "Deleting subjob directory $origdir ");
	$self->{CATALOGUE}->execute("rmdir",$origdir,"-r") or $self->info("Error deleting the directory $origdir");
      }
      $self->putJobLog($masterId,"move", "Moving subjob output of $subId to $destdir");
    } else {
      $self->info("Error copying the file $origdir/job-output to $destdir");	
      $self->putJobLog($masterId,"error", "error moving subjob output of $subId to $destdir");
    }
  }				

  return 1;
}


sub checkMergingSubmission {
  my $self=shift;
  my $job_ca=shift;
  my $queueid=shift;
  my $procDir=shift;
  my $user=shift;
  my $set=shift;
  my $info=shift;

  my $oldmerging=$info->{merging} || "";

  my ($ok,  @merge)=$job_ca->evaluateAttributeVectorString("Merge");
  
  my $outputD="$procDir/merge";
  ($ok, my $t)=$job_ca->evaluateAttributeString("OutputDir");
  $t =~ s/\#.*$//;
  $ok and $outputD=$t;
  ($ok, $t)=$job_ca->evaluateAttributeString("MergeOutputDir");
  $ok and $outputD=$t;
  
  
  my @subjobs;
  foreach my $merge (@merge) {
    $set->{newStatus}="MERGING";
    #Ok, let's submit the job that will merge the output
    $self->info("We have to submit the job $merge");
    my ($file, $jdl, $output)=split(":", $merge);
    
    $self->{CATALOGUE}->{CATALOG}->{ROLE}=$user;
    my ($id)=$self->{CATALOGUE}->execute("submit","$jdl $queueid $file $output $user $procDir $outputD");
    $self->{CATALOGUE}->{CATALOG}->{ROLE}="admin";
    
    $id or $self->info("Error submitting the job $jdl") and return;
    push @subjobs, $id;
  }
  (@subjobs) and $set->{merging}=join(",",$oldmerging,@subjobs);



  return 1;
}

sub checkMergingCollection{
  my $self=shift;
  my $job_ca=shift;
  my $queueid=shift;
  my $procDir=shift;

  $self->info("***Let's check if there are any collections");

  my ($ok, @mergingCollections)=$job_ca->evaluateAttributeVectorString("MergeCollections");
  @mergingCollections or return 1;

  foreach my $entry (@mergingCollections){
    $self->putJobLog($queueid, "info", "Creating the merging collection '$entry'");
    my ($files, $collection)=split(/:/, $entry, 2);

    eval{
      $self->{CATALOGUE}->execute("rm", $collection);
      $self->{CATALOGUE}->execute("createCollection", $collection)
	or die("error creating the collection: '$collection'");
      my @patterns=split(/,/ , $files);
      foreach my $pattern (@patterns){
	my @files=$self->{CATALOGUE}->execute("find", "$procDir/subjobs", $pattern);
	foreach my $file(@files){
	  $self->{CATALOGUE}->execute("addFileToCollection", "-n", $file, $collection) or die("Error adding the file '$file' to the collection");
	}
      }
      $self->{CATALOGUE}->execute("updateCollection", $collection)
	or die("Error updating the collection");
    };

    if ($@){
      $self->putJobLog($queueid, "error", "Error creating the collection: $@");
      return;
    }
  }
  
  return 1;
}
 
1;

