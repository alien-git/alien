package AliEn::Service::Optimizer::Job::Merging;

use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";

  $self->{LOGGER}->$method("Merging", "The merging optimizer starts");

  $self->{DB}->queryValue("SELECT todo from ACTIONS where action='MERGING'")
    or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='MERGING'");

  $self->info("There are some jobs to check!!");

  my $jobs=$self->{DB}->query("SELECT queueid, jdl, status from QUEUE q, JOBSTOMERGE j where q.queueid=j.masterid and status='SPLIT'");
  foreach my $job (@$jobs){
    $self->{DB}->delete("JOBSTOMERGE", "masterId=?", {bind_values=>[$job->{queueid}]});
    use Data::Dumper;
    print Dumper($job);
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
    my $allparts = $self->{DB}->getFieldsFromQueueEx("queueId,status,submitHost", "WHERE split=?", {bind_values=>[$queueid]})
      or die ("Could not get splitted jobs for $queueid");

    my $user = AliEn::Util::getJobUserByDB($self->{DB}, $queueid);
    $self->{CATALOGUE}->execute("user", "-", "$user")
      or die("Error changing to user $user");
    my $procDir = AliEn::Util::getProcDir($user, undef, $queueid);
    $self->{CATALOGUE}->execute("mkdir","-p", "$procDir/job-log");

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

      $self->info( "All the jobs finished. Checking best place for execution");

      my ($info)=$self->{DB}->getFieldsFromQueue($queueid,"submithost,merging")
	or die ("Job $queueid doesn't exist");
      my $host=$info->{submithost};
      my $oldmerging=$info->{merging} || "";
      my $user = "";
      ( $host =~ /^(.*)\@/ ) and ( $user = $1 );

      $self->copyOutputDirectories($allparts, $queueid, $job_ca, $procDir, $user) 
	or die ("error copying the output directories");

      my ($ok,  @merge)=$job_ca->evaluateAttributeVectorString("Merge");


      my @subjobs;
      foreach my $merge (@merge) {
	$newStatus="MERGING";
	#Ok, let's submit the job that will merge the output
	$self->info("We have to submit the job $merge");
	my ($file, $jdl, $output)=split(":", $merge);

	$self->{CATALOGUE}->{CATALOG}->{ROLE}=$user;
	my ($id)=$self->{CATALOGUE}->execute("submit","$jdl $queueid $file $output $user");
	$self->{CATALOGUE}->{CATALOG}->{ROLE}="admin";

	$id or die("Error submitting the job $jdl");
	push @subjobs, $id;
      }
      (@subjobs) and $set->{merging}=join(",",$oldmerging,@subjobs);
    }
  };
  if ($@){
    $newStatus="ERROR_M";
    $self->info("Error updating the job $queueid: $@");
  }
  $self->{CATALOGUE}->execute("user","-", $olduser);

  if ($newStatus) {
    my $message="Job state transition from $status to $newStatus";
    $self->{DB}->updateStatus($queueid,$status,$newStatus, $set) or 
      $message="Failed: $message";
    $self->putJobLog($queueid,"state", $message);
  }
  return 1;
}


sub copyOutputDirectories{
  my $self=shift;
  my $subJobs=shift;
  my $masterId=shift;
  my $job_ca=shift;
  my $procDir=shift;
  my $user=shift;

  # copy all the result files into the master job directory
  my $cnt=0;

#  my ( $ok, $outputdir ) = $job_ca->evaluateExpression("OutputDir");
#  $self->info("Found Outputdir $outputdir");
#    # here we have to replace the organisation name !!!
#  $outputdir=~ s/[\"\{\}\s]//g;
#  my $orgdir = lc $self->{CONFIG}->{ORG_NAME};
#  my $userdir = "/$orgdir/production/$user";
#  $outputdir and $outputdir = "$userdir/$outputdir" and $self->{CATALOGUE}->execute("mkdir","$outputdir");
#
#  $outputdir or 
    my $outputdir = "$procDir/subjobs";

#  ( $ok, my $jdlrun ) = $job_ca->evaluateExpression("Run");
#  ( $ok, my $jdlevent ) = $job_ca->evaluateExpression("Event");

  my ($olduser)=$self->{CATALOGUE}->execute("whoami");
  if (!$self->{CATALOGUE}->execute("user", "-", "$user")){
    $self->info("Error changing to user $user");
    $self->{CATALOGUE}->execute("user", "-", "$olduser");
    return ;
  }

  for (@$subJobs) {
    $_->{status} eq "DONE" or print "Skipping $_->{queueId}\n" and next;
    $cnt++;
    my $subId=$_->{queueId};
    #		    my $newdir = sprintf "%03d",$cnt;
    my $origdir=AliEn::Util::getProcDir(undef, $_->{submitHost}, $subId);
#    my $eventdir=$self->getOutputDir($subId, $jdlrun, $jdlevent) or next;
    my $destdir="$outputdir/$subId";
    $destdir =~ s{//}{/}g;
    $self->info("Copying from $origdir to $destdir");
    if (! $self->{CATALOGUE}->execute("cd", $origdir, "-silent")){
      if (! $self->{CATALOGUE}->execute("cd", $destdir)){
	$self->info("The directory doesn't exist any more (and it has not been copied to $destdir!!)");
	$self->putJobLog($masterId,"error", "error moving subjob output of $subId to $destdir");
      }
      next;
    }

    $self->{CATALOGUE}->execute("mkdir",$destdir,"-p") or
      $self->info("Error creating the destination directory $destdir");
    $self->info("And now, the cp $origdir/job-output $destdir");
    if ($self->{CATALOGUE}->execute("cp", "$origdir/job-output", $destdir) ) {
      #Let's put the log files of the subjobs
      $self->{CATALOGUE}->execute("cp", "$origdir/job-log/execution.out", "$procDir/job-log/execution.$subId.out");
      $self->{CATALOGUE}->execute("cp", "$origdir/job-log/execution.err", "$procDir/job-log/execution.$subId.err");

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

  $self->{CATALOGUE}->execute("user","-", $olduser);

  return 1;
}


#
# This subroutine chooses the directory where the subjob is going to be copied
# By default, it will be /proc/<user>/<master job>/<subjob>
#sub getOutputDir {
#  my $self=shift;
#  my $subId=shift;
#  my $jdlrun=shift;
#  my $jdlevent=shift;
#
#  # get the jdl for this job and extract run and event
#  my $splitjobjdl = $self->{DB}->getFieldsFromQueue($subId);
#  defined $splitjobjdl
#    or $self->{LOGGER}->warning( "Merging", "In updateMerging error during execution of database query for queuejob $_->{queueId}" )
#	  and return;
#  my ($run, $event);#
#
#  $splitjobjdl->{jdl} =~ /\-\-run\s+(\d{1,8})\s+.*/ and $run = $1;
#  $splitjobjdl->{jdl} =~ /\-\-event\s+(\d{1,8})\s+.*/ and $event = $1;
#
#  my $rundir = "";
#  my $eventdir = "";
#  my $newdir = "";
#  if ( (defined $run) && ($run>0) ) {
#    $rundir   = sprintf "%05d", $run;
#    $newdir = "$rundir"; #
#
#    if ( (defined $event) && ($event>0) ) { 
#      $eventdir = sprintf "%05d/%05d", $run,$event;
#      $newdir   = "$eventdir";
#    }
#  } else {
#    ## look for jdl definitions
#    if (( $jdlrun) && ($jdlrun > 0) ) {
#      $rundir = sprintf "%05d",$jdlrun;
#    }#
#
#    if (( $jdlevent) && ($jdlevent ne "") ) {
#      $eventdir = $jdlevent;
#    }#
#
#    if (( $rundir ne "")) {
#      if (($eventdir ne "") ){
#	$newdir = "$rundir".'/'."$eventdir";
#      } else {
#	$newdir = "$rundir";
#      }
#    } else {
#      $eventdir = $subId;
#      $newdir = "$eventdir";
#    }
#  }
#  return $newdir;
#}


1;

