package AliEn::Service::Optimizer::Job::Merging;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::Service::Manager::Job;

use Data::Dumper;

use vars qw(@ISA);
push(@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self   = shift;
  my $silent = shift;

  my $method = "info";
  $silent and $method = "debug";

  $self->{LOGGER}->$method("Merging", "The merging optimizer starts");

  $self->{DB}->queryValue("SELECT todo from ACTIONS where action='MERGING'")
    or return;
  $self->{DB}->update("ACTIONS", {todo => 0}, "action='MERGING'");

  $self->info("There are some jobs to check!!");

  my $jobs = $self->{DB}->query(
    "select queueid,  statusid, origjdl jdl,user from  
     QUEUE q join JOBSTOMERGE j on (q.queueid=j.masterid)
     join QUEUE_USER using (userid)
     join QUEUEJDL using (queueid)"
  );    #SPLIT, FORCEMERGE

  $self->info("We got the jobs from the db");

  foreach my $job (@$jobs) {
    $self->{DB}->delete("JOBSTOMERGE", "masterId=?", {bind_values => [ $job->{queueid} ]});

    $self->info("Doing something with $job->{queueid}");
    if (  $job->{statusid} != AliEn::Util::statusForML('SPLIT')
      and $job->{statusid} != AliEn::Util::statusForML('FORCEMERGE')) {

      $self->info("The job is not in SPLIT or in FORCEMERGE. Skipping it");
      next;
    }

    if ($job->{jdl}) {
      my $job_ca = AlienClassad::AlienClassad->new($job->{jdl});
      if ($job_ca->isOK()) {
        $self->updateMerging($job->{queueid}, $job_ca, $job->{statusid}, $job->{user});
        next;
      }
      $self->info("JobOptimizer: in checkJobs incorrect JDL input\n" . $job->{jdl});
    } else {
      $self->info("Error getting jdl of the job '$job->{queueid}'");
    }
    $self->{DB}->updateStatus($job->{queueid}, "%", "ERROR_M");
  }

  my $done3 = $self->checkJobs($silent, 13, "checkMerging", 100);    #MERGING

  $self->{LOGGER}->$method("Merging", "The merging optimizer finished");

  return;
}

sub checkMerging {
  my $self    = shift;
  my $queueid = shift;
  my $job_ca  = shift;
  my $status  = shift;

  $status = AliEn::Util::statusName($status);

  my $newStatus = "DONE";
  $self->info("Checking if the merging jobs of $queueid have finished");

  eval {
    my $info = $self->{DB}->query(
      "SELECT merging, user from QUEUE join QUEUE_USER using (userid) where
                                queueid=?", undef, {bind_values => [$queueid]}
      )
      or die("Error getting the merging jobs of $queueid");

    my @subjobs = split(",", $info->{merging});
    my $user    = $info->{user};

    $self->{CATALOGUE}->execute("user", "-", "$user")
      or die("Error changing to user $user");
    #my @finished;
    my @running;
    foreach my $subjob (@subjobs) {
      $subjob or next;
      $self->info("Checking if the job $subjob has finished");
      my $status = $self->{DB}->getFieldFromQueue($subjob, "statusId")
        or die("Error getting the status of $subjob");

      $status =~ /DONE/ and next ; #push(@finished, $subjob) and next;
      if ($status =~ /(ERROR_)|(FAILED)/) {
        $self->info("Skipping job $subjob (in $status)");
        $self->putJobLog($queueid, "error", "One of the merging jobs ($subjob) failed or was killed");
        next;
      }
      push @running, $subjob;
    }
#    my $procDir = "~/alien-job-$queueid";
#    foreach my $job (@finished) {
#      my $subProcDir = "~/alien-job-$job";
#      $self->info("Let's copy the output of $job");
#      $self->putJobLog($queueid, "state", "The merging $job finished!!");
#      $self->{CATALOGUE}->execute("mkdir", "-p", "$procDir/merged", "$procDir/merge-logs")
#        or die("error creating the directories");
#      $self->{CATALOGUE}
#        ->execute("cp", "$subProcDir/job-log/execution.out", "$procDir/merge-logs/execution.$job.out", "-user", $user);
#      my @files = $self->{CATALOGUE}->execute("find", "$subProcDir/job-output", "*");
#      foreach my $file (@files) {
#        $file =~ /\/((stdout)|(stderr)|(resources)|(alien_archive.*))$/ and next;
#        $self->info("Copying $file");
#        my $basename = $file;
#        $basename =~ s{^.*/([^/]*)$}{$1};
#        if ($self->{CATALOGUE}->{CATALOG}->isFile("$procDir/merged/$basename")) {
#          $self->info("Removing the previous merge");
#          $self->{CATALOGUE}->execute("rm", "$procDir/merged/$basename");
##        }
#        if (!$self->{CATALOGUE}->execute("cp", $file, "$procDir/merged/")) {
#          $self->info("Error copying $file");
#          $self->putJobLog($queueid, "error", "Error copying $file");
#        }
#      }
#
#    }
    my $newMerging = join(",", @running);
    $self->info("Now we have to wait for '$newMerging'");
    $newStatus = "DONE";
    $self->{DB}->update("QUEUE", {merging => $newMerging}, "queueid=?", {bind_values => [$queueid]});
    if (@running) {
      $self->info("The jobs @running are still running");
      $newStatus = undef;
    }

  };
  if ($@) {
    $self->info("Something failed: $@");
    $newStatus = "ERROR_M";
  }

  $newStatus or return;
  my $message = "Job state transition from $status to $newStatus";
  $self->{DB}->updateStatus($queueid, $status, $newStatus)
    or $message = "Failed: $message";
  $self->putJobLog($queueid, "state", $message);

  return 1;
}

sub checkMasterResubmition {
  my $self    = shift;
  my $user    = shift;
  my $queueid = shift;
  my $job_ca  = shift;
  my $rparts  = shift;
  $self->info("Checking if the masterjob has some kind of resubmition");
  my ($ok, $number) = $job_ca->evaluateAttributeString("MasterResubmitThreshold");
  $ok or return 1;
  ($ok, my $type) = $job_ca->evaluateAttributeString("ResubmitType");
  $type or $type = "system";

  my @status = ();
  if ($type =~ /^system$/) {
    push @status, 'EXPIRED', 'ERROR_IB', 'ERROR_E', 'FAILED', 'ERROR_SV', 'ERROR_A';
  } elsif ($type =~ /^all$/) {
    push @status, 'EXPIRED', 'ERROR_IB', 'ERROR_E', 'FAILED', 'ERROR_SV', 'ERROR_A', 'ERROR_V';
  } else {
    push @status, split(",", $type);

  }
  $self->info("Checking the jobs in @status (they have to be less than $number)");
  my $failed = 0;
  my $total  = 0;

  foreach my $p (@$rparts) {
    $total += $p->{count};
    if (grep (/^$p->{status}$/i, @status)) {
      $self->info("This is a job that will be resubmitted");
      $failed += $p->{count};
    }
  }
  my $resubmit = 0;
  if ($number =~ s/\%//) {
    $self->info("Resubmitting if more than $number % out of  $total");
    if ($total){ 
      $failed * 100.0 / $total > $number and $resubmit = 1;
    }
  } else {
    $failed > $number and $resubmit = 1;
  }
  $resubmit
    or $self->info("We don't resubmit any jobs. Only $failed failures, and we can have up to $number")
    and return 1;

  $self->info("We have to resubmit some of the jobs!!");
  my @newstatus;
  foreach my $s (@status) {
    push @newstatus, "-status", $s;
  }
  $self->putJobLog($queueid, "info", "Automatic resubmition of jobs in $type (there were $failed)");
  eval {

    my $status = $self->{CATALOGUE}->execute("masterJob", $queueid, "resubmit", @newstatus);
    $status eq "-1" and die("resubmition failed: \n");
  };
  if ($@) {
    $self->info("The resubmition didn't work : $@");
  }

  $self->info("Back to @ISA");
  return 1;
}

sub updateMerging {
  my $self    = shift;
  my $queueid = shift;
  my $job_ca  = shift;
  my $statusId  = shift;
  my $user    = shift;

  my $status = AliEn::Util::statusName($statusId);
  my $newStatus = "DONE";
  my $set       = {};
  $self->info("Here we go, with $status (from $statusId) ");
  eval {
    my $rparts = $self->{DB}->query("SELECT count(*) as count, status from QUEUE 
      join QUEUE_STATUS using (statusid)
      WHERE split=? GROUP BY statusId",undef,
      {bind_values => [$queueid]}
      )
      or die("Could not get splitted jobs for $queueid");

    $self->{CATALOGUE}->execute("user", "-", "$user")
      or die("Error changing to user $user");

    if (!$self->checkMaxFailed($job_ca, $rparts)) {
      $self->putJobLog($queueid, "error", "There were too many subjobs failing");
      my ($status, $error) =
        $self->{CATALOGUE}->execute("masterJob", $queueid, "kill", "-status", "WAITING", "-status", "INSERTING");
      return;
    }

    $self->checkMasterResubmition($user, $queueid, $job_ca, $rparts) or return;
    if ($#{$rparts} > -1) {
      $self->info("Jobs for $queueid");
      for (@$rparts) {
        $self->info("Checking $_->{count} Jobs  in $_->{status}");

        # force the copy of a split job
        if ($status eq "TERMSPLIT") {
          if ($_->{status} =~ /RUNNING/) {
            $self->info("There are still jobs running");
            $newStatus = undef;
            return;
          }
        }

        if ($status eq "SPLIT") {
          if ($_->{status} !~ /(DONE)|(FAILED)|(KILLED)|(EXPIRED)|(ERROR_)/) {
            $newStatus = undef;
            $self->info("There are still jobs running");
            return;
          }
        }
      }
      $self->info("All the jobs finished. Checking best place for execution (user $user)");

      my ($info) = $self->{DB}->getFieldsFromQueue($queueid, "merging")
        or die("Job $queueid doesn't exist");

      $self->checkMergingCollection($job_ca, $queueid);
      $self->checkMergingSubmission($job_ca, $queueid, $user, $set, $info)
        or die("Error doing the submission of the merging jobs");
      $self->checkFileBroker($queueid);
      if ($set->{newStatus}) {
        $status = $newStatus;
        delete $set->{newStatus};
      }
    }
  };
  if ($@) {
    $newStatus = "ERROR_M";
    $self->info("Error updating the job $queueid: $@");
  }
  if ($newStatus) {
    my $message = "Job state transition from $status to $newStatus";
    delete $set->{newStatus};
    $self->{DB}->updateStatus($queueid, $status, $newStatus, $set)
      or $message = "Failed: $message";
    $self->putJobLog($queueid, "state", $message);
  }
  return 1;
}

sub checkFileBroker {
  my $self  = shift;
  my $split = shift;
  $self->info("DELETING FROM FILES_BROKER");
  $self->{DB}->do("DELETE from FILES_BROKER where split=?", {bind_values => [$split]});
}

sub checkMaxFailed {
  my $self   = shift;
  my $job_ca = shift;
  my $rparts = shift;

  $self->info("Checking if there are too many failures");
  my ($ok, $failed) = $job_ca->evaluateAttributeString("MaxFailed");
  $ok or ($ok, $failed) = $job_ca->evaluateExpression("MaxFailed");
  ($ok, my $initFailed) = $job_ca->evaluateAttributeString("MaxInitFailed");
  $ok or ($ok, $initFailed) = $job_ca->evaluateExpression("MaxInitFailed");
  $self->info("We accept $failed and $initFailed");
  ($failed or $initFailed) or return 1;

  my $total   = 0;
  my $failure = 0;
  my $success = 0;
  foreach my $p (@$rparts) {
    $total += $p->{count};
    $p->{status} =~ /^ERROR/ and $failure += $p->{count};
    $p->{status} =~ /^DONE/  and $success += $p->{count};
  }
  $self->info("The job has $total ($failure/$success)");
  if ($failed) {
    $self->info("Checking that there are no more than '$failed' jobs");
    if ($failed =~ s/\%//) {
      $failed > ($failure / $total) and return;
    } else {
      $failed < $failure and return;
    }
  }

  if ($initFailed) {
    $self->info("Checking that there are no more thatn '$initFailed' initial failures");
    if (!$success) {
      if ($initFailed =~ s/\%//) {
        $initFailed > ($failure / $total) and return;
      } else {
        $failed < $failure and return;
      }
    }
  }
  return 1;
}

sub checkMergingSubmission {
  my $self    = shift;
  my $job_ca  = shift;
  my $queueid = shift;
  my $user    = shift;
  my $set     = shift;
  my $info    = shift;

  my $oldmerging = $info->{merging} || "";

  my ($ok, @merge) = $job_ca->evaluateAttributeVectorString("Merge");

  my $outputD = "~/alien-job-$queueid/merge";
  ($ok, my $t) = $job_ca->evaluateAttributeString("OutputDir");
  $t =~ s/\#.*$//;
  $ok and $outputD = $t;
  ($ok, $t) = $job_ca->evaluateAttributeString("MergeOutputDir");
  $ok and $outputD = $t;

  my @subjobs;
  foreach my $merge (@merge) {
    $set->{newStatus} = "MERGING";

    #Ok, let's submit the job that will merge the output
    $self->info("We have to submit the job $merge");
    my ($file, $jdl, $output) = split(":", $merge);

    $self->{CATALOGUE}->{CATALOG}->{ROLE} = $user;
    my ($id) = $self->{CATALOGUE}->execute("submit", "$jdl $queueid $file $output $user $outputD");
    $self->{CATALOGUE}->{CATALOG}->{ROLE} = "admin";

    $id or $self->info("Error submitting the job $jdl") and return;
    push @subjobs, $id;
  }
  (@subjobs) and $set->{merging} = join(",", $oldmerging, @subjobs);

  return 1;
}

sub checkMergingCollection {
  my $self    = shift;
  my $job_ca  = shift;
  my $queueid = shift;

  $self->info("***Let's check if there are any collections");

  my ($ok, @mergingCollections) = $job_ca->evaluateAttributeVectorString("MergeCollections");
  @mergingCollections or return 1;

  my $subjobs =
    $self->{DB}
    ->query("select resultsjdl JDL,queueid from QUEUE join QUEUEJDL using (queueid) where statusId=15 and split=?",
    undef, {bind_values => [$queueid]});    #DONE

  my @out = ();
  foreach my $d (@$subjobs) {
    $self->info("Checking the outputdir of $d");
    my $ca = AlienClassad::AlienClassad->new($d->{JDL});
    my ($ok, $dir) = $ca->evaluateAttributeString("OutputDir");
    $ok or $dir = "~/alien-job-$d->{queueid}";
    push @out, $dir;
  }

  $self->info("AT THE END, @out");

  foreach my $entry (@mergingCollections) {
    $self->putJobLog($queueid, "info", "Creating the merging collection '$entry'");
    my ($files, $collection) = split(/:/, $entry, 2);

    eval {
      $self->{CATALOGUE}->execute("rm",               $collection);
      $self->{CATALOGUE}->execute("createCollection", $collection)
        or die("error creating the collection: '$collection'");
      my @patterns = split(/,/, $files);
      foreach my $pattern (@patterns) {

        #This is not going to work, since the files are in different places...
        #my @files=$self->{CATALOGUE}->execute("find", "$procDir/subjobs", $pattern);

        my @files = ();
        foreach my $dir (@out) {
          push @files, $self->{CATALOGUE}->execute("find", $dir, $pattern);
        }
        foreach my $file (@files) {
          $self->{CATALOGUE}->execute("addFileToCollection", "-n", $file, $collection)
            or die("Error adding the file '$file' to the collection");
        }
      }
      $self->{CATALOGUE}->execute("updateCollection", $collection)
        or die("Error updating the collection");
    };

    if ($@) {
      $self->putJobLog($queueid, "error", "Error creating the collection: $@");
      return;
    }
  }

  return 1;
}

1;

