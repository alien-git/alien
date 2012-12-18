#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);

  addFile($cat, "jdl/Split.jdl","Executable=\"CheckInputOuptut.sh\";
Split=\"directory\";
InputData=\"LF:${dir}split/*/*\";") or exit(-2);

  $cat->execute("rmdir", "-rf", "split", "-silent") ;
  $cat->execute("mkdir", "-p", "split/dir1") or exit(-2);
  $cat->execute("mkdir", "-p", "split/dir2") or exit(-2);

  addFile($cat, "split/dir1/file1", "This is a test") or exit(-2);
  $cat->execute("cp", "split/dir1/file1", "split/dir1/file2") or exit(-2);
  $cat->execute("cp", "split/dir1/file1", "split/dir2/file3") or exit(-2);

  my ($id)=$cat->execute("submit", "jdl/Split.jdl") or exit(-2);#
  
  $cat->close();
  print "JOB submitted
\#ALIEN_OUTPUT $id\n";

}

sub executeSplitJob{
  my $cat=shift;
  my $jdl=shift;
  my $options=shift || {};
  $ENV{ALIEN_JOBAGENT_RETRY}=1;

  includeTest("26-ProcessMonitorOutput") or exit(-2);

  killAllWaitingJobs($cat);
  my ($id)=$cat->execute("submit", $jdl) or return;
  print "GOT $id\n";
			 
  print "Checking if there are any warnings during the top...";

  waitForStatus($cat, $id, "SPLIT", 5,5) or return;

  my (@subjobs)=$cat->execute("top", "-split", $id, "-all_status") or return;

  my $number=$#subjobs+1;
  print "Job split in $number entries\n";

  print "Let's wait until the split jobs are ready\n";
  sleep (10);
  system("alien", "proxy-destroy");
  # only the user with role admin can open and add queues
  my $admincat=AliEn::UI::Catalogue::LCM::Computer->new({"user","$ENV{'USER'}","role","admin"});
  $admincat or exit (-1);
  $admincat->execute("debug", "CE ProcessMonitor LQ Service") or exit(-2);
  $admincat->execute("queue", "open $cat->{CONFIG}->{ORG_NAME}::CERN::testCE") 
    or return;
  my $done=0;
  $cat->execute("top", "-split", $id, "-all_status") or return;

  while (1){
    my ($status)=$cat->execute("request") or last;
    $status <0  and last;
    print "Got $status"; 
    $done=1;
    last;
  }
  system("alien", "proxy-destroy");
  $done or print "No jobs were executed \n" and return;
  print "The job finished!! Let's wait until it is merged\n";

  waitForStatus($cat, $id, "DONE", 5,5) or return;

  (@subjobs)=$cat->execute("top", "-split", $id, "-all_status") or return;
  my ($user)=$cat->execute("whoami") or return;
  my $procDir="~/alien-job-$id";

  if (! $options->{noSubjobs}) {
    foreach my $job (@subjobs) {
      $job->{status} eq "DONE" or 
	print "The subjob is not done!! $job->{status}\n" and return;
      my $id=$job->{queueId};
      $cat->execute("ls", "$procDir/subjobs/$id") or print "The directory $procDir/$id doesn't exist" and return;
    }
  }
  print "ExecuteSplitJob finished successfully with $procDir and $number\n\n";
  return (1, $procDir, $number);
}

sub checkSubJobs{
  my $cat=shift;
  my $id=shift;
  my $jobs=shift;
  my $options=shift || {};
  print "Checking if $id was split in $jobs (and all of them finished with DONE\n";
  my ($status)=$cat->execute("top", "-id", $id) or exit(-2);
  my ($info)=$cat->execute("masterJob", $id, "-printid") or exit(-2);
  $status->{status} eq "DONE" or print "The job is not done!!\n" and exit(-2);

  my ($user)=$cat->execute("whoami");
  my $subjobs=0;
  my $expected={DONE=>$jobs};
  $options->{expected} and $expected=$options->{expected};
  use Data::Dumper;
  my $ids={};
  foreach my $s (@$info){
    print Dumper($s);
    my $status=$s->{status};
    $ids->{$status}=$s->{ids};
    $subjobs+=$s->{count};
    $expected->{$status} eq $s->{count} or print "There are $s->{count} $status jobs, and we were expecting $expected\n" and exit(-2);
    $expected->{$status}=0;
  }
  foreach my $t (keys %$expected){

    if ($expected->{$t}){
      print "We expected $expected->{$t} jobs in $t\n" and exit(-2);
    }
  }

  return "~/alien-job-$id", $ids;
}
