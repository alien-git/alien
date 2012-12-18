#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_JOBAGENT_RETRY} = 1;

  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit(-1);
  my ($jobId) = $cat->execute("submit", "jdl/date.slow.jdl") or exit(-2);

  my $id = fork();
  defined $id or print "ERROR DOING THE FORK\n" and exit(-2);

  if (!$id) {

	#the child
	$cat->execute("request");
	print "The kid finished!!\n";
	exit();
  }
  print "The father sleep for 10 seconds, letting the child do the job\n";
  sleep 25;
  print "The father kills the job:\n";

  $cat->execute("kill", $jobId) or exit(-2);
  sleep 20;
  print "Checking if the child ($id) is still there:\n";
  kill 0, $id and print "THE CHILD IS THERE\n";
  system("ps -Ao command |grep $id");
  print "And let's check the status of the job\n";
  my ($done)=$cat->execute("top", "-id", $jobId);
  use Data::Dumper;
  if ($done){
  	print "The job is still there..\n";
  	print Dumper($done);
  	$done->{status} eq 'KILLED' or print 'AND IT IS NOT DEAD!!\n' and exit(-2);
  }
  
  
  ok(1);
}

