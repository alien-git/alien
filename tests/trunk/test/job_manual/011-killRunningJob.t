#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
$ENV{ALIEN_JOBAGENT_RETRY}=1;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
$cat or exit (-1);
my ($jobId)=$cat->execute("submit", "jdl/date.slow.jdl") or exit(-2);
sleep (15);

my $id=fork();
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
#$cat->execute("kill", $jobId) or exit(-2);
sleep 20;
print "Checking if the child ($id) is still there:\n";
kill 0, $id and print "THE CHILD IS THERE\n";
system("ps -Ao command |grep $id");
system("alien", "proxy-destroy");
ok(1);
}

