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

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
$cat or exit (-1);
# only the user with role admin can open and add queues
my $admincat=AliEn::UI::Catalogue::LCM::Computer->new({"user","$ENV{'USER'}","role","admin"});
$admincat or exit (-1);
$admincat->execute("debug", "CE ProcessMonitor LQ Service") or exit(-2);
$admincat->execute("queue", "add $cat->{CONFIG}->{ORG_NAME}::CERN::testCE");
$admincat->execute("queue", "open $cat->{CONFIG}->{ORG_NAME}::CERN::testCE") or exit(-2);
print "Let's sleep until the job is ready";
#sleep(20);
print "\n\n\n\n\n\n\n\n\n\n";
my (@jobs)=$cat->execute("top", "-status", "WAITING");
(@jobs) or print "ERROR: THERE ARE NO JOBS WAITING\n" and exit(-2);


print "ok\nRequesting a new job\n";
$cat->execute("request") or exit (-2);
#system ("alien", "proxy-destroy");

waitForStatus($cat, $jobs[0]->{queueId}, "DONE", 5, 6) or exit(-2);

foreach my $job (@jobs) {
  print "Status of $job->{queueId}\n";

  my ($newJobRef)=$cat->execute("top", "-id", $job->{queueId}, );
  print "TENGO $newJobRef\n";

  my $status=$newJobRef->{status};
  $status or print "The job $job is not there!!!\n" and exit(-2);
  print "Status of the job... '$status'\n";
  ($status eq "DONE") or print "Error the job is not DONE !!\n" and exit(-2);
  checkOutput($cat, $job->{queueId}) or exit(-2);

}
$cat->close;

ok(1);
}

