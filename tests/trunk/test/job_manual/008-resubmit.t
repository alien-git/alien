#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }
{

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
$cat or exit (-1);
print "Finding a job that was executed properly\n";
my @jobs=$cat->execute("top", "-status", "DONE");
#@jobs or return;
@jobs or exit 0;

my ($retsb)=$cat->execute("top", "-id", $jobs[0]->{queueId}, "-r");

print "Resubmitting job $jobs[0]->{queueId} (resubmission before resubmit = $retsb->{resubmission})\n";
my ($jobId)=$cat->execute("resubmit", $jobs[0]->{queueId});
$jobId or exit(-2); 

my ($retsa)=$cat->execute("top", "-id", $jobs[0]->{queueId}, "-r");
print "Resubmission after resubmit = $retsa->{resubmission}\n";

$retsa->{resubmission}-1==$retsb->{resubmission} or print "Error increasing resubmission\n" and exit(-3);


print "ok\n Checking that another user can't kill the job...";
my $cat2=AliEn::UI::Catalogue::LCM::Computer->new({user=>"$ENV{USER}"});
$cat2 or exit(-1);
$cat2->execute("whoami");

$cat2->execute("kill", $jobId) and exit(-2);
print "ok\n Killing job $jobId...";
$cat2->close();
$cat->execute("whoami");
$cat->execute("kill", $jobId) or exit(-2);

print "ok\n Checking that it is still dead...";
my ($info)=$cat->execute("top", "-id", $jobId);
($info and $info->{statusId} != "KILLED")
  and print "Error, the job is still alive!! $info->{statusId}\n" and exit(-2);

print "ok\n Let's try to resubmit a job with inputdata... ";

($jobId)=$cat->execute("submit", "jdl/Input.jdl")
  or print "Error submitting a job\n" and exit(-2);
sleep(20);

my ($retsb2)=$cat->execute("top", "-id", $jobId, "-r");

print "Resubmitting job $jobId (resubmission before resubmit = $retsb2->{resubmission})\n";
my ($resubmitId)=$cat->execute("resubmit", $jobId)
  or print "Error resubmitting the job $jobId\n" and exit(-2);
  
my ($retsa2)=$cat->execute("top", "-id", $jobId, "-r");
print "Resubmission after resubmit = $retsa2->{resubmission}\n";

$retsa2->{resubmission}-1==$retsb2->{resubmission} or print "Error increasing resubmission\n" and exit(-3);

print "ok\nThe job has been resubmitted. Let's see if $resubmitId gets to 'WAITING'...";
($info)=$cat->execute("top", "-id", $resubmitId) or print "Error doing top\n" and exit(-2);
print "GOT $info->{statusId}\n";
$info->{statusId} eq "WAITING"
  or print "THE JOB IS NOT WAITING...\n" and exit(-2);
$cat->close;
ok(1);
}
