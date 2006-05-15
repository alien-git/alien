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
@jobs or return;
print "Resubmitting job $jobs[0]->{queueId}...";
my ($jobId)=$cat->execute("resubmit", $jobs[0]->{queueId});

$jobId or exit(-2); 
print "ok\n Checking that another user can't kill the job...";
my $cat2=AliEn::UI::Catalogue::LCM::Computer->new();
$cat2 or exit(-1);
$cat2->execute("whoami");

$cat2->execute("kill", $jobId) and exit(-2);
print "ok\n Killing job $jobId...";
$cat2->close();
$cat->execute("whoami");
$cat->execute("kill", $jobId) or exit(-2);

print "ok\n Checking that it is still dead...";
my ($info)=$cat->execute("top", "-id", $jobId);
($info and $info->{status} eq "KILLED")
  or print "Error, the job is still alive!! $info->{status}\n" and exit(-2);

print "ok\n Let's try to resubmit a job with inputdata... ";
($jobId)=$cat->execute("submit", "jdl/Input.jdl")
  or print "Error submitting a job\n" and exit(-2);
sleep(20);
my ($resubmitId)=$cat->execute("resubmit", $jobId)
  or print "Error resubmitting the job $jobId\n" and exit(-2);

print "ok\nThe job has been resubmitted. Let's see if $resubmitId gets to 'WAITING'...";
sleep(20);
($info)=$cat->execute("top", "-id", $resubmitId) or print "Error doing top\n" and exit(-2);
print "GOT $info->{status}\n";
$info->{status} eq "WAITING"
  or print "THE JOB IS NOT WAITING...\n" and exit(-2);
$cat->close;

ok(1);
}
