use strict;

use AliEn::UI::Catalogue::LCM::Computer;
$ENV{ALIEN_JOBAGENT_RETRY}=1;
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("26-ProcessMonitorOutput") or exit(-2);

my $file="/tmp/alien_test.141.$$"; 

print "Redirecting the output to $file\n";
open (SAVEOUT, ">&STDOUT") or print "ERROR redirecting the output\n" and exit(-2);
open (SAVEOUT, ">&STDOUT") or print "ERROR redirecting the output\n" and exit(-2);
open (STDOUT, ">$file") or print "Error redircting to $file\n" and exit(-2);
open (STDERR, ">&STDOUT") or print "Error redircting to $file\n" and exit(-2);


eval {
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({user=>'newuser'}) or die("Error creating the UI");
  addFile($cat, "bin/echo.sh","#!/bin/bash
date
echo 'Hello World'
echo \"I've been called with '\$*'\"
") or die ("Error adding the script");


  addFile($cat, "jdl/sendTwoJobs1.jdl","Executable=\"echo.sh\";
Arguments=\"This is the first job\";
Requirements= other.CE==other.CE;
") or die("Error adding file");

  addFile($cat, "jdl/sendTwoJobs2.jdl","Executable=\"echo.sh\";
Arguments=\"This is the second job\";
Requirements= other.HOST==other.HOST;
") or die ("Error adding file");

  killAllWaitingJobs($cat);

  my ($id1)=$cat->execute("submit", "jdl/sendTwoJobs1.jdl") or 
  die("Error submitting job") ;
  my ($id2)=$cat->execute("submit", "jdl/sendTwoJobs2.jdl") or 
  die ("Error submitting job");
  my $ready=0;
  for (my $i=0; $i<4; $i++) {
    sleep(5);
    my ($info)=$cat->execute("top", "-id", $id2);
    $info->{status} eq "WAITING" and $ready=1 and last;
  }
  $ready or print "The job is not WAITING!!\n" and exit(-2);
  $cat->execute("request") or exit(-2); 

  system ("alien", "proxy-destroy");
  sleep(10);

  my ($info)=$cat->execute("top", "-id", $id1);
  $info->{status} eq "DONE" or
    print "NOPE!! the status is $info->{status}\n" and  exit(-2);

  ($info)=$cat->execute("top", "-id", $id2);
  $info->{status} eq "DONE" or
    print "NOPE!! the status is $info->{status}\n" and  exit(-2);
};
my $error=$@;

open (STDOUT, ">&SAVEOUT");
open (STDERR, ">&SAVEOUT");

print "Let's take a look at what happened $error\n";

open(FILE, "<$file") or print "Error looking at the output\n" and exit(-2);
my @output=<FILE>;
close FILE;
unlink $file;
print "GOT @output\n";


if ($error) {
  print "ERROR $error\n"; 
  exit(-2);
}


print "Making sure that we started two agents\n";

my @agents=grep (/Starting 1 agent\(s\) for /,@output);

$#agents>0 or print "We only started $#agents !!\n" and exit(-2);

print "ok!\n";
