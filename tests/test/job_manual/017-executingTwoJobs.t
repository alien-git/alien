use strict;

use AliEn::UI::Catalogue::LCM::Computer;
$ENV{ALIEN_JOBAGENT_RETRY} = 1;
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add")                   or exit(-2);
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $file = "/tmp/alien_test.141.$$";

print "Redirecting the output to $file\n";
open(SAVEOUT, ">&STDOUT") or print "ERROR redirecting the output\n" and exit(-2);
open(SAVEOUT, ">&STDOUT") or print "ERROR redirecting the output\n" and exit(-2);
open(STDOUT,  ">$file")   or print "Error redircting to $file\n"    and exit(-2);
open(STDERR,  ">&STDOUT") or print "Error redircting to $file\n"    and exit(-2);

eval {
	
  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({user => 'newuser'}) or die("Error creating the UI");

  $cat->execute("rm", "bin/echo.sh", "jdl/sendTwoJobs1.jdl", "jdl/sendTwoJobs2.jdl");

  addFile(
	$cat, "bin/echo.sh", "#!/bin/bash
date
echo 'Hello World'
echo \"I've been called with '\$*'\"
"
  ) or die("Error adding the script");

  addFile(
	$cat, "jdl/sendTwoJobs1.jdl", "Executable=\"echo.sh\";
Arguments=\"This is the first job\";
Requirements= other.CE==other.CE;
"
  ) or die("Error adding file");

  addFile(
	$cat, "jdl/sendTwoJobs2.jdl", "Executable=\"echo.sh\";
Arguments=\"This is the second job\";
Requirements= other.HOST==other.HOST;
"
  ) or die("Error adding file");

  killAllWaitingJobs($cat);

  my ($id1) = $cat->execute("submit", "jdl/sendTwoJobs1.jdl")
	or die("Error submitting job");
  my ($id2) = $cat->execute("submit", "jdl/sendTwoJobs2.jdl")
	or die("Error submitting job");
  my $ready = 0;
  for (my $i = 0 ; $i < 4 ; $i++) {
	sleep(5);
	my ($info, $info2) = $cat->execute("top", "-id", $id2, "-id", $id1);
	$info->{status} eq "WAITING" and $info2->{status} eq "WAITING" and $ready = 1 and last;
  }
  if (not $ready) {
	print "THE JOBS WERE NOT WAITING\n";
	system("ps -ef ");
	print "AND ONLY MY USER\n";
	system("ps -f -u $<");
	die("The jobs are not WAITING!!\n");
  }
  $cat->execute("request") or die("Error requesting a job");

  my $i;
  for ($i = 0 ; $i < 4 ; $i++) {
	print "Sleeping before checking the status of the jobs\n";
	sleep(10);
	my ($info, $info2) = $cat->execute("top", "-id", $id1, "-id", $id2);
	 $info->{status}  eq "DONE" or next;
   $info2->{status} eq "DONE" or next;
	 print "Both jobs are done\n" and last;
  }
  $i >3  and  die("NOPE!! the jobs are not done yet\n");
};
my $error = $@;

open(STDOUT, ">&SAVEOUT");
open(STDERR, ">&SAVEOUT");

print "Let's take a look at what happened $error\n";

open(my $fileContent, "<", $file) or print "Error looking at the output\n" and exit(-2);
my @output = <$fileContent>;
close $fileContent;
unlink $file;
print "GOT @output\n";

if ($error) {
  print "ERROR $error\n";
  exit(-2);
}

print "Making sure that we started two agents\n";

if (! grep(/Starting 2 agent\(s\) for /, @output)){
  my @agents = grep (/Starting 1 agent\(s\) for /, @output);

  $#agents > 0 or print "We only started $#agents !!\n" and exit(-2);
}

print "ok!\n";
