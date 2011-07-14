use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_JOBAGENT_RETRY} = 1;
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add")                   or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit(-1);

  $cat->execute("cd") or exit(-2);
  my ($dir) = $cat->execute("pwd") or exit(-2);

  my $sename = "$cat->{CONFIG}->{ORG_NAME}::cern::testse";

  addFile(
	$cat, "bin/largeMemJob.sh", "#!/bin/bash 
echo \"#include <unistd.h>\" > /tmp/growmem.cc 
echo \"int main(int argc, char** argv) { \">>/tmp/growmem.cc 
echo \"double* tmp[30];\" >> /tmp/growmem.cc 
echo \"for(int i=0;i<30;i++){\">>/tmp/growmem.cc 
echo \" tmp[i]=new double[1000000];\" >>/tmp/growmem.cc 
echo \" sleep(5); }\" >>/tmp/growmem.cc
echo \"return 0; } \">>/tmp/growmem.cc 
g++ /tmp/growmem.cc -o /tmp/growmem 
/tmp/growmem 
rm /tmp/growmem*
"
  ) or exit(-2);

  addFile(
	$cat, "jdl/largeMemJob_run.jdl", "Executable=\"largeMemJob.sh\";
Memorysize=\{\"1GB\"\};
"
  ) or exit(-2);

  addFile(
	$cat, "jdl/largeMemJob_kill.jdl", "Executable=\"largeMemJob.sh\";
Memorysize=\{\"0.1GB\"\};
"
  ) or exit(-2);

  #
  # -- just run the kill idl ... people can cross check with the run idl by hand
  #

  my ($jobid)  = $cat->execute("submit", "jdl/largeMemJob_kill.jdl") or exit(-2);
  my ($jobid2) = $cat->execute("submit", "jdl/largeMemJob_run.jdl")  or exit(-2);

  print "We have submitted both jobs
\#ALIEN_OUTPUT $jobid $jobid2\n";
  ok(1);
}
