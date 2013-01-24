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

  my $tempFile="/tmp/growmem\$\$";
  addFile(
	$cat, "bin/largeMemJob.sh", "#!/bin/bash 
cat <<EOF > $tempFile.cc
#include <unistd.h>
#include <stdio.h>
int main(int argc, char** argv) {  
double* tmp[30]; 
for(int i=0;i<30;i++){ 
 tmp[i]=new double[1000000]; 
 printf(\"HELLO WORLD \%i\", i);
 fflush(stdout); 
 sleep(5); }
return 0; } 
EOF
echo \"Compiling the file\"
g++ $tempFile.cc -o $tempFile
echo \"Running the job\"
$tempFile
echo \"The job finished successfully\"
rm $tempFile*
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
