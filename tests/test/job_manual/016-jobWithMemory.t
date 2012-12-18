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
	$cat, "bin/checkMemory.sh", "#!/bin/bash
echo \"Checking how much free memory we have\"
free"
  ) or exit(-2);

  addFile(
	$cat, "jdl/checkMemory.jdl", "Executable=\"checkMemory.sh\";
Memory=20;
"
  ) or exit(-2);

  addFile(
	$cat, "jdl/checkTooMuchMemory.jdl", "Executable=\"checkMemory.sh\";
Memory=200000000;
"
  ) or exit(-2);

  print "Let's submit a file that requests too much memory\n";
  my ($id) = $cat->execute("submit", "jdl/checkTooMuchMemory.jdl") or exit(-2);

  my $ready = 0;
  for (my $i = 0 ; $i < 4 ; $i++) {
	sleep(5);
	my ($info) = $cat->execute("top", "-id", $id);
	$info->{status} eq "WAITING" and $ready = 1 and last;
  }
  $ready or print "The job is not WAITING!!\n" and exit(-2);
  my $fileName="/tmp/alien_output.$$";
  print "Let's try to execute the job... output in $fileName\n";
  open my $SAVEOUT, ">&", STDOUT;
  open my $SAVEERR, ">&", STDERR;
  
  open STDOUT, ">", $fileName;
  open STDERR, ">", $fileName;
  my ($got) = $cat->execute("jobListMatch", $id) or exit(-2);
  open STDOUT, ">&", $SAVEOUT;
  open STDERR, ">&", $SAVEERR;
  open my $FILE2, "<", $fileName;
  my $content=join("", <$FILE2>);
  close $FILE2;
  print "We got $content\n";
  #unlink $fileName;
  if ($content !~ /In total, there are 0 sites that match/) {
	print "There are some sites that match!!\n";
	$cat->execute("top");
	exit(-2);
  }
  print "Yuhuu!!\n";

}
