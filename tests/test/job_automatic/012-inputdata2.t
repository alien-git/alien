use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("catalogue/003-add") or exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit(-1);
  my ($dir) = $cat->execute("pwd") or exit(-2);

  addFile(
	$cat, "bin/inputdata2.sh", "#!/bin/bash
date
echo 'This job is not supposed to have any inputdata'
pwd
ls -al
echo 'Checking if the file inputdata2.jdl exists'
\[ -f  inputdata2.jdl ] && exit -2
echo \"YUHUUUUU\"
"
  ) or exit(-2);

  addFile(
	$cat, "jdl/inputdata2.jdl", "
Executable=\"inputdata2.sh\";
InputData={\"$dir/jdl/inputdata2.jdl,nodownload\"};
", 'r'
  ) or exit(-2);

  print "Let's put also a job that gets a file that is in 'no_se'\n";

  my ($info) = $cat->execute("stat", "jdl/inputdata2.jdl") or exit(-2);
  my $pfn    = "guid:///$info->{guid}";
  my $size   = $info->{size};
  $cat->execute("rm", "-rf", "jdl/inputdata2.jdl.link");
  $cat->execute("add", "-r", "jdl/inputdata2.jdl.link", $pfn, $size, "abcedfe") or exit(-2);

  addFile(
	$cat, "jdl/inputdata3.jdl", "
Executable=\"inputdata2.sh\";
InputData={\"$dir/jdl/inputdata2.jdl.link,nodownload\"};
", 'r'
  ) or exit(-2);

  my ($id)  = $cat->execute("submit", "jdl/inputdata2.jdl") or exit(-2);
  my ($id2) = $cat->execute("submit", "jdl/inputdata3.jdl") or exit(-2);
  print "Jobs submitted!!
\#ALIEN_OUTPUT $id $id2\n";
}
