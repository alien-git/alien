use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);

  addFile($cat, "jdl/SplitFile.jdl","Executable=\"CheckInputOuptut.sh\";
Split=\"file\";
InputData=\"LF:${dir}split/*/*\";") or exit(-2);

  my ($id)=$cat->execute("submit", "jdl/SplitFile.jdl") or exit(-2);
  $cat->close();
  print "ok!!\n
\#ALIEN_OUTPUT $id\n";
}
