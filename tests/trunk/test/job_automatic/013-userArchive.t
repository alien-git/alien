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

  $cat->execute("cd") or exit(-2);
  my ($dir) = $cat->execute("pwd") or exit(-2);

  my $sename = "$cat->{CONFIG}->{ORG_NAME}::cern::testse";

  addFile(
	$cat, "jdl/UserArchive.jdl", "Executable=\"CheckInputOuptut.sh\";
InputFile=\"LF:$dir/jdl/Input.jdl\";
OutputArchive={\"my_archive:stdout,stderr,file.out\@${sename}2\"}"
  ) or exit(-2);

  my ($id) = $cat->execute("submit", "jdl/UserArchive.jdl") or exit(-2);    #

  $cat->close();
  print "JOB submitted
\#ALIEN_OUTPUT $id\n";

}
