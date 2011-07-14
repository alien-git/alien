use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("catalogue/003-add") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);

my $vo      = $cat->{CONFIG}->{ORG_NAME};
my $otherSE = "${vo}::cern::testSE2";
addFile(
  $cat, "jdl/specificSE.jdl", "
executable=\"date\";
OutputFile ={\"stderr\@$otherSE\",\"stdout\@$otherSE\"};", "r"
) or exit(-2);

my ($id) = $cat->execute("submit",, "jdl/specificSE.jdl") or exit(-2);

print "OK\n
\#ALIEN_OUTPUT $id\n";

