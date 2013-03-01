use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);

addFile(
  $cat, "jdl/Archive_without_links.jdl", "Executable=\"CheckInputOuptut.sh\";
OutputArchive={\"my_archive:stdout,stderr\@no_links_registration\"};
"
) or exit(-2);

my ($id) = $cat->execute("submit", "jdl/Archive_without_links.jdl") or exit(-2);

$cat->close();
print "Job submitted!!
\#ALIEN_OUTPUT $id\n"
