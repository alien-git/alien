use strict;

use AliEn::UI::Catalogue::LCM::Computer;

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({user => "newuser"});

print "Let's submit a job requiring a package that doesn't exist\n";
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add") or exit(-2);

addFile(
  $cat, "jdl/wrongPackageDep.jdl", "Executable=\"JobWithPackage.sh\";
Packages=\"MyWRONGLS::1.0\""
) or exit(-2);

$cat->execute("submit", "jdl/wrongPackageDep.jdl")
  and print "We were able to submit a jdl with wrong packages :(\n"
  and exit(-2);

print "ok!\n";
