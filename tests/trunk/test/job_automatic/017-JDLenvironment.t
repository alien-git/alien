use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);

addFile(
  $cat, "bin/jdlEnvironment.sh", "#!/bin/sh
echo \"This checks if some environment variables are created\"
echo \"ALIEN_JDL_MY_VARIABLE=\$ALIEN_JDL_MY_VARIABLE\"
"
) or exit(-2);

addFile(
  $cat, "jdl/jdlEnvironment.jdl", "Executable=\"jdlEnvironment.sh\";
JDLVARIABLES={\"MY_VARIABLE\"};
MY_VARIABLE=\"Hello world\"
"
) or exit(-2);

my ($id) = $cat->execute("submit", "jdl/jdlEnvironment.jdl") or exit(-2);

$cat->close();
print "Job submitted!!
\#ALIEN_OUTPUT $id\n"

