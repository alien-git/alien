use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("catalogue/003-add") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user" => "newuser"}) or exit(-2);

addFile(
  $cat, "bin/bigOutput.sh", "#!/bin/sh
echo \"This is creating a huge file\"
dd if=/dev/zero of=filename bs=4k count=1000
echo \"File created. Let's wait for one minute to see if the job gets killed\"
sleep 100
echo \"The job didn't get killed\"
"
) or exit(-2);

addFile(
  $cat, "jdl/bigOutput.jdl", "Executable=\"bigOutput.sh\";
Workdirectorysize =  { \"2MB\" };
"
) or exit(-2);

addFile(
  $cat, "jdl/bigOutputWorks.jdl", "Executable=\"bigOutput.sh\";
Workdirectorysize =  { \"10MB\" };
"
) or exit(-2);

my ($id)  = $cat->execute("submit", "jdl/bigOutput.jdl")      or exit(-2);
my ($id2) = $cat->execute("submit", "jdl/bigOutputWorks.jdl") or exit(-2);
print "Job Submitted!!
\#ALIEN_OUTPUT $id $id2\n";
