use strict;
use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add")                   or exit(-2);
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser", "debug", 5})
  or exit(-1);
my ($dir) = $cat->execute("pwd") or exit(-2);

my $outputDir = "$dir/job/output";
$cat->execute("rmdir", "-rf", $outputDir);

addFile(
  $cat, "jdl/outputDir.jdl", "Executable=\"echo.sh\";
OutputDir=\"$outputDir\";
Arguments=\"It is still a beautiful day\";
"
) or exit(-2);

my ($id) = $cat->execute("submit", "jdl/outputDir.jdl") or exit(-2);

print "We have submitted the jobs!!\n
\#ALIEN_OUTPUT $id $outputDir\n";

