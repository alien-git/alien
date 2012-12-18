use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add")                   or exit(-2);
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);
my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);

$cat->execute("rmdir", "-rf", "inputCollection", "-silent");
$cat->execute("mkdir", "inputCollection");

addFile(
  $cat, "inputCollection/inputFile.txt", "This is just a test
"
) or exit(-2);

$cat->execute("find", "-x myCollection inputCollection txt ", ">inputCollection/inputDataCollection.xml") or exit(-2);

addFile(
  $cat, "jdl/InputCollection.jdl", "Executable=\"CheckInputOuptut.sh\";
InputDataCollection=\"LF:inputCollection/inputDataCollection.xml\";
"
) or exit(-2);

my $procDir = executeJDLFile($cat, "jdl/InputCollection.jdl") or exit(-2);

my ($output) = $cat->execute("get", "$procDir/stdout") or exit(-2);

open(FILE, "<$output") or print "Error opening $output\n" and exit(-2);
my @content = <FILE>;
close FILE;

print "Got @content\n";

grep (/inputFile.txt/, @content) or print "The file inputFile.txt wasn't there!!\n" and exit(-2);

print "Let's try with the job that is supposed to fail
First, we remove the file\n";

$cat->execute("rm", "inputCollection/inputFile.txt") or exit(-2);
print "And now we try to submit the job\n";
$procDir = executeJDLFile($cat, "jdl/InputCollection.jdl", "ERROR_I")
  or exit(-2);

print "YUHUUU!!\n";
