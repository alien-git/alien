use strict;
use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("26-ProcessMonitorOutput") or exit(-2);


my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my ($dir)=$cat->execute("pwd") or exit (-2);


my $outputDir="$dir/job/output";
$cat->execute("rmdir", "-rf", $outputDir);

addFile($cat, "jdl/outputDir.jdl","Executable=\"echo.sh\";
OutputDir=\"$outputDir\";
Arguments=\"It is still a beautiful day\";
") or exit(-2);

my $procDir=executeJDLFile($cat, "jdl/outputDir.jdl") or exit(-2);

print "Checking if the output was copied to $outputDir\n";
$cat->execute("ls", "-la", $outputDir) or exit(-2);
print "Getting the stdout\n";
$cat->execute("get", "$outputDir/stdout") or exit(-2);

print "OK!!\n";
