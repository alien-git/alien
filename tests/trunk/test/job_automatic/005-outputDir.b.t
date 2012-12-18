use strict;
use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add")                   or exit(-2);
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);
my ($dir) = $cat->execute("pwd") or exit(-2);

my $id        = shift or print "Error getting the job id\n"           and exit(-2);
my $outputDir = shift or print "Error getting the output directory\n" and exit(-2);

my $procDir = checkOutput($cat, $id, $outputDir) or print "Could not check output" and exit(-2);

print "And let's check if the file is in the outputdir\n";

my ($info) = $cat->execute("stat", "$outputDir/stdout") or exit(-2);
use Data::Dumper;
print Dumper($info);

print "ok\n";
