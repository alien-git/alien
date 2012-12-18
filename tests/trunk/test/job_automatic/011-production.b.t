use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("job_automatic/008-split") or exit(-2);

my $id = shift or print "No job to analyze!!\n" and exit(-2);
my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);
my ($dir) = $cat->execute("pwd") or exit(-2);
my $outputDir = "$dir/production";

my ($procDir) = checkSubJobs($cat, $id, 5, {expected => {DONE => 4, "ERROR_V" => 1}})
  or exit(-2);

print "Production executed\n";

print "Let's check that the output dir is ok\n";

my @entries = $cat->execute("ls", $outputDir) or exit(-2);

$#entries eq "3" or print "There are too many entries!! @entries\n" and exit(-2);
foreach (@entries) {
  print "\tChecking $_\n";
  my ($file) = $cat->execute("get", "$outputDir/$_/stdout", "-silent")
	or print "Error getting $_\n" and exit(-2);
  system("grep  'finished successfully' $file")
	and print "Error: event $_ didn't print anything\n"
	and exit(-2);
}

print "DONE!!\n";

