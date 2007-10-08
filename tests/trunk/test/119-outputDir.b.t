use strict;
use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("26-ProcessMonitorOutput") or exit(-2);


my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my ($dir)=$cat->execute("pwd") or exit (-2);

my $id=shift or print "Error getting the job id\n" and exit(-2);
my $outputDir=shift or print "Error getting the output directory\n" and exit(-2);

my $procDir=checkOutput($cat, $id) or exit(-2);

print "And let's check if the file is in the outputdir\n";

my ($info)=$cat->execute("stat", "$outputDir/stdout") or exit(-2);
use Data::Dumper;
print Dumper($info);

print "ok\n";
