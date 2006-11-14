use strict;

use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("26-ProcessMonitorOutput") or exit(-2);


my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);

my $vo=$cat->{CONFIG}->{ORG_NAME};
my $otherSE="${vo}::cern::testSE2";
addFile ($cat, "jdl/specificSE.jdl","
executable=\"date\";
OutputFile ={\"stderr\@$otherSE\",\"stdout\@$otherSE\"};","r") or exit(-2);


my $procDir=executeJDLFile($cat, "jdl/specificSE.jdl") or exit(-2);
#my $procDir="/proc/newuser/73";
print "And the output is in $procDir\n";
my @where=$cat->execute("whereis", "-l", "$procDir/job-output/stdout") or exit(-2);

print "The file is in @where\n";
grep( /^$otherSE$/, @where) or print "The file is not in $otherSE!!\n" and exit(-2);

grep (/^${vo}::cern::testSE$/, @where) and print "The file is not supposed to tbe in the standard SE!!\n" and exit(-2);

print "ok!!\n";

