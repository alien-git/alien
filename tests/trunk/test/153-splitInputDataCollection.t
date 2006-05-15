use strict;

use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);
includeTest("86-split") or exit(-2);


my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my ($dir)=$cat->execute("pwd") or exit(-2);



$cat->execute("find","-x test split * ",">inputCollection/SplitInputDataCollection.xml") or exit(-2);


addFile($cat, "jdl/SplitInputCollection.jdl","Executable=\"CheckInputOuptut.sh\";
InputDataCollection=\"LF:inputCollection/SplitInputDataCollection.xml\";
Split=\"file\";
") or exit(-2);
my ($ok, $procDir, $subjobs)=executeSplitJob($cat, "jdl/SplitInputCollection.jdl") or exit(-2);
$subjobs eq "3" or print "The job is not split in 3 subjobs\n" and exit(-2);
print "ok\n";

