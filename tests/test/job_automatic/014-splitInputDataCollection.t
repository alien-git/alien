use strict;

use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("catalogue/003-add") or exit(-2);



my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my ($dir)=$cat->execute("pwd") or exit(-2);

$cat->execute("rm","inputCollection/SplitInputDataCollection.xml","jdl/SplitInputCollection.jdl");

$cat->execute("find","-x test split/ * ",">inputCollection/SplitInputDataCollection.xml") or exit(-2);

addFile($cat, "jdl/SplitInputCollection.jdl","Executable=\"CheckInputOuptut.sh\";
InputDataCollection=\"LF:inputCollection/SplitInputDataCollection.xml\";
Split=\"file\";
") or exit(-2);

my ($id)=$cat->execute("submit",  "jdl/SplitInputCollection.jdl") or exit(-2);
print "OK!!
\#ALIEN_OUTPUT $id\n";


