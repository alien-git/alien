use strict;

use AliEn::UI::Catalogue;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

includeTest("catalogue/013-cpdir") or exit(-2);

my $c=AliEn::UI::Catalogue::LCM->new({role=>"newuser"}) or exit(-2);

$c->execute("rmdir", "-rf", "cpTarget/");

$c->execute("mkdir", "-p", "cpTarget") or exit(-2);

$c->execute("cp", "/bin/date", "cpTarget") or exit(-2);
$c->execute("cp", "/bin/date", "cpTarget/date2") or exit(-2);
print "We have copied the two files\nChecking if they are there\n";

compareDirectory($c,"cpTarget/", "date","date2") or exit(-2);

print "YUUHUUUU!!\n";


