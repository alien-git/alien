use strict;

use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Database::SE;

BEGIN { plan tests => 1 }


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

includeTest("catalogue/003-add") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
addFile($cat, "file_to_delete.txt","
This file is going to be deleted immediately
","r") or exit(-2);

my ($guid)=$cat->execute("lfn2guid", "file_to_delete.txt") or exit -2;
my ($se, $pfn)=$cat->execute("whereis", "file_to_delete.txt") or exit(-2);

$pfn =~ s{^file://[^/]*}{};
(-f $pfn) or print "The file '$pfn' doesn't exist!!\n" and exit(-2);
$cat->{CATALOG}->{DATABASE}->{LFN_DB}->queryValue("select count(*) from TODELETE where guid=string2binary('$guid')")
  and print "The file is already in the queue to delete!!!\n" and exit(-2);
$cat->execute("rm", "file_to_delete.txt") or exit(-2);
print "OK! the file has been deleted. The second part of the test will
check if the file is gone from the catalogue
\#ALIEN_OUTPUT  $pfn $guid\n";
