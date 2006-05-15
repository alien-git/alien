use strict;
use AliEn::UI::Catalogue::LCM;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM->new({"user", "newuser",})
  or exit (-1);

my $lfn="thisFileHasWrongMD5.txt";
addFile($cat,$lfn,
	"I am not supposed to be able to retrieve this file") or exit(-2);


my @whereis=$cat->execute("whereis", $lfn) or exit(-2);


print "Let's modify $whereis[1]\n";
$whereis[1] =~ s{^file://[^/]*/}{/};

open (FILE, ">$whereis[1]") or print "error opening the file $whereis[1]\n" and exit(-2);
print FILE "\nI'm modifying the file\n\n\n";
close FILE;

$cat->execute("get", $lfn) and print "I was able to get the file!!!\n" and exit(-2);

$cat->close();
print "OK\n";
