use strict;
use Test;

use AliEn::UI::Catalogue::LCM;
  
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
  
my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser"}) or exit(-2);

my $lfn="FileToMirror.txt";
print "Let's start with a showMirror... \n";
addFile($cat, $lfn,"This file should be mirrored in several places
Right now is ".time()."\n","r") or exit(-2);

print "\n\nChecking where the file is\n";
my @list=$cat->execute("whereis", $lfn) or exit(-2);

print "\n\nChecking if there are mirrors\n";
my @mirrors=$cat->execute("showMirror", $lfn) or exit(-2);
my $seName="$cat->{CONFIG}->{SE_FULLNAME}2";
print "\n\nLet's do a 'mirror'\n";
$cat->execute("mirror", "-i", $lfn, $seName) or exit(-2);
my @newMirror=$cat->execute("whereis", $lfn) or exit(-2);

foreach my $file (@newMirror) {
  print "The file is in $file\n";

}
my $source=$newMirror[1];
my $target=$newMirror[3];
($source and $target) or print "Error: the second SE doesn't know anything about $lfn\n" and exit(-2);
 map { s{^file://[^/]*/}{/} } ($source, $target);
print "Comparing the files $source and $target\n";
system("diff", $source, $target) and print "The files are different!!\n" and exit(-2);
print "ok\n";
