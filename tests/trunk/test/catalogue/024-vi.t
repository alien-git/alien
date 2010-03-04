use strict;

use AliEn::UI::Catalogue::LCM;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("catalogue/003-add") or exit(-2);
my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser"}) or exit(-2);

 addFile($cat, "editfile.txt","This file is going to be edited with vi
Will it work??
", "r") or exit(-2);

my ($file)=$cat->execute("get", "editfile.txt") or exit(-2);
open (FILE, "<$file") or exit(-2);

grep (/FILE_MODIFIED/, <FILE>) and
  print "This is not the file that I want :(\n" and exit(-2);
close FILE;

print "Let's modify the file\n";
$cat->execute("debug",5);
$cat->execute("vi", "editfile.txt", "-c 1,2s/Will/FILE_MODIFIED/ -c w -c q") or exit(-2);
$cat->execute("debug");

print "FILE MODIFIED!!\n";

my ($newfile)=$cat->execute("get", "editfile.txt") or exit(-2);

print "Got the file $newfile!!!!\n";

open (FILE, "<$newfile") or exit(-2);

grep (/FILE_MODIFIED/, <FILE>) or 
  print "The file hasn't been modified :(\n" and exit(-2);
close FILE;

print "YUHUU!\n";
