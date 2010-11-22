#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM;

BEGIN { plan tests => 1 }


{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM->new({"user", "newuser",});
  $cat or exit (-1);

  $cat->execute("cd") or exit (-2);
  my ($dir)=$cat->execute("pwd") or exit (-2);
  $cat->execute("rm", "mirror1.txt", "mirror2.txt");

  my $fileName="/tmp/alien_tests_file.$<";
  open(FILE, ">$fileName") or print "Error opening $fileName\n" and exit(-2);
  print FILE "Hello world
It is a nice day
";
  close FILE;
  addFile($cat, "mirror1.txt","Hello world
It is a nice day
") or exit(-2);
 
  $cat->execute("addMirror", "mirror1.txt", $cat->{CONFIG}->{SE_FULLNAME}, "file://$cat->{CONFIG}->{HOST}:7093/$fileName") or exit(-2);

  print "Mirroring a file works!!!!\n\n";
  my @pfns=$cat->execute("whereis", "mirror1.txt") or exit(-2);
  print "Got @pfns ($#pfns entries)\n";
  
  $#pfns<3 and print "There aren't several copies of that file!!!\n" and exit(-2);

  addFile($cat, "mirror2.txt","Hello world
It is not a nice day
") or exit(-2);

  print "This mirror is not supposed to work, since the file has a different md5\n";

  sleep(5);
  $cat->execute("addMirror", "mirror2.txt", $cat->{CONFIG}->{SE_FULLNAME}, "file://$cat->{CONFIG}->{HOST}:7093/$fileName", "-c") and exit(-2);

  print "YUHUU!!\n";

  ok(1);
}
