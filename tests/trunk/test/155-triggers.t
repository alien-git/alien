#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin",});
  $cat or exit (-1);
  $cat->execute("cd", "../../n/newuser");
  $cat->execute("rmdir", "-rf", "/triggers", "triggersTest");
  print "First, let's see if we get an error if the trigger doesn't exist\n";

  $cat->execute("mkdir", "triggersTest", "/triggers") or exit(-2);
  $cat->execute("addTrigger", "triggersTest", "myTrigger") and exit(-2);
  print "Ok, Let's register the trigger\n";
  my $file="/tmp/alien_test.155.$$";
  addFile($cat, "/triggers/myTrigger","#!/bin/bash
echo \"Hello \$1\"
touch $file
echo \$1 >> $file
") or exit(-2);

  $cat->execute("addTrigger", "triggersTest", "myTrigger");# or exit(-2);
  $cat->execute("showTrigger") or exit(-2);
  print "Ok, let's create a file and see if it appears\n";
  $cat->execute("touch", "triggersTest/myfile") or exit(-2);
  my $i=0;
  my $found=0;
  while ($i<7){
    sleep(10);
    $i++;
    print "Checking if the file has been created\n";
    open (FILE, "<$file") or next;
    grep (m{triggersTest/myfile}, <FILE>) and $found=1 and last;
    close FILE;
  }
  $found or print "The file $file doesn't contain the new entry!!\n" and exit(-2);
  close FILE;
  print "So far so good\nLet's remove the trigger\n";
  $cat->execute("removeTrigger", "triggersTest" ) or exit(-2);
  unlink $file;
  print "YUHUU!\n";

}
