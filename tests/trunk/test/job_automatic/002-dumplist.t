use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);

  addFile($cat, "bin/dumplfnlist.sh","#!/bin/bash
date
echo 'Starting the commnand'
pwd
ls -al
echo 'Checking if the file myfile.list exists'
[ -f  myfile.list ] || exit -2
echo 'Checking the content of the file'
cat myfile.list || exit -2
echo 'Checking that the jdl is inside the file'
cat myfile.list |grep  dumplfnlist.jdl  ||exit -2

echo 'The JDL tag inputdatalist works!!'
") or exit(-2);


  addFile($cat, "jdl/dumplfnlist.jdl", "
Executable=\"dumplfnlist.sh\";
InputData={\"LF:$dir/jdl/dumplfnlist.jdl\"};
InputDataList=\"myfile.list\";
") or exit(-2);

  addFile($cat, "jdl/dumplfnlistxml.jdl", "
Executable=\"dumplfnlist.sh\";
InputData={\"LF:$dir/jdl/dumplfnlist.jdl\"};
InputDataList=\"myfile.list\";
InputDataListFormat=\"xml-single\";
") or exit(-2);

  my ($id)=$cat->execute("submit", "jdl/dumplfnlist.jdl") or exit(-2);
  my ($id2)=$cat->execute("submit", "jdl/dumplfnlistxml.jdl") or exit(-2);

  print "We have submitted both jobs!!\n
\#ALIEN_OUTPUT $id $id2\n";


print "ok\n";
exit;
}
