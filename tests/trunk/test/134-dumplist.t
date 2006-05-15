use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
  includeTest("26-ProcessMonitorOutput") or exit(-2);
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


  my $procDir=executeJDLFile($cat, "jdl/dumplfnlist.jdl") or exit(-2);
  print "The job executed properly!!\n";
  my ($out)=$cat->execute("get","$procDir/job-output/stdout") or exit(-2);
  system("cat", "$out");
  system("grep 'The JDL tag inputdatalist works' $out") and
  print "The line is not there!!!" and exit(-2);

  $procDir=executeJDLFile($cat, "jdl/dumplfnlistxml.jdl") or exit(-2);
  print "The job executed properly!!\n";
  ($out)=$cat->execute("get","$procDir/job-output/stdout") or exit(-2);
  system("cat", "$out");
  system("grep 'The JDL tag inputdatalist works' $out") and
  print "The xml line is not there!!!" and exit(-2);
  system("grep '<?xml version=\"1.0\"?>' $out") and print "The output is not in xml!!\n" and exit(-2);

print "ok\n";
exit;
}
