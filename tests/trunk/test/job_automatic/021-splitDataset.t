use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);
  $cat->execute("mkdir", "-p", "splitDataset") or exit(-2);

  addFile($cat, "splitDataset/file1","First file of the collections") or exit(-2);
  addFile($cat, "splitDataset/file2","Second file of the collections") or exit(-2);
  addFile($cat, "splitDataset/list.xml","<?xml version=\"1.0\"?>
<alien>
  <collection name=\"global\">
    <event name=\"1\">
      <file name=\"file1\"
lfn=\"${dir}splitDataset/file1\"
turl=\"alien://${dir}splitDataset/file1\"
evlist=\"1,2\" />
      <file name=\"file2\"
lfn=\"${dir}splitDataset/file2\"
turl=\"alien://${dir}splitDataset/file2\"
evlist=\"3,4\" />

    </event>
  </collection>
</alien>
",'r') or exit(-2);


  addFile($cat, "jdl/SplitDataset.jdl","Executable=\"SplitDataset.sh\";
Split=\"file\";
InputDataCollection=\"LF:${dir}/splitDataset/list.xml\";
InputDataList=\"mylocallist.xml\";
InputDataListFormat=\"merge:${dir}/splitDataset/list.xml\"") or exit(-2);

  addFile($cat, "bin/SplitDataset.sh","#!/bin/bash
date
echo \"I've been called with '\$*'\"
echo \"Checking the file mylocallist.xml\"
cat  mylocallist.xml
") or exit(-2);

  my @files=$cat->execute("find", "${dir}/splitDataset", "*");
  print "Starting with @files\n";

  my ($id)=$cat->execute("submit", "jdl/SplitDataset.jdl") or exit(-2);
  $cat->close();
  print "ok!!\n
\#ALIEN_OUTPUT $id\n";
}
