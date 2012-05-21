use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);
  $cat->execute("mkdir", "se_advanced");
  addFile($cat, "bin/se_advanced.sh","#!/bin/bash
echo 'Putting all the files into the output'
cat file*
") or exit(-2);

  addFile($cat, "se_advanced/file1", "Content 1\n") or exit(-2);
  addFile($cat, "se_advanced/file2", "Content 2\n") or exit(-2);
  addFile($cat, "se_advanced/file3", "Content 3\n") or exit(-2);

  addFile($cat, "jdl/SplitFileBroker.jdl","executable = \"${dir}bin/se_advanced.sh\";
        split = \"se_advanced\";
        inputdata = {\"$dir/se_advanced/file1\"};
"
 ) or exit(-2);

  my ($id)=$cat->execute("submit", "jdl/SplitFileBroker.jdl") or exit(-2);

  

  addFile($cat, "jdl/SplitFileBroker2.jdl","executable = \"${dir}bin/se_advanced.sh\";
        split = \"se_advanced\";
        inputdata = {\"$dir/se_advanced/file1\",
           \"$dir/se_advanced/file2\",
           \"$dir/se_advanced/file3\"};

        SplitMaxInputFileNumber  = \"2\" 
"
 ) or exit(-2);

  my ($id2)=$cat->execute("submit", "jdl/SplitFileBroker2.jdl") or exit(-2);




  $cat->close();
  print "ok!!\n
\#ALIEN_OUTPUT $id \n";
}
