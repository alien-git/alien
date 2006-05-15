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

  $cat->execute("cd") or exit (-2);
  my ($dir)=$cat->execute("pwd") or exit (-2);

  addFile($cat, "bin/CheckReturnCode.sh","#!/bin/bash
echo \"This commands exists with the code received as input (\$1)\"
exit \$1
") or exit(-2);

  addFile($cat, "jdl/CheckReturnCode0.jdl","Executable=\"CheckReturnCode.sh\";
Arguments=\"0\";
") or exit(-2);
  addFile($cat, "jdl/CheckReturnCode2.jdl","Executable=\"CheckReturnCode.sh\";
Arguments=\"2\";
") or exit(-2);

  my $procDir=executeJDLFile($cat, "jdl/CheckReturnCode0.jdl") or exit(-2);
  my $id;
  $procDir=~ m{/(\d+)$} and $id=$1;
  print "Checking the return code of $id (from $procDir\n";

  my ($rc)=$cat->execute("ps", "rc", $id); 
  ($rc eq "0") or print "The return code of $id is not 0 (is $rc)!!\n" and exit(-2);

  $procDir=executeJDLFile($cat, "jdl/CheckReturnCode2.jdl") or exit(-2);
  $procDir=~ m{/(\d+)$} and $id=$1;

  print "Checking the return code of $id\n";

  ($rc)=$cat->execute("ps", "rc", $id); 
  ($rc eq "2") or print "The return code of $id is not 2 (is $rc)!!\n" and exit(-2);

  ok(1);
}
