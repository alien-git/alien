use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }


{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

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



  my ($id)=$cat->execute("submit", "jdl/CheckReturnCode0.jdl") or exit(-2);
  my ($id2)=$cat->execute("submit", "jdl/CheckReturnCode2.jdl") or exit(-2);

  print "We have submitted both jobs!!\n
\#ALIEN_OUTPUT $id $id2\n";

  ok(1);
}
