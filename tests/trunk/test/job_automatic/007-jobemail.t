#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  $cat->execute("mkdir", "-p", "jdl");
  addFile($cat, "jdl/email.jdl", "Executable=\"date\";\nEmail=\"root\@localhost\";\n") or exit(-2);

  $cat->execute("whereis", "-silent", "bin/dateWrong") or 
    $cat->execute("add", "-r", "bin/dateWrong", "file://wrongmachine/path/to/not/existant/file",22) 
      or exit(-2);
  addFile($cat, "jdl/emailWrong.jdl", "Executable=\"dateWrong\";\nEmail=\"root\@localhost\";\n") or exit(-2);



  my ($id)=$cat->execute("submit", "jdl/email.jdl") or exit(-2);
  my ($id2)=$cat->execute("submit", "jdl/emailWrong.jdl") or exit(-2);

  print "We have submitted both jobs!!\n
\#ALIEN_OUTPUT $id $id2\n";

}
