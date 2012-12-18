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

  $cat->execute("cd") or exit (-2);
  my ($dir)=$cat->execute("pwd") or exit (-2);

  addFile($cat, "jdl/OutputArchive.jdl","Executable=\"CheckInputOuptut.sh\";
InputFile=\"LF:$dir/jdl/Input.jdl\";
OutputArchive={\"myarchive:file.out,stdout\"}") or exit(-2);

  my ($id)=$cat->execute("submit", "jdl/OutputArchive.jdl") or exit(-2);

  print "We have submitted the jobs!!\n
\#ALIEN_OUTPUT $id \n";

}
