#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);
  my ($user)=$cat->execute("whoami") or exit(-2);
  my $content="blablabla.".time;
  addFile($cat, "bin/CheckInputData.sh","#!/bin/bash
date
echo 'Starting the commnand'
pwd
ls -al
echo 'Creating a temporaray file'
echo '$content'> file.out
date
","r") or exit(-2);

  addFile($cat, "jdl/InputData.jdl","Executable=\"CheckInputData.sh\";
InputData=\"LF:${dir}jdl/Input.jdl\";
OutputFile={\"file.out\",\"stdout\",\"stderr\",\"resources\"}", "r") or exit(-2);
  my ($id)=$cat->execute("submit", "jdl/InputData.jdl") or exit(-2);
  $cat->close();
  print "Job submitted!! 
\#ALIEN_OUTPUT $id $content\n";
}
