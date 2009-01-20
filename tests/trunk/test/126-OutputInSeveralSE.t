#!/bin/env alien-perl

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

  
  my $sename="$cat->{CONFIG}->{ORG_NAME}::cern::testse";
  addFile($cat, "jdl/MultipleOutput.jdl","Executable=\"CheckInputOuptut.sh\";
InputFile=\"LF:$dir/jdl/Input.jdl\";
OutputFile={\"file.out\@$sename,${sename}2\"}") or exit(-2);

  my ($id)=$cat->execute("submit", "jdl/MultipleOutput.jdl") or exit(-2);#


  print "ok, let's try with userarchives...\n";
  addFile($cat, "jdl/MultipleArchiveOutput.jdl",
	  "Executable=\"CheckInputOuptut.sh\";
InputFile=\"LF:$dir/jdl/Input.jdl\";
OutputArchive={\"my_archive:stdout,stderr,resources,file.out\@$sename,${sename}2\"}") or exit(-2);

  my ($id2)=$cat->execute("submit", "jdl/MultipleArchiveOutput.jdl") or exit(-2);

  print "ok! Finally, let's try also with the localse\n";


  addFile($cat, "jdl/MultipleArchive2Output.jdl",
	  "Executable=\"CheckInputOuptut.sh\";
InputFile=\"LF:$dir/jdl/Input.jdl\";
OutputArchive={\"my_archive:stdout,stderr,resources,file.out\@local,${sename}2\"}") or exit(-2);

  my ($id3)=$cat->execute("submit", "jdl/MultipleArchive2Output.jdl") or exit(-2);

  
  print "ok!
\#ALIEN_OUTPUT $id $id2 $id3\n";

}
