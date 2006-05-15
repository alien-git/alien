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

  my $procDir=executeJDLFile($cat, "jdl/MultipleOutput.jdl") or exit(-2);#


  my (@out)=$cat->execute("whereis","$procDir/job-output/file.out") 
    or exit(-2);

  $out[2] or print "Error: the file is only in one SE\n" and exit(-2);

  print "ok, let's try with userarchives...\n";
  addFile($cat, "jdl/MultipleArchiveOutput.jdl",
	  "Executable=\"CheckInputOuptut.sh\";
InputFile=\"LF:$dir/jdl/Input.jdl\";
OutputArchive={\"my_archive:stdout,stderr,resources,file.out\@$sename,${sename}2\"}") or exit(-2);

  $procDir=executeJDLFile($cat, "jdl/MultipleArchiveOutput.jdl") or exit(-2);


  (@out)=$cat->execute("whereis","$procDir/job-output/my_archive") 
    or exit(-2);

  $out[2] or print "Error: the file is only in one SE\n" and exit(-2);


  ok(1);
}
