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

  print "Let's execute a job\n";
  my $procDir=executeJDLFile($cat, "jdl/date.jdl") or exit(-2);


  my (@out)=$cat->execute("whereis","$procDir/job-output/stdout") or exit(-2);

  $out[0] =~ /^guid:/ or print "The stdout is not a link!!!\n" and exit(-2);

  print "Checking if we can do whereis -r\n";

  (@out)=$cat->execute("whereis","$procDir/job-output/stdout") or exit(-2);

  $out[0] =~ /^guid:/ and print "The stdout is a link!!!\n" and exit(-2);

  $cat->close();
  ok(1);
}
