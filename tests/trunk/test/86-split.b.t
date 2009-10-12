#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{

  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("86-split") or exit(-2);

  my $id=shift or print "Error getting the id of the job\n" and exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);

  my ($dir)=$cat->execute("pwd") or exit(-2);

  my ($procDir)=checkSubJobs($cat, $id, 2) or exit(-2);

  my (@files)=$cat->execute("ls", "-la", $procDir);
  foreach (@files) {
    /^[^#]*###newuser###/ or print "Error the owner of $_ is not 'newuser'\n" and exit(-2);
  }
  print "ok\n";
}
