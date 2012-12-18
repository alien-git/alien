#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
#use AliEn::Service::PackMan; # needed for includeTest 76
use AliEn::PackMan; # needed for includeTest 76
use Cwd; # needed for includeTest 76

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);
  includeTest("packages/02-jobWithPackage") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);

  my $id=shift or print "Error getting the job id\n" and exit(-2);

  my $procDir=checkOutput($cat,$id) or exit(-2);

  my ($out)=$cat->execute("get","$procDir/stdout") or exit(-2);
  open (FILE, "<$out") or print "Error opening $out" and exit(-2);
  my @data=<FILE>;
  close FILE;
  print "Got @data\n";

#  grep ( /Setting the environment to execute MyPS/, @data ) or print "Error the package MyPS is not  initialized!!\n" and exit(-2);
#  grep ( /Setting the environment to execute MyLS/, @data ) or print "Error the package MyLS is not  initialized!!\n" and exit(-2);
#  grep ( /MyPS: command not found/, @data ) and print "Error the command MyPS is not in the PATH\n" and exit(-2);

  ok(1);
}
