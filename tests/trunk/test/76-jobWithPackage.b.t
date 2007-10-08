#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);
use AliEn::Service::PackMan;
use Cwd;
use AliEn::Util;

BEGIN { plan tests => 1 }


{
  my $id=shift or print "Error getting the job id\n" and exit(-2);

  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("26-ProcessMonitorOutput") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);


  my $procDir=checkOutput($cat,$id) or exit(-2);

  my ($out)=$cat->execute("get","$procDir/job-output/stdout") or exit(-2);
  open (FILE, "<$out") or print "Error opening $out" and exit(-2);
  my @data=<FILE>;
  close FILE;
  print "Got @data\n";

  grep ( /Setting the environment to execute MyPS/, @data ) or print "Error the package is not  initialized!!\n" and exit(-2);
  grep ( /MyPS: command not found/, @data ) and print "Error the command MyPS is not in the PATH\n" and exit(-2);

  ok(1);
}
