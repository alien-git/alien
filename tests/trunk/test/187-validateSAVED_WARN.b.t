#!/bin/env alien-perl

use strict;
use Test;
use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  $ENV{ALIEN_JOBAGENT_RETRY}=1;
  includeTest("16-add") or exit(-2);
  includeTest("26-ProcessMonitorOutput") or exit(-2);

  my $id=(shift || exit(-2));


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
  $cat or exit (-1);

  my ($status)=$cat->execute("top", "-id", $id);
  

  $status->{status} eq "DONE_WARN" or exit(-2);
  
  print "JOBS STATUS is ".$status->{status};

  checkOutput($cat, $id) or exit(-2);

  print "OUTPUT FILES ARE THERE\n";

  system ("alien", "proxy-destroy");

  $cat->close();

  print "OK, TEST WAS FINE\n";
  ok(1);
}



