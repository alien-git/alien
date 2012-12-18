#!/bin/env alien-perl

use strict;
use Test;
use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  $ENV{ALIEN_JOBAGENT_RETRY} = 1;
  includeTest("catalogue/003-add")                   or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $outputstring = (shift || exit(-2));

  ((!$outputstring) or ($outputstring eq "")) and exit(-2);

  my ($idfine, $idwarn, $iderror ) = split(/,/, $outputstring);
  ($idfine and $idwarn and $iderror )
    or print "Missing the ids of the jobs!\n" and  exit(-2);

  print "Starting...";

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
  $cat or exit(-1);

  my ($status) = $cat->execute("top", "-id", $idfine);
  $status->{status} eq "DONE" or exit(-2);
  print "$idfine: JOBS STATUS is " . $status->{status};
  checkOutput($cat, $idfine) or exit(-2);
  print "$idfine: OK, OUTPUT FILES ARE THERE\n";

  ($status) = $cat->execute("top", "-id", $idwarn);
  $status->{status} eq "DONE_WARN" or exit(-2);
  print "$idwarn: JOBS STATUS is " . $status->{status};
  checkOutput($cat, $idwarn) or exit(-2);
  print "$idwarn: DONE_WARN: OK, OUTPUT FILES ARE THERE\n";

  ($status) = $cat->execute("top", "-id", $iderror);
  $status->{status} eq "ERROR_SV" or exit(-2);
  print "$iderror: JOBS STATUS is " . $status->{status};
  checkOutput($cat, $iderror) and exit(-2);
  print "$iderror: ERROR_SV OK, OUTPUT FILES ARE NOT THERE\n";

  #system ("alien", "proxy-destroy");

  $cat->close();

  print "OK, TEST WAS FINE\n";
  ok(1);
}

