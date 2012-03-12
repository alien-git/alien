#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  my $id  = shift;
  $id or print "Error getting the id\n" and exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit(-1);

  print "Checking that $id is in ERROR_EW";
  my ($info)=$cat->execute("top", "-id", $id);
  ($info and $info->{status} eq "ERROR_EW")
    or print "Error, the job is $info->{status} (should be in ERROR_EW)\n" and exit(-2);

  print "OK!\n";
  $cat->close();
  ok(1);
}
