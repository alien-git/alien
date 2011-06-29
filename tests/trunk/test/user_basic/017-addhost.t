#!/bin/env alien-perl

use AliEn::UI::Catalogue::LCM;
use strict;
use Test;

BEGIN { plan tests => 2 }
{
  print "This test is redundant\n";

  my $cat = AliEn::UI::Catalogue::LCM->new({user=>'admin'}) or exit(-2);
  $cat->execute("rmdir","-silent", "/remote", "-r");
  $cat->execute("mkdir", "-p", "/remote/") and print "/remote directory made\n" or exit(-2);
  $cat->close;
    
  ok(1);
}
