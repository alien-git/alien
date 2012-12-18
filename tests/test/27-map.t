#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

{
my $org=$ENV{ALIEN_ORGANISATION};
print "LD ->$ENV{LD_LIBRARY_PATH}\n";
system ("ldd", "$ENV{ALIEN_ROOT}/lib/perl5/site_perl/5.6.1/i386-linux/auto/Image/Imlib2/Imlib2.so");
print "Making the map for a new organisation ($org)\n";
chdir ("/home/alienmaster/AliEn/Html/map");
system ('pwd');
system ("$ENV{ALIEN_ROOT}/bin/alien", "-x", "makemap.pl", "-org", "$org")
  and print "Error making the map!\n $! and $?\n" and exit (-2);
system ("$ENV{ALIEN_ROOT}/bin/alien", "-x", "makemap.pl", "-org", "$org", "-split=1")
  and print "Error making the map!\n $! and $?\n" and exit (-2);


ok(1);
}
