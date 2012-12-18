#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }



{

open (FILE, "|$ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/CreateOrgPortal.pl");

print FILE "$ENV{ALIEN_ORGANISATION}

pass











";

close FILE or print "ERROR!!" and exit (-1); 
ok(1);
}
