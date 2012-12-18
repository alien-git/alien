#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

my $egee=1;
my $cvs="-d:pserver:cvs\@alisoft.cern.ch:2401/soft/cvsroot";
my $module="AliEn";
my $admin="admin-install";
if ($egee) {
  $cvs="-d:pserver:anonymous\@jra1mw.cvs.cern.ch:/cvs/jra1mw";
  $module="org.glite.prototype.alien";
  $admin="admin";
}

{
my $dir="/tmp/ALIEN_CVS_$$";

mkdir($dir);
chdir ($dir) or print "ERROT WITH Chdir\n"and exit(-1);
system ("pwd") and print "ERROR WITH chdir\n"  and exit(-1);

my $done=system("cvs", $cvs, "-q","co", "-P", $module)  
  and print "Errror contacting the cvs server\n" and exit(-1);


chdir ($module)  or print "ERROT WITH Chdir\n"and exit(-1);

$done=system("$ENV{ALIEN_ROOT}/bin/alien-perl", "Makefile.PL")
  and print "Errror making the Makefile\n" and exit(-1);

foreach ("install", "api", $admin) {
  print "\n\nDoing make $_\n";
  system("make", $_)
    and print "Errror Doing the make $_\n" and exit(-1);
}

system("rm", "-rf", "$dir");
ok(1);
}
