#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }



{
  my $subject="";
  my $file="$ENV{ALIEN_HOME}/globus/usercert.pem";
  if (-f $file) {
    if (open( TEMP, "openssl x509 -noout -in $file -subject|")){
      $subject=<TEMP>;
      $subject=~ s/^subject=\s+//;
      chomp $subject;
      close(TEMP);
    }
  }
my $org=`hostname -s`;
chomp $org;
my $fqd=`hostname -f`;
chomp $fqd;
open (FILE, "|$ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/CreateOrgLDAP.pl");

print FILE "Y
$org
dc=cern,dc=ch

ldap-pass
ldap-pass


7070
$fqd:3307

cern.ch
$subject

R
";

close FILE or print "ERROR!!" and exit (-1); 
ok(1);
}
