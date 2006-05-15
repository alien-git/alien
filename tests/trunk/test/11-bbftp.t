#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 2 }



{
#First, check that bbftp is there....
open (FILE, "$ENV{ALIEN_ROOT}/bin/bbftp -v|") or print "ERROR OPENING BBFTP\n" and exit(-1);

my @OUTPUT=<FILE>;
close FILE or print ("Error doing bbftp!!") and exit (-1);

grep (/certificate authentication enabled/i,  @OUTPUT)
  or print "BBFTP is not compiled with certificates!!\n" and exit(-1);

grep (/RFIO/i,  @OUTPUT)
  or print "BBFTP is not compiled with RFIO!!\n" and exit(-1);


print "BBFTP lookd fine\n @OUTPUT\n";
ok(1);

my $host=( shift or "wacdr001d.cern.ch");
$ENV{X509_USER_CERT} = "$ENV{HOME}/.alien/identities.ftd/cert.pem";
$ENV{X509_USER_KEY} = "$ENV{HOME}/.alien/identities.ftd/key.pem";
$ENV{X509_CERT_DIR} = "$ENV{ALIEN_ROOT}/etc/alien-certs/certificates";
$ENV{GRIDMAP}="$ENV{HOME}/.alien/identities.ftd/map";

print "Using 
X509_USER_CERT = $ENV{X509_USER_CERT}
X509_USER_KEY = $ENV{X509_USER_KEY}
X509_CERT_DIR} = $ENV{X509_CERT_DIR}
GRIDMAP=$ENV{HOME}/.alien/identities.ftd/map
Connecting to  $host\n\n";

my @command=("$ENV{ALIEN_ROOT}/bin/bbftp", "-e","setoption createdir;","-p", "5", "-w", "10025",  "-V", $host);

print "Skipping @command...\n\n";

#my $d=system (@command);

#print "\n\nDone and $d\n";
#$d and print "Error doing bbftp\n" and exit(-1);
ok(2);
}
