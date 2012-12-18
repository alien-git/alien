#!/bin/env alien-perl

use strict;
use Test;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }



{
my $org=Net::Domain::hostname();

system ("rm", "-rf", "/tmp/$org/*");
system ("mkdir -p /tmp/$org/tmp");
system ("mkdir -p /tmp/$org/log");
#system ("chown -R alienmaster /tmp/$org");
system ("touch /tmp/$org/tmp/AliEn_TEST_SYSTEM");

# Delete proxy cert. Otherwise if there exists a valid proxy the tests will fail
system ("$ENV{ALIEN_ROOT}/bin/alien proxy-destroy");  

open (FILE, "|$ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/CreateOrgServices.pl");

my $user="";
my $secondUser="\n";
$< and $user="Y\n" and $secondUser="";
my $suffix=Net::Domain::hostdomain();print "\nthis is the suffix $suffix";

$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
my $host=Net::Domain::hostname();
print FILE "$user$org
$host:8389/o=$org,$suffix
Y

AliEn2-TEST-$org

$secondUser
pass
ldap-pass

N

";

close FILE or print "ERROR!!" and exit(1); 

print "Checking that the services are up...\n";
open (FILE, "$ENV{ALIEN_ROOT}/etc/rc.d/init.d/aliend status|") 
  or print "Error checking the status\n$? $!\n" and exit(-2);

my @output=<FILE>;
close FILE or 


  print "Error checking the status\nGot @output\n$? $!\n" and exit(-2);

grep (/FAILED/i, @output) and print "Error! Some services are dead!!\n@output\n" and exit(-2);

ok(1);
}
