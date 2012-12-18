#!/bin/env alien-perl

use strict;
use Test;
use AliEn::Config;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

{
  my $config=new AliEn::Config;
print "Checking if lynx exists...";
system(" lynx -version > /dev/null") and print "Error! lynx is not in the path\n$! $?\n" and exit(-2);

print "ok\nGetting the default page...";
my $host=Net::Domain::hostfqdn();
open (FILE, "lynx -dump http://$host/$config->{ORG_NAME} |") 
  or print "Error doing lynx!!\n$! $?\n" and exit(-2);

my @output=<FILE>;

close (FILE)  or print "Error closing lynx!!\n$! $?\n" and exit(-2);

#print "GOT @output\n";

ok(1);
}
