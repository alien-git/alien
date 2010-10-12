#!/bin/env alien-perl

eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

use AliEn::UI::Catalogue;
BEGIN {
  unless(grep /blib/, @INC) {
    chdir 't' if -d 't';
    unshift @INC, '../lib' if -d '../lib';
  }
}

use strict;
use Test;

BEGIN { plan tests => 3 }


{
my $h=$ENV{ALIEN_NTP_HOST} || "pool.ntp.org";
print "Synchronizing with $h\n"; 
print "Creating a new proxy...";
open (FILE, "$ENV{ALIEN_ROOT}/bin/alien proxy-init|") 
  or print "ERROR OPENING alien proxy-init\n" and exit(-1);

my @OUTPUT=<FILE>;
close FILE or print ("Error doing alien proxy-init!!\n output: @OUTPUT") and exit (-1);

grep (/Your proxy is valid until/i,  @OUTPUT)
  or print "Alien proxy-init did not create a proxy" and exit(-1);

print "ok\nUploading the certificate";
ok(1);
open SAVEOUT,  ">&STDOUT";
my $file="/tmp/$$";
open STDOUT, ">$file" or print "Error opening $file\n" and exit (-1);
open (FILE, "|$ENV{ALIEN_ROOT}/bin/alien register-cert --user newuser");

print FILE "testPass
";

my $done=close FILE;
close STDOUT;
open STDOUT, ">&SAVEOUT";
$done  or print "ERROR Doing the command!!" and exit (-2);

open (FILE, "<$file");
my @FILE=<FILE>;
close FILE;
system ("rm", "-rf", "$file");
grep (/FAIL/, @FILE) and print "Error uploading te certificate\n@FILE\n" and exit(-2);
print "Certificate uploaded and @FILE\n";

open (FILE, "$ENV{ALIEN_ROOT}/bin/alien proxy-destroy|") 
  or print "ERROR OPENING alien proxy-destroy\n" and exit(-1);

@OUTPUT=<FILE>;
close FILE or print ("Error doing alien proxy-destroy!!") and exit (-1);

setDirectDatabaseConnection();

my $cat=AliEn::UI::Catalogue->new({role=>"admin"}) or exit(-2);
$cat->execute("resyncLDAP") or exit(-2);
$cat->close();
ok(1);

}
