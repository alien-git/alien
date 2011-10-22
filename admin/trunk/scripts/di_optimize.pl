#!/bin/env alien-perl

use strict;
use Data::Dumper;
use AliEn::Database;
use AliEn::UI::Catalogue::LCM;
use AliEn::UI::Catalogue;
use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

### Set direct connection
$ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/rpub.pem";
$ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/rpriv.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/lpub.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/lpriv.pem";
$ENV{ALIEN_DATABASE_ROLE}='admin';
$ENV{ALIEN_DATABASE_PASSWORD}='pass';

### Get connections and DB objects
my $db = AliEn::Database->new({DRIVER => "mysql",
                               HOST   => Net::Domain::hostfqdn().":3307",
                               DB     => "al_system",
                               ROLE   => "admin"});
#my $cat = AliEn::UI::Catalogue::LCM->new({ROLE => "admin"});

print "Getting an instance of the catalogue";
my $cat_ad =AliEn::UI::Catalogue->new({"role", "admin"});
    $cat_ad or print "Unable to login as admin";


print "\n".scalar(localtime(time))."\n";
print "Checking the status of tables using the di <l> command\n ";
my $done=$cat_ad->execute("di","l");
    $done or print "Error doing di <l> \n";
print "\n".scalar(localtime(time))."\n";

my $max_lim=5000000;
my $min_lim=1000000;
print "Optimize the L#L tables.";
#$done=$cat_ad->execute("di","optimize",$max_lim,$min_lim);
#    $done or print "Error optimizing the catalogue\n";
print "\n".scalar(localtime(time))."\n";
    
print "Checking the status of tables using the di <l> command\n ";
$done=$cat_ad->execute("di","l");
    $done or print "Error doing di <l> \n";


print "\n".scalar(localtime(time))."\n";

