#!/usr/bin/perl

use strict;

my $MODE = shift;

if(!($MODE)) {
    usage();
    exit;
}
if(($MODE ne 'host') && ($MODE ne 'user')) {
    usage("No mode argument\n"); 
}
my $extra_args;
if($MODE eq "host") {
    $extra_args .= "-nodes";
}

$ENV{ALIEN_ROOT}
  or print "Error: The environment variable ALIEN_ROOT is not defined\n" and exit;
my $alien = $ENV{ALIEN_ROOT};
my $ftdDir="$ENV{ALIEN_HOME}/identities.ftd";

if ( !( -d  $ftdDir ) ) {
  print "Creating  $ftdDir\n";
  my $dir = "";
  foreach ( split ( "/",  $ftdDir ) ) {
    $dir .= "/$_";
    mkdir $dir, 0777;
  }
}
    
my $KEY_FILE = "$ftdDir/key.pem";
my $REQUEST_FILE = $MODE."req.pem";
my $DAYS = "364";


#my $alienCA = "/home/alienMaster/CA";
my $command = "$alien/bin/openssl req -new $extra_args -config $alien/etc/alien-certs/alien-$MODE-ssl.conf -days $DAYS -keyout $KEY_FILE -out $REQUEST_FILE";


my $err = `$command`;
if($err) {
    print "An error occured\n";
    exit;
}

my $mailadd = "alien-cert-request\@alien.cern.ch";
 chmod 0600, $KEY_FILE;
print "**********************************************************

  Your key is stored in: $KEY_FILE
  Now send you request to $mailadd by doing

  cat $REQUEST_FILE | mail $mailadd

**********************************************************\n";

sub usage() {
    my $err = shift;
    print "$err

Usage: requestCertificate.pl <MODE>

MODE  :  either host or user
";
    exit;
}
