#!/bin/env alien-perl

use strict;
use Test;
use AliEn::UI::Catalogue;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

push @INC, $ENV{ALIEN_TESTDIR};
require functions;
{
open my $SAVEOUT,  ">&", STDOUT;
my $file="/tmp/$$";
open STDOUT, ">","$file" or print "Error opening $file\n" and exit (-1);
open (my $FILE, "|-", "$ENV{ALIEN_ROOT}/bin/alien create-keys --user newuser");

print $FILE "testPass
";

my $done=close $FILE;
close STDOUT;
open STDOUT, ">&", $SAVEOUT;
$done  or print "ERROR Doing the command!!" ;
uploadKey() or  exit (-2);

#open (FILE, "<$file");
#my @FILE=<FILE>;
#close FILE;
#system ("rm", "-rf", "$file");
#grep (/FAILED/, @FILE)  and print "FAILED!! @FILE" and uploadKey() and exit (-3);
setDirectDatabaseConnection();

my $cat=AliEn::UI::Catalogue->new({role=>"admin"}) or exit(-1);
$cat->execute("resyncLDAP") or exit(-2);
$cat->close();

unsetDirectDatabaseConnection();


$cat=AliEn::UI::Catalogue->new({user=>"newuser"}) or exit(-1);
my ($user)=$cat->execute("whoami") or exit(-2);
$cat->close();
ok(1);
}

sub uploadKey {
  my $c=AliEn::Config->new();
  my $host=Net::Domain::hostfqdn();
  print "Uploading the public key to ldap...";
  my $ldap = Net::LDAP->new("$host:8389", "onerror" => "warn") 
    or print "failed\nError conecting to the ldap server\n $? and $! and  $@\n" 
      and exit (-3);
my $suffix=Net::Domain::hostdomain();
my $domain=$suffix;
$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
  my $result=$ldap->bind("cn=manager,$suffix", "password" => "ldap-pass");
  $result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error 
  and exit (-4);
  my $key="uid=newuser,ou=People,$c->{LDAPDN}";
  my $file="$ENV{ALIEN_HOME}/identities.\L$c->{ORG_NAME}\E/sshkey.newuser.public";
  open (my $FILE , "<", $file) or print "Error opening $file\n" and exit(-2);
  my $sshkey=join("", <$FILE>);
  close $FILE;
  my $mesg=$ldap->modify( $key, replace=>{"sshkey", $sshkey});
  $mesg->code && print "failed\nCould not modify  $key: ",$result->error and exit (-5);
  $ldap->unbind;


  print "ok\n";
  return 1;
}
