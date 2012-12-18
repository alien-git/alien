#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue;
use AliEn::X509;
use Net::LDAP;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 2 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("user_basic/021-se") or exit(-2);
  print "Creating a new user...";
  ok(1);
  my $host   = Net::Domain::hostname();
  my $org    = $host;
  my $suffix = Net::Domain::hostdomain();

  $suffix =~ s/\./,dc=/g;
  $suffix = "dc=$suffix";
  my $key = "uid=newuser,ou=People,o=$org,$suffix";

  my $subject = "";
  eval {
	my $x509 = AliEn::X509->new() or die("Error starting the x509");
	$x509->load("$ENV{ALIEN_HOME}/globus/usercert.pem") or die("Error loading he certificate");
	$subject = $x509->getSubject();
  };
  if ($@) {
	print "Error $@\n";
  }
  my @data = (
	"objectClass", [ "AliEnUser", "posixAccount", "pkiUser", "top" ],
	"cn",            "newuser",
	"uid",           "newuser",
	"uidNumber",     "222",
	"gidNumber",     "222",
	"userPassword",  "{crypt}x",
	"homeDirectory", "/$org/user/n/newuser",
	"loginShell",    "false",
  );
  $subject and push @data, "subject", $subject;

  addLdapEntry($key, \@data) or exit(-2);
  if (!$subject) {
	print "Error gettting the subject of the certificate\n";
	exit(-2);
  }
  my $user = getpwuid($<);
  print "Modifying the entry of $user\n";

  my $ldap = Net::LDAP->new("$host:8389", "onerror" => "warn")
	or print "failed\nError conecting to the ldap server\n $? and $! and  $@\n" and exit(-3);
  my $result = $ldap->bind("cn=manager,$suffix", "password" => "ldap-pass");
  $result->code && print "failed\nCould not bind to LDAP-Server: ", $result->error
	and exit(-4);

  my $config = AliEn::Config->new();

  $key = "uid=$user,ou=People,$config->{LDAPDN}";
  my $mesg = $ldap->modify($key, replace => {"subject", $subject});
  $mesg->code && print "failed\nCould not modify  $key: ", $result->error and exit(-5);
  $ldap->unbind;

  ok(2);

}
