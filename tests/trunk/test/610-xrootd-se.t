#!/bin/env alien-perl

use strict;
use Test;


use AliEn::Service::SE;
use AliEn::X509;
use AliEn::UI::Catalogue;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }



{
  my $host=Net::Domain::hostname();
  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

  my $key="name=xrootSE,ou=SE,ou=Services,$config->{FULLLDAPDN}";

  print"Getting the subject of the certificate...";

  my $c=AliEn::X509->new();
  $c->load("$ENV{ALIEN_HOME}/globus/usercert.pem");
  my $subject=$c->getSubject();

  print "ok\n";
  addLdapEntry($key, ["objectClass",["AliEnSE", "AliEnMSS", "AliEnSOAPServer"],
		      "name", "xrootdSE",
		      "host", "$host",
		      "mss", "File",
		      "savedir", "$config->{LOG_DIR}/SE_DATA",
		      "port", "9092",
		      "ioDaemons","xrootd:port=54321",
		      "certsubject",$subject,
		     ]) or exit(-2);
  $key="host=$config->{HOST},ou=Config,ou=CERN,ou=Sites,$config->{LDAPDN}";
  addLdapEntry($key, ["objectClass", ["AliEnHostConfig"],
		      host=>$config->{HOST},
		      se=>"xrootdSE",
		     ]) or exit(-2);

  $config=$config->Reload({force=>1});
  print "ok\nCreating the database...";

  my $ui=AliEn::UI::Catalogue->new({role=>"admin"}) or exit(-2);

  $ui->execute("addSE", "-p", "cern", "xrootdSE") or exit(-2);
  $ui->close();
 
  print "ok\n";
  print "Creating the TkAuthz Configuration File for xrootd ... \n";
  if ((! -e "$ENV{'HOME'}/.alien/gapiserver/lpub.pem" ) ||
      (! -e "$ENV{'HOME'}/.alien/gapiserver/rpriv.pem" )) {
      print "Missing the envelope keys - run test 500 before!\n";
      exit (-2);
  }
  system("mkdir -p $ENV{'HOME'}/.authz/xrootd/; chmod 700 $ENV{'HOME'}/.authz/xrootd/");
  system("cp -v $ENV{'HOME'}/.alien/gapiserver/lpub.pem $ENV{'HOME'}/.authz/xrootd/pubkey.pem");
  system("cp -v $ENV{'HOME'}/.alien/gapiserver/rpriv.pem $ENV{'HOME'}/.authz/xrootd/privkey.pem");

  open TKOUT ,">$ENV{'HOME'}/.authz/xrootd/TkAuthz.Authorization";
  print TKOUT <<EOF
####################################################################
# libTokenAuthz - Configuration File
# Andreas-Joachim Peters: CERN/ARDA
# mail-to: Andreas.Joachim.Peters\@cern.ch
#####################################################################
# Description:
# -------------------------------------------------------------------
# This file describes, which namespace paths are exported and can
# enforce token authorization for specific VO's and paths.
#
# Structure:
# -------------------------------------------------------------------
# The file contiains three section:
# KEYS:
# =======
# this section assigns to each VO the private and public key pairs
# to be used, to decode and verify authorization tokens
#
# EXPORT:
# =======
# this section defines, which namespace path's are exported.
# The rules can allow or deny part of the namespace for individual
# VO's and certificates
#
# RULES:
# =======
# this section contains specific ruls for each namespace path, if
# token authorization has to be applied, to which operations and
# for which VO and certificates it has to be applied.

# ------------------------------ Warning ----------------------------
# the key words
#       KEY, EXPORT, RULE
#       VO, PRIVKEY, PUBKEY
#       PATH, AUTHZ, NOAUTHZ, CERT
# have to be all uppercase! Values are assigned after a ':'
# -------------------------------------------------------------------

#####################################################################
# Key section
#####################################################################
#
# Syntax:KEY  VO:<voname>     PRIVKEY:<keyfile>      PUBKEY:<keyfile>
#
#  ------------------------------------------------------------------
# VO:* defines the default keys for unspecified vo

KEY VO:*       PRIVKEY:$ENV{'HOME'}/.authz/xrootd/privkey.pem PUBKEY:$ENV{'HOME'}/.authz/xrootd/pubkey.pem
#KEY VO:CMS    PRIVKEY:<pkey>  PUBKEY:<pubkey>
#KEY VO:*      PRIVKEY:<pkey>  PUBKEY:<pubkey>

######################################################################
# Export Section
#####################################################################
#
# Syntax: EXPORT PATH:<path>    VO:<vo> ACCESS:<ALLOW|DENY>     CERT:<*|cert>
#
#  ------------------------------------------------------------------
# - PATH needs to be terminated with /
# - ACCESS can be ALLOW or DENY
# - VO can be wildcarded with VO:*
# - CERT can be wildcarded with CERT:*
# - the first matching rule is applied

EXPORT PATH:/            VO:*     ACCESS:ALLOW CERT:*
#EXPORT PATH:/tmp/cms/   VO:CMS   ACCESS:DENY CERT:*
#EXPORT PATH:/castor/    VO:*     ACCESS:ALLOW CERT:*

######################################################################
# RULES Section
######################################################################
#
#  Syntax: RULE PATH:<path> AUTHZ:<tag1|tag2|...|> NOAUTHZ:<tag1|tag2|...|> VO:<vo1|vo2|....|> CERT:<IGNORE|*|cert>
#
#  ------------------------------------------------------------------
# - PATH  defines the namespace path
# - AUTHZ defines the actions which have to be authorized
# - NOAUTHZ defines the actions which don't have to be authorized
# - VO is a list of VO's, where this rule applies
# - CERT can be IGNORE,* or a specific certificate subject
#   IGNORE means, that the envelope certificate must not match the
#   USER certificate subject. * means, that the rule applies for any
#   certificate and the certificate subjects have to match.


RULE PATH:/ AUTHZ:read|write|delete|write-once| NOAUTHZ:| VO:* CERT:*
#RULE PATH:/tmp/ AUTHZ:read| NOAUTHZ:| VO:ALICE|CMS| CERT:*
EOF
;
  system("chmod 600 $ENV{'HOME'}/.authz/xrootd/TkAuthz.Authorization");


  startService("SE") or exit(-2);
  print "Let's see if the IS knows that the service is up...";

  my $soap=new AliEn::SOAP or exit(-2); 
  $soap->CallSOAP("IS", "getSE", $config->{SE_FULLNAME}) or exit(-2);
  ok(1);



}
sub removeLdapEntry {
  my $key=shift;
  my $ldap=shift;
  my $disconnect=0;
  my $host=Net::Domain::hostname();

  print "ok\nRemoving $key from ldap...";
  if (! $ldap) {
    $disconnect=1;
    $ldap = Net::LDAP->new("$host:8389", "onerror" => "warn") 
      or print "failed\nError conecting to the ldap server\n $? and $! and  $@\n" 
	and return;
    my $result=$ldap->bind("cn=manager,dc=cern,dc=ch", "password" => "ldap-pass");
    $result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error and return;
	
  }
  my $mesg=$ldap->delete($key);
  $mesg->code && print "failed\nCould not delete $key: ",$mesg->error and exit (-5);

  ($disconnect) and $ldap->unbind;
  print "ok\n";
  return 1;
}

sub addLdapEntry {
  my $dn=shift;
  my $attributes=shift;

  print "Connecting to ldap...";
  my $host=Net::Domain::hostname();
  my $ldap = Net::LDAP->new("$host:8389", "onerror" => "warn") 
    or print "failed\nError conecting to the ldap server\n $? and $! and  $@\n" 
      and exit (-3);
  my $result=$ldap->bind("cn=manager,dc=cern,dc=ch", "password" => "ldap-pass");
  $result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error 
  and exit (-4);

  my $ldapDN=$dn;
#  my $filter;
#  $ldapDN =~ s/^([^,]*),// and $filter="($1)";
  my $mesg=$ldap->search(base   => $ldapDN,
			 filter => "(objectClass=*)"
			);
  print "Searching for $ldapDN \n";
  if ($mesg->count) {
    print "THE SE EXISTS!!\nDeleting the se... ";
    my $repeat=1;
    my @entries=$mesg->entries();
    while ($repeat) {
      $repeat=0;
      my @todo;
      foreach my $entry (@entries) {
	print "DELETING ". $entry->dn(). "\n";
	my $meg=$ldap->delete($entry->dn());
	if ($meg->code) {
	  print "\n\twarning: error deleting",$result->error, " (we'll try again)...";
	  push @todo, $entry;
	  $repeat=1;
	}
      }
      @entries=@todo;
    }
    if (@entries){
      print "We didn't delete all the entries!!\n" and exit(-2);
    }
  }

  print "ok\nAdding '$dn' to ldap...";

  $mesg=$ldap->add ($dn,
	    attr => $attributes);
  $mesg->code && print "failed\nCould not add  $dn: ",$result->error and exit (-5);
  $ldap->unbind;
  print "ok\n";
  return 1;
}

sub startService {
  my $service=shift;

  print "Starting the $service...\n";
  my $command="$ENV{ALIEN_ROOT}/bin/alien Start$service";
  $< or $command="su - alienmaster -c \"$command\"";
  system($command) and return;
  print "sleeping...";
  sleep (20);
  print "ok\nChecking if $service is still up ...";
  my $config=new AliEn::Config;
  my $logFile="$config->{LOG_DIR}/$service.log";
  $service eq "Monitor" and $logFile=~ s{/Monitor\.}{/ClusterMonitor.};
  if (system("$ENV{ALIEN_ROOT}/bin/alien Status$service") ) {
    print "The $service is dead...\n";
    system("cat", $logFile);
    return;
  }

  print "ok\nChecking if the service is listening...\t";
  open (FILE, "<$logFile") or print "Error opening the log file $logFile" and return;
  my @file=<FILE>;
  close FILE;
  grep (/info\s+Starting \S+ on /i, @file) or print "The service is not listening:\n@file\n" and return;



  print "ok\nAdding it to the startup services\n";

  my $vo=Net::Domain::hostname();
  my $file="/etc/aliend/$vo/startup.conf";
  $< and $file="$ENV{ALIEN_HOME}$file";
  open (FILE, "<$file") or print "Error reading the file $file\n" and return;
  my @FILE=<FILE>;
  close FILE;
  my @line=grep (/^AliEnServices=/, @FILE);
  $line[0] =~ /[\" ]$service[ \"]/ and print "done\n" and return 1;

  print "\nAdding the entry";

  $line[0]=~ s/([^=])\"/$1 $service\"/;

  @FILE= (grep (!/^AliEnServices=/, @FILE), $line[0]);

  open (FILE, ">$file") or print "Error opening the file $file\n" and exit(-2);

  print FILE @FILE;
  close FILE;
  print "...ok\n";

  return 1;
}
