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

  my $key="name=testSE,ou=SE,ou=Services,$config->{FULLLDAPDN}";

  print"Getting the subject of the certificate...";

  my $c=AliEn::X509->new();
  $c->load("$ENV{ALIEN_HOME}/globus/usercert.pem");
  my $subject=$c->getSubject();

  print "ok\n";
  addLdapEntry($key, ["objectClass",["AliEnSE", "AliEnMSS", "AliEnSOAPServer"],
		      "name", "testSE",
		      "host", "$host",
		      "mss", "File",
		      "savedir", "$config->{LOG_DIR}/SE_DATA",
		      "port", "8092",
		      "certsubject",$subject,
		      "QoS", 'custodial',
		     ]) or exit(-2);
  $key="host=$config->{HOST},ou=Config,ou=CERN,ou=Sites,$config->{LDAPDN}";
  addLdapEntry($key, ["objectClass", ["AliEnHostConfig"],
		      host=>$config->{HOST},
		      se=>"testSE",
		     ]) or exit(-2);

  $config=$config->Reload({force=>1});
  print "ok\nCreating the database...";

  my $ui=AliEn::UI::Catalogue->new({role=>"admin"}) or exit(-2);

  $ui->execute("addSE", "-p", "cern", "testSE") or exit(-2);
  $ui->close();
 
  print "ok\n";
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
  sleep (40);
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
