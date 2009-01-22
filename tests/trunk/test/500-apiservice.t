#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  print "Getting a proxy ...";
  if (system("$ENV{'ALIEN_ROOT'}/bin/alien proxy-init 2>&1 | grep -w valid")) {
      exit(-2);
  }
  print "ok\n";

  system("killall -9 gapiserver");  
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
#  includeTest("14-se") or exit(-2);
  if (! defined $ENV{HOST}){
    print "WARNING!!!!!!!!! The environment variable HOST is not defined!\n";
    if (defined $ENV{HOSTNAME}){
      print "Taking it from HOSTNAME\n";
      $ENV{HOST}=$ENV{HOSTNAME};
    } else{
      $ENV{HOST}=`hostname`;
      chomp $ENV{HOST};
    }
  }
  print "THE HOSTNAME IS $ENV{HOST}\n";

  my $host=Net::Domain::hostname();
  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

  my $keyname= "/tmp/". rand() . rand() . rand() ."key";
  my $certname = "/tmp/". rand() . rand() . rand() ."cert";
  my $pkeyname= "/tmp/". rand() . rand() . rand() . "pkey";

  unlink $keyname;
  unlink $certname;
  unlink $pkeyname;

  #create envelope keys
  {
      print "Creating symmetric key pair 1 for gapiserver + SEs...";
      
      system("$ENV{ALIEN_ROOT}/bin/openssl genrsa -rand 12938467 -out $keyname.pem 1024 && \
        $ENV{ALIEN_ROOT}/bin/openssl req -batch -new -inform PEM -key $keyname.pem -outform PEM -out ${certname}req.pem && $ENV{ALIEN_ROOT}/bin/openssl x509 -days 3650 -signkey $keyname.pem -in ${certname}req.pem -req -out $certname.pem && $ENV{ALIEN_ROOT}/bin/openssl x509 -pubkey -in $certname.pem > ${pkeyname}.pem");
      
      if ((! -e "$keyname.pem" )|| (! -e "$pkeyname.pem")) {
	  exit (-2);
      }
      
      mkdir "$ENV{'HOME'}/.alien/gapiserver";
      system("mv $keyname.pem $ENV{'HOME'}/.alien/gapiserver/lpriv.pem");
      system("mv $pkeyname.pem $ENV{'HOME'}/.alien/gapiserver/lpub.pem");
      
      print "ok\n";
  }

  unlink $keyname;
  unlink $certname;
  unlink $pkeyname;

  {     
      print "Creating symmetric key pair 1 for gapiserver + SEs...";
      
      system("$ENV{ALIEN_ROOT}/bin/openssl genrsa -rand 12938467 -out $keyname.pem 1024 && \
        $ENV{ALIEN_ROOT}/bin/openssl req -batch -new -inform PEM -key $keyname.pem -outform PEM -out ${certname}req.pem && $ENV{ALIEN_ROOT}/bin/openssl x509 -days 3650 -signkey $keyname.pem -in ${certname}req.pem -req -out $certname.pem && $ENV{ALIEN_ROOT}/bin/openssl x509 -pubkey -in $certname.pem > ${pkeyname}.pem");
      
      if ((! -e "$keyname.pem" )|| (! -e "$pkeyname.pem")) {
	  exit (-2);
      }
      
      mkdir "$ENV{'HOME'}/.alien/gapiserver";
      system("mv $keyname.pem $ENV{'HOME'}/.alien/gapiserver/rpriv.pem");
      system("mv $pkeyname.pem $ENV{'HOME'}/.alien/gapiserver/rpub.pem");
      
      print "ok\n";
  }      

  unlink $keyname;
  unlink $certname;
  unlink $pkeyname;

  # add the ApiService Service Entry
  my $servicekey="ou=ApiService,ou=Services,$config->{FULLLDAPDN}";
  addLdapEntry($servicekey,["objectClass",["organizationalUnit"],
		"ou","ApiService",
                	     ]) ;

  my $key="name=testGAPI,ou=ApiService,ou=Services,$config->{FULLLDAPDN}";
	
  addLdapEntry($key, ["objectClass",["AliEnApiService"],
                      "name", "testGAPI",
                      "host", "$host",
                      "port", "10000",
	              "localprivkeyfile","$ENV{'HOME'}/.alien/gapiserver/lpriv.pem",
	              "localpubkeyfile","$ENV{'HOME'}/.alien/gapiserver/lpub.pem",
	              "remoteprivkeyfile","$ENV{'HOME'}/.alien/gapiserver/rpriv.pem",
	              "remotepubkeyfile","$ENV{'HOME'}/.alien/gapiserver/rpub.pem",
		      "prefork","4",
		      "perlmodule","AlienAS.pl",
		      "sessionlifetime","86000",
		      "sslport","10001",
		      "user","$ENV{'USER'}",
		      "role","admin",
		      "commandlogging","1",
                     ]) or exit(-2);

  print "ok\n";
  print "Starting the service ....\n";
  startService("ApiService") || exit(-2);

  ok(1);

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
    print "THE SE EXISTS!!\nDeleting the apiservice... ";
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
  return 1;
}


sub startService {
  my $service=shift;

  print "Starting the $service...\n";
  my $command="$ENV{ALIEN_ROOT}/bin/alien Start$service";
  $< or $command="su - alienmaster -c \"$command\"";
  system($command) and return;
  print "sleeping...";
  sleep (10);
  print "ok\nChecking if $service is still up ...";
  my $config=new AliEn::Config;
  my $logFile="$config->{LOG_DIR}/$service.log";
	
  # if we run with cronolog, we have to find the latest logfile
  my $latestlogFile=`find $config->{LOG_DIR} -name \"ApiService.log\" 2>/dev/null | tail -1`;
  chomp $latestlogFile;
  if ($latestlogFile ne "$logFile" ) {
	#create a symbolic link to the latest logfile in the standard logfile location
	system("echo Creating symbolic link $latestlogFile $logFile");
	system("unlink $logFile; ln -s $latestlogFile $logFile");
  }	

  $service eq "Monitor" and $logFile=~ s{/Monitor\.}{/ClusterMonitor.};
  if (system("$ENV{ALIEN_ROOT}/bin/alien Status$service") ) {
    print "The $service is dead...\n";
    system("find $config->{LOG_DIR}");
    system("cat", $logFile);
    return;
  }

  print "ok\nChecking if the service is listening...\t";
  open (FILE, "<$logFile") or print "Error opening the log file $logFile" and return;
  my @file=<FILE>;
  close FILE;
  print "Before doing the grep\n$file[0]\n";
  
  grep (/connection\s+successful/i, @file) or print "The gapi service is not listening:\n@file\n" and return;


  return 1;
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
