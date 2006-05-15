#!/bin/env alien-perl

use strict;
use Test;


use AliEn::Service::SE;
use AliEn::X509;
use AliEn::UI::Catalogue;
BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  
  includeTest("14-se") or exit(-2);

  my $host=`hostname -s`;
  chomp $host;
  my $fqdn=`hostname -f`;
  chomp $fqdn;
  
  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

  my $key="ou=MonaLisa,ou=Services,$config->{FULLLDAPDN}";

  print "ok\n";

  addLdapEntry($key, ["objectClass", ["AliEnMonaLisa"],
		      "name", "AliEn2-TEST-$host",
		      "shouldUpdate", "false",
		      "host", $fqdn,
		      "apmonConfig", "['pcardaab.cern.ch']",
		     ]) or exit(-2);

  $config=$config->Reload({force=>1});
  print "ok\nStarting MonaLisa...";

  if (system("alien StartMonaLisa")) {
    removeLdapEntry($key);
    exit(-2);
  }
  print "MonaLisa started successfully!!!\n";
  ok(1);
}
