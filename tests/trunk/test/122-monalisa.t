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
  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

  my $key="ou=MonaLisa,ou=Services,$config->{FULLLDAPDN}";

  print "ok\n";
  addLdapEntry($key, ["objectClass", ["AliEnSE"],
		      "name", "testSE",
		      "host", "$host",
		      "mss", "File",
		      "savedir", "$config->{LOG_DIR}/SE_DATA",
		      "port", "8092",
		      "certsubject",$subject,
		     ]) or exit(-2);

  $config=$config->Reload({force=>1});
  print "ok\nCreating the database...";

  system("alien StartMonaLisa") and exit(-2);

  print "Monalisa started successfully!!!\n";
  ok(1);



}
