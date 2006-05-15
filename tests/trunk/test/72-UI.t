#!/bin/env alien-perl

use strict;
use Test;
use AliEn::EGEE::WSRF;
use AliEn::SOAP::WSRF;
use AliEn::X509;
use AliEn::Config;
use AliEn::EGEE::MCAttributes;
use XML::Simple;
use AliEn::EGEE::UI;

BEGIN { plan tests => 1 }

use SOAP::Lite;

{ # check 'UI ...'


  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("71-GAS") or exit(-2);


  checkgContainer();

  createMyproxyProxy();

  my $soap=createGAS();

  print "Creating UI...";

  my $ui = new AliEn::EGEE::UI();
  $ui
    or print "failed.\n"
    and exit(-50);

  print "successful.\nExecuting filecatalog command...";

  my $result = $ui->execute("ls", "/");
  $result
    or print "failed.\n"
    and exit(-70);

  print "successful.\n";

  print "Destroying GAS...";

  my $soapResult = $soap->CallSOAP("GAS", "stop");
  $soapResult
    or print "failed.\n"
    and exit(-80);

  print "successful\n";

  print "Destroying myproxy...";
  $result = system("$ENV{ALIEN_ROOT}/bin/alien -user newuser myproxy-destroy >& /dev/null");
  print "successful.\n";

  ok(1);
}

sub timeOut {
  die "TimeOut!\n";
}
