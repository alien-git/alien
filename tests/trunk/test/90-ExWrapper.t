#!/bin/env alien-perl

use strict;
use Test;

use AliEn::SOAP::WSRF;
use XML::Simple;

BEGIN { plan tests => 1 }

use SOAP::Lite;

{ # check 'ExWrapper ...'
  print "Checking for script ($ENV{ALIEN_ROOT}/scripts/gContainer.pl)...";
  (-e "$ENV{ALIEN_ROOT}/scripts/gContainer.pl")
    or print "failed.\n"
    and exit(-10);

  print "successful.\n";

  print "Checking if gContainer is up...";

  my $result = system("su - alienmaster -c \"$ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/gContainer.pl status >& /dev/null\"");
  $result == 0
    or print "failed.\n"
    and exit(-11);

  print "successful.\n";

  my $userContext = { 'name' => $ENV{ALIEN_USER} };
  $userContext->{debug} = 1;

  my $ip = `hostname -i`;
  chomp($ip);
  $userContext->{location} = $ip;

  my $execName = "/tmp/sleeper" . rand;
  my $delay = 5;
  open FILE, ">$execName";
  print FILE "sleep $delay\n";
  close FILE;
  chmod 0777, $execName;

  my $serviceRequest = { 'name' => "ExWrapper",
                        'type' =>  1,
                        'params' => [ {'user' => $ENV{ALIEN_USER}, 'debug' => 5,
                                        'exec' => $execName} ]
                      };

  $ENV{URL} or $ENV{URL} = "";

  open SAVEOUT, ">&STDOUT";
  open SAVEERR, ">&STDERR";

  if (1) {
    print "Creating ExWrapper...";

    my $soap = new AliEn::SOAP::WSRF();
    $soap->Connect({uri => $AliEn::EGEE::WSRFHelper::staticConfiguration->{namespace} . "gFactory",
                            name => "gFactory",
                            address => 'https://localhost:50000/Session/gFactory/gFactory'});

    my $r=$soap->CallSOAP("gFactory", "getService", $userContext, $serviceRequest);
    my ($errorCode, $errorString, $wsAddress) = ($r->result, $r->paramsout);

    if ($errorCode != 0 or !$wsAddress) {
      print "failed ($errorCode, $errorString).\n";
      exit(-40);
    }

    print "successful (Got service $wsAddress->{'EndpointReference'}->{'ReferenceProperties'}->{'ResourceID'}).\n";

    print "Pinging ExWrapper...";

    my $wsa  =  WSRF::GSutil::createWSAddress( module=> 'ExWrapper',
                                              path  => '/WSRF/ExWrapper/',
                                              ID => $wsAddress->{'EndpointReference'}->{'ReferenceProperties'}->{'ResourceID'});
    $soap->Connect({uri => $AliEn::EGEE::WSRFHelper::staticConfiguration->{namespace} . "ExWrapper",
                            address => $wsAddress->{'EndpointReference'}->{'Address'},
                            name => "ExWrapper",
                            wsaddress=>$wsa });

    my $soapResult = $soap->CallSOAP("ExWrapper", "ping");
    $soapResult
      or print "failed.\n"
      and exit(-50);

    print "successful.\n";

    print "Waiting...";
    sleep ($delay + 5);
    print "done.\n";

    print "Pinging ExWrapper (the service should be dead by now)...";

    open(STDOUT, ">/dev/null");
    open(STDERR, ">/dev/null");
    $soapResult = $soap->CallSOAP("ExWrapper", "ping");
    open STDOUT, ">&SAVEOUT";
    open STDERR, ">&SAVEERR";
    $soapResult
      and print "successful (not good!).\n"
      and exit(-60);

    print "failed (very good!).\n";
  }

  print "Creating ExWrapper (providing lifetime)...";

  my $lifetime = 5;
  $serviceRequest->{params}->[0]->{lifetime} = $lifetime;

  my $soap2 = new AliEn::SOAP::WSRF;
  $soap2->Connect({uri => $AliEn::EGEE::WSRFHelper::staticConfiguration->{namespace} . "gFactory",
                          name => "gFactory",
                          address => 'https://localhost:50000/Session/gFactory/gFactory'});

  my $r=$soap2->CallSOAP("gFactory", "getService", $userContext, $serviceRequest);
  my ($errorCode, $errorString, $wsAddress) = ($r->result, $r->paramsout);

  if ($errorCode != 0 or !$wsAddress) {
    print "failed ($errorCode, $errorString).\n";
    exit(-61);
  }

  print "successful (Got service $wsAddress->{'EndpointReference'}->{'ReferenceProperties'}->{'ResourceID'}).\n";

  print "Pinging ExWrapper...";

  my $wsa  =  WSRF::GSutil::createWSAddress( module=> 'ExWrapper',
                                             path  => '/WSRF/ExWrapper/',
                                             ID => $wsAddress->{'EndpointReference'}->{'ReferenceProperties'}->{'ResourceID'});
  $soap2->Connect({uri => $AliEn::EGEE::WSRFHelper::staticConfiguration->{namespace} . "ExWrapper",
                          address => $wsAddress->{'EndpointReference'}->{'Address'},
                          name => "ExWrapper",
                          wsaddress=>$wsa });

  my $soapResult = $soap2->CallSOAP("ExWrapper", "ping");
  $soapResult
    or print "failed.\n"
    and exit(-62);

  print "successful.\n";

  print "Waiting...";
  sleep ($lifetime + 5);
  print "done.\n";

  print "Pinging ExWrapper (the service should be dead by now)...";

  open(STDOUT, ">/dev/null");
  open(STDERR, ">/dev/null");
  $soapResult = $soap2->CallSOAP("ExWrapper", "ping");
  open STDOUT, ">&SAVEOUT";
  open STDERR, ">&SAVEERR";
  $soapResult
    and print "successful (not good!).\n"
    and exit(-70);

  print "failed (very good!).\n";

  unlink $execName;

  ok(1);
}

exit;

# small hack to prevent annoying warning: name "main::SAVEERR" used only once: possible typo at ...
<SAVEOUT>;
<SAVEERR>;
