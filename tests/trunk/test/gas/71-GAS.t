#!/bin/env alien-perl

use strict;
use Test;
#use AliEn::EGEE::WSRF;
use AliEn::SOAP::WSRF;
use AliEn::X509;
use AliEn::Config;
#use AliEn::EGEE::MCAttributes;
use XML::Simple;

BEGIN { plan tests => 1 }

use SOAP::Lite;

{ # check 'GAS ...'
  checkgContainer();

  createMyproxyProxy();

  my $soap=createGAS() or exit(-2);

#  print "Executing metacatalog command...";

#  $soapResult = $soap->CallSOAP("GAS", "queryByAttributes", new AliEn::EGEE::MCAttributes(["server"], ["bla"]), 1000, 0);
#  $soapResult and ref($soapResult->paramsall) eq "ARRAY"
#    or print "failed.\n"
#    and exit(-60);

#  print "successful (" . join(" ", @{$soapResult->paramsall}) . ").\n";

  print "Executing filecatalog command...";

  my $soapResult = $soap->CallSOAP("GAS", "ls", "/");
  $soapResult and $soapResult->result
    or print "failed.\n"
    and exit(-70);

  my $tree = XML::Simple::XMLin($soapResult->result);
  $tree
    and (ref($tree->{Body}->{EntryList}->{Entry}) eq "ARRAY" or
         ref($tree->{Body}->{EntryList}->{Entry}) eq "HASH")
    or print "failed.\n"
    and exit(-80);

  my $array = $tree->{Body}->{EntryList}->{Entry};
  (ref($array) eq "ARRAY")
    or $array = [$tree->{Body}->{EntryList}->{Entry}];

  my $list;
  foreach (@$array) {
    $list .= $_->{Name} . " ";
  }

  print "successful (" . $list . ").\n";

  print "Destroying GAS...";

  $soapResult = $soap->CallSOAP("GAS", "stop");
  $soapResult
    or print "failed.\n"
    and exit(-90);

  print "successful\n";

  print "Destroying myproxy...";
  my $result = system("$ENV{ALIEN_ROOT}/bin/alien -user newuser myproxy-destroy >& /dev/null");
  print "successful.\n";

  ok(1);
}

sub timeOut {
  die "TimeOut!\n";
}

sub checkgContainer{
  print "Checking for script ($ENV{ALIEN_ROOT}/scripts/gContainer.pl)...";
  (-e "$ENV{ALIEN_ROOT}/scripts/gContainer.pl")
    or print "failed.\n"
    and exit(-10);

  print "successful.\n";

  print "Checking if gContainer is up...";
  alarm(60);
  my $command="$ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/gContainer.pl status >& /dev/null";
  $< or $command="su - alienmaster -c \"$command\"";
  my $result = system($command);
  $result == 0
    or print "failed.\n"
    and exit(-11);
  alarm(0);
  print "successful.\n";


}
sub createMyproxyProxy {
  my $config = new AliEn::Config();

  print "Creating myproxy proxy...";
  my $result = 0;
  $ENV{ALIEN_USER} = "newuser";
  $ENV{ALIEN_MYPROXY_SERVER}="localhost";
#  my $pid = fork();
#  unless ($pid) {
    #child
  if (open (MYPROXY, "$ENV{ALIEN_ROOT}/bin/alien -user newuser myproxy-init -S <<EOF
test123
test123
EOF|")) {
    my @output=<MYPROXY>;
    my $error=close MYPROXY;
    $result = 1 if (grep (/A proxy valid for/, @output));
    print "GOT @output\n";
  }

#    exit $result;
#  } else {
#    #parent
#    eval {
#      $SIG{ALRM} = \&timeOut;
#      alarm(30);
#      wait();
#      $result = $?;
#      alarm(0);
#    };
#    if ($@) {
#      print "timeout (Killing PID $pid)";
#      kill 9, $pid;
#    }
#  }

  if (!$result) {
    print "Do we have a certificate??\n";
    system ("ls", "-la", "$ENV{ALIEN_HOME}/globus");
    print "failed.\n";
    exit(-20);
  }
  print "successful.\n";
}

sub createGAS {

  print "Creating GAS...";

  my $userContext = { 'name' => $ENV{ALIEN_USER} };

  my $cert=AliEn::X509->new();
  unless ($cert->load("$ENV{ALIEN_HOME}/globus/usercert.pem")) {
    print "failed (could not load certificate).\n";
    exit(-30);
  }
  my $subject=$cert->getSubject();

  $userContext->{subject} = $subject;
  $userContext->{'myproxy-password'} = "test123";
  $userContext->{debug} = 1;
  my $ip = `hostname -i`;
  chomp($ip);
  $userContext->{location} = $ip;

  my $soap = new AliEn::SOAP::WSRF;
  $soap->Connect({uri => $AliEn::EGEE::WSRFHelper::staticConfiguration->{namespace} . "gFactory",
                          name => "gFactory",
                          address => 'https://localhost:50000/Session/gFactory/gFactory'});

  my $serviceRequest = { 'name' => "GAS",
                         'type' =>  1,
                         'params' => [ {'user' => $ENV{ALIEN_USER}, 'debug' => 5} ]
                       };

  $serviceRequest->{params}->[0]->{lifetime} = 300;

  my $r=$soap->CallSOAP("gFactory", "getService", $userContext, $serviceRequest);

  my ($errorCode, $errorString, $wsAddress) = ($r->result, $r->paramsout);

  if ($errorCode != 0 or !$wsAddress) {
    print "failed ($errorCode, $errorString).\n";
    exit(-40);
  }

  print "successful (Got service $wsAddress->{'EndpointReference'}->{'ReferenceProperties'}->{'ResourceID'}).\n";

  print "Pinging GAS...";

  $ENV{URL} or $ENV{URL} = "";
  my $wsa  =  WSRF::GSutil::createWSAddress( module=> 'GAS',
                                             path  => '/WSRF/GAS/',
                                             ID => $wsAddress->{'EndpointReference'}->{'ReferenceProperties'}->{'ResourceID'});
  $soap->Connect({uri => $AliEn::EGEE::WSRFHelper::staticConfiguration->{namespace} . "GAS",
                          address => $wsAddress->{'EndpointReference'}->{'Address'},
                          name => "GAS",
                          wsaddress=>$wsa });

  my $soapResult = $soap->CallSOAP("GAS", "ping");
  $soapResult
    or print "failed.\n"
    and exit(-50);

  print "successful.\n";
  return $soap;
}
