#!/bin/env alien-perl

use strict;
use Test;
use AliEn::Config;
use AliEn::X509;

BEGIN { plan tests => 1 }

use SOAP::Lite;

{ # check 'gContainer ...'
  print "WARNING: This test script modifies your Environment file and your myproxy configuration.\n";
  print "         If this is a production system check the files afterwards.\n";

  print "Checking for script ($ENV{ALIEN_ROOT}/scripts/gContainer.pl)...";
  (-e "$ENV{ALIEN_ROOT}/scripts/gContainer.pl")
    or print "failed.\n"
    and exit(-10);

  print "successful.\n";

  my $config = new AliEn::Config();
  $config
    or die "Could not get config";
  my $user=getpwuid($<);
  #if we run the tests as root, we are going to do a su to alienmaster
  $< or $user="alienmaster";
  my @list=getpwnam($user);
  my $gContainerDir = "$list[7]/.alien/identities.".lc($config->{ORG_NAME})."/gContainer";
  my $CertDir = $gContainerDir . "/$config->{HOST}";
  print "Checking if certificates are in place ($CertDir) ...";

  if (-s "$CertDir/key.pem" and -s "$CertDir/cert.pem") {
    print "successful.\n";
  } else {
    print "not found.\n";
    print "Trying to copy certificates...";

    my @locations = ("$ENV{HOME}/identities.".lc($config->{ORG_NAME}), "$ENV{ALIEN_HOME}/globus", "$ENV{HOME}/.globus");
    my $location;

    for (@locations) {
      if (-s "$_/userkey.pem" and -s "$_/usercert.pem") {
        $location = $_;
        last;
      }
    }

    unless ($location) {
      print "failed (certitificates not found).\n";
      exit(-20);
    }

    system("mkdir -p $CertDir");
    system("cp $location/userkey.pem $CertDir/key.pem");
    system("cp $location/usercert.pem $CertDir/cert.pem");

    $< or system("chown -R alienmaster:alienmaster $gContainerDir");

    if (-s "$CertDir/key.pem" and -s "$CertDir/cert.pem") {
      print "successful.\n";
    } else {
      print "failed (copying failed).\n";
      exit(-30);
    }
  }

  my $cert = AliEn::X509->new();
  unless ($cert->load("$CertDir/cert.pem")) {
    print "ERROR getting subject of server certificate ($CertDir/cert.pem).\n";
    exit(-40);
  }
  my $serverSubject = $cert->getSubject();

  print "Adding subject information to environment file...";

  open FILE, "$ENV{ALIEN_HOME}/Environment";
  my @env = <FILE>;
  close FILE;

  my $addDomain = !(grep /^export ALIEN_MYPROXY_DOMAIN/, @env);
  my $addServer = !(grep /^export MYPROXY_SERVER_DN/, @env);
  if ($addDomain or $addServer) {
    open FILE, ">>$ENV{ALIEN_HOME}/Environment";
    print FILE "export ALIEN_MYPROXY_DOMAIN=\"$serverSubject\"\n" if ($addDomain);
    print FILE "export MYPROXY_SERVER_DN=\"$serverSubject\"\n" if ($addServer);
    close FILE;

    print "successful.\n";
  } else {
    print "already there.\n";
  }

  print "Checking for myproxy server... (".($config->{MYPROXY_SERVER} ||"undef").")";
  if (!$config->{MYPROXY_SERVER} or $config->{MYPROXY_SERVER} eq "localhost") {
    $ENV{X509_USER_CERT}="$ENV{ALIEN_HOME}/globus/usercert.pem";
    $ENV{X509_USER_KEY}="$ENV{ALIEN_HOME}/globus/userkey.pem";

    system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-myproxy stop >& /dev/null");

    open FILE, "$ENV{ALIEN_ROOT}/etc/myproxy-server.config";
    my @myproxyConfig = <FILE>;
    close FILE;
    my $addAccCred = !(grep /^accepted_credentials "\*"/, @myproxyConfig);
    my $addAuthRetr = !(grep /^authorized_retrievers "\*"/, @myproxyConfig);

    if ($addAccCred or $addAuthRetr) {
      open FILE, ">>$ENV{ALIEN_ROOT}/etc/myproxy-server.config";
      print FILE "accepted_credentials \"*\"\n" if ($addAccCred);
      print FILE "authorized_retrievers \"*\"\n" if ($addAuthRetr);
      close FILE;
    }

    system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-myproxy start");

    my $result = system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-myproxy status >& /dev/null");

      # hack, until alien-myproxy status is working properly
#    $result = 0;
    $result=system("ps -Ao command |grep myproxy |grep -v grep");
    if (! $result ) {
      print "ok (started and running).\n";
    } else {
      print "failed (starting).\n";
      print `cat $ENV{ALIEN_ROOT}/etc/myproxy/myproxy.log`;
      exit(-50);
    }
  } else {
    print "not local (assuming to be ok)\n";
  }

  my $prepend="";
  my $append="";
  if (! $<) {
    $prepend="su - alienmaster -c\"";
    $append="\"";
  }
  print "Stopping gContainer (if running).\n";
  system("$prepend $ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/gContainer.pl stop >& /dev/null $append");

  alarm(60);
  print "Starting gContainer...";
  my $result = system("$prepend export MYPROXY_SERVER_DN='$serverSubject'; $ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/gContainer.pl start --debug");# >& /dev/null$append");
  alarm(0);
  $result == 0
    or print "failed.\n"
    and exit(-60);

  print "successful.\n";
  print "Checking if gContainer is up...";

  $result = system("$prepend $ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/gContainer.pl status >& /dev/null$append");
  $result == 0
    or print "failed.\n"
    and exit(-70);

  print "successful.\n";

  ok(1);
}
