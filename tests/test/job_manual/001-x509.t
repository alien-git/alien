use strict;

use AliEn::X509;

my $d=AliEn::X509->new();
$d->{LOGGER}->debugOn();
my $proxy="/tmp/proxy.$$";
$ENV{X509_USER_PROXY}=$proxy;
$ENV{X509_USER_CERT}="$ENV{ALIEN_HOME}/globus/usercert.pem";
$ENV{X509_USER_KEY}="$ENV{ALIEN_HOME}/globus/userkey.pem";

print "Removing the proxy\n";
unlink $ENV{X509_USER_PROXY};


$d->checkProxy() or print "Error checking the proxy\n" and exit(-2);
(-f $ENV{X509_USER_PROXY}) or print "THERE IS NO PROXY\n" and exit(-2);
$d->checkProxy() or print "Error checking the proxy\n" and exit(-2);
unlink $ENV{X509_USER_PROXY};

print "YUUUHUU \n";
exit
