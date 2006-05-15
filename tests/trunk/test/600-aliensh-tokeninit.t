use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
    my $UID =`echo \$UID\n`;
    chomp $UID;
    if ( $UID eq "") {
	print "UID is undefined!\n";
	exit (-2);
    }

    $ENV{alien_API_HOST}=Net::Domain::hostfqdn();
    $ENV{alien_API_PORT}="10000";		   

    system("rm /tmp/gclient_token_$UID");
    $ENV{"GCLIENT_NOPROMPT"}="1";
    print "alien-token-init                 ... ";
    if (system("printenv |grep alien; printenv | grep GCLIENT; $ENV{ALIEN_ROOT}/api/bin/alien-token-init $ENV{'USER'}")) {
	exit (-2);
    }
    if ( ! -e "/tmp/gclient_token_$UID" ) {
	exit (-2);
    }
    print "ok\n";

  ok(1);
}

