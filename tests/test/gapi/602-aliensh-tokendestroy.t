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
    print "alien-token-destroy              ... ";
    if (system("$ENV{ALIEN_ROOT}/api/bin/alien-token-destroy")) {
	exit (-2);
    }
    if ( -e "/tmp/gclient_token_$UID" ) {
	exit (-2);
    }
    print "ok\n";

    
  ok(1);
}

