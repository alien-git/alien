#!/bin/env alien-perl

use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
    $ENV{"X509_CERT_DIR"}="$ENV{'GLOBUS_LOCATION'}/share/certificates";
    $ENV{"GCLIENT_NOPROMPT"}="1";
    print "Connecting to API service ...";
    my $host=Net::Domain::hostname();
    my $gapi = new gapi({host=>"$host",port=>"10000",user=>"$ENV{'USER'}"});
    if (!defined $gapi) {
	exit(-2)
    } else  {
	print "ok\n";

    }

  ok(1);
}
