#!/bin/env alien-perl

use strict;
use Test;

eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

use AliEn::UI::Catalogue;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }



{

    system("whoami");
    system('echo $HOME');
    my $org=Net::Domain::hostname();
    system ("$ENV{ALIEN_ROOT}/bin/alien proxy-destroy >/dev/null 2>&1");
    local $SIG{ALRM} = sub {die("We can't connect\n")};
    $ENV{ALIEN_ORGANISATION}="$org";
    print "TENGO $ENV{ALIEN_ORGANISATION}";
    my $pid=$$;
    my $id=fork;
    (defined $id) or print "Error doing the fork \n" and exit(-2);
    if (!$id){
      sleep(60);
      print "THE CHILDREN KILLS THE FATHER\n";
      kill 9, $pid;
      exit;
    }
    #setDirectDatabaseConnection();

    my $cat=AliEn::UI::Catalogue->new({"role", "admin"});
    $cat or exit (-1);
    
    $cat->execute("pwd") or exit (-2);
    $cat->execute("ls","-al") or print "Error doing ls" and exit (-3);
    $cat->close;
    
    print "ok\n";
    ok(1);
}
