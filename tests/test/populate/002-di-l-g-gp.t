#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue;

{
    print "Getting an instance of the catalogue";
    my $cat =AliEn::UI::Catalogue->new({"role", "admin"});
    $cat or exit(-2);

    ##########################################################################################
    print "1. lets check the status of the L#L tables using the di l command\n ";
    my $done=$cat->execute("di","l");
    $done or print "Error doing the di command\n" and exit(-2);
    ##########################################################################################
    print "ok\n2. Now, lets check the status of the G#L tables using the di g command\n ";
    $done=$cat->execute("di","g");
    $done or print "Error doing the di command\n" and exit(-2);
    ##########################################################################################
    print "ok\n3. Now, lets check the status of the G#L_PFN tables using the di gp command\n ";
    $done=$cat->execute("di","gp");
    $done or print "Error doing the di command\n" and exit(-2);
    print "1,2,3 DONE \n\n";
		print "ok\n";
		$cat->close();
    ok(1);
}
