#!/bin/env alien-perl

use strict;
use Test;



BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue::LCM;

{

    print "Getting an instance of the catalogue";
    my $cat =AliEn::UI::Catalogue::LCM->new({"role", "admin"});
    $cat or exit(-2);
    my $dir="/rmdir_test";
    print "ok\nCreating a new directory\t";

    my ($done)=$cat->execute("mkdir", $dir);
    $done or print "Error creating the directory $dir\n" and exit(-2);

    print "ok\nRemoving the directory\t";

    ($done)=$cat->execute("rmdir", $dir);
    $done or print "Error removing the directory $dir\n" and exit(-2);
    print "ok\nChecking that the directory is not there\n";
    my @dirs=$cat->execute("ls", "/");
    $dir=~ s%/%%;

    grep (/$dir/ , @dirs) and 
      print "The directory is still there!!\n" and exit(-2);

    ok(1);
}
