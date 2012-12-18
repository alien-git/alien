#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue::LCM;

{

    print "Getting an instance of the catalogue";
    my $cat =AliEn::UI::Catalogue::LCM->new({"role", "admin"});
    $cat or exit(-2);
    my $dir="populate_test";
    print $dir;
    print "ok\nCreating a new directory\t";

    my ($done)=$cat->execute("mkdir", $dir);
    $done or print "Error creating the directory $dir\n" and exit(-2);

    ($done)=$cat->execute("silent", 1);
    ##########################################################################################
    ($done) = $cat->execute("populate", $dir,  "2", "4","5");
    $done or print "Error populating in the directory $dir\n" and exit(-2); 

    print "ok\nPopulated the directory\n";
    print "ok\nNow, Removing the directory\t";

    ($done)=$cat->execute("rmdir", $dir);
    $done or print "Error removing the directory $dir\n" and exit(-2);
    print "ok\nChecking that the directory is not there\n";
    my @dirs=$cat->execute("ls", "/");
    $dir=~ s%/%%;

    grep (/$dir/ , @dirs) and 
      print "The directory is still there!!\n" and exit(-2);

    ##########################################################################################
		print "ok\nTrying to populate in a non existing directory...";
    ($done)=$cat->execute("populate", "myDir1","2","4","5");
    $done or print "Error making the directory 'myDir1' worked!\n" and exit(-2);

    ##########################################################################################
		print "ok\nDoing the populate directory with +5 etc i.e. using randomeness\n ";
    ($done)=$cat->execute("populate", "myDir2","+4","+2","+2","+10");

    $done or print "Error populating the directory using + (randomness) in 'myDir2'\n" and exit(-2);
    ##########################################################################################
		print "ok\n";
    ($done)=$cat->execute("rmdir", "-r", "myDir2");
    ($done)=$cat->execute("rmdir", "-r", "myDir1");
    $done or print "Error removing the directory myDir2\n" and exit(-2);

		$cat->close();
    ok(1);
}
