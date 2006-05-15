#!/bin/env alien-perl

use strict;
use Test;



BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue::LCM;

{

    print "Getting an instance of the catalogue";
    my $cat =AliEn::UI::Catalogue::LCM->new({"role", "admin"});
    $cat or exit(-2);
    my $dir="/mkdir_test.$$";
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

		print "ok\nTrying to create a directory in a non existing directory...";
    ($done)=$cat->execute("mkdir", "$dir/child");
    $done and print "Error making the directory '$dir/child' worked!\n" and exit(-2);

		print "ok\nDoing the directory with -p ";
    ($done)=$cat->execute("mkdir", "-p", "$dir/child");
    $done or print "Error making the directory  -p '$dir/child'\n" and exit(-2);

		print "ok\nDoing the directory again with -p (should not fail!!)..";
    ($done)=$cat->execute("mkdir", "-p","$dir/child");
    $done or print "Error making the directory '$dir/child' again failed!\n" and exit(-2);
		
		print "ok\n";


    ($done)=$cat->execute("rmdir", "-r", $dir);
    $done or print "Error removing the directory $dir\n" and exit(-2);

		$cat->close();
    ok(1);
}
