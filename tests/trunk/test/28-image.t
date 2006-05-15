#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

{
    eval {
	require Image::Imlib2;
    };
    if ($@) {
	print "ERROR requiring the Image\n $@ \n";
	exit(-2);
    }
	print "Creating the image\n";
    # create a new image
    my $image = Image::Imlib2->new(200, 200);
    
    $image or print "Error creating the new image\n$!" and exit(-2);
    # set a colour (rgba, so this is transparent orange)
    $image->set_color(255, 127, 0, 127);
    
    # draw a rectangle
    $image->draw_rectangle(50, 50, 50, 50);
    
    # draw a filled rectangle
    $image->fill_rectangle(150, 50, 50, 50);
    
    # draw a line
    $image->draw_line(0, 0, 200, 50);
    
    # save out
    my $ext="png";
    my $file="out.$ext";
    (-e $file) and print "Removing $file\n" and (system ('rm', "$file"));
    my $done=$image->save($file);
    print "Image saved\nChecking if it's there...";

    system("ls", "$file") and 
	print "Error the file is not there!!" and exit(-2);


    my $filename="/home/alienmaster/AliEn/Html/map/earth.png";
    my $image2= Image::Imlib2->load($filename);
    $image2 or print "Error creating the image from $filename\n$!\n" and exit(-2);
    ok(1);
}
