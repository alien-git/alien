#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  my $id  = shift;
  $id or print "Error getting the id\n" and exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit(-1);

  
  print "Checking that $id is in ERROR_EW";
  my ($top)=$cat->execute("top", "-id", $id);
  
  ($top) and ($top->{status} ne "ERROR_EW") and 
    print "Error, job is in $top->{status}, should be in ERROR_EW\n" and exit(-2);
  
  if (!$top){ 
	my ($info)=$cat->execute("ps", "trace", $id, "all");
	   
	my $found=0;
	foreach my $entry (@$info){
	if ($entry->{trace}=~ /ERROR_EW/) {
	  $found=1;
	  last;
	  }
	};
  
    $found or print "Error, there was no trace about the job being in ERROR_EW\n" and exit(-2);
  }

  print "OK!\n";
  $cat->close();
  ok(1);
}
