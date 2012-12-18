#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue::LCM::Computer;

{
  print "This test will stop the CE if it was already running....\n";

  system ("alien StopCE");
  print "And let's kill all the waiting jobs";
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user","$ENV{'USER'}","role","admin"}) or exit(-1);
  
  my (@jobs)=$cat->execute("top","-status SPLIT");
  foreach my $job (@jobs){
    $cat->execute("kill", $job->{queueId}) 
   }
 (@jobs)=$cat->execute("top");
  foreach my $job (@jobs){
     $cat->execute("kill", $job->{queueId}) 
   }
   
  use Data::Dumper;

  print "done\n";
  ok(1);
}
