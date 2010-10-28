#!/bin/env alien-perl

use strict;
use Test;
use Net::Domain qw(hostname hostfqdn hostdomain);



BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue::LCM;

{
    
  print "Getting an instance of the catalogue";
  my $cat =AliEn::UI::Catalogue::LCM->new({role=>"newuser"});
  $cat or exit(-2);
  my $dir=Net::Domain::hostname();
  print "ok\nChecking if the directory /$dir is there...\t";

  my @dirs=$cat->execute("ls", "/");
  print "Got @dirs\n";

  grep (/$dir/ , @dirs) or print "The directory $dir not there!!\n" and exit(-2);

  ok(1);
}
