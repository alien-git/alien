#!/bin/env alien-perl

use strict;
use Test;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

{
  my $org=Net::Domain::hostname();
  eval {
    require AliEn::Portal::PieChart;
  };
  if ($@) {
    print "ERROR requiring AliEn::Portal::PieChart\n $@ \n";
    exit(-2);
  }
  
  my $name="AliEn::Portal::AliEn::$org";		
  eval "require $name";
  if ($@) {
    print "ERROR requiring AliEn::Portal::PieChart\n $@ \n";
    exit(-2);
  }
  my  $tmpfile="/home/alienmaster/AliEn/Html/fonts/arial.ttf";
  
  
  my $font = #Imager::Font->new(face => 'Times New Roman') or
    Imager::Font->new(file=> $tmpfile) or 
	print "Cannot create font object: ",Imager->errstr,"\n" and exit(-2);
  
  my $round="2003-02";
  my $pietype="CPU";
  
  my $l=new AliEn::Logger();
  $l->debugOn("PieChart");
  
  my $piechart = AliEn::Portal::PieChart->new();
  
  $piechart or print "Error creating a new piechart\n" and exit(-2);
  print "Got a pie!!\nRequesting data...\n";
  my $arg = $piechart->RequestData($round,$pietype,0);
  print "Got the data!!\nDoing the pie\n";
  #    $piechart->RequestPie();
  
  print "Done!!\n";
  ok(1);
}

