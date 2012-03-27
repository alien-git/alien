#!/bin/env alien-perl

use strict;
use Test;
use AliEn::Database::TaskQueue;

use AliEn::UI::Catalogue::LCM::Computer;


BEGIN { plan tests => 1 }

{
  my $db=AliEn::Database::TaskQueue->new({ROLE=>'admin', PASSWD=>'pass'}) or exit(-2);
  print "Got the database\n";

  my $total=$db->queryValue("select count(*) from JOBTOKEN ");
  print "There are $total entries there.... there should be zero!!!\n"; 
  if ($total){
    print "Let's not panic... If it is because the STAGED job, we are also fine\n";
    my $cat=AliEn::UI::Catalogue::LCM::Computer->new({role=>'admin'}) or exit(-2);
    my @jobs=$cat->execute("top", "-status", "TO_STAGE");
    (@jobs) and $cat->execute("kill", @jobs);
    print "Let's count again\n";
    $total=$db->queryValue("select count(*) from JOBTOKEN ");
    print "There are $total entries there.... there should be zero!!!\n"; 
    $total and exit(-2);  
  } 
  ok(1);
}





