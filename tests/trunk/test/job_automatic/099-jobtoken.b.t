#!/bin/env alien-perl

use strict;
use Test;
use AliEn::Database::Admin;
BEGIN { plan tests => 1 }

{
  my $db=AliEn::Database::Admin->new({ROLE=>'admin', PASSWD=>'pass'}) or exit(-2);
  print "Got the database\n";

  my $total=$db->queryValue("select count(*) from jobToken ");
  print "There are $total entries there.... there should be zero!!!\n"; 
  $total and exit(-2);
  ok(1);
}





