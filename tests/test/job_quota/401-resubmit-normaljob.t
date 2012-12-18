#!/bin/env alien-perl
use strict;
use Test;


use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

my $user = "JQUser";
my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});

$cat or exit(-1);
print "Let's get the jobid submitted in the previous test\n";
  
my (@ids)=$cat->execute("top", "-all_status", "-user", $user);
my $id= shift @ids;
($id  and $id->{queueId} )or print "THERE WERE NO IDS" and exit(-2);
print "And we have the id : $id->{queueId}
#ALIEN_OUTPUT $id->{queueId}\n";

ok(1);
