#!/bin/env alien-perl
use strict;
use Test;


use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }
 $ENV{ALIEN_JOBAGENT_RETRY}=1;

my $user = "JQUser";
my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});

$cat or exit(-1);
print "Let's get the jobid submitted in the previous test\n";
  
my (@ids)=$cat->execute("top", "-all_status", "-user", $user);
my $id= shift @ids;
my $id2= shift @ids;
($id  and $id->{queueId} )or print "THERE WERE NO IDS" and exit(-2);
($id2  and $id2->{queueId} )or print "THERE WERE NO IDS" and exit(-2);
print "And we have the id : $id->{queueId} and $id2->{queueId}
#ALIEN_OUTPUT $id->{queueId} and $id2->{queueId}\n";

ok(1);
