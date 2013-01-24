#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue;

BEGIN { plan tests => 1 }



{

my $cat=AliEn::UI::Catalogue->new({"role", "admin"});
$cat or exit (-1);

$cat->execute("pwd") or exit (-2);
$cat->execute("cd", "/") or exit (-2);
my ($done)=$cat->execute("tree");
print "Got $done\n";
$cat->close;

print "ok\n";
ok(1);
}
