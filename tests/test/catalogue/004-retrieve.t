#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM;

BEGIN { plan tests => 1 }



{

my $cat=AliEn::UI::Catalogue::LCM->new({"role", "admin",});
$cat or exit (-1);
my $file="/bin/date";

my @orig=$cat->execute("whereis", "$file");
@orig or exit (-2);

my @copy=$cat->execute("get", "-f", "$file");
@copy or exit (-2);
my $origFile=$orig[1];
$origFile=~ s/^file:\/\/[^\/]*//;
print "Checking that both files are identical...\n";
my $diff=system("diff", $origFile, $copy[0]);

$diff and print "THE FILES ARE NOT IDENTICAL!!\n" and exit(-3);
ok(1);
}
