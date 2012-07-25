use strict;

use AliEn::UI::Catalogue::LCM;
use Data::Dumper;

my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser"}) or exit(-2);
my $se=$cat->{CONFIG}->{SE_FULLNAME};
print "Checking the entries of $se\n";
my $info=$cat->execute("masterSE", $se) or exit(-2);
print Dumper($info);


print "OK\n";
