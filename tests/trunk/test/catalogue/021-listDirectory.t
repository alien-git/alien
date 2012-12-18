use strict;

use AliEn::UI::Catalogue;


my $cat=AliEn::UI::Catalogue->new({user=>"newuser"}) or exit(-2);


my (@entries)=$cat->execute("ls", "*") or exit(-2);

print "Got @entries\n";
