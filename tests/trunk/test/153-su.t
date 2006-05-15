use strict;

use AliEn::UI::Catalogue;

my $cat=AliEn::UI::Catalogue->new({role=>"admin"}) or exit(-2);

print "Connected as admin\n";
$cat->execute("user", "-", "newuser") or exit(-2);

my ($user)=$cat->execute("whoami");
($user eq "newuser") or exit(-2);
print "I'm new user\n";
$cat->execute("user", "-", "admin") or exit(-2);
 ($user)=$cat->execute("whoami");
($user eq "admin") or exit(-2);
