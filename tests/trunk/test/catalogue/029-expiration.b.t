use strict;

use AliEn::UI::Catalogue::LCM;
my $cat=AliEn::UI::Catalogue::LCM->new({"user", "newuser",});
  $cat or exit (-1);

print "And now let's check if they are there";
$cat->execute('ls', 'expired/touch') and exit(-2);
$cat->execute('ls', 'expired/file') and exit(-2);
$cat->execute('ls', 'expired/touch.expired') or exit(-2);
$cat->execute('ls', 'expired/file.expired') or exit(-2);

print "ok\n";
