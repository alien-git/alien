use strict;

use AliEn::UI::Catalogue::LCM;
use Data::Dumper;


my $ui=AliEn::UI::Catalogue::LCM->new() or exit(-2);

my ($space)=$ui->execute('df') or exit(-2);

print Dumper($space);
$space->{name} or print "This is not a hash\n" and exit(-2);

print "ok\n";
