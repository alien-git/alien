use strict;

use AliEn::UI::Catalogue::LCM::Computer;

my $ui=AliEn::UI::Catalogue::LCM::Computer->new() or exit(-2);


$ui->execute("queueinfo") or exit(-2);

print "DONE!!\n";


