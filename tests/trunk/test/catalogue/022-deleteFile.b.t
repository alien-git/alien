use strict;

use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Database::SE;

BEGIN { plan tests => 1 }

my $pfn=shift or print "Error getting the pfn\n" and exit(-2);
my $guid=shift or print "Error getting the guid\n" and exit(-2);

#my $admCat = AliEn::UI::Catalogue::LCM->new({user=>"admin"});
#$admCat->execute("removeExpiredFiles");
#$admCat->close();

if (-f $pfn) {
  print "The file is still there :(\n";
  exit(-2);
}

print "ok!\n";





