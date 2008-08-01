use strict;

use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Database::SE;

BEGIN { plan tests => 1 }

my $pfn=shift or print "Error getting the pfn\n" and exit(-2);
my $guid=shift or print "Error getting the guid\n" and exit(-2);




if (-f $pfn) {
  print "The file is still there :(\n";
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
    or exit (-1);

  if (! $cat->{CATALOG}->{DATABASE}->{LFN_DB}->queryValue("select count(*) from TODELETE where guid=string2binary('$guid')")){
    exit(-2);
  }
  print "At least, is in the queue to be deleted\n";
}

print "ok!\n";





