use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{

  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("26-ProcessMonitorOutput") or exit(-2);
  my $id=shift;
  $id or print "Error getting the job id!\n" and exit(-2);
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  checkOutput($cat, $id) or exit(-2);
  $cat->close();

  print "ok\n";

}
