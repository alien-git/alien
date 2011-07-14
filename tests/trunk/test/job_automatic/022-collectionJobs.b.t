use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{

  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("job_automatic/008-split") or exit(-2);
  my $id = shift;
  $id or print "Error getting the job id!\n" and exit(-2);
  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
	or exit(-1);
  checkSubJobs($cat, $id, 2) or exit(-2);
  $cat->close();

  print "ok\n";

}
