use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("job_automatic/008-split") or exit(-2);

my $id = shift or print "Error getting the id of the job\n" and exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);

checkSubJobs($cat, $id, 3) or exit(-2);
