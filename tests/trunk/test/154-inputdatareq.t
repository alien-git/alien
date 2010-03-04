use strict;

use AliEn::UI::Catalogue::LCM::Computer;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("catalogue/003-add") or exit(-2);
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);
my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
