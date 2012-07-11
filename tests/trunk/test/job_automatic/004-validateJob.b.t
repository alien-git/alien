use strict;

use AliEn::UI::Catalogue::LCM::Computer;



$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add")                   or exit(-2);
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
 or exit(-1);
my $id  = shift or print "Error getting the job id\n" and exit(-2);
my $id2 = shift or print "Error getting the job id\n" and exit(-2);


my $procDir = checkOutput($cat, $id) or exit(-2);

print "Let's try with the job that is supposed to fail\n";

my ($info) = $cat->execute("top", "-id", $id2);
$info->{statusId} eq "ERROR_V"
  or print "The job isn't in ERROR_V!!\n" and exit(-2);

print "YUHUUU!!\n";

