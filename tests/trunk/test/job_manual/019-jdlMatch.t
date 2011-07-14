use strict;

use AliEn::UI::Catalogue::LCM::Computer;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add")                   or exit(-2);
includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);

addFile(
  $cat, "jdl/matching.jdl", "
executable=\"date\";
"
) or exit(-2);

addFile(
  $cat, "jdl/nomatching.jdl", "
executable=\"date\";
requirements= other.ce==\"wrongce\";
"
) or exit(-2);

my ($jobid) = $cat->execute("submit", "jdl/matching.jdl") or exit(-2);
print "Checking if any job would match $jobid\n";
my ($done) = $cat->execute("jobListMatch", $jobid);
$done or exit(-2);

($jobid) = $cat->execute("submit", "jdl/nomatching.jdl") or exit(-2);
print "Checking if any job would match $jobid\n";
($done) = $cat->execute("jobListMatch", $jobid);
$cat->execute("kill", $jobid);
$done and print "This is not supposed to match!!\n" and exit(-2);

