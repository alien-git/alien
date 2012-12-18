use strict;

use AliEn::UI::Catalogue::LCM::Computer;

push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit(-1);

addFile(
  $cat, "jdl/jobToKill.jdl", "
executable=\"date\";
inputdata=\"/bin/date\";
requirements=other.ce=='blabla';
"
) or exit(-2);

my ($jobid) = $cat->execute("submit", "jdl/jobToKill.jdl") or exit(-2);
print "Now, wait until the job is 'waiting'\n";
waitWaiting($cat, $jobid) or exit(-2);

print "Let's resubmit the job\n";

my ($newid) = $cat->execute("resubmit", $jobid) or exit(-2);
print "The newid is $newid\n";
waitWaiting($cat, $newid) or exit(-2);

my ($jdl) = $cat->execute("ps", "jdl", $newid);
$cat->execute("kill", $newid);

my $req;
$jdl =~ /\srequirements\s=\s([^;]*)/i and $req = $1;
print "removing the requirement once...";
unless ($req =~ s/member\(other.CloseSE,//g) {
  print "We can't remove the first requirements!!\n";
  exit(-2);
}
print "Now $req\n";
if ($req =~ /member\(other.CloseSE,/) {
  print "There were still restrictions!!\n";
  exit(-2);
}

print "And making sure that we have the original\n";
$req !~ /blabla/ and print "Nope!\n" and exit(-2);
print "ok\n";

sub waitWaiting {
  my $cat   = shift;
  my $jobid = shift;
  my $status;
  my $i = 10;
  while ($i) {
	$i--;
	my ($info) = $cat->execute("top", "-id", $jobid);
	$info and $status = $info->{status};
	$status =~ /WAITING/ and return 1;
	sleep 5;
  }
  $cat->execute("kill", $jobid);

  return;
}
