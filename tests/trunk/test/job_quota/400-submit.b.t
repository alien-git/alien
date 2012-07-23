#!/bin/env alien-perl
use strict;
use Test;

use AliEn::Database::TaskQueue;
use AliEn::Service::Optimizer::Job::Quota;
use Net::Domain qw(hostname hostfqdn hostdomain);

use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

print "Connecting to database...";
my $host = Net::Domain::hostfqdn();
my $d =
   AliEn::Database::TaskQueue->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
  or print "Error connecting to the database\n" and exit(-2);


my $id1=shift;
my $id2=shift;

print "Checking if the job $id1 finished correctly\n";
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);
includeTest("job_automatic/008-split") or exit(-2);
includeTest("job_quota/400-submit") or exit(-2);

  my $user = "JQUser";    
  my $userSplit = "JQUserSplit";

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);
  my $cat_split = AliEn::UI::Catalogue::LCM::Computer->new({"user", $userSplit});
  $cat_split or exit(-1);
  my $cat_adm = AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin"});
  $cat_adm or exit(-2);
  
  my $procDir = checkOutput($cat, $id1) or print "Could not check output of $id1" and exit(-2);
  
  waitForProcInfo($d, $id1) or exit(-2);
  
  $cat_adm->execute("calculateJobQuota", "1");

  my $cpucost1 = $d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY join QUEUE_USER using (userid) WHERE user='$user'");
  (defined $cpucost1) or print "Error checking the totalCpuCostLast24h of the user\n" and exit(-2);
  ($cpucost1 > 0) or print "FAILED: totalCpuCost: $cpucost1, not increased at all\n" and exit(-2);

  my $rtime1 = $d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY join QUEUE_USER using (userid) WHERE user='$user'");
  (defined $rtime1) or print "Error checking the totalRunningTimeLast24h of the user\n" and exit(-2);
  ($rtime1 > 0) or print "FAILED: totalRunningTime: $rtime1, not increased at all\n" and exit(-2);
  print "3. PASSED\n\n";

  checkSubJobs($cat_split, $id2, 2) or exit(-2);

  
  waitForSubjobsProcInfo($d, $cat_split, $id2) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");

  my $cpucost2 = $d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY  join QUEUE_USER using (userid) WHERE user='$userSplit'");
  (defined $cpucost2) or print "Error checking the totalCpuCostLast24h of the user\n" and exit(-2);
  ($cpucost2 > 0) or print "FAILED: totalCpuCost: $cpucost2, not increased at all\n" and exit(-2);

  my $rtime2 = $d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY  join QUEUE_USER using (userid) WHERE user='$userSplit'");
  (defined $rtime2) or print "Error checking the totalRunningTimeLast24h of the user\n" and exit(-2);
  ($rtime2 > 0) or print "FAILED: totalRunningTime: $rtime2, not increased at all\n" and exit(-2);
  print "4. PASSED\n\n";

  print "5. Modify the maxTotalRunningTime as $rtime2\n";
  my $userSplitId=$d->queryValue("SELECT userid from QUEUE_USER where user='$userSplit'");
  $d->update("PRIORITY", {maxTotalRunningTime => $rtime2}, "userid='$userSplitId'");
  $cat->execute("jquota", "list", "$userSplit");
  assertEqualJobs($d, $userSplit, "maxTotalRunningTime", $rtime2) or exit(-2);
  print "5. DONE\n\n";

  print "6. Submit 1 job and 2 jobs\n";
  $cat_split->execute("submit", "jdl/sum.jdl")        and print "FAILED: MUST BE DENIED\n" and exit(-2);
  $cat_split->execute("submit", "jdl/Split2Jobs.jdl") and print "FAILED: MUST BE DENIED\n" and exit(-2);
  print "6. PASSED\n\n";

  print "7. Modify the maxTotalCpuCost as $cpucost2 and the maxTotalRunningTime as 1000 back \n";
  $d->update("PRIORITY", {maxTotalCpuCost => $cpucost2, maxTotalRunningTime => 1000}, "userid='$userSplitId'");
  $cat->execute("jquota", "list", "$userSplit");
  assertEqualJobs($d, $userSplit, "maxTotalCpuCost",     $cpucost2) or exit(-2);
  assertEqualJobs($d, $userSplit, "maxTotalRunningTime", 1000)      or exit(-2);
  print "7. DONE\n\n";

  print "8. Submit 1 job and 2 jobs\n";
  $cat_split->execute("submit", "jdl/sum.jdl")        and print "FAILED: MUST BE DENIED\n" and exit(-2);
  $cat_split->execute("submit", "jdl/Split2Jobs.jdl") and print "FAILED: MUST BE DENIED\n" and exit(-2);
  print "8. PASSED\n\n";

  print "9. Modify the maxUnfinishedJobs as 0 and the maxTotalCpuCost as 1000 back \n";
  $d->update("PRIORITY", {maxUnfinishedJobs => 0, maxTotalCpuCost => 1000}, "userid='$userSplitId'");
  $cat->execute("jquota", "list", "$userSplit");
  assertEqualJobs($d, $userSplit, "maxUnfinishedJobs", 0)    or exit(-2);
  assertEqualJobs($d, $userSplit, "maxTotalCpuCost",   1000) or exit(-2);
  print "9. DONE\n\n";

  print "10. Submit 1 job and 2 jobs\n";
  $cat_split->execute("submit", "jdl/sum.jdl")        and print "FAILED: MUST BE DENIED\n" and exit(-2);
  $cat_split->execute("submit", "jdl/Split2Jobs.jdl") and print "FAILED: MUST BE DENIED\n" and exit(-2);
  print "10. PASSED\n\n";



sub waitForProcInfo {
  my $d     = shift;
  my $id    = shift;
  my $sleep = (shift or 10);
  my $times = (shift or 5);

  my $counter = 0;
  while (1) {
	my $cost    = $d->queryValue("SELECT cost FROM QUEUEPROC WHERE queueId=$id");
	my $runtime = $d->queryValue("SELECT runtimes FROM QUEUEPROC WHERE queueId=$id");
	print "ProcInfo Job $id (Cost:$cost Runtimes:$runtime)\n";
	($cost > 0 or $runtime > 0) and print "OK\n" and return 1;

	($counter > $times)
	  and print "We have been waiting for more than $counter *$sleep seconds.... let's quit\n"
	  and return;
	print "The father sleeps (waiting for ProcInfo)\n";
	sleep $sleep;
	$counter++;
  }

  return;
}

sub waitForSubjobsProcInfo {
  my $d   = shift;
  my $cat = shift;
  my $id  = shift;

  my @subids;
  my @list = $cat->execute("ps", "-As", "-id $id");
  foreach (@list) {
	my $jid = (split /\s+/, $_)[1];
	($jid =~ /^-(\d+)/) and push @subids, $1;
  }

  my $counter = 0;
  foreach (@subids) {
	waitForProcInfo($d, $_) and return 1;
  }
  return;
}
