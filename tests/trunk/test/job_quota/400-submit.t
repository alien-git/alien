#!/bin/env alien-perl
use strict;
use Test;

use AliEn::Database::TaskPriority;
use AliEn::Service::Optimizer::Job::Quota;
use Net::Domain qw(hostname hostfqdn hostdomain);

use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

print "Connecting to database...";
my $host = Net::Domain::hostfqdn();
my $d =
   AliEn::Database::TaskPriority->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
  or print "Error connecting to the database\n" and exit(-2);

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add")                or exit(-2);
  includeTest("file_quota/01-calculateFileQuota") or exit(-2);

  my $user = "JQUser";
  my $cat_adm = AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin"});
  $cat_adm or exit(-1);
  $cat_adm->execute("addUser", $user);
  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);
  my ($pwd) = $cat->execute("pwd") or exit(-2);
  $cat->execute("cd") or exit(-2);

  cleanDir($cat, $pwd);

  #  cleanDir($cat, "/proc/$user");
  $cat->execute("mkdir", "-p", "jdl")        or exit(-2);
  $cat->execute("mkdir", "-p", "bin")        or exit(-2);
  $cat->execute("mkdir", "-p", "split/dir1") or exit(-2);
  $cat->execute("mkdir", "-p", "split/dir2") or exit(-2);

  print "Connecting to Database alien_system... Need to update the quota tables .. :P  \n";
  my $d = AliEn::Database->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "alien_system", "ROLE", "admin" })
    or print "Error connecting to the database\n" and exit(-2);
  #refreshLFNandGUIDtable($cat_adm);
  print "-1. Set the file quota (maxNbFiles 10000, maxTotalSize 10000000)\n";
  $d->update("FQUOTAS", {maxNbFiles => 10000, maxTotalSize => 10000000}, "user='$user'");
  assertEqual($d, $user, "maxTotalSize", 10000000) or exit(-2);
  assertEqual($d, $user, "maxNbFiles",   10000)    or exit(-2);
  print "-1. DONE\n\n";

  print "Reconnecting to Database processes \n";
  $d = AliEn::Database::TaskPriority->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
    or print "Error connecting to the database\n" and exit(-2);
  
  print "0. Set the job quotas (maxTotalRunningTime, 2000, maxUnfinishedJobs, 2000, maxparallelJobs, 2000)\n";
  $d->update("PRIORITY", {maxTotalRunningTime => 2000, maxUnfinishedJobs => 2000, maxparallelJobs=> 2000}, "user='$user'");
  assertEqualJobs($d, $user, "maxTotalRunningTime", 2000) or exit(-2);
  assertEqualJobs($d, $user, "maxUnfinishedJobs",   2000)    or exit(-2);
  assertEqualJobs($d, $user, "maxparallelJobs",   2000)    or exit(-2);
  print "0. DONE\n\n";

  addFile($cat, "split/dir1/file1", "This is a test") or exit(-2);
  $cat->execute("cp", "split/dir1/file1", "split/dir1/file2") or exit(-2);
  $cat->execute("cp", "split/dir1/file1", "split/dir2/file3") or exit(-2);

  addFile(
	$cat, "bin/sum", "#!/bin/sh
sum=0
for ((i=1; i<=1000000; i++))
do
    sum=\$((\$sum+\$i));
done
echo \"sum: \$sum\"
", "r"
  ) or exit(-2);

  addFile($cat, "jdl/sum.jdl", "Executable=\"sum\";", "r") or exit(-2);

  my ($dir) = $cat->execute("pwd") or exit(-2);
  addFile(
	$cat, "jdl/Split2Jobs.jdl", "Executable=\"sum\";
Split=\"directory\";
InputData=\"LF:${dir}split/*/*\";", "r"
  ) or exit(-2);
  addFile(
	$cat, "jdl/Split3Jobs.jdl", "Executable=\"sum\";
Split=\"file\";
InputData=\"LF:${dir}split/*/*\";", "r"
  ) or exit(-2);

  print "1. Killing all my previous jobs\n";
  my @jobs = $cat->execute("top", "-all_status", "-user $user", "-silent");
  foreach my $job (@jobs) {
        print "KILLING jobs $job->{queueId}\n";
        $cat->execute("kill", $job->{queueId});
  }
  print "1. DONE\n\n";

  print "2. Set the limit (maxUnfinishedJobs 1000, maxTotalCpuCost 1000, maxTotalRunningTime 1000)\n";
  $d->update("PRIORITY", {maxUnfinishedJobs => 1000, maxTotalCpuCost => 1000, maxTotalRunningTime => 1000},
	"user='$user'");
  waitForNoJobs($cat, $user);
  $cat_adm->execute("calculateJobQuota", "1");    # 1 for silent
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "unfinishedJobsLast24h",   0)    or exit(-2);
  assertEqualJobs($d, $user, "totalRunningTimeLast24h", 0)    or exit(-2);
  assertEqualJobs($d, $user, "totalCpuCostLast24h",     0)    or exit(-2);
  assertEqualJobs($d, $user, "maxUnfinishedJobs",       1000) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalRunningTime",     1000) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalCpuCost",         1000) or exit(-2);
  print "2. DONE\n\n";

  print "3. Submit 1 job\n";
  my ($id1) = $cat->execute("submit", "jdl/sum.jdl") or exit(-2);
  waitForStatus($cat, $id1, "WAITING", 10) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "unfinishedJobsLast24h", 1) or exit(-2);
  $cat->execute("request") or exit(-2);
  waitForStatus($cat, $id1, "DONE", 60) or exit(-2);
  waitForProcInfo($d, $id1) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");

  my $cpucost1 = $d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY WHERE user='$user'");
  (defined $cpucost1) or print "Error checking the totalCpuCostLast24h of the user\n" and exit(-2);
  ($cpucost1 > 0) or print "FAILED: totalCpuCost: $cpucost1, not increased at all\n" and exit(-2);

  my $rtime1 = $d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY WHERE user='$user'");
  (defined $rtime1) or print "Error checking the totalRunningTimeLast24h of the user\n" and exit(-2);
  ($rtime1 > 0) or print "FAILED: totalRunningTime: $rtime1, not increased at all\n" and exit(-2);
  print "3. PASSED\n\n";

  print "4. Submit 2 jobs\n";
  my ($id2) = $cat->execute("submit", "jdl/Split2Jobs.jdl") or exit(-2);
  waitForStatus($cat, $id2, "SPLIT", 10) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "unfinishedJobsLast24h", 2) or exit(-2);
  $cat->execute("top") or exit(-2);
  sleep(30);
  $cat->execute("top") or exit(-2);
  $cat->execute("request") or exit(-2);
  $cat->execute("top","-id",$id2) or exit(-2);
  waitForStatus($cat, $id2, "DONE", 60) or exit(-2);
  waitForSubjobsProcInfo($d, $cat, $id2) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");

  my $cpucost2 = $d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY WHERE user='$user'");
  (defined $cpucost2) or print "Error checking the totalCpuCostLast24h of the user\n" and exit(-2);
  ($cpucost2 > $cpucost1) or print "FAILED: totalCpuCost: $cpucost2, not increased at all\n" and exit(-2);

  my $rtime2 = $d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY WHERE user='$user'");
  (defined $rtime2) or print "Error checking the totalRunningTimeLast24h of the user\n" and exit(-2);
  ($rtime2 > 0) or print "FAILED: totalRunningTime: $rtime2, not increased at all\n" and exit(-2);
  print "4. PASSED\n\n";

  print "5. Modify the maxTotalRunningTime as $rtime2\n";
  $d->update("PRIORITY", {maxTotalRunningTime => $rtime2}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxTotalRunningTime", $rtime2) or exit(-2);
  print "5. DONE\n\n";

  print "6. Submit 1 job and 2 jobs\n";
  $cat->execute("submit", "jdl/sum.jdl")        and print "FAILED: MUST BE DENIED\n" and exit(-2);
  $cat->execute("submit", "jdl/Split2Jobs.jdl") and print "FAILED: MUST BE DENIED\n" and exit(-2);
  print "6. PASSED\n\n";

  print "7. Modify the maxTotalCpuCost as $cpucost2 and the maxTotalRunningTime as 1000 back \n";
  $d->update("PRIORITY", {maxTotalCpuCost => $cpucost2, maxTotalRunningTime => 1000}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxTotalCpuCost",     $cpucost2) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalRunningTime", 1000)      or exit(-2);
  print "7. DONE\n\n";

  print "8. Submit 1 job and 2 jobs\n";
  $cat->execute("submit", "jdl/sum.jdl")        and print "FAILED: MUST BE DENIED\n" and exit(-2);
  $cat->execute("submit", "jdl/Split2Jobs.jdl") and print "FAILED: MUST BE DENIED\n" and exit(-2);
  print "8. PASSED\n\n";

  print "9. Modify the maxUnfinishedJobs as 0 and the maxTotalCpuCost as 1000 back \n";
  $d->update("PRIORITY", {maxUnfinishedJobs => 0, maxTotalCpuCost => 1000}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 0)    or exit(-2);
  assertEqualJobs($d, $user, "maxTotalCpuCost",   1000) or exit(-2);
  print "9. DONE\n\n";

  print "10. Submit 1 job and 2 jobs\n";
  $cat->execute("submit", "jdl/sum.jdl")        and print "FAILED: MUST BE DENIED\n" and exit(-2);
  $cat->execute("submit", "jdl/Split2Jobs.jdl") and print "FAILED: MUST BE DENIED\n" and exit(-2);
  print "10. PASSED\n\n";

  ok(1);
}

sub waitForNoJobs {
  my $cat   = shift;
  my $user  = shift;
  my $sleep = (shift or 20);
  my $times = (shift or 10);

  my $counter = 0;
  while (1) {
	my @jobs = $cat->execute("top", "-all_status", "-user $user", "-silent");
	my $nbJobs = scalar(@jobs);

	($nbJobs eq 0) and return 1;

	print "$nbJobs jobs left\n";
	($counter > $times)
	  and print "We have been waiting for more than $counter *$sleep seconds.... let's quit\n"
	  and return;
	print "The father sleeps (waiting for no jobs)\n";
	sleep $sleep;
	$counter++;
  }

  return;
}

sub waitForStatus {
  my $cat     = shift;
  my $id      = shift;
  my $Wstatus = shift;
  my $sleep   = (shift or 30);
  my $times   = (shift or 10);
  my $strict  = (shift or 0);

  my $list = {
	'INSERTING'    => 0,
	'SPLITTING'    => 1,
	'SPLIT'        => 2,
	'QUEUED'       => 3,
	'WAITING'      => 4,
	'OVER_WAITING' => 4,
	'ASSIGNED'     => 5,
	'STARTED'      => 6,
	'INTERACTIV'   => 7,
	'IDLE'         => 8,
	'RUNNING'      => 9,
	'SAVING'       => 10,
	'SAVED'        => 12
  };

  my $WstatusNb = $list->{$Wstatus};
  (defined $WstatusNb) or $WstatusNb = 13;

  my $counter = 0;
  while (1) {
	  print "Inside WaittForStatus .. :P\n";
  	my ($status) = $cat->execute("top", "-id", $id);
	  $status or print "Error checking the status of the job\n" and return;
	  $status = $status->{status};
	  $status eq $Wstatus and return 1;
	  my $statusNb = $list->{$status};
	  (defined $statusNb) or $statusNb = 13;
	  print "Status -> $status($statusNb)\n";
	  if ($statusNb > $WstatusNb) {
	    print "already passed\n";
	    $strict and return 0;
	    return 1;
	  }

	  $status =~ /((ERROR_)|(FAILED)|(DONE_WARN)|(DONE))/
	    and print "THE job finished with $1!!\n"
	    and return;
	  ($counter > $times)
	    and print "We have been waiting for more than $counter *$sleep seconds.... let's quit"
	    and return;
	  print "The father sleeps (waiting for $Wstatus($WstatusNb))\n";
	  sleep $sleep;
	  $counter++;
  }

  return;
}

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

sub assertEqualJobs {
  my $d     = shift;
  my $user  = shift;
  my $field = shift;
  my $value = shift;

  my $result = 0;

  #  if($field eq "nbFiles" || $field eq "totalSize")
  #  {
  #  	$result=$d->queryValue("SELECT $field+tmpIncreased$field FROM PRIORITY WHERE user='$user'");
  #  }
  #  else
  #  {
  $result = $d->queryValue("SELECT $field FROM PRIORITY WHERE user='$user'");

  #  }
  (defined $result) or print "Error checking the $field of the user\n" and exit(-2);
  ($result eq $value) or print "FAILED: $field expected:<$value> but was: $result\n";
  return ($result eq $value);
}
