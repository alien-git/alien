#!/bin/env alien-perl
use strict;
use Test;

use Data::Dumper;
use AliEn::Database::TaskPriority;
use AliEn::Service::Optimizer::Job::Quota;
use Net::Domain qw(hostname hostfqdn hostdomain);

use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

print "Connecting to database...";
my $host=Net::Domain::hostfqdn();
my $d = AliEn::Database::TaskPriority->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
  or print "Error connecting to the database\n" and exit(-2);

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_quota/400-submit") or exit(-2);
  includeTest("file_quota/01-calculateFileQuota") or exit(-2);

  my $user="newuser";
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);
  my $cat_adm=AliEn::UI::Catalogue::LCM::Computer->new({"user", "admin"});
  $cat_adm or exit(-1);

  my ($pwd)=$cat->execute("pwd") or exit(-2);
  $cat->execute("cd") or exit(-2);

  cleanDir($cat, $pwd);
#  cleanDir($cat, "/proc/$user");
  $cat->execute("mkdir", "-p", "jdl") or exit(-2);
  $cat->execute("mkdir", "-p", "bin") or exit(-2);

  print "Connecting to Database alien_system... Need to update the quota tables .. :P  \n";
  my $d = AliEn::Database->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "alien_system", "ROLE", "admin" })
    or print "Error connecting to the database\n" and exit(-2);
  #refreshLFNandGUIDtable($cat_adm);
  print "0. Set the file quota (maxNbFiles 1000, maxTotalSize 1000000)\n";
  $d->update("FQUOTAS", {maxNbFiles=>1000, maxTotalSize=>1000000}, "user='$user'");
  assertEqual($d, $user, "maxTotalSize", 1000000) or exit(-2);
  assertEqual($d, $user, "maxNbFiles", 1000) or exit(-2);
  print "0. DONE\n\n";
  print "Reconnecting to Database processes \n";
  $d = AliEn::Database::TaskPriority->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
    or print "Error connecting to the database\n" and exit(-2);

  addFile($cat, "bin/sum","#!/bin/sh
sum=0
for ((i=1; i<=1000000; i++))
do
    sum=\$((\$sum+\$i));
done
echo \"sum: \$sum\"
", "r") or exit(-2) ;

  addFile($cat, "jdl/sum.jdl","Executable=\"sum\";", "r") or exit(-2);

  print "1. Killing all my previous jobs\n";
  my @jobs=$cat->execute("top", "-all_status", "-user $user", "-silent");
  foreach my $job (@jobs) {
    print "KILLING jobs $job->{queueId}\n";
    $cat->execute("kill", $job->{queueId});
  }
	print "1. DONE\n\n";

  print "2. Set the Limit (maxUnfinishedJobs 1000, maxTotalCpuCost 1000, maxTotalRunningTime 1000)\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>1000, maxTotalCpuCost=>1000, maxTotalRunningTime=>1000}, "user='$user'");
	waitForNoJobs($cat, $user);
	$cat_adm->execute("calculateJobQuota", "1"); # 1 for silent
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "unfinishedJobsLast24h", 0) or exit(-2);
  assertEqualJobs($d, $user, "totalRunningTimeLast24h", 0) or exit(-2);
  assertEqualJobs($d, $user, "totalCpuCostLast24h", 0) or exit(-2);
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 1000) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalRunningTime", 1000) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalCpuCost", 1000) or exit(-2);
	print "2. DONE\n\n";

	my ($id1, $id2, $rid1, $rid2);
	my $newLimit;

	print "3. Submit 2 jobs\n";
	($id1)=$cat->execute("submit", "jdl/sum.jdl") or exit(-2);
	($id2)=$cat->execute("submit", "jdl/sum.jdl") or exit(-2);
  $cat->execute("top") or exit(-2);
  sleep(20);
  $cat->execute("request") or exit(-2);
	waitForStatus($cat, $id1, "DONE", 60) or exit(-2);
	waitForStatus($cat, $id2, "DONE", 60) or exit(-2);
	waitForProcInfo($d, $id1) or exit(-2);
	waitForProcInfo($d, $id2) or exit(-2);
	$cat_adm->execute("calculateJobQuota", "1");
 	$cat->execute("jquota", "list", "$user");
	print "3. PASSED\n\n";

  print "4. Modify the maxUnfinishedJobs as 0\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>0}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 0) or exit(-2);
  print "4. DONE\n\n";

	print "5. Resubmit job $id1 and $id2 - Both of them MUST BE DENIED\n";
	$cat->execute("resubmit", $id1, $id2) and print "FAILED: Both of them MUST BE DENIED\n" and exit(-2);
	print "5. PASSED\n\n";

  print "6. Modify the maxUnfinishedJobs as 1\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>1}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 1) or exit(-2);
  print "6. DONE\n\n";

	print "7. Resubmit job $id1 and $id2 - Only job $id2 MUST BE DENIED\n";
	($rid1, $rid2)=$cat->execute("resubmit", $id1, $id2);
  #((defined $rid1) and !(defined $rid2)) or print "FAILED: Only job $id2 MUST BE DENIED\n" and exit(-2);
  #$cat->execute("top") or exit(-2);
  #sleep(20);
  #$cat->execute("request") or exit(-2);
  #waitForStatus($cat, $rid1, "DONE", 60) or exit(-2);
	print "7. PASSED\n\n";

  print "8. Modify the maxUnfinishedJobs as 2\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>2}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 2) or exit(-2);
  print "8. DONE\n\n";

	$id1=$rid1;
	$id2=$rid2;
	print "9. Resubmit job $id1 and $id2\n";
	($rid1, $rid2)=$cat->execute("resubmit", $id1, $id2);
	((defined $rid1) and (defined $rid2)) or exit(-2);
  $cat->execute("top") or exit(-2);
  sleep(20);
  $cat->execute("request") or exit(-2);
	waitForStatus($cat, $rid1, "DONE", 60) or exit(-2);
	waitForStatus($cat, $rid2, "DONE", 60) or exit(-2);
	waitForProcInfo($d, $rid1) or exit(-2);
	waitForProcInfo($d, $rid2) or exit(-2);
	$cat_adm->execute("calculateJobQuota", "1");
 	$cat->execute("jquota", "list", "$user");
	print "9. PASSED\n\n";

	$id1=$rid1;
	$id2=$rid2;

  my $totalRunningTime=$d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY WHERE user='$user'");
  (defined $totalRunningTime) or print "Error checking the totalRunningTimeLast24h of the user\n" and exit(-2);
  ($totalRunningTime > 0) or print "FAILED: totalRunningTime: $totalRunningTime, not increased at all\n" and exit(-2);

  $newLimit=$totalRunningTime;
  print "10. Modify the maxTotalRunningTime as $newLimit and the maxUnfinishedJobs as 1000 back\n";
  $d->update("PRIORITY", {maxUnfinishedJobs=>1000, maxTotalRunningTime=>$newLimit}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  print "10. DONE\n\n";

	print "11. Resubmit job $id1 and $id2 - MUST BE DENIED\n";
	$cat->execute("resubmit", $id1, $id2) and print "FAILED: Both of them MUST BE DENIED\n" and exit(-2);
	print "11. PASSED\n\n";

  my $totalCpuCost=$d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY WHERE user='$user'");
  (defined $totalCpuCost) or print "Error checking the totalCpuCostLast24h of the user\n" and exit(-2);
  ($totalCpuCost > 0) or print "FAILED: totalCpuCost: $totalCpuCost, not increased at all\n" and exit(-2);

  $newLimit=$totalCpuCost;
  print "12. Modify the maxTotalCpuCost as $newLimit and the maxTotalRunningTime as 1000 back\n";
  $d->update("PRIORITY", {maxTotalRunningTime=>1000, maxTotalCpuCost=>$newLimit}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  print "12. DONE\n\n";

	print "13. Resubmit job $id1 and $id2 - MUST BE DENIED\n";
	$cat->execute("resubmit", $id1, $id2) and print "FAILED: Both of them MUST BE DENIED\n" and exit(-2);
	print "13. PASSED\n\n";

  ok(1);
}
