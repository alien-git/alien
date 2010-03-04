#!/bin/env alien-perl
use strict;
use Test;

use AliEn::Database::TaskPriority;
use AliEn::Service::Optimizer::Job::Quota;
use Net::Domain qw(hostname hostfqdn hostdomain);

use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

print "Connecting to database...";
my $host=Net::Domain::hostfqdn();
my $d=AliEn::Database::TaskPriority->new({DRIVER=>"mysql", HOST=>"$host:3307", DB=>"processes", "ROLE", "admin"})
  or print "Error connecting to the database\n" and exit(-2);


{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
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
  cleanDir($cat, "/proc/$user");
  $cat->execute("mkdir", "-p", "jdl") or exit(-2);
  $cat->execute("mkdir", "-p", "bin") or exit(-2);

  refreshLFNandGUIDtable($cat_adm);

  print "0. Set the file quota (maxNbFiles 100, maxTotalSize 100000)\n";
  $d->update("PRIORITY", {maxNbFiles=>100, maxTotalSize=>100000}, "user='$user'");
  assertEqual($d, $user, "maxTotalSize", 100000) or exit(-2);
  assertEqual($d, $user, "maxNbFiles", 100) or exit(-2);
  print "0. DONE\n\n";

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
  assertEqual($d, $user, "unfinishedJobsLast24h", 0) or exit(-2);
  assertEqual($d, $user, "totalRunningTimeLast24h", 0) or exit(-2);
  assertEqual($d, $user, "totalCpuCostLast24h", 0) or exit(-2);
  assertEqual($d, $user, "maxUnfinishedJobs", 1000) or exit(-2);
  assertEqual($d, $user, "maxTotalRunningTime", 1000) or exit(-2);
  assertEqual($d, $user, "maxTotalCpuCost", 1000) or exit(-2);
	print "2. DONE\n\n";

	my ($id1, $id2, $rid1, $rid2);

  print "3. Submit 1 job\n";
  ($id1)=$cat->execute("submit", "jdl/sum.jdl") or exit(-2);
	waitForStatus($cat, $id1, "DONE", 60) or exit(-2);
  waitForProcInfo($d, $id1) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  $cat->execute("jquota", "list", "$user");

  my $cpucost1=$d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY WHERE user='$user'");
  (defined $cpucost1) or print "Error checking the totalCpuCostLast24h of the user\n" and exit(-2);
  ($cpucost1 > 0) or print "FAILED: totalCpuCost: $cpucost1, not increased at all\n" and exit(-2);
  
  my $rtime1=$d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY WHERE user='$user'");
  (defined $rtime1) or print "Error checking the totalRunningTimeLast24h of the user\n" and exit(-2);
  ($rtime1 > 0) or print "FAILED: totalRunningTime: $rtime1, not increased at all\n" and exit(-2);
  print "3. PASSED\n\n";

  print "4. Submit 1 job\n";
  ($id2)=$cat->execute("submit", "jdl/sum.jdl") or exit(-2);
  waitForStatus($cat, $id2, "DONE", 60) or exit(-2);
  waitForProcInfo($d, $id2) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  $cat->execute("jquota", "list", "$user");

  my $cpucost2=$d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY WHERE user='$user'");
  (defined $cpucost2) or print "Error checking the totalCpuCostLast24h of the user\n" and exit(-2);
  ($cpucost2 > $cpucost1) or print "FAILED: totalCpuCost: $cpucost2, not increased at all\n" and exit(-2);

  my $rtime2=$d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY WHERE user='$user'");
  (defined $rtime2) or print "Error checking the totalRunningTimeLast24h of the user\n" and exit(-2);
  ($rtime2 > $rtime1) or print "FAILED: totalRunningTime: $rtime2, not increased at all\n" and exit(-2);
  print "4. PASSED\n\n";

	print "5. Change the status of job $id2 as FAILED for the -i option\n";
	$d->update("QUEUE", {status=>"FAILED"}, "queueId=$id2");
	waitForStatus($cat, $id2, "FAILED") or exit(-2);
	print "5. DONE\n\n";

  print "6. Modify the maxUnfinishedJobs as 0\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>0}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqual($d, $user, "maxUnfinishedJobs", 0) or exit(-2);
  print "6. DONE\n\n";

	print "7. Resubmit job $id1 (no option) and job $id2 (-i option)\n";
	$cat->execute("resubmit", $id1) and print "FAILED: MUST BE DENIED\n" and exit(-2);
	$cat->execute("resubmit", "noconfirm", "-i", $id2) and print "FAILED: MUST BE DENIED\n" and exit(-2);
	print "7. PASSED\n\n";

  print "8. Modify the maxTotalCpuCost as $cpucost2 and the maxUnfinishedJobs as 1000 back\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>1000, maxTotalCpuCost=>$cpucost2}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqual($d, $user, "maxTotalCpuCost", $cpucost2) or exit(-2);
  assertEqual($d, $user, "maxUnfinishedJobs", 1000) or exit(-2);
	print "8. DONE\n\n";

	print "9. Resubmit job $id1 (no option) and job $id2 (-i option)\n";
	$cat->execute("resubmit", $id1) and print "FAILED: MUST BE DENIED\n" and exit(-2);
	$cat->execute("resubmit", "noconfirm", "-i", $id2) and print "FAILED: MUST BE DENIED\n" and exit(-2);
	print "9. PASSED\n\n";

  print "10. Modify the maxTotalRunningTime as $rtime2 and the maxCpuCost as 1000 back\n";	
	$d->update("PRIORITY", {maxTotalCpuCost=>1000, maxTotalRunningTime=>$rtime2}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqual($d, $user, "maxTotalRunningTime", $rtime2) or exit(-2);
  assertEqual($d, $user, "maxTotalCpuCost", 1000) or exit(-2);
	print "10. DONE\n\n";

	print "11. Resubmit job $id1 (no option) and job $id2 (-i option)\n";
	$cat->execute("resubmit", $id1) and print "FAILED: MUST BE DENIED\n" and exit(-2);
	$cat->execute("resubmit", "noconfirm", "-i", $id2) and print "FAILED: MUST BE DENIED\n" and exit(-2);
	print "11. PASSED\n\n";

  print "12. Set the Limit (maxUnfinishedJobs 1000, maxTotalCpuCost 1000, maxTotalRunningTime 1000)\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>1000, maxTotalCpuCost=>1000, maxTotalRunningTime=>1000}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqual($d, $user, "maxUnfinishedJobs", 1000) or exit(-2);
  assertEqual($d, $user, "maxTotalRunningTime", 1000) or exit(-2);
  assertEqual($d, $user, "maxTotalCpuCost", 1000) or exit(-2);
	print "12. DONE\n\n";

	print "13. Resubmit job $id1\n";
	($rid1)=$cat->execute("resubmit", $id1) or exit(-2);
	print "13. PASSED\n\n";

	print "14. Resubmit job $id2 with -i option\n";
	($rid2)=$cat->execute("resubmit", "noconfirm", "-i", $id2) or exit(-2);
	print "14. PASSED\n\n";

  ok(1);
}
