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
my $d=AliEn::Database::TaskPriority->new({DRIVER=>"mysql", HOST=>"$host:3307", DB=>"processes", "ROLE", "admin"})
  or print "Error connecting to the database\n" and exit(-2);

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
	includeTest("400-jobquota-submit") or exit(-2);

	my $user="newuser";
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);

  $cat->execute("pwd") or exit (-2);
  $cat->execute("cd") or exit (-2);
	$cat->execute("mkdir", "-p", "jdl") or exit(-2);
	$cat->execute("mkdir", "-p", "bin") or exit(-2);

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
	$cat->execute("calculateJobQuota", "1"); # 1 for silent
  $cat->execute("quota", "list", "$user");
  assertEqual($d, $user, "unfinishedJobsLast24h", 0) or exit(-2);
  assertEqual($d, $user, "totalRunningTimeLast24h", 0) or exit(-2);
  assertEqual($d, $user, "totalCpuCostLast24h", 0) or exit(-2);
  assertEqual($d, $user, "maxUnfinishedJobs", 1000) or exit(-2);
  assertEqual($d, $user, "maxTotalRunningTime", 1000) or exit(-2);
  assertEqual($d, $user, "maxTotalCpuCost", 1000) or exit(-2);
	print "2. DONE\n\n";

	my ($id1, $id2, $rid1, $rid2);
	my $newLimit;

	print "3. Submit a job and then modify the maxTotalRunningTime as 0 and check if the status is changed into OVER_WAITING\n";
	($id1)=$cat->execute("submit", "jdl/sum.jdl") or exit(-2);
  $d->update("PRIORITY", {maxTotalRunningTime=>0}, "user='$user'");
  $cat->execute("quota", "list", "$user");
  assertEqual($d, $user, "maxTotalRunningTime", 0) or exit(-2);
	waitForStatus($cat, $id1, "INSERTING", 5) or exit(-2);
	$cat->execute("calculateJobQuota", "1");
	waitForStatus($cat, $id1, "OVER_WAITING", 10) or exit(-2);
	print "3. PASSED\n\n";

  print "4. Modify the maxTotalRunningTime as 1000 and check if the status is changed back into WAITING\n";
  $d->update("PRIORITY", {maxTotalRunningTime=>1000}, "user='$user'");
  $cat->execute("quota", "list", "$user");
  assertEqual($d, $user, "maxTotalRunningTime", 1000) or exit(-2);
	$cat->execute("calculateJobQuota", "1");
	waitForStatus($cat, $id1, "WAITING", 10) or exit(-2);
	print "4. PASSED\n\n";

	print "5. Killing job $id1\n";	
	$cat->execute("kill", $id1); 
	waitForNoJobs($cat, $user);
	$cat->execute("calculateJobQuota", "1");
  $cat->execute("quota", "list", "$user");
	print "5. DONE\n\n";

  print "6. Set the Limit (maxUnfinishedJobs 1000, maxTotalCpuCost 1000, maxTotalRunningTime 1000)\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>1000, maxTotalCpuCost=>1000, maxTotalRunningTime=>1000}, "user='$user'");
  $cat->execute("quota", "list", "$user");
  assertEqual($d, $user, "unfinishedJobsLast24h", 0) or exit(-2);
  assertEqual($d, $user, "totalRunningTimeLast24h", 0) or exit(-2);
  assertEqual($d, $user, "totalCpuCostLast24h", 0) or exit(-2);
  assertEqual($d, $user, "maxUnfinishedJobs", 1000) or exit(-2);
  assertEqual($d, $user, "maxTotalRunningTime", 1000) or exit(-2);
  assertEqual($d, $user, "maxTotalCpuCost", 1000) or exit(-2);
	print "6. DONE\n\n";

  print "7. Submit an another job and then modify the maxTotalCpuCost as 0 and check if the status is changed into OVER_WAITING\n";
  ($id2)=$cat->execute("submit", "jdl/sum.jdl") or exit(-2);
  $d->update("PRIORITY", {maxTotalCpuCost=>0}, "user='$user'");
  $cat->execute("quota", "list", "$user");
  assertEqual($d, $user, "maxTotalCpuCost", 0) or exit(-2);
  waitForStatus($cat, $id2, "INSERTING", 5) or exit(-2);
  $cat->execute("calculateJobQuota", "1");
  waitForStatus($cat, $id2, "OVER_WAITING", 10) or exit(-2);
  print "7. PASSED\n\n";

  print "8. Modify the maxTotalCpuCost as 1000 and check if the status is changed back into WAITING\n";
  $d->update("PRIORITY", {maxTotalCpuCost=>1000}, "user='$user'");
  $cat->execute("quota", "list", "$user");
  assertEqual($d, $user, "maxTotalCpuCost", 1000) or exit(-2);
  $cat->execute("calculateJobQuota", "1");
  waitForStatus($cat, $id2, "WAITING", 10) or exit(-2);
  print "8. PASSED\n\n";

  ok(1);
}
