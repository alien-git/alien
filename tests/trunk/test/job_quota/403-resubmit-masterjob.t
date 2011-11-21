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
my $host = Net::Domain::hostfqdn();
my $d = AliEn::Database::TaskPriority->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
  or print "Error connecting to the database\n" and exit(-2);

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("catalogue/003-add")                or exit(-2);
  includeTest("job_quota/400-submit")             or exit(-2);
  includeTest("file_quota/01-calculateFileQuota") or exit(-2);

  my $user = "newuser";
  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);
  my $cat_adm = AliEn::UI::Catalogue::LCM::Computer->new({"user", "admin"});
  $cat_adm or exit(-1);

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
  print "0. Set the file quota (maxNbFiles 1000, maxTotalSize 1000000)\n";
  $d->update("FQUOTAS", {maxNbFiles => 1000, maxTotalSize => 1000000}, "user='$user'");
  assertEqual($d, $user, "maxTotalSize", 1000000) or exit(-2);
  assertEqual($d, $user, "maxNbFiles",   1000)    or exit(-2);
  print "0. DONE\n\n";
  print "Reconnecting to Database processes \n";
  $d = AliEn::Database::TaskPriority->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
    or print "Error connecting to the database\n" and exit(-2);

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

  print "2. Set the Limit (maxUnfinishedJobs 1000, maxTotalCpuCost 1000, maxTotalRunningTime 1000)\n";
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

  my ($id1, $rid1);
  my $newLimit;

  print "3. Submit 2 jobs\n";
  ($id1) = $cat->execute("submit", "jdl/Split2Jobs.jdl") or exit(-2);
  waitForStatus($cat, $id1, "SPLIT", 10) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "unfinishedJobsLast24h", 2) or exit(-2);
  $cat->execute("top") or exit(-2);
  sleep(20);
  $cat->execute("request") or exit(-2);
  waitForStatus($cat, $id1, "DONE", 60) or exit(-2);
  waitForSubjobsProcInfo($d, $cat, $id1) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  print "3. PASSED\n\n";

  ok(1);
}
