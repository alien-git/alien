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
{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add")                or exit(-2);
  includeTest("file_quota/01-calculateFileQuota") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $user = "JQUser";
  my $userSplit = "JQUserSplit";
  my $cat_adm = AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin"});
  $cat_adm or exit(-1);
  $cat_adm->execute("addUser", $user);
  $cat_adm->execute("addUser", $userSplit);
  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);
  my $cat_split = AliEn::UI::Catalogue::LCM::Computer->new({"user", $userSplit});
  $cat_split or exit(-1);

  my ($pwd) = $cat->execute("pwd") or exit(-2);
  $cat->execute("cd") or exit(-2);

  cleanDir($cat, $pwd);

  #  cleanDir($cat, "/proc/$user");
  $cat->execute("mkdir", "-p", "jdl", "bin") or exit(-2);
  
  $cat_split->execute("mkdir", "-p", "jdl", "bin","split/dir1", "split/dir2") or exit(-2);

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
  $d = AliEn::Database::TaskQueue->new({ PASSWD=> "pass" , "ROLE"=> "admin", })
    or print "Error connecting to the database\n" and exit(-2);
  my $userid=$d->queryValue("SELECT userid from QUEUE_USER where user='$user'");
  my $userSplitid=$d->queryValue("SELECT userid from QUEUE_USER where user='$userSplit'");

  
  print "0. Set the job quotas (maxTotalRunningTime, 2000, maxUnfinishedJobs, 2000, maxparallelJobs, 2000)\n";
  $d->update("PRIORITY", {maxTotalRunningTime => 2000, maxUnfinishedJobs => 2000, maxparallelJobs=> 2000}, "userid='$userid'");
  $d->update("PRIORITY", {maxTotalRunningTime => 2000, maxUnfinishedJobs => 2000, maxparallelJobs=> 2000}, "userid='$userSplitid'");
  assertEqualJobs($d, $user, "maxTotalRunningTime", 2000) or exit(-2);
  assertEqualJobs($d, $user, "maxUnfinishedJobs",   2000)    or exit(-2);
  assertEqualJobs($d, $user, "maxparallelJobs",   2000)    or exit(-2);
  print "0. DONE\n\n";

  addFile($cat_split, "split/dir1/file1", "This is a test") or exit(-2);
  addFile($cat_split, "split/dir1/file2", "This is also a test") or exit(-2);
  addFile($cat_split, "split/dir2/file3", "This is another test") or exit(-2);

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
  addFile(
	$cat_split, "bin/sum", "#!/bin/sh
sum=0
for ((i=1; i<=1000000; i++))
do
    sum=\$((\$sum+\$i));
done
echo \"sum: \$sum\"
", "r"
  ) or exit(-2);

  
  addFile($cat, "jdl/sum.jdl", "Executable=\"sum\";", "r") or exit(-2);
  addFile($cat_split, "jdl/sum.jdl", "Executable=\"sum\";", "r") or exit(-2);

  my ($dir) = $cat_split->execute("pwd") or exit(-2);
  addFile(
	$cat_split, "jdl/Split2Jobs.jdl", "Executable=\"sum\";
Split=\"directory\";
InputData=\"${dir}split/*/*\";", "r"
  ) or exit(-2);
  addFile(
	$cat_split, "jdl/Split3Jobs.jdl", "Executable=\"sum\";
Split=\"file\";
InputData=\"${dir}split/*/*\";", "r"
  ) or exit(-2);

  print "1. Killing all my previous jobs\n";
  my @jobs = $cat->execute("top", "-all_status", "-user $user", "-user $userSplit", "-silent");
  foreach my $job (@jobs) {
        print "KILLING jobs $job->{queueId}\n";
        $cat_adm->execute("kill", $job->{queueId});
  }
  print "1. DONE\n\n";

  print "2. Set the limit (maxUnfinishedJobs 1000, maxTotalCpuCost 1000, maxTotalRunningTime 1000)\n";
  $d->update("PRIORITY", {maxUnfinishedJobs => 1000, maxTotalCpuCost => 1000, maxTotalRunningTime => 1000},
	"userid='$userid'");
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

  print "3. Let's submit 2 jobs (so that we can resubmit multiple jobs)\n";
  my ($id1) = $cat->execute("submit", "jdl/sum.jdl") or exit(-2);
	$cat->execute("submit", "jdl/sum.jdl") or exit(-2);
  waitForStatus($cat, $id1, "WAITING", 10,5) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "unfinishedJobsLast24h", 2) or exit(-2);
  
  print "4. Submit 2 jobs\n";
  my ($id2) = $cat_split->execute("submit", "jdl/Split2Jobs.jdl") or exit(-2);
  waitForStatus($cat_split, $id2, "SPLIT", 10,10) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  $cat_split->execute("jquota", "list", "$userSplit") or exit(-2);
  assertEqualJobs($d, $userSplit, "unfinishedJobsLast24h", 2) or exit(-2);
  
  
print "The job was submitted correctly
#ALIEN_OUTPUT $id1 $id2
";
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
  $result = $d->queryValue("SELECT $field FROM PRIORITY join QUEUE_USER using (userid) WHERE user='$user'");

  #  }
  (defined $result) or print "Error checking the $field of the user\n" and exit(-2);
  ($result eq $value) or print "FAILED: $field expected:<$value> but was: $result\n";
  print "user $user and result $result and value $value\n";
  return ($result eq $value);
}
