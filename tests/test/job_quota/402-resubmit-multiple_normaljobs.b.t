#!/bin/env alien-perl
use strict;
use Test;

use Data::Dumper;
use AliEn::Database::TaskQueue;
use AliEn::Service::Optimizer::Job::Quota;
use Net::Domain qw(hostname hostfqdn hostdomain);

use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_quota/400-submit") or exit(-2);
  includeTest("file_quota/01-calculateFileQuota") or exit(-2);

print "Connecting to database...";
my $host=Net::Domain::hostfqdn();
my $d = AliEn::Database::TaskQueue->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
  or print "Error connecting to the database\n" and exit(-2);

my $id1=shift;
my $id2=shift;

my $user = "JQUser";
my $cat_adm = AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin"});
$cat_adm or exit(-1);
  #$cat_adm->execute("addUser", $user);
my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
$cat or exit(-1);
	print "3. PASSED\n\n";

  print "4. Modify the maxUnfinishedJobs as 0\n";	
  my $userid=$d->queryValue("Select userid from QUEUE_USER where user='$user'");
  $d->update("PRIORITY", {maxUnfinishedJobs=>0}, "userid='$userid'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 0) or exit(-2);
  print "4. DONE\n\n";

	print "5. Resubmit job $id1 and $id2 - Both of them MUST BE DENIED\n";
	my @info=$cat->execute("resubmit", $id1, $id2);
	print Dumper(@info);
	($info[0] and $info[0] eq "-1" ) or print "FAILED: Both of them MUST BE DENIED\n" and exit(-2);
	
	print "5. PASSED\n\n";

  my $totalRunningTime=$d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY WHERE userid='$userid'");
  (defined $totalRunningTime) or print "Error checking the totalRunningTimeLast24h of the user\n" and exit(-2);
  ($totalRunningTime > 0) or print "FAILED: totalRunningTime: $totalRunningTime, not increased at all\n" and exit(-2);

  print "10. Modify the maxTotalRunningTime as $totalRunningTime and the maxUnfinishedJobs as 1000 back\n";
  $d->update("PRIORITY", {maxUnfinishedJobs=>1000, maxTotalRunningTime=>$totalRunningTime}, "userid='$userid'");
  $cat->execute("jquota", "list", "$user");
  print "10. DONE\n\n";

	print "11. Resubmit job $id1 and $id2 - MUST BE DENIED\n";
	@info=$cat->execute("resubmit", $id1, $id2);
	($info[0] and $info[0] eq "-1" ) or print "FAILED: Both of them MUST BE DENIED\n" and exit(-2);
	
	print "11. PASSED\n\n";

  my $totalCpuCost=$d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY WHERE userid='$userid'");
  (defined $totalCpuCost) or print "Error checking the totalCpuCostLast24h of the user\n" and exit(-2);
  ($totalCpuCost > 0) or print "FAILED: totalCpuCost: $totalCpuCost, not increased at all\n" and exit(-2);

  print "12. Modify the maxTotalCpuCost as $totalCpuCost and the maxTotalRunningTime as 1000 back\n";
  $d->update("PRIORITY", {maxTotalRunningTime=>1000, maxTotalCpuCost=>$totalCpuCost}, "userid='$userid'");
  $cat->execute("jquota", "list", "$user");
  print "12. DONE\n\n";

	print "13. Resubmit job $id1 and $id2 - MUST BE DENIED\n";
	@info=$cat->execute("resubmit", $id1, $id2);
	($info[0] and $info[0] eq "-1" ) or print "FAILED: Both of them MUST BE DENIED\n" and exit(-2);
	print "13. PASSED\n\n";


  print "8. Modify the maxUnfinishedJobs as 2\n";	
	$d->update("PRIORITY", {maxUnfinishedJobs=>2}, "userid='$userid'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 2) or exit(-2);
  print "8. DONE\n\n";

	print "9. Resubmit job $id1 and $id2\n";
	@info=$cat->execute("resubmit", $id1, $id2);
	print Dumper(@info);
	
  ok(1);

