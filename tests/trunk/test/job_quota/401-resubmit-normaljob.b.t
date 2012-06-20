use strict;
use Test;

use Net::Domain qw(hostname hostfqdn hostdomain);
use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("job_quota/400-submit") or exit(-2);

my $user = "JQUser";
my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});


print "Connecting to database...";
my $host = Net::Domain::hostfqdn();

my $d =
   AliEn::Database::TaskQueue->new({DRIVER => "mysql", HOST => "$host:3307", PASSWD=> "pass" , DB => "processes", "ROLE", "admin", })
  or print "Error connecting to the database\n" and exit(-2);
  
my $id=shift;




  print "6. Modify the maxUnfinishedJobs as 0\n";
  $d->update("PRIORITY", {maxUnfinishedJobs => 0}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 0) or exit(-2);
  print "6. DONE\n\n";

  print "7. Resubmit job $id\n";
  my (@info)=$cat->execute("resubmit", $id);
   ($info[0] and $info[0] eq "-1" ) or print "FAILED: MUST BE DENIED\n" and exit(-2);
  
  print "7. PASSED\n\n";

  my $cpucost2 = $d->queryValue("SELECT totalCpuCostLast24h FROM PRIORITY WHERE user='$user'");

  print "8. Modify the maxTotalCpuCost as $cpucost2 and the maxUnfinishedJobs as 1000 back\n";

  $d->update("PRIORITY", {maxUnfinishedJobs => 1000, maxTotalCpuCost => $cpucost2}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxTotalCpuCost",   $cpucost2) or exit(-2);
  assertEqualJobs($d, $user, "maxUnfinishedJobs", 1000)      or exit(-2);
  print "8. DONE\n\n";

  print "9. Resubmit job $id \n";
  (@info)=$cat->execute("resubmit", $id);
   ($info[0] and $info[0] eq "-1" ) or print "FAILED: MUST BE DENIED\n" and exit(-2);
  
  print "9. PASSED\n\n";

  my $rtime2 = $d->queryValue("SELECT totalRunningTimeLast24h FROM PRIORITY WHERE user='$user'");

  print "10. Modify the maxTotalRunningTime as $rtime2 and the maxCpuCost as 1000 back\n";
  $d->update("PRIORITY", {maxTotalCpuCost => 1000, maxTotalRunningTime => $rtime2}, "user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxTotalRunningTime", $rtime2) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalCpuCost",     1000)    or exit(-2);
  print "10. DONE\n\n";

  print "11. Resubmit job $id\n";
  (@info)=$cat->execute("resubmit", $id);
   ($info[0] and $info[0] eq "-1" ) or print "FAILED: MUST BE DENIED\n" and exit(-2);
  
  
  print "11. PASSED\n\n";

  print "12. Set the Limit (maxUnfinishedJobs 1000, maxTotalCpuCost 1000, maxTotalRunningTime 1000)\n";
  $d->update("PRIORITY", {maxUnfinishedJobs => 1000, maxTotalCpuCost => 1000, maxTotalRunningTime => 1000},
	"user='$user'");
  $cat->execute("jquota", "list", "$user");
  assertEqualJobs($d, $user, "maxUnfinishedJobs",   1000) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalRunningTime", 1000) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalCpuCost",     1000) or exit(-2);
  print "12. DONE\n\n";

  print "13. Resubmit job $id\n";
  (@info)=$cat->execute("resubmit", $id);
   ($info[0] and $info[0] eq "-1" ) and  print "FAILED: THIS ONE SHOULD HAVE WORKED\n" and exit(-2);
  
  
  print "13. PASSED\n\n";

ok(1);