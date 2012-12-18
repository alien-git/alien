#!/bin/env alien-perl
use strict;
use Test;

use Data::Dumper;

use AliEn::Service::Optimizer::Job::Quota;
use Net::Domain qw(hostname hostfqdn hostdomain);

use AliEn::Database::TaskQueue;
use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

print "Connecting to database...";
my $d = AliEn::Database::TaskQueue->new({ PASSWD=> "pass" , "ROLE", "admin", })
  or print "Error connecting to the database\n" and exit(-2);

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

  includeTest("job_manual/010-ProcessMonitorOutput")             or exit(-2);
  includeTest("job_quota/400-submit")             or exit(-2);

  my $user = "JQUser";
  my $cat_adm = AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin"});
  $cat_adm or exit(-1);
  #$cat_adm->execute("addUser", $user);
  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);
  
  print "Let's kill all the jobs of this user\n";
  my (@jobs)=$cat->execute("top", "-all_status -user $user");
  foreach my $job (@jobs){
   $cat->execute("kill", $job->{queueId});
   #print "KILLING $job->{queueid}\n";
  }
  
  #exit(-2);
  
  
   $cat_adm->execute("calculateJobQuota");
  

  $cat_adm->execute("jquota", "set $user maxUnfinishedJobs 1000 maxTotalCpuCost 1000 maxTotalRunningTime 1000");


  print
"1. Submit a job and then modify the maxTotalRunningTime as 0 and check if the status is changed into OVER_WAITING\n";
  my ($id1) = $cat->execute("submit", "jdl/sum.jdl") or exit(-2);
  $cat_adm->execute("jquota", "set $user maxTotalRunningTime  0") or exit(-2);

  assertEqualJobs($d, $user, "maxTotalRunningTime", 0) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  waitForStatus($cat, $id1, "WAITING", 3, 10) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  waitForStatus($cat, $id1, "OVER_WAITING", 10, 10, 1) or exit(-2);
  print "3. PASSED\n\n";

  print "4. Modify the maxTotalRunningTime as 1000 and check if the status is changed back into WAITING\n";
  $cat_adm->execute("jquota", "set $user maxTotalRunningTime  1000") or exit(-2);
  
  assertEqualJobs($d, $user, "maxTotalRunningTime", 1000) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  waitForStatus($cat, $id1, "WAITING", 10) or exit(-2);
  print "4. PASSED\n\n";

  print "5. Killing job $id1\n";
  $cat->execute("kill", $id1);
  $cat_adm->execute("calculateJobQuota", "1");
  $cat->execute("jquota", "list", "$user");
  print "5. DONE\n\n";

  print "6. Set the Limit (maxUnfinishedJobs 1000, maxTotalCpuCost 1000, maxTotalRunningTime 1000)\n";
  $cat_adm->execute("jquota", "set $user maxUnfinishedJobs 1000 maxTotalCpuCost 1000 maxTotalRunningTime  1000") or exit(-2);
  
  assertEqualJobs($d, $user, "maxUnfinishedJobs",       1000) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalRunningTime",     1000) or exit(-2);
  assertEqualJobs($d, $user, "maxTotalCpuCost",         1000) or exit(-2);
  print "6. DONE\n\n";

  print
"7. Submit an another job and then modify the maxTotalCpuCost as 0 and check if the status is changed into OVER_WAITING\n";
  my ($id2) = $cat->execute("submit", "jdl/sum.jdl") or exit(-2);
  $cat_adm->execute("jquota", "set $user maxTotalCpuCost  0");

  assertEqualJobs($d, $user, "maxTotalCpuCost", 0) or exit(-2);
  waitForStatus($cat, $id2, "WAITING", 5) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  waitForStatus($cat, $id2, "OVER_WAITING", 10) or exit(-2);
  print "7. PASSED\n\n";

  print "8. Modify the maxTotalCpuCost as 1000 and check if the status is changed back into WAITING\n";
  $cat_adm->execute("jquota", "set $user maxTotalCpuCost  1000");

  assertEqualJobs($d, $user, "maxTotalCpuCost", 1000) or exit(-2);
  $cat_adm->execute("calculateJobQuota", "1");
  waitForStatus($cat, $id2, "WAITING", 10) or exit(-2);
  print "And let's kill it $id2\n";
  $cat->execute("kill", $id2);

  print "8. PASSED\n\n";

  ok(1);

