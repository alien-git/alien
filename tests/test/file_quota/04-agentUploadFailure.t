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

  refreshLFNandGUIDtable($cat_adm);

  cleanDir($cat, $pwd);
#  cleanDir($cat, "/proc/$user");
  $cat->execute("mkdir", "-p", "dir1") or exit(-2);
  $cat->execute("mkdir", "-p", "bin") or exit(-2);

  refreshLFNandGUIDtable($cat_adm);

  print "1. Check the intial status\n";
	$cat_adm->execute("calculateFileQuota", "1"); # 1 for silent
	$cat->execute("fquota", "list $user");
	assertEqual($d, $user, "nbFiles", 0) or exit(-2);
	assertEqual($d, $user, "totalSize", 0) or exit(-2);
	print "1. DONE\n\n";

  print "2. Set the limit (maxNbFiles 2, maxTotalSize 100)\n";	
	$d->update("PRIORITY", {maxNbFiles=>2, maxTotalSize=>100}, "user='$user'");
  assertEqual($d, $user, "maxTotalSize", 100) or exit(-2);
  assertEqual($d, $user, "maxNbFiles", 2) or exit(-2);
	print "2. DONE\n\n";

	print "3. add files for submitting a job\n";
	addFile($cat, "bin/date","#!/bin/sh
echo 'This is a test' > file.out
", "r") or exit(-2);
  addFile($cat, "dir1/saveoutput.jdl","
Executable=\"date\";
OutputFile={\"file.out\"}
", "r") or exit(-2);
  $cat_adm->execute("calculateFileQuota");
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles", 2) or exit(-2);
  assertEqual($d, $user, "totalSize", 87) or exit(-2);
  print "3. PASSED\n\n";

  print "4. Submit a job\n";
  my ($id1)=$cat->execute("submit", "dir1/saveoutput.jdl") or exit(-2);
  waitForStatus($cat, $id1, "ERROR_SV") or exit(-2);	
	print "Waiting for job log... for 100 seconds\n";
	sleep(100);
	my ($log)=$cat->execute("ps", "trace", $id1);
	my $found=0;
	foreach my $entry (@$log){
		if ($entry->{trace}=~ /because of quota overflow/) {
			$found=1;
			last;
		}
	};
	$found or print "There is nothing in the log about quota overflow\n" and exit(-2);
	print "4. PASSED\n\n";

}
