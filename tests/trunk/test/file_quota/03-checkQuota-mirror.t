#!/bin/env alien-perl
use strict;
use Test;

use AliEn::Database;
use AliEn::Service::Optimizer::Job::Quota;
use Net::Domain qw(hostname hostfqdn hostdomain);

use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

print "Connecting to database...";
$ENV{ALIEN_DATABASE_ROLE}='admin';
$ENV{ALIEN_DATABASE_PASSWORD}='pass';
my $host=Net::Domain::hostfqdn();
my $d=AliEn::Database->new({DRIVER=>"mysql", HOST=>"$host:3307", DB=>"alien_system", "ROLE", "admin"})
  or print "Error connecting to the database\n" and exit(-2);

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("catalogue/003-add") or exit(-2);
  includeTest("file_quota/01-calculateFileQuota") or exit(-2);

  my $user="FQUser";
  my $cat_ad=AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin"});
  $cat_ad or exit(-1);
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);

  my ($pwd)=$cat->execute("pwd") or exit(-2);
  $cat->execute("cd") or exit(-2);

  cleanDir($cat, $pwd);
  $cat_ad->execute("removeExpiredFiles");
#  cleanDir($cat, "/proc/$user");

  $cat->execute("mkdir", "-p", "dir1") or exit(-2);

	my $lfn="dir1/file";
	my $seName="$cat->{CONFIG}->{SE_FULLNAME}2";

  print "1. Check the intial status\n";
	$cat->execute("fquota", "list $user");
	assertEqual($d, $user, "nbFiles", 0) or exit(-2);
	assertEqual($d, $user, "totalSize", 0) or exit(-2);
	print "1. DONE\n\n";

  print "2. Set the limit (maxNbFiles 10, maxTotalSize 20)\n";	
	$d->update("FQUOTAS", {maxNbFiles=>10, maxTotalSize=>20}, "user='$user'");
	print "2. DONE\n\n";

	print "3. Add a file (size 14)\n";
  addFile($cat, $lfn, "This is a test") or exit(-2);
  $cat->execute("fquota", "list $user");
	assertEqual($d, $user, "nbFiles", 1) or exit(-2);
	assertEqual($d, $user, "totalSize", 14) or exit(-2);
	print "3. PASSED\n\n";

	print "4. Mirror a file (size 14)\n";
	$cat->execute("mirror", "-w", $lfn, $seName);# and print "FAILED: MUST BE DENIED\n" and exit(-2);
	assertEqual($d, $user, "nbFiles", 1) or exit(-2);
	assertEqual($d, $user, "totalSize", 14) or exit(-2);
	print "4. PASSED\n\n";

  print "5. Modify the maxTotalSize as 100 and the maxNbFiles as 1\n";
  $d->update("FQUOTAS", {maxTotalSize=>100, maxNbFiles=>1}, "user='$user'");
  assertEqual($d, $user, "maxTotalSize", 100) or exit(-2);
  assertEqual($d, $user, "maxNbFiles", 1) or exit(-2);
  print "5. DONE\n\n";

	print "6. Mirror a file (size 14)\n";
	$cat->execute("mirror", "-w", $lfn, $seName);# and print "FAILED: MUST BE DENIED\n" and exit(-2);
	assertEqual($d, $user, "nbFiles", 1) or exit(-2);
	assertEqual($d, $user, "totalSize", 14) or exit(-2);
	print "6. PASSED\n\n";

  print "7. Modify the maxNbFiles as 10 back\n";
  $d->update("FQUOTAS", {maxNbFiles=>10}, "user='$user'");
  assertEqual($d, $user, "maxNbFiles", 10) or exit(-2);
  print "7. DONE\n\n";

	print "8. Mirror a file (size 14)\n";
	$cat->execute("mirror", "-w", $lfn, $seName) or exit(-2);
	my @newMirror=$cat->execute("whereis", $lfn) or exit(-2);
  $cat->execute("fquota", "list $user"); 
  assertEqual($d, $user, "nbFiles", 2) or exit(-2);
  assertEqual($d, $user, "totalSize", 28) or exit(-2);
	print "8. PASSED\n\n";

  ok(1);
}

