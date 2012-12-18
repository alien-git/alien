#!/bin/env alien-perl
use strict;
use Test;

use AliEn::Service::Optimizer::Job::Quota;
use Net::Domain qw(hostname hostfqdn hostdomain);

use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

print "Connecting to database...";
$ENV{ALIEN_DATABASE_ROLE}     = 'admin';
$ENV{ALIEN_DATABASE_PASSWORD} = 'pass';
my $host = Net::Domain::hostfqdn();
my $d = AliEn::Database->new({DRIVER => "mysql", HOST => "$host:3307", DB => "alien_system", ROLE => "admin"})
  or print "Error connecting to the database\n" and exit(-2);

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);

  my $user = "FQUser";
  my $cat_ad = AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin"});
  $cat_ad or exit(-1);
  $cat_ad->execute("addUser", $user);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
  $cat or exit(-1);

  my ($pwd) = $cat->execute("pwd") or exit(-2);
  $cat->execute("cd") or exit(-2);

  cleanDir($cat, $pwd);
  $cat_ad->execute("removeExpiredFiles");
  $cat->execute("mkdir", "-p", "dir1") or exit(-2);

  my $seName = "$cat->{CONFIG}->{SE_FULLNAME}2";

  print "1. Check the intial status\n";
  $cat->execute("fquota", "list $user");
  $cat_ad->execute("removeExpiredFiles");
  assertEqual($d, $user, "nbFiles",   0) or exit(-2);
  assertEqual($d, $user, "totalSize", 0) or exit(-2);
  print "1. DONE\n\n";

  print "2. Set the limit (maxNbFiles 10, maxTotalSize 100)\n";
  $d->update("FQUOTAS", {maxNbFiles => 10, maxTotalSize => 100}, "userId in (select uId from USERS where Username like '$user')");
  print "2. DONE\n\n";

  print "3. Add a file (size 14)\n";
  addFile($cat, "dir1/file1", "This is a test") or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   1)  or exit(-2);
  assertEqual($d, $user, "totalSize", 14) or exit(-2);
  print "3. PASSED\n\n";

  print "4. Mirror a file (size 14)\n";
  my $lfn = "dir1/file1";
  $cat->execute("mirror", "-w", $lfn, $seName) or exit(-2);
  my @newMirror = $cat->execute("whereis", $lfn) or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   2)  or exit(-2);
  assertEqual($d, $user, "totalSize", 28) or exit(-2);
  print "4. PASSED\n\n";

  print "5. Add a file (size 14)\n";
  addFile($cat, "dir1/file2", "This is a test") or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   3)  or exit(-2);
  assertEqual($d, $user, "totalSize", 42) or exit(-2);
  print "5. PASSED\n\n";

  print "6. copy a file (size 14)\n";
  $cat->execute("cp", "dir1/file1", "dir1/file3") or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   4)  or exit(-2);
  assertEqual($d, $user, "totalSize", 56) or exit(-2);
  print "6. PASSED\n\n";

  print "7. copy a directory\n";
  $cat->execute("cp", "dir1", "dir2") or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   7)  or exit(-2);
  assertEqual($d, $user, "totalSize", 98) or exit(-2);
  print "7. PASSED\n\n";

  print "8. move a file\n";
  $cat->execute("mv", "dir2/file3", "dir2/file4") or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   7)  or exit(-2);
  assertEqual($d, $user, "totalSize", 98) or exit(-2);
  print "8. PASSED\n\n";

  print "9. move a directory\n";
  $cat->execute("mv", "dir2", "dir3") or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   7)  or exit(-2);
  assertEqual($d, $user, "totalSize", 98) or exit(-2);
  print "9. PASSED\n\n";

  print "10. remove a directory\n";
  $cat->execute("rmdir", "dir3") or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   4)  or exit(-2);
  assertEqual($d, $user, "totalSize", 56) or exit(-2);
  print "10. PASSED\n\n";

  print "11. remove a file\n";
  $cat->execute("rm", "dir1/file3") or exit(-2);
  $cat->execute("fquota", "list $user");
  assertEqual($d, $user, "nbFiles",   3)  or exit(-2);
  assertEqual($d, $user, "totalSize", 42) or exit(-2);
  print "11. PASSED\n\n";

  print "12. remove a file\n";
  $cat->execute("rm", "dir1/file1") or exit(-2);
  $cat->execute("fquota", "list $user");
  $cat_ad->execute("removeExpiredFiles");
  assertEqual($d, $user, "nbFiles",   1)  or exit(-2);
  assertEqual($d, $user, "totalSize", 14) or exit(-2);
  print "12. PASSED\n\n";

  print "13. remove a directory\n";
  $cat->execute("rmdir", "dir1") or exit(-2);
  $cat->execute("fquota", "list $user");
  $cat_ad->execute("removeExpiredFiles");
  assertEqual($d, $user, "nbFiles",   0) or exit(-2);
  assertEqual($d, $user, "totalSize", 0) or exit(-2);
  print "13. PASSED\n\n";

  ok(1);
}

sub cleanDir {
  my $cat = shift;
  my $pwd = shift;
  print "Cleaning $pwd\n";
  my @list = $cat->execute("ls", "-F", "$pwd");
  foreach my $dir (@list) {
    if ($dir=~/\/$/) {
      $cat->execute("rmdir", "-rf", "$pwd/$dir", "-silent");
    } else {
      $cat->execute("rm", "$pwd/$dir", "-silent");
    }
  }
}

sub assertEqual {
  my $d     = shift;
  my $user  = shift;
  my $field = shift;
  my $value = shift;

  my $result = 0;
  $result = $d->queryValue("SELECT $field FROM FQUOTAS WHERE userId in (select uId from USERS where Username like '$user')");
  (defined $result) or print "Error checking the $field of the user\n" and exit(-2);
  ($result eq $value) or print "FAILED: $field expected:<$value> but was: $result\n";
  return ($result eq $value);
}
