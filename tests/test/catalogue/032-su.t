use strict;

use AliEn::UI::Catalogue;

my $cat=AliEn::UI::Catalogue->new({role=>"admin"}) or exit(-2);

$cat->execute("mkdir", "-p", "/remote/newuser");
$cat->execute("rmdir", "-rf", "/remote/newuser/by_admin");
$cat->execute("chown", "newuser", "/remote/newuser");
$cat->close();
#we have to close, so that we don't have an open connection to the remote database

$cat=AliEn::UI::Catalogue->new({role=>"admin"}) or exit(-2);
print "Connected as admin\n";
$cat->execute("user", "-", "newuser") or exit(-2);

my ($user)=$cat->execute("whoami");
($user eq "newuser") or exit(-2);
$cat->execute("cd");
print "I'm new user\nLet's try creating a couple of directories\n";
#$cat->execute("ls", "/remote/newuser");
createDir($cat, "by_admin") or exit(-2);
createDir($cat, "/remote/newuser/by_admin") or exit(-2);

$cat->execute("user", "-", "admin") or exit(-2);
($user)=$cat->execute("whoami");
($user eq "admin") or exit(-2);


sub createDir{
  my $cat=shift;
  my $dir=shift;
  $cat->execute("rmdir","-silent", "-rf", $dir);
  print "Creating $dir\n";
  $cat->execute("mkdir", $dir) or return;
  print "Checking that the owner is newuser\n";
  my (@info)=$cat->execute("ls", "-ltdz", $dir) or return;
  use Data::Dumper;
  print Dumper(@info);
  $info[0]->{user} ne "newuser" and print "The owner of the directory is not 'newuser'\n" and return;
  return 1;
}
