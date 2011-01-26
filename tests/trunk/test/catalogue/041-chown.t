use strict;

use AliEn::UI::Catalogue;
use Data::Dumper;

my $cat=AliEn::UI::Catalogue->new({role=>"admin"}) or exit(-2);
$cat->execute("rmdir", "-rf", "/tmp/chown");
$cat->execute("mkdir", "-p", "/tmp/chown") or exit(-2);
print "Admin can change the owner\n";
$cat->execute("chown", "newuser", "/tmp/chown") or exit(-2);
print "Changed\n";
my ($info)=$cat->execute("ls", "-lt /tmp/chown/");
print Dumper($info);
$info or exit(-2);
$cat->close();
#we have to close, so that we do not have an open connection to the remote database

$cat=AliEn::UI::Catalogue->new({user=>"newuser"}) or exit(-2);
print "Connected as newuser\n";
$cat->execute("chown", "admin", "/tmp/chown") and print "We could change the owner!!\n" and exit(-2);
print "OK\n";
