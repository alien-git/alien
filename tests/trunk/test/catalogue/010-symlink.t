use strict;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser"});

$cat->execute("rm", "-silent", "nonExistantFile");
$cat->execute("add", "-r", "nonExistantFile", "file://nohost/path/to/non/existant/directory",22) or exit(-2);

print "And now, let's try to get the file....\n";
$cat->execute("get", "-l", "nonExistantFile") and print "I GOT THE FILE!!! :(\n" and exit(-2);

print "Ok, let's try as well with something that has to work...\n";

$cat->execute("rm", "-silent", "existantFile");
my $host=Net::Domain::hostname();
$cat->execute("add", "-r", "existantFile", "file://$host/etc/passwd",-s "/etc/passwd") or exit(-2);

print "And now, let's try to get the file....\n";
$cat->execute("get", "-l", "existantFile") or print "I GOT THE FILE!!! :(\n" and exit(-2);
system("alien", "proxy-destroy");
