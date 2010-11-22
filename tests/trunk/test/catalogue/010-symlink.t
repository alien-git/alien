use strict;
use AliEn::UI::Catalogue::LCM;
use AliEn::SE::Methods;
use AliEn::MD5;
use Net::Domain qw(hostname hostfqdn hostdomain);

my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser"});

my $config=new AliEn::Config;

$cat->execute("rm", "-silent", "nonExistantFile");
$cat->execute("add", "-r", "nonExistantFile", "file://$config->{HOST}:8092//path/to/non/existant/directory",22,"abdc") or exit(-2);

print "And now, let's try to get the file....\n";
	$cat->execute("get", "-l", "nonExistantFile") and print "I GOT THE FILE!!! :(\n" and exit(-2);

print "Ok, let's try as well with something that has to work...\n";

$cat->execute("rm", "-silent", "existantFile");
my $host=Net::Domain::hostname();

my  $pfn = "file://$config->{HOST}:8092//etc/passwd";

my $size=AliEn::SE::Methods->new($pfn)->getSize();
my $md5=AliEn::MD5->new($pfn);

$cat->execute("add", "-r", "existantFile", $pfn, $size, $md5) or exit(-2);

print "And now, let's try to get the file....\n";
$cat->execute("get", "-l", "existantFile") or print "I DIDN't GOT THE FILE!!! :(\n" and exit(-2);
#system("alien", "proxy-destroy");
#
print "ok";


