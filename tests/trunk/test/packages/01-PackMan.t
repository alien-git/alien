use strict;
use AliEn::Service::PackMan;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";

push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("user_basic/021-se") or exit(-2);
includeTest("catalogue/003-add") or exit(-2);

my $config=new AliEn::Config;

my $key="name=testP,ou=PackMan,ou=Services,$config->{FULLLDAPDN}";
my $host=Net::Domain::hostfqdn();
chomp $host;

print "Putting host to '$host'\n";
addLdapEntry($key, ["objectClass", ["AliEnPackMan"],
		    "name", "testP",
		    "host", $host,
		    "port", 9991,
		   ]) or exit(-2);


$config=$config->Reload({force=>1});

#Let's remove all the packages that have been already installed
system("rm -rf $ENV{ALIEN_HOME}/packages");



startService("PackMan") or exit(-2);
print "YUHUUU\n";
my $vo=Net::Domain::hostname();
chomp $vo;
my $cat=AliEn::UI::Catalogue::LCM->new({"role", "admin",});
$cat or exit (-1);
$cat->execute("mkdir", "-p", "/$vo/tags"); 
addFile($cat, "/$vo/tags/PackageDef", "dependencies varchar(255), executable varchar(255), description varchar(255),size int(10), md5sum int(1),setup varchar(255),unpack varchar(255), compile varchar(255), install varchar(255), pre_install varchar(255), post_install varchar(255), pre_rm varchar(255), post_rm varchar(255), config varchar(255), path varchar(255), shared int(1) default 0") or exit(-2);
$cat->execute("rmdir", "/$vo/user/n/newuser/packages");
$cat->close();
print "Let's restart the monitor, so that it gets the new config\n";
startService("Monitor") or exit(-2);
print "ok!!\n";
