select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;
print "ESTO AQUI\n";
exit;
use strict;

use Net::LDAP;
use AliEn::Config;
use Crypt::OpenSSL::RSA;

print "This script will configure the startup of the AliEn Services for a new AliEn Organisation\n\n";


my $user=getpwuid($<);

("$user" eq "root") or 
  print "This script has to be called being root\n" and exit;



#############################################################################
print "Please, enter the following information:\n";
my $orgName       = getParam("Organisation name","ALICE");
my $ldapDN        = getParam("LDAP host and DN (leave it empty if you want to look for it in the alien ldap server)", "");

$ldapDN and $ENV{ALIEN_LDAP_DN}=$ldapDN;

my $config=AliEn::Config->new({"organisation", $orgName});

$config or exit(-2);

my $hostname=`hostname -f`;
chomp $hostname;
my $names={};
$names->{AUTH}="Authen";
$names->{QUEUE}="Server";
$names->{LOG}="Logger";

my $install="";
foreach my $service ("Proxy", "IS", "AUTH", "QUEUE", "LOG", "Broker") {
  if ($config->{"\U${service}\E_HOST"} eq "$hostname") {
    my $name=$service;
    $names->{$service} and $name=$names->{$service};
    $install.="$name ";
  }
}
foreach my $service ("Manager", "Broker", "Optimizer"){
  if ($config->{"TRANSFER_\U${service}\E_ADDRESS"} =~ /^$hostname:/) {
    $install.="Transfer$service ";
  }
}
foreach my $service ("Job", "Catalogue"){
  if ($config->{"\U${service}\E_OPTIMIZER_ADDRESS"} =~ /^$hostname:/) {
    $install.="${service}Optimizer ";
  }
}
if ($config->{"GAS_FACTORY_ADDRESS"} &&  $config->{"GAS_FACTORY_ADDRESS"} =~ /^https?:\/\/$hostname:/) {
  $install.="GAS_FACTORY ";
}
$install=~ s/ $//;
print "This machine has to run: $install\n";
getParam("Is this right?", "Y") eq "Y" or print "Please, change the LDAP configuration and run this script again\n" and exit(-2);


my $userName=getParam("Username under which the services will run", "alienmaster");

my @list = getpwnam($userName);

if (! @list){
  print "User $userName does not exist!!\n";
  getParam("Do you want me to create it (Y/N)", "Y") eq "Y" or print "bye\n" and exit(-2);
  my $adduser=getParam("Enter path to adduser","/usr/sbin/adduser"); 
  system("$adduser", "$userName") and print "Error creating the new user\n$@ $? and $!\n" and exit (-2);
  @list = getpwnam($userName);

}
my ($name, $userID, $userGroup, $homedir)=($list[0], $list[2], $list[3], $list[7]);
my $environment=getParam("Create an environment file in user directory (Y/N)", "Y");


my ($mysqlPasswd, $ldapPasswd, $rootdn, $shadow);
if ($install =~ /Proxy/) {
  print "To install the proxy you need the mysql admin password to $config->{AUTHEN_HOST}\n";
  $mysqlPasswd=getParam("Enter admin password for mysql on $config->{AUTHEN_HOST}","", "secret");
}
if ($install =~ /Authen/) {
  print "To install the authen you need the ldap root password to $config->{LDAPHOST}\n";
  $ldapPasswd=getParam("Enter admin password for $config->{LDAPHOST}", "", "secret");
  my $managerDN=( $config->{LDAPMANAGER} || "cn=Manager,dc=cern,dc=ch");
  $rootdn=getParam("Enter root dn for $config->{LDAPHOST}",$managerDN);
	$shadow=getParam("Do you want to run Authen as 'root' (necessary if you use shadow passwords)?[Y/N]","Y");
}

my $logDir=getParam("Log directory", "$homedir/AliEn/log");
my $tmpDir=getParam("Directory for temporary files", "/tmp/AliEn/tmp");
my $cacheDir=$tmpDir;
$cacheDir=~ s/(tmp)?$/cache/;
$cacheDir=getParam("Directory for cache", $cacheDir);


print "Setting up the services with the following information
********************************************

Organisation name:     $orgName
Administrator password: ******
Username:              $userName
Environment file:      $environment
Services to start:     $install
Shadow passwor:        $shadow
********************************************\n";

(getParam ("Proceed with creation","Y") eq "Y")
  or  print "Exiting\n"  and exit (-2);

############################################################################
##     NOW WE START WITH THE CREATION OF THE DATABASES!!! 
############################################################################
print "Trying to connect to the catalogue...\n";
my $aliendir="$homedir/.alien";


foreach my $dir ($aliendir, "$aliendir/.startup", "$aliendir/identities.\L$orgName\E",
		 "/etc/aliend", "/etc/aliend/$orgName", "/var/log/AliEn", 
		"/var/log/AliEn/$orgName") { 
  if (!  -d  $dir) {
    print "Creating $dir...\t\t\t\t\t";
    mkdir ($dir, 0777) or print "failed!\nError creating $dir\n$@ $? and $!" and exit(-2);
    print "ok\n";
  }
}

chmod (0700, "$aliendir/.startup");
system ("chown","$userName.$userGroup", "/var/log/AliEn/$orgName"); 

if ($environment eq "Y") {
  print "Creating Environemnt file...\t\t\t\t\t";
  open (FILE, ">$aliendir/Environment")
    or print "failed!\nError creating $aliendir/Environment\n$@ $? and $!" and exit(-2);
  
  print FILE "ALIEN_ORGANISATION=$orgName
export ALIEN_LDAP_DN=$config->{LDAPHOST}/$config->{LDAPDN}
export GLOBUS_LOCATION=/opt/globus
export LD_LIBRARY_PATH=/opt/globus/lib:/opt/glite/externals/swig-1.3.21/lib/:/usr/local/lib:\$LD_LIBRARY_PATH
";
  close FILE;
  print "ok\n";
  if ($shadow eq "Y") {
    print "Creating the root environment file\n";
    mkdir "$ENV{HOME}/.alien";
    open (FILE, ">$ENV{HOME}/.alien/Environment")
      or print "failed!\nError creating $ENV{HOME}/.alien/Environment\n$@ $? and $!" and exit(-2);
    
    print FILE "ALIEN_ORGANISATION=$orgName
export ALIEN_LDAP_DN=$config->{LDAPHOST}/$config->{LDAPDN}
export ALIEN_USER=$userName
export GLOBUS_LOCATION=/opt/globus
export LD_LIBRARY_PATH=/opt/globus/lib:/opt/glite/externals/swig-1.3.21/lib/:/usr/local/lib:\$LD_LIBRARY_PATH";
  close FILE;

  }  
}
my $startup="#Startup configuration for alien
#user under which services will run
AliEnUser=$userName
AliEnCommand=\"$ENV{ALIEN_ROOT}/bin/alien\"
#services to start
#possible: Authen Monitor Logger Server Proxy

AliEnServices=\"$install\"\n";

if ($install=~ /Proxy/) {
  $startup.="AliEnUserP=\"$aliendir/.startup/.passwd.$orgName\"\n";
  checkMysqlConnection() or exit(-2);
}

if ($install=~ /Authen/) {
  $startup.="AliEnLDAPP=\"$aliendir/.startup/.ldap.secret.$orgName\"\n";
  checkLDAPConnection() or exit(-2);
  if ($shadow eq "Y") {
    print "Modifying the aliend to start with shadow passwords\n";
    my %files;
    $files{aliend}={};
    $files{aliend}->{'Shadow=0'}='Shadow=1';
    
    modifyFiles ("$ENV{ALIEN_ROOT}/etc/rc.d/init.d", %files);
  }
}

print "Changing the ownership of $aliendir...\t\t";
system ("chown", "$userName.$userGroup", "-R", "$aliendir") and print "Error changing the ownership\n$@ $? and $! \n" and exit(-2);



my $content="#Startup configuration for alien\n\nALIEN_ORGANISATIONS=\"$orgName\"\n";;
my $startFile="/etc/aliend/startup.conf";
if (-e  $startFile) {
  print "ok\nChecking old /etc/aliend/startup.conf...\t\t\t";
  open (FILE, "<$startFile") or 
    print "failed!\nError reading old $startFile\n$@ $? and $!" and exit(-2);  
  my @file=<FILE>;
  close FILE;
  my $orgs=join ("", grep (/^ALIEN_ORGANISATIONS/, @file));
  @file= grep (! /^ALIEN_ORGANISATIONS/, @file);
  $orgs =~ s/$orgName//;
  $orgs =~ s/^([^\"]*)\"([^\"]*)\"/$1\"$2 $orgName\"/;
  rename("$startFile", "$startFile.old");

  $content=join("", @file, $orgs);
}
print "ok\nCreating $startFile...\t\t\t\t";

open (FILE, ">$startFile") or print "failed!\nError creating $startFile\n$@ $? and $!" 
  and exit(-2);
print FILE $content;
close FILE;




print "ok\nCreating startup file...\t\t\t\t\t";

open (FILE, ">/etc/aliend/$orgName/startup.conf")
  or print "failed!\nError creating /etc/aliend/startup.conf\n$@ $? and $!" and exit(-2);
print FILE "$startup";
close FILE;




print "ok\nStarting the services...\n";
system ("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/aliend", "start", "$orgName")
  and print "failed!\n Error starting the alien services\n$@ $? and $!\n" and exit(-2);

print "ok\nChecking that the services are up...";
open (FILE, "$ENV{ALIEN_ROOT}/etc/rc.d/init.d/aliend status|") 
  or print "Error checking the status\n$? $!\n" and exit(-2);

my @output=<FILE>;
close FILE or 
  print "Error checking the status\nGot @output\n$? $!\n" and exit(-2);

grep (/FAILED/i, @output) and print "Error! Some services are dead!!\n@output\n" and exit(-2);


print "ok\nAdding the first user\n";

system ("su", "-", $userName, "-c", "$ENV{ALIEN_ROOT}/bin/alien --org $orgName -r admin -exec addUser  $config->{CLUSTER_MONITOR_USER} ") and print "FAILED!!\nError adding user  $config->{CLUSTER_MONITOR_USER}" and exit(-2);

system ("su", "-", $userName, "-c", "$ENV{ALIEN_ROOT}/bin/alien --org $orgName -r admin -exec mkdir '/\L$orgName\E/user/a/admin -- -p'") and print "FAILED!!\nError creating the directory /\L$orgName\E/user/a/admin" and exit(-2);


############################################################################
print "\n\n******************************************************************
\tInstallation finished sucessfully. 
alien services are up and running in $hostname
To stop or start the service, please use:

$ENV{ALIEN_ROOT}/etc/rc.d/init.d/aliend [start/stop]

(We recommend putting a link from /etc/rc.d/init.d/ to that file)

If you have any problems, please contact alice-project-alien\@cern.ch
";

############################################################################
############################################################################
########### INTERNAL FUNCTIONS
sub getParam {
    my ($text,$defValue, $options) = @_;
    my $value;
    $options or $options="";
    print "$text [$defValue]:";
    ($options eq "secret") and system("stty -echo");

    ($value=<STDIN>);
    $value and chomp($value);
    ($value) or $value="$defValue";

    ($options eq "secret") and print "\n" and system("stty echo");
#    print "\n";
    return $value;
}
sub checkMysqlConnection {
  my ($hostName, $portNumber)=split ":", $config->{AUTHEN_HOST};
  print "Connecting to mysql $hostName $portNumber...\t\t\t";
  open (MYSQL, "| mysql -h $hostName -P $portNumber -u admin --password='$mysqlPasswd'") 
    or print "Error connecting to mysql in  $hostName -P $portNumber\n" and return;
  print  MYSQL "GRANT INSERT,DELETE ON processes.MESSAGES TO $config->{CLUSTER_MONITOR_USER};\n";
  print  MYSQL "GRANT INSERT,DELETE ON processes.MESSAGES TO $config->{CLUSTER_MONITOR_USER};\n";

  close MYSQL or print "Error closing connection to mysql in  $hostName -P $portNumber\n" and return;

  print "ok\nCreating password file...\t\t\t\t\t";
  open (FILE, ">$aliendir/.startup/.passwd.$orgName")
    or print "failed!\nError creating $aliendir/.startup/.passwd\n$@ $? and $!" and return;  
  print FILE "$mysqlPasswd";
  close FILE;
  chmod 0600, "$aliendir/.startup/.passwd";
  print "ok\n";
  return 1;
}

sub checkLDAPConnection {
  my ($hostName, $portNumber)=split ":", $config->{AUTHEN_HOST};

  print "Connecting to ldap server on $config->{LDAPHOST}...\t";
  my $ldap = Net::LDAP->new("$config->{LDAPHOST}", "onerror" => "warn") or print "failed\nError conecting to the ldap server $config->{LDAPHOST}\n $? and $! and  $@\n" and return;
  
  my $result=$ldap->bind($rootdn, "password" => "$ldapPasswd");
  $result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error and return;

  print "ok\nCreating ssh keys for $userName...\t\t\t\t";

  Crypt::OpenSSL::RSA->import_random_seed();
  my $rsa = Crypt::OpenSSL::RSA->generate_key( 2048 );
  $rsa->use_pkcs1_oaep_padding();

  print "ok\nWriting private key to disc...\t\t\t\t\t";
  my $keyFile="$aliendir/identities.\L$orgName\E/sshkey.$userName";

  open( FILE, ">$keyFile" ) or 
    print "failed!\nError opening $keyFile\n$@ $? and $!\n" and return;

  print FILE $rsa->get_private_key_string;
  close FILE;

  chmod 0600, $keyFile;
  print "ok\nWriting public key to disc...\t\t\t\t\t";

  open( FILE, ">$keyFile.public" ) or 
    print "failed!\nError opening $keyFile.public\n$@ $? and $!\n" and return;

  print FILE $rsa->get_public_key_string;
  close FILE;
  chmod 0644, "$keyFile.public";
  my $mesg=$ldap->search(base   => "ou=People,$config->{LDAPDN}",
			 filter => "(uid=$userName)");
  if ($mesg->count) {
    print "ok\nDeleting the use $userName...\t\t\t";
    $ldap->delete("uid=$userName,ou=People,$config->{LDAPDN}");
    $mesg->code && print "failed\nCould not delete $key: ",$result->error and exit (-5);
  }
  print "ok\nAdding the user '$userName' to ldap...\t\t\t";

  $mesg=$ldap->add("uid=$userName,ou=People,$config->{LDAPDN}", 
		      attr=>["objectClass", ["AliEnUser", "posixAccount", "pkiUser", "top"],
			     "sshkey", $rsa->get_public_key_string,
			     "roles", "admin",
			     "cn", "$userName",
			     "uid", "$userName",
			     "uidNumber", "$userID",
			     "userPassword", "{crypt}x",
			     "loginShell", "false",
			    ]);
  $mesg->code && print "failed\nCould not add $userName: ",$result->error and return;
  print "ok\nGiving admin privileges to $userName...\t\t\t";

  $ldap->modify("uid=admin,ou=Roles,$config->{LDAPDN}",
		add => {"users", "$userName"});
  $mesg->code && print "failed\nCould not modify admin: ",$result->error and return;


  $ldap->modify("uid=$config->{CLUSTER_MONITOR_USER},ou=Roles,$config->{LDAPDN}",
		add => {"users", "$userName"});
  $mesg->code && print "failed\nCould not modify admin: ",$result->error and return;

  $ldap->unbind;
	print "ok\nCopying the directory to /root/.alien...";

	mkdir "$ENV{HOME}/.alien";
	system ("cp", "-r", "$aliendir/identities.\L$orgName\E/", 
					"$ENV{HOME}/.alien");
	

  print "ok\nCreating password file...\t\t\t\t\t";
  open (FILE, ">$aliendir/.startup/.ldap.secret.$orgName")
    or print "failed!\nError creating $aliendir/.startup/.ldap.secret\n$@ $? and $!"
      and return;  
  print FILE "$ldapPasswd";
  close FILE;
  chmod 0600, "$aliendir/.startup/.ldap.secret";
  print "ok\n";
  return 1;
}
sub createPasswd {
    my $passwd = "";
    my @Array  = (
        'st', '2', '!', '^', '1', '2', "\$",
        '3', '4',  '5',  '6',  '7', '8', '9', 'po', '{', ')', '}', '[',
        ']', 'gh', 'e9', '|',  'm', 'n', 'b', 'v', 'c', 'x',
        'z', 'a',  's',  'd',  'f', 'g', 'h', 'j', 'k', 'l',
        ':', 'p',  'o',  'i',  'u', 't', 'r', 'e', 'w', 'q', 
	'y', 'Q',  'W',  'E',  'R',
        'T', 'Y',  'U',  'I',  'O', 'P', 'A', 'S', 'D', 'F',
        'G', 'H',  'J',  'K',  'L', 'Z', 'X', 'C', 'V', 'B', 'N', 
        'M'
    );
    my $i;
    for ( $i = 0 ; $i < 10 ; $i++ ) {
      $passwd .= $Array[ rand(@Array) ];
    }
    return $passwd;
}

sub modifyFiles {
  my $dir=shift;
  my %files=@_;

#  print "Modifying the files of $dir";
  print"\n";
  foreach my $file (keys %files){
    my $name="$dir/$file";
    open (FILE, "<$name") or 
      print "Error reading $name\n$! $?\n" and return;
    my @file=<FILE>;
    close FILE;
    foreach my $change (keys %{$files{$file}}) {
      print "\tChanging $change for $files{$file}->{$change}\n";
      map {$_=~ s/$change/$files{$file}->{$change}/g} @file;
    }
    open (FILE, ">$name") or 
      print "Error writing $name\n$! $?\n" and return;
    print FILE  @file;
    close FILE;
    
  }
  print "all changes done!!...";
  return 1;
}
