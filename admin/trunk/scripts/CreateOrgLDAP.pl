use strict;

use Net::LDAP;
use Net::Domain qw(hostname hostfqdn hostdomain);

print "This script will create the ldap server for a new AliEn Organisation\n\n";


my $user=getpwuid($<);

my $userInstallation=0;
if ("$user" ne "root") { 
  print "Warning! You are not running this as root. All the services will be running under the identity of '$user'.\n";
  my $validate=getParam("Is that what you really wnat? (Y/N)", "N");
  $validate=~ /y/i or exit(-1);
  $userInstallation=1;
}

#############################################################################
######## CHEKING IF THERE ARE HOST CERTIFICATES
my $ldapDir="/etc/aliend/ldap";
$userInstallation and $ldapDir="$ENV{ALIEN_HOME}$ldapDir";
if (! -e "$ldapDir/certs/host.cert.pem"){
  if (! -e "$ldapDir/certs/host.key.pem"){
    requestCertificate();
  } else {
    print "You do not have a certificate in $ldapDir/certs, but you have the key\n";
    print "Please, wait until you get the certificate, or delete the key and request a new certificate\n";
  }
  exit (-1);
}


print "Please, enter the following information:\n";
my $orgName       = getParam("Organisation name","ALICE");

my $suffix=Net::Domain::hostdomain();
my $topD=$suffix;
$topD =~ s/\..*$//;
$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
$suffix=getParam("Suffix", $suffix);
my $rootdn=getParam("Root DN", "cn=Manager,$suffix");

my $passwd;
my $origPass=$passwd=createPasswd();
$passwd =getParam("LDAP root password","$origPass", "secret");

if ("$passwd" ne "$origPass"){
  my $check=getParam("Enter the password again", "", "secret");
  ("$check" eq "$passwd")
    or print "ERROR: Passwords are not the same\n" and exit(-1);
}

my $hostName=Net::Domain::hostfqdn();
$hostName=getParam("Host name", $hostName);


my $prodUser=getParam("Production user", "aliprod");
my $portNumber=getParam("Initial Port number", "8080");

my $mysqlHost=getParam("alien-mysql host",getDefaultMysql($orgName,$hostName));
my $siteName=getParam("Site name", "CERN");
my $domainName=Net::Domain::hostdomain();
$domainName=getParam("Domain of $siteName", $domainName);
my $authenSubject=getParam("Certificate subject", getCertificateSubject());

print "Creating ldap with following information
********************************************

Organisation name:     $orgName
Administrator password: ******
Host name:             $hostName
Production user:       $prodUser
Initial AliEn Port     $portNumber
Site name:             $siteName
Domain name:           $domainName
Certificate subject:   $authenSubject
********************************************\n";

my $OK = getParam ("Proceed with creation","Y");
if($OK ne "Y") {
    print "Exiting\n";
    exit(-1);
}
############################################################################
##     NOW WE START WITH THE CREATION OF THE DATABASES!!! 
############################################################################

my $action="";
my $ldapdir="/var/lib/alien-ldap";
$userInstallation and $ldapdir="$ENV{ALIEN_HOME}$ldapdir";

my $etcDir="$ENV{ALIEN_ROOT}/etc/";
if ( -d  $ldapdir) {
  print "Warning!! $ldapdir already exists! This can mean that alien-ldap is already running in this machine\n";
  $action=getParam("What do you want to do ([R(einstall)/A(dd)/E(xit)])?", "A");
  ($action eq "R") or ($action eq "A") or ( print "exiting...\n" and exit(-1));

  if ($action eq "R") {
    print "Stopping ldap daemon (this can fail if the daemon was not running\n";
    system("$etcDir/rc.d/init.d/alien-ldap", "stop");
    print "Deleting the directory $ldapdir...\t\t\t";
    system ("rm", "-rf", $ldapdir) and print "failed\nError deleting $ldapdir $? $!\n" and exit(-1);
    print "ok\n";
  }
}
( -d  $ldapdir) or  createLDAPDirectory($ldapdir);

modifyLDAPConf() or exit(-2);

print "ok\nStarting the daemon...\n";

system("$etcDir/rc.d/init.d/alien-ldap", "start")
  and print "failed!!\nError starting the daemon\n$! and $?\n" and exit(-1);

sleep (1);

############################################################################
############################################################################
############ UPDATING THE INFORMATION OF THE DATABASE
############################################################################
print "Connecting to the ldap server $hostName:8389...\t";

my $ldap = Net::LDAP->new("$hostName:8389", "onerror" => "warn") or print "failed\nError conecting to the ldap server\n $? and $! and  $@\n" and exit(-1);

my $result=$ldap->bind($rootdn, "password" => "$passwd");
$result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error and exit(-1);

my $done=1;
my @list=();

my $orgDN="o=\L$orgName\E,$suffix";
($action  ne "A") and 
  push(@list,("$suffix",["objectClass", [ "domain"], dc=>$topD ]));

push(@list,("$orgDN",[o=> $orgName,"objectClass", [ "organization"]]));

my %config;
$config{ou}="Config";
$config{objectClass}=["top",  "AliEnVOConfig"];
$config{proxyPort}=$portNumber+8;
$config{authPort}=$portNumber;
$config{catalogPort}=$portNumber+2;
$config{queuePort}=$portNumber+3;
$config{logPort}=$portNumber+9;
$config{isPort}=$portNumber+1;
$config{clustermonitorPort}=$portNumber+4;
$config{brokerPort}=$portNumber+10;
$config{ldapmanager}=$rootdn;
$config{processPort}=[$portNumber+5,$portNumber+6,$portNumber+7,$portNumber+8,$portNumber+9 ];

$config{brokerHost}=$config{isHost}=$config{logHost}=$config{catalogHost}=$config{queueHost}=$config{authHost}=$config{proxyHost}=$hostName;
$config{authenDatabase}="ADMIN";
$config{catalogDatabase}="alien_system";
$config{isDatabase}="INFORMATIONSERVICE";
$config{queueDatabase}="processes";

$config{isDbHost}=$config{queueDbHost}=$config{catalogHost}=$config{authenHost}=$mysqlHost;
$config{isDriver}=$config{queueDriver}=$config{catalogDriver}=$config{authenDriver}="mysql";
$config{userDir}="/\L$orgName\E/user";
$config{clusterMonitorUser}="$prodUser";

$config{proxyAddress}="$hostName:".($portNumber+8);
$config{transferManagerAddress}="$hostName:".($portNumber+15);
$config{transferBrokerAddress}="$hostName:".($portNumber+16);
$config{transferOptimizerAddress}="$hostName:".($portNumber+17);
$config{transferDatabase}="$mysqlHost/mysql/transfers";

$config{jobOptimizerAddress}="$hostName:".($portNumber+18);
$config{jobDatabase}="$mysqlHost/mysql/processes";

$config{catalogueOptimizerAddress}="$hostName:".($portNumber+19);
$config{catalogueDatabase}="$mysqlHost/mysql/alien_system";

$config{lbsgAddress}  = "https://"."$hostName".":18051";
$config{lbsgDatabase} = $config{jobDatabase};

#$config{si2kNominalPrice} = "1";
#$config{bankAdmin} 

$config{jobManagerAddress}="$hostName:".($portNumber+3);
$config{jobBrokerAddress}="$hostName:".($portNumber+10);



$config{authenSubject}=$authenSubject;
$config{packmanmasterAddress}="$hostName:".($portNumber+12);
$config{messagesmasterAddress}="$hostName:".($portNumber+13);
$config{semasterManagerAddress}="$hostName:".($portNumber+14);
$config{semasterDatabase}="$mysqlHost/mysql/alien_system";
$config{jobinfoManagerAddress}="$hostName:".($portNumber+20);


push(@list,("ou=Config,$orgDN", [%config]));

foreach my $subdir ("Packages", "People", "Roles", "Sites", "Partitions", "Services") {
  push(@list,("ou=$subdir,$orgDN",["ou", "$subdir",
			"objectClass", ["organizationalUnit"]]));
}
push (@list, "uid=admin,ou=Roles,$orgDN", ["objectClass", "AliEnRole",
					   "uid", "admin",
					   ]);
push (@list, "uid=$prodUser,ou=Roles,$orgDN", ["objectClass", "AliEnRole",
					       "uid", "$prodUser",
					      ]);

push (@list, "ou=gContainer,ou=Services,$orgDN", ["objectClass", "AliEngContainer",
					   "ou", "gContainer",
             "geoIPDatabase", "$mysqlHost/mysql/geoip",
             "reportTime", "300",
             "wakeupTime", "300",
					   ]);

push (@list, "ou=Judges,ou=gContainer,ou=Services,$orgDN", ["objectClass", "organizationalUnit",
					   "ou", "Judges",
					   ]);

push (@list, "name=LogicalDistance,ou=Judges,ou=gContainer,ou=Services,$orgDN", ["objectClass", "AliEngContainerJudge",
					   "name", "LogicalDistance",
             "weight", "1.0",
             "active", "1",
					   ]);

push (@list, "ou=Services,ou=gContainer,ou=Services,$orgDN", ["objectClass", "organizationalUnit",
					   "ou", "Services",
					   ]);

push (@list, "name=GAS,ou=Services,ou=gContainer,ou=Services,$orgDN", ["objectClass",
              [ "AliEngContainerServiceGAS" ],
					   "name", "GAS",
             "authentication", "myproxy",
             "global", "0",
             "gasModules", ["AliEnFileCatalog", "AliEnMetaCatalog", "AliEnTaskQueue"],
             "creatorClass", "gFactory::GAS",
					   ]);

push (@list, "alias=AliEnFileCatalog,name=GAS,ou=Services,ou=gContainer,ou=Services,$orgDN", ["objectClass", ["AliEnGASMODULE",], "alias", "AliEnFileCatalog",
                      "type", 1,
                      "mandatory", 1,
                      "interface", "AliEn::EGEE::Interface::Catalogue",
                      "options", "AliEn::EGEE::Service::Catalogue",
                      ]);
push (@list, "alias=AliEnMetaCatalog,name=GAS,ou=Services,ou=gContainer,ou=Services,$orgDN", ["objectClass", ["AliEnGASMODULE",], "alias", "AliEnMetaCatalog",
                      "type", 1,
                      "mandatory", 1,
                      "interface", "AliEn::EGEE::Interface::MetaCatalogue",
                      "options", "AliEn::EGEE::Service::MetaCatalogue",
                      ]);
push (@list, "alias=AliEnTaskQueue,name=GAS,ou=Services,ou=gContainer,ou=Services,$orgDN", ["objectClass", ["AliEnGASMODULE"], "alias", "AliEnTaskQueue",
                      "type", 1,
                      "mandatory", 0,
                      "interface", "AliEn::EGEE::Interface::WMS",
                      "options", "AliEn::EGEE::Service::WMS",
                      ]);

 push (@list, "name=ExWrapper,ou=Services,ou=gContainer,ou=Services,$orgDN", ["objectClass",
               ["AliEngContainerServiceExWrapper" ],
 					   "name", "ExWrapper",
              "global", "0",
              "creatorClass", "gFactory::ExWrapper",
 					   ]);

my $tmpDir="/tmp/$orgName";

my @info=("ou", $siteName, 
	  "domain", $domainName,
	  "logdir", "$tmpDir/log",
	  "cachedir", "$tmpDir/cache",
	  "tmpdir", "$tmpDir/tmp",);
push(@list, addSite($orgDN, \@info));
while (@list){
  my ($key, $value)=(shift @list, shift @list); 
  print "ok\nAdding $key...\t";
  my  $mesg=$ldap->add ($key,attr => $value);
  $mesg->code && print "failed\nCould not add  $key: ",$result->error 
    and exit (-1);
    ;
}

$ldap->unbind;
$done or exit(-1);
print "ok\n";


############################################################################
print "\n\n******************************************************************
\tInstallation finished sucessfully. 
alien-ldap is up and running in $hostName:8389
To stop or start the service, please use:

$etcDir/rc.d/init.d/alien-ldap [start/stop]

(We recommend putting a link from /etc/rc.d/init.d/ to that file)

We also recommend that you send us the name of your organisation, and your ldap address, so that anyone trying to contact your Virtual Organisation can find it. To do it, just do:

  echo \"New Virtual organisation: $orgName in $hostName:8389 $orgDN\" |mail alien-cert-request\@alien.cern.ch

If you have any problems, please contact alice-project-alien\@cern.ch
";

############################################################################
############################################################################
########### INTERNAL FUNCTIONS
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
sub getParam {
    my ($text,$defValue, $options) = @_;
    my $value;
    $options or $options="";
    print "$text [$defValue]:";
    ($options eq "secret") and system("stty -echo");

    chomp($value=<STDIN>);
    ($value) or $value="$defValue";

    ($options eq "secret") and print "\n" and system("stty echo");
#    print "\n";
    return $value;
}

sub createLDAPDirectory {
  my $ldapdir=shift;

  print "Creating $ldapdir...\t\t\t\t\t";
  if (!-d  $ldapdir) {
    my $dir = "";
    foreach ( split ( "/", $ldapdir ) ) {
      $dir .= "/$_";
      (-d $dir) and next;
      mkdir $dir, 0755 or print "failed\nError making the directory $? $!\n" and exit(-1);;
    }
  }
  chdir ($ldapdir) or print "failed\nError changing to $ldapdir\n" and exit(-1);
#  print "ok\nUncompressing the default database...\t\t\t\t";
  
#  my $tar="$ENV{ALIEN_ROOT}/etc/AliEn-ldap.tar.gz";
#  system ("tar", "zxf", "$tar")
#  and print "failed\nError uncompressing $tar\n $! $?\n" and exit;
  
  print "ok\nCrypting password...\t\t\t\t\t\t";

 my $slappasswd;
  foreach my $dir ($ENV{ALIEN_ROOT}, "/usr/") {
    -e "$dir/sbin/slappasswd" and $slappasswd="$dir/sbin/slappasswd" and last;
  }
  $slappasswd or print "Error: couldn't find slappasswd\n" and exit(-2);
  open (FILE, "$slappasswd -s '$passwd'|")
    or print "failed\nError using $slappasswd\n$?  $?\n" and exit(-1);
  my $crypPass=<FILE>;
  close FILE  or print "failed\nError executing $slappasswd\n$?  $?\n" and exit(-1);

  $crypPass or print "failed\nError $slappasswd did not produce the crypted password\n" and exit(-1);

  my $slapConf="$etcDir/openldap/slapd.conf";
  print "ok\nReading old $slapConf...\t\t";
  open (FILE, "<$slapConf") 
    or print "failed\nError opening the file $? $!\n" and exit(-1);
  my @file=<FILE>;
  close FILE;
  

  my $file=join("", @file);
  $file=~ s/^\#suffix.*\n?//m;
  $file=~ s/^\#rootdn.*\n?//m;
  $file=~ s/^\#rootpw.*\n?//m;

  $file=~ s/^(suffix.*)/\#$1\nsuffix\t\t\"$suffix\"/m;
  $file=~ s/^(rootdn.*)/\#$1\nrootdn\t\t\"$rootdn\"/m;
  $file=~ s/^(rootpw.*)/\#$1\nrootpw\t\t$crypPass/m;
  $file=~ s{^\s*(TLSCertificate\S+)\s+\S+/etc/openldap/certs}{$1\t$ldapDir/certs}mg;

 print "ok\nWriting the new configuration...\t\t\t\t";
  open (FILE, ">$ldapDir/slapd.conf") 
    or print "failed\nError opening the file $? $!\n" and exit(-1);
  print FILE $file;
  close FILE;

}
sub getDefaultMysql() {
  my $orgName=shift;
  my $hostName=shift;

  my $conf="/etc/aliend/mysqld.conf";

  (-e $conf) or print "Warning! not able to find the file $conf\n" and return "";
  open (FILE, "</etc/aliend/mysqld.conf") or print "Warning! not able to open the file $conf\n" and return "";
  my @file=<FILE>;
  close FILE;
  @file=grep (/ALIEN_ORGANISATION/, @file);
  my $file=join("", @file);
  $file =~ /\s*$orgName:(\d+)/mi and return "$hostName:$1";
  print "Warning! alien-mysql is not configured for $orgName\n";
  return "";
}

sub addSite{
#  my $siteName=shift;
#  my $domainName=shift;
  my $orgDN=shift;
  my $siteInfo=shift;

  my @siteInfo=@{$siteInfo};
  my $siteDN="ou=$siteName,ou=Sites,$orgDN";
  my $dir="/tmp/$orgName";
  my @list=();
  push (@list,$siteDN,["objectClass",["AliEnSite"],
		       @siteInfo		
		      ]);
  push (@list,"ou=Config,$siteDN",["objectClass", "organizationalUnit",
				   "ou", "Config"]);
  push (@list,"ou=Services,$siteDN",["objectClass", "organizationalUnit",
				     "ou", "Services"]);
  foreach my $service ("SE", "CE", "FTD", "PackMan") {
    push (@list, "ou=$service,ou=Services,$siteDN", ["objectClass", "organizationalUnit",
						     "ou", "$service"]);
  }

	
  return @list;
}

sub requestCertificate {

  my $certDir="$ldapDir/certs";
  print "You do not have host certificates for this machine. An AliEn host certificate is required.
Please, leave 'ch' as country Name and 'AliEn' as Organization Name. After that, enter your organisation name (eg Alice), and your host name (eg pcepalice45.cern.ch)\n\n";
  mkdir ("/etc/aliend/ldap", 0777);
  mkdir ($certDir, 0777);
  
  my $command = "openssl req -new -nodes -config $ENV{ALIEN_ROOT}/etc/alien-certs/alien-host-ssl.conf -days 364 -keyout $certDir/host.key.pem -out $certDir/host.req.pem";
  my $err = `$command`;
  if($err) {
    print "An error occured\n$err\n";
    return;
  }
  chmod 0600, "$certDir/host.key.pem";
  my $mailadd = "alien-cert-request\@alien.cern.ch";
  print "**********************************************************

  Your key is stored in: $certDir/host.key.pem
  Now send you request to $mailadd by doing

  cat $certDir/host.req.pem | mail $mailadd

**********************************************************\n";

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
sub getCertificateSubject {
  open (FILE, "openssl x509 -in $ldapDir/certs/host.cert.pem -noout -subject|") or print "Error executing openssl\n" and return "";
  my @FILE=<FILE>;
  close FILE;
  my $cert=join("", @FILE);
  chomp $cert;
  $cert=~ s/^subject=\s*//;
  return $cert;
}  

sub modifyLDAPConf {

  my %files;
  $files{"slapd.conf"}={};

  $files{"slapd.conf"}->{"\/opt\/alien\/"}="$ENV{ALIEN_ROOT}\/";
  $files{"slapd.conf"}->{"directory\\s+\/var\/lib\/alien-ldap"}="directory    $ldapdir";
  modifyFiles($ldapDir, %files) or return;

  my %files2;
  $files2{"alien-ldap"}={};

  $files2{"alien-ldap"}->{"\/opt\/alien\/"}="$ENV{ALIEN_ROOT}\/";
  $files2{"alien-ldap"}->{"DIRECTORY=\/opt\/alien.*"}="DIRECTORY=$ENV{ALIEN_ROOT}";

  modifyFiles("$etcDir/rc.d/init.d", %files2) or return;

  return 1;
}
