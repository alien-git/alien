use strict;
use AliEn::Config;
use Net::LDAP;

print "This script will create the ldap configuration for a new AliEn Site\n\n";


#my $user=getpwuid($<);
#
#("$user" eq "root") or 
#  print "This script has to be called being root\n" and exit(-1);


#############################################################################


print "Please, enter the following information:\n";
my $orgName       = getParam("Organisation name","ALICE");
my $ldapDN        = getParam("LDAP host and DN (leave it empty if you want to look for it in the alien ldap server)", "");

$ldapDN and $ENV{ALIEN_LDAP_DN}=$ldapDN;
my $config=AliEn::Config->new({"organisation", $orgName});

$config or exit(-2);

my   $rootdn=getParam("Enter root dn for $config->{LDAPHOST}","cn=Manager,dc=cern,dc=ch");
my  $ldapPasswd=getParam("Enter admin password for $config->{LDAPHOST}", "", "secret");

my $ldap=checkLDAPConnection() or exit(-2);

my $siteName=getParam("Site Name", "CERN");
my $siteDomain=getParam("Site Domain", "cern.ch");
my $cityName=getParam("Name of the city","Geneva");
my $admin=getParam("Site administrator", "Pablo Saiz");
my $frontEnd=getParam("Name of the front-end machine where alien is installed","alien.cern.ch");
my $logDir=getParam("Path in that machine to keep log files","/home/alienmaster/AliEn/log");
my $tmpDir=getParam("Path in the machine to keep temporary files","/tmp/AliEn/tmp"); 

print "\nCreating the site with the following information
********************************************

Site name:             $siteName
Site domain:           $siteDomain
City name:             $cityName
Administrator name:    $admin
Front End:             $frontEnd
Log directory:         $logDir
Temprorary directory:  $tmpDir
********************************************\n";

my $OK = getParam ("Proceed with creation","Y");
if (($OK ne "Y") and ($OK ne "y")) {
    print "Exiting\n";
    exit(-1);
}
############################################################################
##     NOW WE START WITH THE CREATION OF THE DATABASES!!! 
############################################################################



############################################################################
############################################################################
############ UPDATING THE INFORMATION OF THE DATABASE
############################################################################
print "Connecting to the ldap server $config->{LDAPHOST}...\t";


$ldap = Net::LDAP->new($config->{LDAPHOST}, "onerror" => "warn") or print "failed\nError conecting to the ldap server\n $? and $! and  $@\n" and exit(-1);

my $result=$ldap->bind($rootdn, "password" => $ldapPasswd);
$result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error and exit(-1);

my $done=1;
my $orgDN=$config->{LDAPDN};
my @info=("ou", $siteName, 
	  "domain", $siteDomain,
	  "location", $cityName,
	  "administrator", $admin,
	  "logdir", $logDir,
	  "cachedir", "$tmpDir/cache",
	  "tmpdir", "$tmpDir/tmp",);
my @list=addSite( $orgDN, \@info);

#print "VAMOS A ANNADIR @list\n";
#exit;

while (@list){
  my ($key, $value)=(shift @list, shift @list); 
  print "ok\nAdding $key...\t";
  my  $mesg=$ldap->add ($key,attr => $value);
  $mesg->code && print "failed\nCould not add  $key: ",$result->error 
    and exit (-1);

}

$ldap->unbind;
$done or exit(-1);
print "ok\n";


############################################################################
print "\n\n******************************************************************
\tInstallation finished sucessfully. 
There is a new site in $orgName called $siteName

If you have any problems, please contact alice-project-alien\@cern.ch
";

############################################################################
############################################################################
########### INTERNAL FUNCTIONS
sub getParam {
    my ($text,$defValue, $options) = @_;
    $options or $options="";
    my $value;
    print "$text [$defValue]:";
    ($options eq "secret") and system("stty -echo");

    chomp($value=<STDIN>);
    ($value) or $value="$defValue";

    ($options eq "secret") and print "\n" and system("stty echo");
#    print "\n";
    return $value;
}

sub addSite{
  my $orgDN=shift;

  my $siteInfo=shift;

  my @siteInfo=@{$siteInfo};


  my $siteDN="ou=$siteName,ou=Sites,$orgDN";
  my $dir="/tmp/$orgName";
  my @list=();
  push (@list, $siteDN, ["objectClass", ["top","organizationalUnit", "AliEnSite"],
			 @siteInfo,
			]);
  push (@list, "ou=Config,$siteDN", ["objectClass", ["top","organizationalUnit"],
				     "ou", "Config"]);
  push (@list, "ou=Services,$siteDN", ["objectClass", ["top","organizationalUnit"],
			"ou", "Services"]);
  foreach my $service ("SE", "CE", "FTD") {
    push (@list, "ou=$service,ou=Services,$siteDN", ["objectClass", ["top","organizationalUnit"],
			"ou", "$service"]);    
  }

	
  return @list;
}

sub checkLDAPConnection {
  print "Connecting to ldap server on $config->{LDAPHOST}...\t";
  my $ldap = Net::LDAP->new("$config->{LDAPHOST}", "onerror" => "warn") or print "failed\nError conecting to the ldap server $config->{LDAPHOST}\n $? and $! and  $@\n" and return;
  
  my $result=$ldap->bind($rootdn, "password" => "$ldapPasswd");
  $result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error, "\n" and return;


  $ldap->unbind;
  print "ok\n";
  return 1;
}
