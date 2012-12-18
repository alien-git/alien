use strict;

print "This script will create the web portal for a new AliEn Organisation
It uses apache, running in the default port.\n\n";


my $user=getpwuid($<);

("$user" eq "root") or 
  print "This script has to be called being root\n" and exit(-2);

print "Please, enter the following information:\n";
my $orgName       = getParam("Organisation name","ALICE");
#my $alienRoot     =  getParam("AliEn Root directory", "$ENV{ALIEN_ROOT}");
my $hostName=`hostname`;
chomp $hostName;

my $mysqlHost=getParam("alien-mysql host",getDefaultMysql($orgName,$hostName));
my $mysqlPasswd=getParam("Mysql password","", "secret");

checkMysqlConnection() or exit(-2);

my $webUser      =getParam("User who will run apache", "wwwd");
my $alienUser    =getParam("User who is running the alien services", "alienmaster");
my (@data) = getpwnam($alienUser);
@data or print "Error user $alienUser does not exist\n" and exit(-2);
my $alienDir=$data[7];


$hostName      =getParam("Server name", $hostName);
my  $cvsDir=getParam("Directory of the CVS version of AliEn","$alienDir/AliEn");
my $webdir="$cvsDir-$orgName";

$webdir=getParam("Directory for the web templates", $webdir);
if (-d $webdir){
  print "Warning! The directory already exists. Do you want to delete it?";
  getParam("Y/N", "Y") eq "Y" or print "exiting...\n" and exit;
  print "removing the directory...ok\n";
  system("rm", "-rf","$webdir");
}
my $cert=getParam("Host certificate", "$ENV{ALIEN_ROOT}/apache/conf/certs/host.cert.pem");
my $key=getParam("Host key", "$ENV{ALIEN_ROOT}/apache/conf/certs/host.key.pem");
my $httpLog=getParam("Log dir for httpd", "/var/log/httpd");


print "Creating http.conf following information
********************************************

Organisation name:     $orgName
AliEn user name:       $alienUser
Host name:             $hostName
Web User name:         $webUser
Directory:             $webdir
Log directory:         $httpLog
********************************************\n";

my $OK = getParam ("Proceed with creation","Y");
if($OK ne "Y") {
    print "Exiting\n";
    exit;
}

############################################################################
##     NOW WE START WITH THE CREATION OF THE DATABASES!!! 
############################################################################
print "Stopping the old http server (this might fail if the server was not running)...";
system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-httpd", "stop");
my $apacheDir="$ENV{ALIEN_ROOT}/apache";


modifyHTTPDConf() or exit(-2);

if (! -d $httpLog ) {
  print "ok\nMaking the log directory $httpLog...";
  mkdir($httpLog) or print "Error making $httpLog\n" and exit(-2);
}
print "ok\nCopying the directory of the templates...";

-d "$webdir" or (mkdir("$webdir") or 
		 print "Error making the directory $webdir\n$! $?\n" 
		 and exit(-2));
 
print "ok\nChecking if $webUser exists";
if (! getpwnam($webUser)) {
  print "\n\tUser does not exist!! creating it...";
  system("/usr/sbin/adduser", $webUser) and print "Error creating the new user\n$@ $? and $!\n" and exit;
}
print "ok\nChecking if $webUser can read $webdir";
my $check=system("su - $webUser -c 'ls -l $webdir >/dev/null'");
$check and print "\nError the webUser can't see the directory $webdir\n" 
    and exit(-2);

system ("cp", "-r","$cvsDir/Html", $webdir)
  and print "Error copying the directory $webdir\n$! $?\n" 
  and exit(-2); 

modifyTemplates() or exit(-2);

createKeys() or exit(-2);


makePerlModule() or exit(-2);

print"\nStarting the daemon...\n";
system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-httpd", "start") and
  print "Error starting the httpd\n$! and $?\n" and exit(-2);


############################################################################
############################################################################
############ UPDATING THE INFORMATION OF THE DATABASE

############################################################################
print "\n\n******************************************************************
\tInstallation finished sucessfully. 
alien-http is up and running in $hostName
To stop or start the service, please use:

$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-httpd [start/stop]

(We recommend putting a link from /etc/rc.d/init.d/ to that file)\n

If you have any problems, please contact alice-project-alien\@cern.ch
";

############################################################################
############################################################################
########### INTERNAL FUNCTIONS
sub createKeys {
  print "Creating the keys to connect to the database...\n";
  my $dir="$webdir/Html/templates/.alien";
  if (-d ("$dir")){
    print "Deleting the old directory  $dir ...";
    system("rm", "-rf", "$dir") and 
      print "Error deleting the directory\n" and return;
    print "ok\n";
  }
  print "Making the directory $dir...";
  mkdir ("$dir") or print "Error making the directory\n$? $!\n" and return;
  print "ok\nCopying the keys of alienmaster...";
  system ("cp", "-r", "$alienDir/.alien/identities.\L$orgName\E", "$dir") 
    and print "Error copying the keys!!\n" and return;
  print "ok\nMaking $webUser the owner of that directory";
  system("chown", "$webUser", "$dir","-R")  
    and print "Error changing the ownership!!\n" and return;

  my ($hostName2, $portNumber)=split ":", $mysqlHost;
  print "ok\nConnecting to mysql...";
   open (MYSQL, "| mysql -h $hostName2 -P $portNumber -u admin --password='$mysqlPasswd'") 
    or print "Error connecting to mysql in  $hostName2 -P $portNumber\n" and return;

  print "ok\nGranting read access to the databases to user $webUser\@\"$hostName...";
  print MYSQL "GRANT SELECT on *.* to $webUser\@\"$hostName\"\n";
  close MYSQL or print "Error closing connection to mysql in  $hostName2 -P $portNumber\n" and return;
 
  print "ok\n";
  return 1;
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
sub modifyTemplates {
  print "ok\nModifying the templates...";
  my %files;
  $files{"login.tpl"}={};
  $files{"login.tpl"}->{"alien.cern.ch"}=$hostName;
  $files{"login.tpl"}->{"Alice"}=$orgName;
  
  $files{"index.tpl"}={};
  $files{"index.tpl"}->{"\"\/Alien"}="\"\/$orgName";

  $files{"header.tpl"}={};
  $files{"header.tpl"}->{"https:\/\/alien.cern.ch"}="https:\/\/$hostName";
  $files{"header.tpl"}->{"\/Alien\/"}="\/$orgName\/";
  #$files{"index.tpl"}->{"Alice"}=$orgName;

  modifyFiles("$webdir/Html/templates", %files) or return;
  my %files2;
  $files2{"Menu.js"}={};
 $files2{"Menu.js"}->{"\/Alien\/main"}="\/$orgName\/main";
 $files2{"Menu.js"}->{"\/Alien\/Map"}="\/$orgName\/Map";
  modifyFiles("$webdir/Html/Menu/Alien", %files2) or return;
  my %files3;
  $files3{"Makefile"}={};
 $files3{"Makefile"}->{"ORGANISATIONS="}="ORGANISATIONS=$orgName ";
 $files3{"Makefile"}->{"\/home\/alienmaster\/AliEn-1.29.11\/alien"}=
   "$ENV{ALIEN_ROOT}";
  modifyFiles("$webdir/Html/map", %files3) or return;
  print "ok\nCreating the map for the organisation...\n";

  chdir ("$webdir/Html/map");
  system ("make")
    and print "Error making the map!\n $! and $?\n" and return;

  return 1;
}

sub modifyHTTPDConf {
  my $conf="$apacheDir/conf/httpd.conf";
  if (! -e $conf) {
    print "Error: The apache configuration in $conf does not exist
Is the AliEn-Portal rpm installed??\n";
    return;
  }
  if ((! -e $cert) or (! -e $key)) {
    print "Error: The host certificate is not there (in $key and $cert)\n";
    return;
  }
  print "ok\nMoving $conf to $conf.back...";
  (-e "$conf.back") and 
    (unlink "$conf.back" or 
     print "Error deleting $conf.back\n$! $?\n" and return);
  #link ($conf, "$conf.back") or print "Error making the copy of the old conifguration\n$! $?\n"
  #  and exit(-1);
  system("cp", $conf, "$conf.back") and
  print "Error making the copy of the old conifguration\n$! $?\n"
  and return;

  print "ok\nReading the configuration...";

  open (FILE, "<$conf") or 
    print "Error opening the configuration\n$! $?\n" and return;
  my @conf=<FILE>;
  close FILE;
  print "ok\nWriting the configuration...";
  if (grep ( /Location\s*\/$orgName/, @conf)){
    print "Warning: The organisation $orgName already exists in this configuration\n";
    getParam("Do you want to delete it:(Y/N)", "Y") eq "Y" or print "exiting...\n" and return;
    my $temp=join("",@conf);
    $temp=~ s/<Location \/$orgName>.*?<\/Location>//sg;
    @conf=split("\n", $temp);
    map {$_="$_\n"} @conf;
  }

  map {$_=~ s/^(User\s*wwwd)/\#$1\nUser $webUser/} @conf;
  map {$_=~ s/^(\s*ServerName\s*alien.cern.ch)/\#$1\nServerName $hostName/g} @conf;
  map {$_=~ s/^(\s*ServerAdmin\s*(\S+)\@alien.cern.ch)/\#$1\nServerAdmin $2\@$hostName/g} @conf;
  map {$_=~ s/^(\#NEW LOCATION 1)/$1
  <Location \/$orgName>
    SetHandler perl-script
    PerlSetupEnv On
    PerlHandler AliEn::Portal::AliEn::$orgName
    Options +ExecCGI
   <IfDefine SSL>
    SSLOptions +StdEnvVars +CompatEnvVars
    PerlSetEnv X509_RUN_AS_SERVER 1
    PerlSetEnv X509_USER_CERT "$cert"
    PerlSetEnv X509_USER_KEY "$key"
   <\/IfDefine>
  <\/Location>/} @conf;

  map {$_=~ s/^(\#NEW LOCATION 2)/$1
<Location \/$orgName>
  order deny,allow
  allow from all
<\/Location>/} @conf;
  map {$_=~ s/^(SSLCertificateFile\s*\S*)/\#$1\nSSLCertificateFile $cert/} @conf;
  map {$_=~ s/^(SSLCertificateKeyFile\s*\S*)/\#$1\nSSLCertificateKeyFile $key/} @conf;

  map {$_=~ s/\/var\/log\/httpd/$httpLog/} @conf;
  

  map {$_=~ s/^(<VirtualHost alien.cern.ch>)/\#$1\n<VirtualHost $hostName>/} @conf;
  my $host=$hostName;
  $host=~ s/^([^\.]*)\..*/$1/;
  map {$_=~ s/^(<VirtualHost alien>)/\#$1\n<VirtualHost $host>/} @conf;
  map {$_=~ s/^(\s*DocumentRoot\s+\S+)/\#$1\nDocumentRoot $webdir\/Html/} @conf;

  map {$_=~ s/^((\s*)PerlRequire\s+\S+)/\#$1\n$2PerlRequire $cvsDir\/Html\/scripts\/startup.pl/} @conf;

  map {$_=~ s/\/opt\/alien\//$ENV{ALIEN_ROOT}\//} @conf;

  open (FILE, ">$conf")or print "Error writing the configuration\n$! $?\n" and return;
  print FILE @conf;
  close FILE;

  my %file;
  $file{"alien-httpd"}={};
  $file{"alien-httpd"}->{"\/opt\/alien\/"}="$ENV{ALIEN_ROOT}\/";
  $file{"alien-httpd"}->{"\/opt\/alien\""}="$ENV{ALIEN_ROOT}\"";
  $file{"alien-httpd"}->{"STATUSURL=\S+"}="STATUSURL=\"http:\/\/$hostName\"";

  modifyFiles("$ENV{ALIEN_ROOT}/etc/rc.d/init.d", %file) or return;

  return 1;
}
sub makePerlModule {
  
  print "ok\nCreating the module AliEn::Portal::$orgName";

  system("cp", "$cvsDir/Portal/Portal/AliEn/Atlas.pm", 
	 "$cvsDir/Portal/Portal/AliEn/$orgName.pm") and 
	   print "Error making the perl module\n$! and $?\n" and return;

  my %files2;
  $files2{"$orgName.pm"}={};
  $files2{"$orgName.pm"}->{"Atlas"}="$orgName";
  $files2{"$orgName.pm"}->{"/home/httpd/html/Atlas/templates"}="$webdir/Html/templates";

  modifyFiles("$cvsDir/Portal/Portal/AliEn", %files2) or return;
  print "ok\nMaking the Makefile again...";
  chdir("$cvsDir") or print "Error going to $cvsDir\n" and return;
  
  system("$ENV{ALIEN_ROOT}/bin/alien-perl Makefile.PL >/dev/null") and 
    print "Error making the Makefiles\n$! and $?\n" and return;
  print "ok\nInstalling everything...";
  system("make install >/dev/null") and
    print "Error making the Makefiles\n$! and $?\n" and return;
  print "ok\nChanging the ownership of $webdir...";
  system("chown", "-R", "alienmaster.alienmaster", $cvsDir, 
	 $ENV{ALIEN_ROOT},$webdir);
  system("chown", "$webUser", "$webdir/Html/templates/.alien","-R")  
    and print "Error changing the ownership!!\n" and return;

  return 1;
}
sub getParam {
    my ($text,$defValue, $options) = @_;
    my $value;
    print "$text [$defValue]:";
    ($options eq "secret") and system("stty -echo");

    chomp($value=<STDIN>);
    ($value) or $value="$defValue";

    ($options eq "secret") and print "\n" and system("stty echo");
#    print "\n";
    return $value;
}
sub getDefaultMysql() {
  my $orgName=shift;
  my $hostName=shift;

  my $conf="/etc/aliend/mysqld.conf";

  (-e $conf) or print "Warning! not able to find the file $conf\n" and return;
  open (FILE, "</etc/aliend/mysqld.conf") or print "Warning! not able to open the file $conf\n" and return;
  my @file=<FILE>;
  close FILE;
  @file=grep (/ALIEN_ORGANISATION/, @file);
  my $file=join("", @file);
  $file =~ /\s*$orgName:(\d+)/mi and return "$hostName:$1";
  print "Warning! alien-mysql is not configured for $orgName\n";
  return;
}

sub checkMysqlConnection {
  my ($hostName, $portNumber)=split ":", $mysqlHost;
  print "Connecting to mysql $hostName $portNumber...\t\t\t";
  open (MYSQL, "| mysql -h $hostName -P $portNumber -u admin --password='$mysqlPasswd'") 
    or print "Error connecting to mysql in  $hostName -P $portNumber\n" and return;
  close MYSQL or print "Error closing connection to mysql in  $hostName -P $portNumber\n" and return;

  print "ok\n";
  return 1;
}
