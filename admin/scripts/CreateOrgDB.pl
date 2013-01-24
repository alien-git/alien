use strict;
use AliEn::Database::Catalogue;
use AliEn::Database::Accesses;
use Net::Domain qw(hostname hostfqdn hostdomain);

print "This script will create the databases for the alien catalogue for a new AliEn Organisation
The mysqld for AliEn does not run in the standard mysql port (3306). Therefore, you can have the default daemon of mysql and several alien-mysql daemons running in the same machine\n\n";


my $user=getpwuid($<);

my $mysqlDir="/home/mysql/";

if ($user ne "root") { 
  print "The databases will run under the uid of '$user'.\n";
  getParam("Are you sure you want to continue (Y/N)?","N") =~ /y/i 
    or print "exiting...\n" and exit (-1);
  $mysqlDir="$ENV{ALIEN_HOME}/mysql/";
}

print "Please, enter the following information:\n";
my $orgName       = getParam("Organisation name","ALICE");

$mysqlDir.="$orgName";
if (-d "$mysqlDir"){
  print "Warning!! The directory $mysqlDir already exists (did you already install AliEn for $orgName?). This directory will be deleted\n";
  my $confirm=getParam("Are you sure you want to continue (Y/N)?","N");
  ($confirm eq "Y") or print "exiting...\n" and exit (-1);
}
my $passwd;
my $origPass=$passwd=createPasswd();
#my $rootPass=createPasswd();

$passwd =getParam("Administrator password","$origPass", "secret");

my $token=createToken();

if ("$passwd" ne "$origPass"){
  my $check=getParam("Enter the password again", "", "secret");
  ("$check" eq "$passwd")
    or print "ERROR: Passwords are not the same\n" and exit (-2);

}

my $hostName=Net::Domain::hostfqdn();
my $suffix=Net::Domain::hostdomain();
$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
$hostName=getParam("Host name", $hostName);

my $portNumber=getParam("Port number", getDefaultPort());
my $ldapDN        = getParam("LDAP host and DN (put -1 if you want to look for it in the alien ldap server)", "$hostName:8389/o=$orgName,$suffix");
$ldapDN eq "-1" and $ldapDN="";

print "Creating round with following information
********************************************

Organisation name:     $orgName
Administrator password: ******
Host name:             $hostName
Port number:           $portNumber
********************************************\n";

my $OK = getParam ("Proceed with creation","Y");
if($OK ne "Y") {
  print "Exiting\n";
  exit -2;
}
############################################################################
##     NOW WE START WITH THE CREATION OF THE DATABASES!!! 
############################################################################
$ENV{ALIEN_ORGANISATION}=$orgName;
$ENV{ALIEN_LDAP_DN}=$ldapDN;
my $etcDir="$ENV{ALIEN_ROOT}/etc/";
if (-d $mysqlDir) {
  print "Stopping the previous alien-mysql (this may fail if it was not running)\n";
  system ("$etcDir/rc.d/init.d/alien-mysqld", "stop", "$orgName");
  print "Deleting the directory $mysqlDir...\t\t\t";
  system ("rm", "-rf", $mysqlDir) and print "failed\nError deleting $mysqlDir $? $!\n" and exit (-2);  
  print "ok\n";
}

print "Making the directory...\t\t\t\t\t\t";
if (!-d  $mysqlDir) {
  my $dir = "";
  foreach ( split ( "/", $mysqlDir ) ) {
    $dir .= "/$_";
    (-d $dir) and next;
    mkdir $dir, 0755 or print "failed\nError making the directory $? $!\n" and exit(-1);;
  }
}
chdir ($mysqlDir) or print "failed\nError changing to $mysqlDir\n" and exit(-2);
#print "ok\nUncompressing the default database...\t\t\t\t";#
#
#my $tar="$etcDir/AliEnCatalogue-mysql.tar.gz";
#system ("tar", "zxf", "$tar")
#  and print "failed\nError uncompressing $tar\n $! $?\n" and exit(-2);
print "ok\nCalling mysql_install_db...";
system("$ENV{ALIEN_ROOT}/scripts/mysql_install_db", "--datadir=$mysqlDir/mysql", "--skip-name-resolve ", "--basedir=$ENV{ALIEN_ROOT}", "--no-defaults")
   and print "Error creating the empty database\n" and exit(-2);


if (! $<) {
  print "ok\nChanging owner of the directory...\t\t\t\t";
  system("chown",  "mysql.mysql", "-R", $mysqlDir) 
    and print "failed\n Error changing the owner of $mysqlDir\n $? $!\n" and exit(-2);
}

print "Creating my.cnf\n";
my $FILE;
open ($FILE, ">","mysql/my.cnf") or print "Error opening my.cnf\n" and exit(-2);
print $FILE "
[mysqld]
#set-variable    = max_connections=2000

#ignore_builtin_innodb
#plugin-load=innodb=ha_innodb_plugin.so;innodb_trx=ha_innodb_plugin.so;
#  innodb_locks=ha_innodb_plugin.so;innodb_lock_waits=ha_innodb_plugin.so


default-storage-engine=InnoDB
innodb_file_per_table=1
innodb_file_format=barracuda
innodb_strict_mode=1

";
close $FILE;
my $configDir="/etc/aliend";
($<) and $configDir="$ENV{ALIEN_HOME}/etc/aliend";

if (! -d $configDir) {
  print "ok\nCreating the directory $configDir...\t\t\t\t"; 
  mkdir ($configDir, 0755) or 
    print "failed\nError making the directory $? $!\n" and exit(-2);
}

if (-e "$configDir/mysqld.conf") {
  print "ok\nMoving $configDir/mysqld.conf to $configDir/mysqld.conf.old...\t";
  rename ("$configDir/mysqld.conf", "$configDir/mysqld.conf.old")
    or print "failed\nError renaming the file\n$?  $?\n" and exit(-2);

  print "ok\nReading old $configDir/mysqld.conf...\t\t\t\t";
  open ($FILE, "<","$configDir/mysqld.conf.old") 
    or print "failed\nError opening the file $? $!\n" and exit(-2);
  my @file=<$FILE>;
  close $FILE;
  my @orgs=grep (/ALIEN_ORGANISATIONS=/, @file);
  my $newline=join("", @orgs);
  @file = grep (! /ALIEN_ORGANISATIONS=/, @file);
  $newline=~ s/\s*$orgName:\d+\s*/ /g;
  $newline=~ s/([^=])\"/$1 $orgName:$portNumber\"/;
  
  print "ok\nWriting the new configuration...\t\t\t\t";
  open ($FILE, ">","$configDir/mysqld.conf") 
    or print "Error opening the file $? $!\n" and exit(-2);
  print $FILE "@file\n$newline";
  close $FILE;
  
} else {
  print "ok\nCreating the file $configDir/mysqld.conf...\t\t\t";
  open ($FILE, ">","$configDir/mysqld.conf") 
    or print "Error opening the file $? $!\n" and exit(-2);

  print $FILE "#AliEn Organisations\n
ALIEN_ORGANISATIONS=\"$orgName:$portNumber\"\n";
  close $FILE;
}

print "ok\nStarting the daemon...\n";

system("$etcDir/rc.d/init.d/alien-mysqld", "start", "$orgName:$portNumber")
  and print "failed!!\nError starting the daemon\n$! and $?\n" and exit(-2);

sleep (2);

############################################################################
############################################################################
############ UPDATING THE INFORMATION OF THE DATABASE
#
# First of all, let's check the version:
#
#my $mysql="$ENV{ALIEN_ROOT}/bin/mysql";
#open (FILE, "$mysql -V|") or print "Error using mysql\n" and exit(-2);
#my $version=join ("", <FILE>);
#close FILE or print "Error finding the version of mysql" and exit(-2);
#print "Using mysql $version";
my $socket="/tmp/alien.mysql.$orgName.sock3";
#
#if ($version=~ /Distrib\s+4\.(\d+)\.(\d+)/) {
#  my $number=($1*100+$2);
#  print "This is version 4 ($number)\n";
#  my $file="$ENV{ALIEN_ROOT}/bin/mysql_fix_privilege_tables";
#  if ($number>19) {
#    system("$file --socket=$socket") and exit(-2);
#  }else {
#    open (FILE, "<$file") or print "Error searching for $file\n" and exit(-2);
#    my @commands=<FILE>;
#    close FILE or print "ERROR opening $file\n" and exit(-2);
#    map { s{cmd=.*}{cmd="$mysql -u root -S $socket mysql"}} @commands;
#    #  print "Let's do @commands\n";
#    open (FILE, "|/bin/sh")or print "ERRROR DOING @commands\n" and exit(-2);
#    print FILE @commands;
#    close FILE or print "ERRROR DOING @commands\n" and exit(-2);
#  }
#  print "YUHUUU\n";#
#
#} elsif ($version =~ /Distrib\s+5/ ){
#  print "This is version 5!!\n";
#  my $file="$ENV{ALIEN_ROOT}/bin/mysql_fix_privilege_tables";
#  print "LET's see if we can connect to the database \n";
#  system("ps -A -o \"pid command\" |grep mysql");
#  sleep 30;
#  system("$file --socket=$socket --basedir=$ENV{ALIEN_ROOT}") and exit(-2);
#}
print "Let's change the root password";

#my $rootP=createPasswd();
sleep(10);

#system ("$ENV{ALIEN_ROOT}/bin/mysqladmin -u root password '$rootPass' -S $socket")
#  and exit(-2);

print "update mysql.user set password=PASSWORD('$passwd') where User='root'\n\n";

open($FILE, "| $ENV{ALIEN_ROOT}/bin/mysql  -u root -S $socket") or print "Error conecting to mysql \n" and exit(-2);
print $FILE "update mysql.user set password=PASSWORD('$passwd') where User='root';
delete from mysql.user where user !='root';
GRANT ALL PRIVILEGES ON *.* TO admin IDENTIFIED BY '$passwd' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO admin\@localhost IDENTIFIED BY '$passwd' WITH GRANT OPTION;
flush privileges;
create database if not exists alien_system;
create database if not exists processes;
create database if not exists accesses;
create database if not exists transfers;
create database if not exists INFORMATIONSERVICE;
create database if not exists ADMIN;";
close $FILE or print "Error updating the password!\n" and exit(-2);
print "DONE!!\n";

print "Connecting to mysql on $hostName:$portNumber...\t\t";

$ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'} = "$ENV{ALIEN_HOME}/authen/lpriv.pem";

$ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} = "$ENV{ALIEN_HOME}/authen/lpub.pem";
$ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'} = "$ENV{ALIEN_HOME}/authen/rpriv.pem";
$ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'} = "$ENV{ALIEN_HOME}/authen/rpub.pem";
my $db=AliEn::Database::Catalogue->new({USE_PROXY=>0,
					USER=>"admin",
					ROLE=>"admin",
					PASSWD=>$passwd,
#					DEBUG=>5,
				       });

if (! $db) {
  print "We couldn't connect to the database\n";
  print "Let's try as root\n";
  open ($FILE, "| $ENV{ALIEN_ROOT}/bin/mysql  -p$passwd -u root -S $socket") or print "Error conecting to mysql \n" and exit(-2);
  print $FILE "select * from mysql.user;";
  close $FILE;
  exit(-2);
}

my $now=`date "+%b %d %H:%M"`;
$now =~ s/\n//;

print "Creating the tables in the database\n";
$db->createCatalogueTables() or exit(-2);


my $dbAcc=AliEn::Database::Accesses->new({USE_PROXY=>0,
					USER=>"admin",
					ROLE=>"admin",
					PASSWD=>$passwd,
#					DEBUG=>5,
				       });

if (! $dbAcc) {
  print "We couldn't connect to the database\n";
  print "Let's try as root\n";
  open ($FILE, "| $ENV{ALIEN_ROOT}/bin/mysql  -p$passwd -u root -S $socket") or print "Error conecting to mysql \n" and exit(-2);
  print $FILE "select * from mysql.user;";
  close $FILE;
  exit(-2);
}

print "Creating the tables in the database\n";
$dbAcc->createAccessesTables() or exit(-2);


foreach my $dbtype ('TaskQueue', 'Transfer', 'IS',) {
  print "Creating the $dbtype...";
  my $s="AliEn::Database::$dbtype";

  eval "require $s;";
  my $d=$s->new({USE_PROXY=>0,
		 USER=>'admin',
		 ROLE=>'admin',
		 PASSWD=>$passwd,
		 #					DEBUG=>5,
		}
	       ) or exit(-2);
  print "Done with $?\n";
}
my @q=(
       "INSERT INTO USERS (Username) values ('admin')",
       "INSERT INTO GRPS (Groupname) values ('admin')",
       "INSERT INTO L0L(lfn,ownerId, gownerId,perm,type) values ('',1 , 1,'755','d')",
       "INSERT INTO INDEXTABLE(lfn,tableName) values  ('/', 0)",
       "INSERT INTO GUIDINDEX(guidTime,tableName) values  ('', 0)",
       "INSERT INTO SE(seName) VALUES ('no_se')",
       "insert into UGMAP values (1,1,1)",

);

my $subject="";
  my $file="$ENV{ALIEN_HOME}/globus/usercert.pem";
  if (-f $file) {
    if (open(my  $TEMP, "openssl x509 -noout -in $file -subject|")){
      $subject=<$TEMP>;
      $subject=~ s/^subject=\s+//;
      chomp $subject;
      close($TEMP);
    }
  }

$subject and 
	push (@q,"GRANT ALL PRIVILEGES ON *.* TO adminssl REQUIRE SUBJECT '$subject' WITH GRANT OPTION"); 

foreach my $query (@q) {
  print "Doing $query\n";
  $db->do($query) or exit(-3);
}



print "ok\n";

############################################################################
print "\n\n******************************************************************
\tInstallation finished sucessfully. 
alien-mysql is up and running in $hostName:$portNumber
To stop or start the service, please use:

$etcDir/rc.d/init.d/alien-mysqld [start/stop]

(We recommend putting a link from /etc/rc.d/init.d/ to that file)\n

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

sub createToken  {
    my $token = "";
    my @Array = (
        'X', 'Q', 't', '2', '!', '^', '9', '5', "\$", '3', '4', '5', 'o',
        'r', 't', '{', ')', '}', '[', ']', 'h', '9', '|', 'm', 'n', 'b', 'v',
        'c', 'x', 'z', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', ':', 'p',
        'o', 'i', 'u', 'y', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
        'A', 'S', 'D', 'F', 'G', 'H', 'J', 'Z', 'X', 'C', 'V', 'B', 'N', 'M'
    );
    my $i;
    for ( $i = 0 ; $i < 32 ; $i++ ) {
        $token .= $Array[ rand(@Array) ];
    }
    return $token;
}
sub getDefaultPort{
  
  my $default="3307";
  my $confFile="/etc/aliend/mysqld.conf";
  ($<) and $confFile="$ENV{ALIEN_HOME}$confFile";
  ( -e "$confFile") or  return $default;
  open (my $FILE, "<", "$confFile") or return $default;
  my @list=<$FILE>;
  close ($FILE);
  @list=grep (/ALIEN_ORGANISATIONS/, @list);

  my $line=join ("", @list);
  while (1) {
    print "Checking $default\n";
    $line =~ /$default/m or return $default;
    $default ++;
  }
}

