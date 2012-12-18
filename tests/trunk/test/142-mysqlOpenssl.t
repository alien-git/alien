use strict;

use AliEn::Database;
use AliEn::Config;
use AliEn::X509;
use IPC::Open2;



my $config=AliEn::Config->new() or exit(-2);
my $mysqlDir="$ENV{ALIEN_HOME}/mysql/$config->{ORG_NAME}/mysql";
my $file="$mysqlDir/my.cnf";
unlink $file;

open (FILE, ">$file") or print "Error creating the file $file" and exit(-2);
print FILE "[mysqld]
# ssl-capath=$ENV{ALIEN_ROOT}/globus/share/certificates/
 ssl-cert=$ENV{ALIEN_HOME}/globus/usercert.pem
 ssl-key=$ENV{ALIEN_HOME}/globus/userkey.pem
 ssl-ca=$ENV{ALIEN_HOME}/etc/aliend/ldap/certs/host.cert.pem
set-variable    = max_connections=8000
set-variable    = wait_timeout=1800
set-variable    = interactive_timeout=3600
# Try number of CPU's*2 for thread_concurrency
set-variable    = thread_concurrency=2
";
close FILE or exit(-2);
chmod 0644, $file;
my $db;

if (!restartMysql()) {
  print "Error starting mysql!!\n";
  unlink $file;
  restartMysql();
  exit(-2);
}
$db=AliEn::Database->new({DB=>"processes", 
			     HOST=>$config->{CATALOG_HOST},
			     DRIVER=>"mysql",  USE_PROXY=>0,
			     PASSWD=>"pass",
			     ROLE=>"admin"}) or exit(-2);
my $x509=AliEn::X509->new() or exit(-2);
$ENV{X509_USER_CERT}="$ENV{ALIEN_HOME}/etc/aliend/ldap/certs/host.cert.pem";
$ENV{X509_USER_KEY}="$ENV{ALIEN_HOME}/etc/aliend/ldap/certs/host.key.pem";
$x509->load($ENV{X509_USER_CERT}) or exit(-2);
my $subject=$x509->getSubject() or exit(-2);
print "Connected to the database\n $subject\n";
$db->do("GRANT SELECT on processes.* to userssl REQUIRE SUBJECT '$subject'") or exit(-2);

$db->disconnect();
sleep(2);
$ENV{ALIEN_DATABASE_SSL}="userssl";

print "First, let's connect to mysql and see if it has openssl support\n";
my $hostName=Net::Domain::hostname();
my $portNumber="3307";
my $mysqlPasswd="pass";

my $pid = open2(\*MYSQLREAD, \*MYSQLWRITE,  "mysql -h $hostName -P $portNumber -u admin --password='$mysqlPasswd'");
print MYSQLWRITE "SHOW VARIABLES LIKE 'have_openssl';\n";
close MYSQLWRITE;
my @m=<MYSQLREAD>;
close MYSQLREAD;
my ($support)=grep (s/^\s*have_openssl\s+//, @m)
  or print "We can't find the line of support!!\n" and exit(-2);
chomp $support;
#$support=~ /^YES/ or print "The support is not enabled ($support)!!\n" and exit(-2);



print "OK\n";

print "Trying to connect as userssl\n";
my $options={DB=>"processes", HOST=>$config->{CATALOG_HOST},
	     DRIVER=>"mysql",
#	     USE_PROXY=>0, PASSWD=>"pass", ROLE=>"admin" 
	    };
$db=AliEn::Database->new($options) or exit(-2);
$db->disconnect();
print "YUHUUU!\n";


print "Let's try the race condition...\n";


my $clients=9;
my $start=$clients;
my @pids;
my $parent=$$;
while ($start--) {
  my $pid=fork or last();
  push @pids, $pid;
}
$start++;
open (FILE, ">/tmp/alien.client.$start") or print "Error opening the output\n" and exit(-2);
for (my $j=0; $j<10; $j++) {
  my $message="OK";
  $db=AliEn::Database->new($options) or $message="nok";
  print FILE "$$ $message\n";
}
close FILE;
$$ eq "$parent" or exit(-2);
print "Parent waiting for kids\n";
foreach (@pids){
  print "Waiting for $_\n";
  waitpid ($_, 0);
}

eval{
  for (my $j=0; $j<$clients; $j++) {
    my $file="/tmp/alien.client.$j";
    print "Checking file $file\n";
    open (FILE, "<$file") or exit(-2);
    my @lines=<FILE>;
    close FILE;
    grep (/nok/, @lines) and print "NOK!!!\n" and die("The file $file is not ok");
    grep (/OK/, @lines) or print "The file is not there!!\n" and die("The file $file is not there");
  }
};
if ($@) {
  print "It didn't work :(\n";
  unlink $file;
  restartMysql();
  exit(-2);
}

print "ok!!\n";

sub restartMysql{

  system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-mysqld","stop");
  sleep(3);
  print "Mysql is supposed to be dead...\n";
  system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-mysqld","status");# and return 0;
  print "Let's kill all of them just in case\n";
  system ("ps -Ao command -w -w |grep mysqld");
  system("kill -9 `ps -Ao pid,command -w -w|grep mysqld |awk {'print \$1'}`");
  sleep(3);
  print "After killing it\n";
  system ("ps -Ao command -w -w |grep mysqld");
  system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-mysqld","start");
  sleep (2);
  system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-mysqld","status");# and return 0;
  print "Mysql is supposed to be running...\n";
  system ("ps -Ao command -w -w |grep mysqld");

  return 1;
}
