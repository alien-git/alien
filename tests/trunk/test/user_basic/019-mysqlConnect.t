use strict;

use AliEn::UI::Catalogue;
use AliEn::Util;
use Net::Domain;

use IPC::Open2;

eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
setDirectDatabaseConnection();


my $hostName=Net::Domain::hostfqdn();
print "Hola $hostName\n";
my $userName = $ENV{LOGNAME};
#my $allThreadsFlag = ((AliEn::Util::isMac()) ? "-M" : "-m"); # excellent design of MAC, eh?
print "Running as user $userName\n";#, allThreadsFlag is $allThreadsFlag\n";

sub countInstances {
  my $when = shift;
  
  my $pid=open2(\*READ, \*WRITE, "mysql -u admin -ppass -h$hostName -P 3307") or print "Error connecting to mysql: $@ \n" and exit(-2);
  
  print WRITE "show processlist;\n";
  
  close WRITE;
  my @list=<READ>;
  close READ;

  my $mysql=$#list;#`ps -U $userName $allThreadsFlag -o pid,ppid,command |grep mysql |wc -l`;
  
  print "Doing the Proxy count for user \"$userName\"\n";
  print "The Proxy running processes are:\n";  
  my $commandResult = `ps -U $userName |grep -i Proxy| grep -v grep`;
  print "$commandResult\n";
  
  my $proxy=`ps -U $userName -o pid,ppid,command  |grep -i Proxy| grep -v grep |wc -l`;
  chomp ($mysql);
  chomp ($proxy);
  print "$when,  we have $mysql mysql and $proxy ProxyServer instances\n";
  return (int($mysql), int($proxy));
}
sub compareNumber {
  my $mysql=shift;
  my $proxy=shift;
  my $message=shift;

  my ($during, $proxyDuring) = countInstances($message);

  ($during eq $mysql) or
    print "There are too many connections at the moment ($during, and at the beginning there were only $mysql\n" and return;
  ($proxyDuring eq $proxy) or
    print "There are too many proxy connections at the moment ($proxyDuring, and at the beginning there were only $proxy)\n" and return;

  return 1;
}

sub stopServices{
  print "Stopping all the services but the proxy";
  system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/aliend", "stop");
  sleep(10);
#  open (FILE, "| $ENV{ALIEN_ROOT}/bin/alien StartProxy");
#  print FILE "pass\n";
#  close FILE;

  return 1;
}

sub startServices{
  print "Restarting the services\n";
  system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/aliend", "start");
  sleep(10);
  return 1;
}

print "HELLO\n";
stopServices();
 countInstances("Before connecting");
print "Let's sleep a little bit to give time to close everything\n";
sleep (30);
 countInstances("Before connecting");

my $ok=0;
for (my $i=0; $i<1; $i++){
  my ($before, $proxyBefore) = countInstances("Before connecting");

  #system("ps -U $userName -o pid,ppid,command |grep -i Proxy|grep -v grep");

  my $c=AliEn::UI::Catalogue->new() or last;

  compareNumber($before+1, $proxyBefore, "During the connection") or last;

  $c->close();
  print "closed!!!\n";
  sleep (3);
  if (!compareNumber($before, $proxyBefore, "After login out")) {
    print "Let's try sleeping again...\n";
    sleep (10);
    compareNumber($before, $proxyBefore, "After 10 sec")  or last;
    print "But now it is fine...\n";
  }

  print "Ok!\n";

  print "Let's try again with another catalogue\n";

  $c=AliEn::UI::Catalogue->new() or last;
  $c->execute("ls", "/", "-la") or last;
  compareNumber($before+1, $proxyBefore, "During the second connection") or last;
  $c->close();
  print "closed!!!\n";
  sleep (3);
  if (!compareNumber($before, $proxyBefore, "After logout")) {
    print "Let's try sleeping again...\n";
    sleep (10);
    compareNumber($before, $proxyBefore, "After 10 sec")  or last;
    print "But now it is fine...\n";
  }
  $ok=1;
  last;
}
startServices();
$ok or exit(-2);
print "OK!!!\n";
exit(1);
