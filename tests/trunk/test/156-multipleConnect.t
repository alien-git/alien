use strict;

use AliEn::UI::Catalogue;
use AliEn::Util;
use Net::Domain;

use IPC::Open2;
my $hostName=Net::Domain::hostfqdn();
print "Hola $hostName\n";
my $userName = $ENV{LOGNAME};

print "Running as user $userName\n";#, allThreadsFlag is $allThreadsFlag\n";

sub countInstances {
  my $when = shift;
  
  my $pid=open2(\*READ, \*WRITE, "mysql -u admin -ppass -h$hostName -P 3307") or print "Error connecting to mysql: $@ \n" and exit(-2);
  
  print WRITE "show processlist;\n";
  
  close WRITE;
  my @list=<READ>;
  close READ;

  my $mysql=$#list;#`ps -U $userName $allThreadsFlag -o pid,ppid,command |grep mysql |wc -l`;
  my $proxy=`ps -U $userName -o pid,ppid,command  |grep -i Proxy| grep -v grep |wc -l`;
  chomp ($mysql);
  chomp ($proxy);
  print "$when,  we have $mysql mysql and $proxy ProxyServer instances\n";
  return (int($mysql), int($proxy));
}

#my $c2=AliEn::UI::Catalogue->new() or exit(-2);
#$c2->close();

print "LETS START\n";
my ($before, $proxyBefore) = countInstances("Before connecting");

#system("ps -U $userName -o pid,ppid,command |grep -i Proxy|grep -v grep");


my $c=AliEn::UI::Catalogue->new() or exit(-2);
$c->execute("ls", "/remote");
#
# This test is not reliable - some other services can create new connection
# while we are trying to connect - disabling it temorary
#

my ($during, $proxyDuring) = countInstances("During the connection");

$during -= 2;
$proxyDuring -= 2;

#($during eq $before) or 
#  print "There are too many connections at the moment ($during, and at the beginning there were only $before\n" and exit(-2);

($proxyDuring eq $proxyBefore) or
  print "There are too many proxy connections at the moment ($proxyDuring, and at the beginning there were only $proxyBefore)\n" and exit(-2);

#system("ps -U $userName -o pid,ppid,command |grep -i Proxy|grep -v grep");

$c->close();
print "closed!!!\n";
sleep (3);

my ($after, $proxyAfter) = countInstances("After login out");

if ( int($before) < int($after) ) { 
  print "Error: we used to have $before and now we have $after\n" and exit(-2);
}

if ( int($proxyBefore) < int($proxyAfter) ) {
  print "Error: we used to have proxy $proxyBefore and now we have $proxyAfter, trying again in 10 sec.\n";
  sleep (10);
  ($after, $proxyAfter) = countInstances("After 10 sec");
  
  ($proxyBefore eq $proxyAfter) or
    print "Error: we used to have $proxyBefore and now we still have $proxyAfter\n" and exit(-2);

  print "But now it is fine...\n";
}
print "Ok!\n";
exit(0);
