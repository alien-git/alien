use strict;

use AliEn::UI::Catalogue;
use AliEn::Util;

my $userName = getlogin();
$userName
	or print "getlogin() did not return the user name, taking ENV{LOGNAME}...\n"
	and $userName = $ENV{LOGNAME};
	
my $allThreadsFlag = ((AliEn::Util::isMac()) ? "-M" : "-m"); # excellent design of MAC, eh?

print "Running as user $userName, allThreadsFlag is $allThreadsFlag\n";

my $before=`ps -U $userName $allThreadsFlag -o command |grep mysql |wc -l`;
my $proxyBefore=`ps -U $userName -o command  |grep -i Proxy| grep -v grep |wc -l`;
chomp ($before);
chomp ($proxyBefore);
$before=$before+0;
$proxyBefore=$proxyBefore+0;

print "Before connecting, we have $before ($proxyBefore) instances\n";
system("ps -U $userName -o command |grep -i Proxy|grep -v grep");

my $c=AliEn::UI::Catalogue->new() or exit(-2);

#
# This test is not reliable - some other services can create new connection
# while we are trying to connect - disabling it temorary
#
my $during=`ps -U $userName -o command |grep mysql |wc -l`;
my $proxyDuring=`ps -U $userName -o command  |grep -i Proxy| grep -v grep |wc -l`;

chomp $during;
chomp $proxyDuring;
$during=$during-1;
$proxyDuring=$proxyDuring-1;

print "During the connection we have $during ($proxyDuring) instances\n";
#($during eq $before) or 
#  print "There are too many connections at the moment ($during, and at the beginning there were only $before\n" and exit(-2);

($proxyDuring eq $proxyBefore) or
  print "There are too many proxy connections at the moment ($proxyDuring, and at the beginning there were only $proxyBefore)\n" and exit(-2);
system("ps -U $userName -o command |grep -i Proxy|grep -v grep");
$c->close();
print "closed!!!\n";
sleep (3);
system("ps -U $userName -o command |grep -i Proxy|grep -v grep");
my $after=`ps -U $userName $allThreadsFlag -o command |grep mysql |wc -l`;
chomp $after;
my $proxyAfter=`ps -U $userName -o command |grep -i Proxy| grep -v grep |wc -l`;
chomp $proxyAfter;

print "After login out we have $after ($proxyAfter)\n";

$after=$after+0;
$proxyAfter=$proxyAfter+0;

if ( int($before) < int($after) ) { 
  print "Error: we used to have $before and now we have $after\n" and exit(-2);
}

if ( int($proxyBefore) < int($proxyAfter) ) {
  print "Error: we used to have proxy $proxyBefore and now we have $proxyAfter\n";
  sleep (10);
  system("ps -U $userName -o command |grep -i Proxy|grep -v grep");

  $proxyAfter=`ps -U $userName -o command |grep -i Proxy| grep -v grep |wc -l`;
  chomp $proxyAfter;
  $proxyAfter=$proxyAfter+0;
  
  ($proxyBefore eq $proxyAfter) or
    print "Error: we used to have $proxyBefore and now we have $proxyAfter\n" and exit(-2);

  print "But now it is fine...\n";
}


exit(0);
