use strict;
use Net::Domain qw(hostname hostfqdn hostdomain);

my $org=Net::Domain::hostname();
my $suffix=Net::Domain::hostdomain();
my $domain=$suffix;
$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
$ENV{ALIEN_LDAP_DN}="$org.$domain:8389/o=$org,$suffix";
$ENV{ALIEN_ORGANISATION}=$org;
#First, let's make sure that the CE is not running
system("$ENV{ALIEN_ROOT}/bin/alien StopCE >/dev/null");

my $before=system("$ENV{ALIEN_ROOT}/bin/alien StatusCE >/dev/null");

system("$ENV{ALIEN_ROOT}/bin/alien StartCE -blabla") and
  print "Error starting a service\n";# and exit(-1);

#let's give it a couple of seconds to crash...
sleep (5);
system("ps -Ao command |grep CE");
sleep (6);
system("ps -Ao command |grep CE");
my $after=system("$ENV{ALIEN_ROOT}/bin/alien StatusCE >/dev/null");

print "Before we had $before. Now $after\n";
system("$ENV{ALIEN_ROOT}/bin/alien StopCE");
$after or print "Error the service is not supposed to be running!!!\n" and exit(-2);
require AliEn::Config;
my $c=AliEn::Config->new();
$c or exit(-2);
open (FILE, "<$c->{LOG_DIR}/CE..log") or print "The file $c->{LOG_DIR}/CE..log doesn't exist!!\n" and exit(-1);


my @data=<FILE>;
close FILE or print "Error reading from the file $! and $@" and exit(-2);
print "Got @data\n";
$#data>-1 or print "There is nothing in the log file\n" and exit(-2);
