#!/bin/env alien-perl

use strict;
use Test;
use Expect;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }



{
my $org=Net::Domain::hostname();

my $directory="/home/mysql";
my $prepend="";
my $user="";
if ($<) {
  print "Running as a user\n";
  $directory="$ENV{ALIEN_HOME}/mysql";
  $prepend="$ENV{ALIEN_HOME}";
  $user ="Y\n";
}


my $exists="";
(-e "$directory/$org") and $exists.="Y\n";

my $uname=`uname -m`;
chomp $uname;
if ($uname eq "ia64") {
#  print "*****************WE ARE IN ITANIUM *****************************\n";
#  print "REMOVING BY HAND libgcc_s.so\n";
#  system("rm -rf $ENV{ALIEN_ROOT}/lib/libgcc_s.*");
#  system("ln -s $ENV{ALIEN_ROOT}/lib/mysql/* $ENV{ALIEN_ROOT}/lib");
   $ENV{LD_PRELOAD}="/lib/libgcc_s.so.1";
   $ENV{LD_LIBRARY_PATH}="$ENV{LD_LIBRARY_PATH}:$ENV{ALIEN_ROOT}/lib/mysql";
}

system("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-mysqld stop");
system("pkill mysql 2>&1");
#system ("kill `ps -Ao command |grep mysql |awk '{print \$2}'` ");
system("rm", "$prepend/etc/aliend/mysqld.conf");
#to give some time to mysql
sleep 3;


open (FILE, "|-","$ENV{ALIEN_ROOT}/bin/alien -x $ENV{ALIEN_ROOT}/scripts/CreateOrgDB.pl");

my $exp = Expect->exp_init(\*FILE);
$exp->raw_pty(1); 
print $exp "$user$org
${exists}pass
pass




";

if (!close FILE){
  print "ERROR!!\n";
  ok(0);
  exit(-2);
}

$exp->soft_close();

print "Checking that the databases are there...";

my $hostName=Net::Domain::hostfqdn();
my $portNumber=$ENV{ALIEN_MYSQL_PORT} || "3307";
my $mysqlPasswd="pass";

my $mysql="mysql";
open my $SAVEOUT,  ">&", STDOUT;
my $file="/tmp/$$";
open STDOUT, ">", "$file" or print "Error opening $file\n" and exit (-1);

open (my $MYSQL, "|-", "$mysql -h $hostName -P $portNumber -u admin --password='$mysqlPasswd'") 
    or print "Error connecting to mysql in  $hostName -P $portNumber\n" and exit(-2);

print $MYSQL "show databases;\n";

close STDOUT;
open STDOUT, ">&", $SAVEOUT;


close $MYSQL or print "Error closing connection to mysql in  $hostName -P $portNumber\n" and exit(-2);


$exists=`grep alien_system $file`;
system("rm", "-f", "$file");

$exists or print "Error: there are no databases!!!\n" and exit(-3);

ok(1);
}
