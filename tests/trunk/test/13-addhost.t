#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue;
use Net::Domain qw(hostname hostfqdn hostdomain);
use DBI;

BEGIN { plan tests => 2 }



{
print "Connecting to mysql...";

my $hostName=Net::Domain::hostname();
my $portNumber="3307";
my $mysqlPasswd="pass";

open SAVEOUT,  ">&STDOUT";
my $file="/tmp/$$";
open STDOUT, ">$file" or print "Error opening $file\n" and exit (-1);

open (MYSQL, "| mysql -h $hostName -P $portNumber -u admin --password='$mysqlPasswd'") 
    or print "Error connecting to mysql in  $hostName -P $portNumber\n" and exit(-2);

print MYSQL "show databases;\n";

close STDOUT;
open STDOUT, ">&SAVEOUT";


close MYSQL or print "Error closing connection to mysql in  $hostName -P $portNumber\n" and exit(-2);


my $exists=`grep alien_cat2 $file`;
system("rm", "-f", "$file");
open (MYSQL, "| mysql -h $hostName -P $portNumber -u admin --password='$mysqlPasswd'")
    or print "Error connecting to mysql in  $hostName -P $portNumber\n" and exit(-2);

my $cat=AliEn::UI::Catalogue->new({"role", "admin",});
$cat or exit (-1);
$cat->execute("rmdir","-silent", "/remote", "-r");
$cat->execute("cd", "/");
if ($exists){
  print "ok\nDropping the database...";
  print MYSQL "drop database alien_cat2;\n";
  print MYSQL "delete from alien_system.HOSTS where db='alien_cat2';\n";
}
print "ok\nCreating the database...";
print MYSQL "create database alien_cat2;\n";

close MYSQL or print "Error closing connection to mysql in  $hostName -P $portNumber\n" and exit(-2);


print "ok\nDoing the alien command...";


$cat->execute("addHost", "$hostName:3307", "mysql", "alien_cat2") or exit (-2);

print "ok\nDoing mkremdir...";

$cat->execute("mkremdir","$hostName:3307", "mysql", "alien_cat2","/remote")
  or print "Error making the remote directory\n" and exit(-2);

my $host=$cat->execute("host");
print "Got $host\n";

$cat->execute("cd", "/remote") or exit(-2);
$host=$cat->execute("host");
print "Got $host\n";
$cat->execute("ls", "-a","/remote") or exit(-2);

$cat->execute("cd") or exit(-2);

print "Let's try to create a directory in that host\n";

$cat->execute("mkdir", "-p", "/remote/mytest") or exit(-2);
$cat->execute("rmdir", "-rf", "/remote/mytest") or exit(-2);

$cat->close;

#print "OK\n";

#print 
ok(1);

ok(2);

}
