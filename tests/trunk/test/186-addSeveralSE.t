#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }


{
my $before=getNumberOfProcesses();
my $host=Net::Domain::hostname();


print "Before we start, there are $before processes\n";
my $cat=AliEn::UI::Catalogue::LCM->new({"role", "admin"});
$cat or exit (-1);

$cat->execute("pwd") or exit (-2);
$cat->execute("mkdir", "-p", "/bin") or exit (-2);
$cat->execute("mkdir", "-p", "/$host/bin") or exit (-2);

my @testTable = ();
my $res;

$res = addFile($cat, "/bin/multipleSEaddSelect1of3","#!/bin/sh
echo This is a test
date
", "${host}::CERN::TESTSE,${host}::CERN::TESTSE2,${host}::CERN::TESTSE3,select=1","1");
push @testTable, $res;
  
$res = addFile($cat, "/bin/multipleSEaddSelect2of3","#!/bin/sh
echo This is a test
date
", "${host}::CERN::TESTSE,${host}::CERN::TESTSE2,${host}::CERN::TESTSE3,select=2","2");
push @testTable, $res;
  
$res = addFile($cat, "/bin/multipleSEaddSelect3of3","#!/bin/sh
echo This is a test
date
", "${host}::CERN::TESTSE,${host}::CERN::TESTSE2,${host}::CERN::TESTSE3,select=3","3");
push @testTable, $res;
  
  
$res = addFile($cat, "/bin/multipleSEaddTestDisk1","#!/bin/sh
echo This is a test
date
","disk=1","1");
push @testTable, $res;
  
$res = addFile($cat, "/bin/multipleSEaddTestDisk2","#!/bin/sh
echo This is a test
date
","disk=2","2");
push @testTable, $res;
  

$res = addFile($cat, "/bin/multipleSEaddTape1","#!/bin/sh
echo This is a test
date
","tape=1","1");
push @testTable, $res;
  
$cat->close;

my $after=getNumberOfProcesses();
if ($before ne $after) {
  print "Error: There is one process left alive!!";
  exit(-2);
}

my $result = 1;

print "\nAll tests finished.\n\n";
for my $j(0..$#testTable) {
   print "Test No $j is ";
   $testTable[$j] and print "OK\n" and next;
   print "False/Error\n";
   $result and $testTable[$j] or $result=-1;
}

$result eq 1 or print "\nTest ended overall with Error\n" and exit (-2);
ok(1);
}

sub getNumberOfProcesses{
  my $d=`ps -Ao command | grep Prompt | grep AliEn | grep -v grep`;
  print "Active Processes:\n $d";


  my @s=$d=~ /\n/g;
#    print "En $d tengo $#s";
  return $#s;
}

#SUBROUTINE TO ADD A FILE
sub addFile {
  my $cat=shift;
  my $file=shift;
  my $content=shift;
  my $options=(shift or "");
  my $check=(shift or 0);
  print "Registering the file $file...";
  $cat->execute("rm", "-silent", $file);

  my $name="/tmp/test186.$$";
  open (FILE, ">$name") 
    or print "Error opening the file $name\n" and return;
  print FILE $content;
  close FILE;

  my $done=$cat->execute("add", "$file", $name, $options);

  my(@outputarch)=$cat->execute("whereis","$file") or exit(-2);
  my @ses = ();
  for my $entry (@outputarch) {
         if( ($entry =~ /::/)) {
             push @ses, $entry;
         }
  }     
  print "file has ".scalar(@ses)." copies\n";
  scalar(@ses)  eq $check or print "Error, file $file has not as much copies as specified.\n" and return; 

  system("rm", "-f", "$name");
  $done or return;
  print "ok\n";
  return 1;
}
#END OF REGISTERFILE
