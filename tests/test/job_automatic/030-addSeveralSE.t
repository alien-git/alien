#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("catalogue/003-add") or exit(-2);

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
my $name="/bin/multipleSEadd";
my $content="#!/bin/sh
echo This is a test
date
";

addFile($cat, "${name}Select1of3",$content,'r', 
 {options=>"${host}::CERN::TESTSE,${host}::CERN::TESTSE2,${host}::CERN::TESTSE3,select=1",
   check=>"1"}) or exit(-2);

  
addFile($cat, "${name}Select2of3",$content, 'r', 
{options=>"${host}::CERN::TESTSE,${host}::CERN::TESTSE2,${host}::CERN::TESTSE3,select=2",
  check=>"2"}) or exit(-2);

  
addFile($cat, "${name}Select3of3",$content, 'r', 
{options=>"${host}::CERN::TESTSE,${host}::CERN::TESTSE2,${host}::CERN::TESTSE3,select=3",
  check=>"3"}) or exit(-2);
 
  
addFile($cat, "${name}TestDisk1",$content, 'r', 
{options=>"disk=1",check=>"1"})
  or exit(-2);

  
 addFile($cat, "${name}TestDisk2",$content,'r', 
{options=>"disk=2",check=>"2"})
 or exit(-2);

  

addFile($cat, "${name}TestTape1", $content,'r', 
{options=>"tape=1",check=>"1"})
or exit(-2);

  
$cat->close;

my $after=getNumberOfProcesses();
if ($before ne $after) {
  print "Error: There is one process left alive!!";
  exit(-2);
}

my $result = 1;

print "\nAll tests finished.\n\n";
ok(1);
}

sub getNumberOfProcesses{
  my $d=`ps -Ao command | grep Prompt | grep AliEn | grep -v grep`;
  print "Active Processes:\n $d";


  my @s=$d=~ /\n/g;
#    print "En $d tengo $#s";
  return $#s;
}
