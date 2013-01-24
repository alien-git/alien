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
addFile($cat, "/bin/date","#!/bin/sh
echo This is a test
date
", "r") or exit(-2);
  
addFile($cat, "/bin/date.slow","#!/bin/sh
echo This is a test
date
echo Sleeping for one minute
sleep 60
date
echo Done
") or exit(-2) ;

$cat->close;

my $after=getNumberOfProcesses();
if ($before ne $after) {
  print "Error: There is one process left alive!!";
  exit(-2);
}

print "yes\n";

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
  my $extra= (shift or {});
  print "Registering the file $file...";
  $options=~ /r/ and  $cat->execute("rm", "-silent", $file);

  $cat->execute("whereis", "-i", "-silent", $file) and print "ok\nThe file  $file already exists\n" 
		and return 1;

  my $name="/tmp/test16.$$";
  open (my $FILE, ">", "$name") 
    or print "Error opening the file $name\n" and return;
  print $FILE $content;
  close $FILE;

  my $arguments=$extra->{options} || "";
  my $done=$cat->execute("add", "$file", $name, $arguments);
  
  if ($extra->{check}){
    my(@outputarch)=$cat->execute("whereis","$file") or return;
    my @ses = grep (/::/, @outputarch);
    print "file has ".scalar(@ses)." copies\n";
    scalar(@ses)  eq $extra->{check} or 
      print "Error, file $file has not as much copies as specified (".scalar(@ses)." instead of $extra->{check}).\n" 
       and return;    
  }
  
  system("rm", "-f", "$name");
  $done or return;
  print "ok\n";
  return 1;
}
#END OF REGISTERFILE
