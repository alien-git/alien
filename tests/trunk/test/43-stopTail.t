use strict;

use Test;

BEGIN { plan tests => 1 }


{
  my $pidFile="/tmp/alien.Test.42";


  open (FILE, $pidFile) or 
    print "Error opening the file $pidFile\n" and exit(-2);
  my @all=<FILE>;
  close FILE;
  my $pid=join("",@all);
  my $response="/tmp/alien.42.$pid";

  system ("ps -Ao command |grep $pid");
  
  my $d=`ps -Ao "pid ppid"|grep "$pid"|awk '{print \$1}'`;
  my @pids=split(/\n/, $d);
  @pids = grep (! /^$pid$/, @pids);
  print "ok\nKill the child @pids\n";
  kill (9, @pids) or print "Error killing the process\n" and exit(-2);
  sleep 5;
  print "Reading the response from $response";
  open (FILE, "<$response") or print "Error opening the file $response\n" and exit(-2);
  my @file=<FILE>;
  close FILE;
  $response=join ("", @file);
  print "ok\nGot $response\n";
  exit ($response);

}
