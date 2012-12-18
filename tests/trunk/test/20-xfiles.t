#!/bin/env alien-perl

use strict;
use Test;



BEGIN { plan tests => 1 }



{
print "Cheking that the module is there...\t\t";
eval "require AliEn::GUI::Xfiles::RightClickWindow" 
  or print "Error requiring the package\n $! $@\n" and exit(-2);

print "ok\nChecking that the script xfiles is there...\t";

(-e "$ENV{ALIEN_ROOT}/scripts/GUI/xfiles.pl" )
  or print "Error: the file $ENV{ALIEN_ROOT}/scripts/GUI/xfiles.pl is not there!!\n"
  and exit(-2);
print "ok\n";
my $done=fork();

(defined $done) or print "Error forking!!\n" and exit(-2);

if (!$done) {
  print "The child is going to start the xfiles\n"; 
  open (FILE, " $ENV{ALIEN_ROOT}/bin/alien xfiles -r admin|") or 
    print "Error doing alien xfiles\n" and exit(-2);
  my @out=<FILE>;
  close FILE;
  exit;
}
sleep(1);
print "The father goes to sleep...";
sleep (10);
my $d=`ps -Ao "pid ppid"|grep -v $$|grep "$done"|awk '{print \$1}'`;
my @pids=split(/\n/, $d);

print "ok\nChecking if the child (@pids) is still there...";
my $alive=kill(9, @pids);

$alive or print "Error! The child died!!!\n" and exit(-2);
ok(1);
}
