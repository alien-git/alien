#!/bin/env alien-perl

use strict;
use Test;


BEGIN { plan tests => 1 }



{
  eval {
    require AliEn::Logger;
  };
  if ($@) {
    print "Error requiring the module AliEn::Logger\n$@\n";
    exit (-2);
  }

  print "Getting a logger...";
  my $log=new AliEn::Logger;

  $log or print "Error getting a new logger\n" and exit(-2); 
  print "ok\n";
  my $file="/tmp/test29.$$";

  my $i=0;
  while ($i<20) {
    open my $SAVEOUT,  ">&", STDOUT;
    open my $SAVEOUT2, ">&", STDERR;

    if ( !open STDOUT, ">$file" ) {
      open STDOUT, ">&", $SAVEOUT;
      print "Error opening the file $file\n$!\n";
      exit(-2);
    }
    if ( !open( STDERR, ">&STDOUT" ) ) {
      open STDOUT, ">&", $SAVEOUT;
      open STDERR, ">&", $SAVEOUT2;
      print STDERR "Could not open stderr file\n";
      die;
    }
    my $Stime=time;
  $log->info("Test"," This is a test message") or print "Error printing a message\n" and exit(-2);
    $log->error("Test"," This is an error message") or print "Error printing a message\n" and exit(-2);
  my $Etime=time;
    my $diff=$Etime-$Stime;
    close STDOUT;
    close STDERR;

    open STDERR, ">&", $SAVEOUT2;
    open STDOUT, ">&", $SAVEOUT;
    print "It took  $diff seconds to do the call\n";
    if ($diff>60)  {
      print "The call took more than one minute!!\n";
      exit (-2);
    }
    my $FILE;
    if ( !open $FILE, "<", "$file" ) {
      print "Error reading $file\n$!\n";
      exit (-2);
    } 
    my @content=<$FILE>;
    close $FILE;
#    print "GOT @content\n";

    if (grep (/^Error contacting Logger/, @content) ) {
      print "There was an error contacting the logger!!\n";
      exit (-2);
    }
    $i++;
  }
  unlink $file;
  ok(1);
}
