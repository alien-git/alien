use strict;

use Test;

BEGIN { plan tests => 1 }


{
  my $file="/var/log/messages";
  my $pidFile="/tmp/alien.Test.42";

  my $pid=fork;
  defined $pid or print "Error doing the fork\n" and exit(-2);
  if ($pid) {

    sleep (1);
    if (-f  $pidFile) {
      print "Killing the old pid";
      open (FILE, $pidFile);
      my @pids=<FILE>;
      close FILE;
      print "Killing @pids\n";
      my $d=`ps -Ao "pid ppid"|grep "@pids"|awk '{print \$1}'`;
      print "Killing @pids\n";
      @pids=split(/\n/, $d);
      kill (9, @pids);
    }
    open (FILE, ">$pidFile") or print "Error opening $pidFile\n" and exit(-2);
    print FILE $pid;
    close FILE;

    print "The father exists\n";
    exit (0);
  }

  my $response="/tmp/alien.42.$$";

  print "Checking the file $file...\n";
  my $content=`tail -0f $file`;
  my $return=0;
  print "Got $content Writing to $response\n";
  
  my @errors=("DBD::mysql::st execute failed", "No such object: AliEn::Services::ProxyServer:");
  foreach (@errors) {
    if ($content=~ /$_:/s) {
      #we have to skip the line of the error that we force:
      $content=~ /bla bla bla/ or
	print "Error the line  $_ is there!!!\n" and $return=1;
    }
  }


  open (FILE, ">$response") or print "Error opening the file $response\n" and exit(-2);
  print FILE $return;	print FILE $content;
  close FILE;
  print "child done!!\n";
}
