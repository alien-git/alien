use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
  $ENV{"GCLIENT_NOPROMPT"}="1";

  my $input = "/tmp/.620-aliensh-cp-l2se.t.". rand() .rand() .rand();


  # working commands
  my @cmd = ("cd", "rm -f passwd", "cp file:/etc/passwd passwd");
  foreach (@cmd) {
      system("echo $_ > $input");
      print "===================================================\n";
      print "Testing '$_' ...";
      if (system("export PATH=/bin:\$PATH;$ENV{ALIEN_ROOT}/api/bin/aliensh file:$input 2> /dev/null")) {
	  unlink $input;
 	  exit (-2);
      }
      print "ok\n";
  }
  unlink $input;
  ok(1);
}

