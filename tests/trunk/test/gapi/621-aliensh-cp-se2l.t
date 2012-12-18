use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
  $ENV{"GCLIENT_NOPROMPT"}="1";

  my $input = "/tmp/.621-aliensh-cp-se2l.t.". rand() .rand() .rand();
  my $rndcp = "/tmp/passwd" . rand();

  # working commands
  my @cmd = ("cp passwd file:$rndcp");
  foreach (@cmd) {
      system("echo $_ > $input");
      print "===================================================\n";
      print "Testing '$_' ...";
      if (system("export PATH=/bin:\$PATH;$ENV{ALIEN_ROOT}/api/bin/aliensh file:$input 2> /dev/null")) {
	  unlink $input;
 	  exit (-2);
      }
      my $diff =`diff /etc/passwd $rndcp`;
      if ($diff ne "") {
	  print "Copied File differ's from original!\n";
	  unlink $input;
	  exit(-2);
      }
      print "ok\n";
  }
  unlink $rndcp;
  unlink $input;
  ok(1);
}

