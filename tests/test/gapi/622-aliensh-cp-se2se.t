use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
  $ENV{"GCLIENT_NOPROMPT"}="1";

  my $input = "/tmp/.622-aliensh-cp-se2se.t.". rand() .rand() .rand();
  my $rnd1 = "/tmp/passwd1".rand();
  my $rnd2 = "/tmp/passwd2".rand();

  # working commands
  my @cmd = ("cd","rm -f passwd2", "cp passwd passwd2","cp passwd file:$rnd1", "cp passwd file:$rnd2");
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

  my $diff = `diff $rnd1 $rnd2`;
  if ($diff ne "") {
      print "Copied Files differ $rnd1 != $rnd2\n";
      unlink $input;
      exit(-2);
  }
  unlink $input;
  unlink $rnd1;
  unlink $rnd2;
  ok(1);
}

