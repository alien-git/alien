use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
  $ENV{"GCLIENT_NOPROMPT"}="1";
  $ENV{"alien_NOCOLOUR_TERMINAL"}="1";

  my $input = "/tmp/.631-aliensh-trace.t.". rand() .rand() .rand();
  
  # working commands
  my @cmd = ("job=\\\`ps | tail -1 | awk '{ print \\\$2}'\\\`; ps -trace \\\$job | grep DONE;");
  foreach (@cmd) {
      system("echo \"$_ \"> $input");
      print $input,"\n";

      print "===================================================\n";
      print "Testing '$_' ...";
      if (system("export PATH=/bin:\$PATH;$ENV{ALIEN_ROOT}/api/bin/aliensh file:$input ")) {
	  unlink $input;
 	  exit (-2);
      }
      print "ok\n";
  }

  unlink $input;

  ok(1);
}

