use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
  $ENV{"GCLIENT_NOPROMPT"}="1";

  my $input = "/tmp/.624-aliensh-rm.t.". rand() .rand() .rand();


  # working commands
  my @cmd = ("rm -f passwd","rm -f passwd2");
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

  # not working commands
  @cmd = ("rm passwd","rm passwd2");
  foreach (@cmd) {
      system("echo $_ > $input");
      print "===================================================\n";
      print "Testing '$_' ...";
      if (!system("export PATH=/bin:\$PATH;$ENV{ALIEN_ROOT}/api/bin/aliensh file:$input 2> /dev/null")) {
	  unlink $input;
 	  exit (-2);
      }
      print "ok\n";
  }


  unlink $input;
  ok(1);
}

