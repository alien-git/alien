use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
  $ENV{"GCLIENT_NOPROMPT"}="1";

  my $input = "/tmp/.603-aliensh-basics.t.". rand() .rand() .rand();


  # working commands
  my @cmd = ("whoami","cd","cd /","pwd","ls","ls -la","ls /","ps","ps -a -Fl");
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

  # commands to fail
  @cmd = ("asdf","cd /illegaldir","submit /illegaljdl");
  foreach (@cmd) {
      system("echo $_ > $input");
      print "===================================================\n";
      print "Testing '$_' ...";
      if (!system("export PATH=/bin:\$PATH;$ENV{ALIEN_ROOT}/api/bin/aliensh file:$input ")) {
 	  unlink $input;
	  exit (-2);
      }
      print "ok\n";
  }
  unlink $input;
  ok(1);
}

