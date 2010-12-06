use strict;
use Test;
use Net::Domain;
use gapi;

BEGIN { plan tests => 1;}

{
  $ENV{"GCLIENT_NOPROMPT"}="1";

  my $input = "/tmp/.631-aliensh-getzipoutput.t.". rand() .rand() .rand();
  my $stdoutfile = "/tmp/.631-aliensh-getzipoutput-bin".rand().rand().rand();
  my $stdoutfile2 = "/tmp/.631-aliensh-getzipoutput-bin".rand().rand().rand();
  my $stdoutfile3 = "/tmp/.631-aliensh-getzipoutput-bin".rand().rand().rand();
  
  # working commands
  my @cmd = ("dir=\"\\\`ls /proc/$ENV{'USER'}/ \| tail -1\\\`; cp /proc/$ENV{'USER'}/\\\$dir/stdout file:$stdoutfile\"");
  foreach (@cmd) {
      system("echo $_ > $input");
      print $input,"\n";

      print "===================================================\n";
      print "Testing '$_' ...";
      if (system("export PATH=/bin:\$PATH;$ENV{ALIEN_ROOT}/api/bin/aliensh file:$input ")) {
	  unlink $input;
	  unlink $stdoutfile;
	  unlink $stdoutfile2;
	  unlink $stdoutfile3;
 	  exit (-2);
      }
      print "ok\n";
  }
  system("cat $stdoutfile | grep magic > $stdoutfile2");
  system("echo magic test > $stdoutfile3");
  my $diff=`diff $stdoutfile3 $stdoutfile2`;

  if ($diff ne "" ) {
      print "Diff |$diff|\n";
      print "Joboutput seems not to be right\n";
      system("cat $stdoutfile3; cat $stdoutfile2");
      exit(-2);
  }

  unlink $input;
  unlink $stdoutfile;
  unlink $stdoutfile2;
  unlink $stdoutfile3;

  ok(1);
}

