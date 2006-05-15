use strict;
use Test;
use Net::Domain;
use gapi;
use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1;}

{
  $ENV{"GCLIENT_NOPROMPT"}="1";

  my $input = "/tmp/.630-aliensh-submit.t.". rand() .rand() .rand();
  my $binfile = "/tmp/.630-aliensh-submit-bin".rand().rand().rand();
  my $jdlfile = "/tmp/.630-aliensh-submit-bin".rand().rand().rand();
  
  open BINOUT , ">$binfile";
  print BINOUT<<EOF
#!/bin/bash
echo "magic test"
EOF
;
  close BINOUT;

  open JDLOUT , ">$jdlfile";
  print JDLOUT<<EOF
Executable="testjob";
EOF
;
  close JDLOUT;

  # working commands
  my @cmd = ("cd","mkdir -p bin","mkdir -p jdl", "rm -f bin/testjob", "rm -f jdl/testjob.jdl","cp file:$binfile bin/testjob","cp file:$jdlfile jdl/testjob.jdl","submit jdl/testjob.jdl","ps \\\| grep testjob");
  foreach (@cmd) {
      system("echo $_ > $input");
      print "===================================================\n";
      print "Testing '$_' ...";
      if (system("export PATH=/bin:\$PATH;$ENV{ALIEN_ROOT}/api/bin/aliensh file:$input ")) {
	  unlink $input;
	  unlink $binfile;
	  unlink $jdlfile;
 	  exit (-2);
      }
      print "ok\n";
  }
  unlink $input;
  unlink $binfile;
  unlink $jdlfile;

  my $host=Net::Domain::hostname();
	
  # open the queue
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"$ENV{'USER'}"});  
  if (! $cat) {
    print STDERR "Couldn't authenticate as admin\n";
    exit(-2);
  }	
  $cat->execute("user","admin");
  $cat->execute("queue", "open $cat->{CONFIG}->{ORG_NAME}::CERN::testCE") or exit(-2);

  # start the cluster monitor 
  system("$ENV{'ALIEN_ROOT'}/bin/alien StartBroker");
  system("$ENV{'ALIEN_ROOT'}/bin/alien StartJobOptimizer");
  system("$ENV{'ALIEN_ROOT'}/bin/alien StartMonitor");
  system("$ENV{'ALIEN_ROOT'}/bin/alien StartSE");
  sleep(30);
  system("$ENV{'ALIEN_ROOT'}/bin/alien login -exec request");
  ok(1);
}

