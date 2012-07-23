#!/bin/env alien-perl

use strict;
use Test;

use AliEn::Database::TaskQueue;
#use Net::Domain qw(hostname hostfqdn hostdomain);
use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  
#  my $host=Net::Domain::hostfqdn();
#  my $port=$ENV{ALIEN_MYSQL_PORT} ||3307;
  my $d=AliEn::Database::TaskQueue->new({"ROLE", "admin", PASSWD=>"pass"}) 
    or print "Error connecting to the database\n" and exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit(-1);

  print "Adding jdls to catalogue with different MaxWaitingTime and submitting";
  my @idsjobs=();
  my @times=(5,5,5,300,300,18000,18000);
  my @times_text=("5", "5s", "5S", "5m", "5M", "5h", "5H");
  
  for my $time (@times_text){
    addFile($cat, "jdl/maxwaitingtime$time.jdl", "Executable=\"date\"; MaxWaitingTime=\"$time\";\n") or exit(-2);
    my ($id) = $cat->execute("submit", "jdl/maxwaitingtime$time.jdl");
    $id or exit(-2);
    push(@idsjobs, $id);
  }
	  
	  
  addFile($cat, "jdl/maxwaitingtimeQ.jdl", "Executable=\"date\"; MaxWaitingTime=\"4Q\";\n") or exit(-2);
  my ($idQ) = $cat->execute("submit", "jdl/maxwaitingtimeQ.jdl");
  !$idQ or print "Job with MaxWaitingTime=5Q shouldn't be submitted!\n" and exit(-2);
  
  addFile($cat, "jdl/maxwaitingtimeMax.jdl", "Executable=\"date\"; MaxWaitingTime=\"2000000\";\n") or exit(-2);
  my ($idMax) = $cat->execute("submit", "jdl/maxwaitingtimeMax.jdl");
  $idMax or exit(-2);
	  
  
  print "Jobs submitted, now checking expires value";
  
  for(my $i = 0; $i < @idsjobs; $i++) {
    my $mwt=$d->queryValue("SELECT expires FROM QUEUE where queueId=$idsjobs[$i]");
	  if ( !$mwt || $mwt!=$times[$i] ){
	  	print "Error defining expires ($times_text[$i])\n" and exit(-2);
	  }
  }
  
  my $mwt2=$d->queryValue("SELECT expires FROM QUEUE where queueId=$idMax");
  !$mwt2 or print "Job with MaxWaitingTime=2000000 shouldn't have MaxWaitingTime\n" and exit(-2);
  
  
  print "All MaxWaitingTime fields correctly parsed";

  waitForStatus($cat, $idsjobs[0], "ERROR_EW", 10,7) or exit(-2);
  $cat->close();
  $d->close();

  #sleep(60);
  print "OK!!
\#ALIEN_OUTPUT $idsjobs[0]\n";
}
