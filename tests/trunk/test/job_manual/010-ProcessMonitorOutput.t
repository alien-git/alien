#!/bin/env alien-perl

use strict;
use Test;
use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  $ENV{ALIEN_JOBAGENT_RETRY}=1;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
  $cat or exit (-1);

  $cat->execute("pwd") or exit (-2);

  $cat->execute("cd") or exit (-2);
  addFile($cat, "jdl/date.slow.jdl","Executable=\"date.slow\";\n") or exit(-2);


  killAllWaitingJobs($cat);
  my ($id)=$cat->execute("submit", "jdl/date.slow.jdl") or exit(-2);

  sleep (20);

  my $done=fork();
  defined $done or print "Error doing the fork\n" and exit(-2);

  if (! $done) {
    #The child executes the command
    $cat->execute("request");
    $cat->close();
    exit;
  }

	
  waitForStatus($cat, $id, "RUNNING") or exit(-2);
  my $host=Net::Domain::hostfqdn();

  print "The father asks http://$host:$cat->{CONFIG}->{CLUSTERMONITOR_PORT} for the output of the job $id\n";
  my $stdout="";
  my $request=SOAP::Lite
    -> uri( "AliEn/Service/ClusterMonitor" )
      -> proxy("http://$host:$cat->{CONFIG}->{CLUSTERMONITOR_PORT}",
	       options => {compress_threshold => 10000} )
	-> getStdout($id);

  if ($request) { 
    $stdout=$request->result();
  }
  print "The father got \n_________________________________________\n$stdout\n_________________________________________\n";
  print "The father exists\n";

  $stdout or print "Error getting the output of the job!!\n" and exit(-2);
  $stdout =~ /No output/i and print  "Error getting the output of the job!!\n" and exit(-2);

  print "Ok, let's wait until the job finishes\n";
  waitForStatus($cat, $id, "DONE") or exit(-2);
  system ("alien", "proxy-destroy");

  checkOutput($cat, $id) or exit(-2);

  $cat->close();
  ok(1);
}
sub waitForStatus{
  my $cat=shift;
  my $id=shift;
  my $Wstatus=shift;
  my $sleep=(shift or 30);
  my $times=(shift or 10);

  my $counter=0;
  while (1) {
    my ($status)=$cat->execute("top", "-id", $id);
    $status or print "Error checking the status of the job\n" and return;
    $status = $status->{status};
    $status eq $Wstatus and return 1;
    print "Status -> $status\n";
    $status=~ /((ERROR_)|(FAILED)|(DONE_WARN)|(DONE))/ and 
      print "THE job finished with $1!!\n" and return;
    ($counter>$times) and print "We have been waiting for more than $counter *$sleep seconds.... let's quit" and return;
    print "The father sleeps (waiting for $Wstatus)\n";
    sleep $sleep;
    $counter++;

  }

  return ;
}
#SUBROUTINE TO KILL JOBS
sub killAllWaitingJobs {
  my $cat=shift;
  print "Killing all the waiting jobs...\n";
  my @jobs=$cat->execute("top", "-status INSERTING", "-silent");
  @jobs=(@jobs, $cat->execute("top", "-status WAITING", "-silent"));
  foreach my $job (@jobs) {
    print "KILLING jobs $job->{queueId}\n";
    $cat->execute("kill", $job->{queueId});
  }
  return 1;

}
#SUBROUTINE TO EXECUTE A JDL FILE
#if it receives $status, the job has to finish with that status
sub executeJDLFile{
  my $cat=shift;
  my $jdl=shift;
  my $status=shift || "DONE";
  killAllWaitingJobs($cat);
  $ENV{ALIEN_JOBAGENT_RETRY}=1;

  my ($id)=$cat->execute("submit", $jdl);
  $id or return
    

  $cat->execute("top") or return;
  $cat->execute("ps") or print "Error doing ps\n" and return;

  print "Checking if there are any warnings during the top...\n";
  if (!waitForStatus($cat, $id, "WAITING", 5, 10)) {
    print "The job is not WAITING!!\n";
    my ($statusI)=$cat->execute("top", "-id", $id);
    $statusI->{status} eq $status and 
      print "Howeer, $status is what we were expecting\n" and return 1;
    exit(-2);
  }
  print "Let's try to execute the job...\n";
  system("alien proxy-destroy 2> /dev/null");
  # only the user with role admin can open and add queues
  my $admincat=AliEn::UI::Catalogue::LCM::Computer->new({"user","$ENV{'USER'}","role","admin"});
  $admincat or exit (-1);
  $admincat->execute("debug", "CE ProcessMonitor LQ Service") or exit(-2);
  $admincat->execute("queue", "open $cat->{CONFIG}->{ORG_NAME}::CERN::testCE") 
    or return;
  $cat->execute("request") or return;
  system ("alien proxy-destroy 2> /dev/null");
  print "The job finished!! Let's wait until the output has been registered\n";

  waitForStatus($cat, $id, $status, 5, 10) or return;
    


  my ($user)=$cat->execute("whoami") or return;

  $cat->execute("ls","-al", "~/alien-job-$id") or print "The directory ~/alien-job-$id does not exist\n" and return;
  print "Job executed successfully!!\n";
  if ($status) {
    print "Checking that the status is $status\n";
    my ($info)=$cat->execute("top", "-id", $id);
    $info->{status} eq "$status" or
      print "NOPE!! the status is $info->{status}\n" and  return;
  }
  return "~/alien-job-$id";
}
sub checkOutput{
  my $cat=shift;
  my $id=shift;
  my $procDir=shift||"~/alien-job-$id";
  my ($user)=$cat->execute("whoami") or return;
  print "Checking if the output of the job is registered... $procDir\n";
  $cat->execute("ls", "$procDir/", "-l") or return;
  $cat->execute("ls", "$procDir", "-l") or return;
  print "Getting the output\n";
  $cat->execute("cat", "$procDir/stdout") or return;
  return $procDir;

}

sub getJobAgentLog{
  my $cat=shift;
  my $procDir=shift;

  my $id=$procDir;
  $id=~ s/^.*\-//;
  print "Registering the output of the job $id ($cat and $procDir)\n";
  $cat->execute("registerOutput", $id) or return;
  
  my ($file)=$cat->execute("get", "~/recycle/alien-job-$id/execution.out");
  $file or print "Error getting the execution log of job $id";
  return $file;  

 
}
