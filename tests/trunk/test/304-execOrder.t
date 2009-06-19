#!/bin/env alien-perl

use strict;
use Test;
use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);
use AliEn::SOAP;
use Time::Local;

BEGIN { plan tests => 1 }
{
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  $ENV{ALIEN_JOBAGENT_RETRY}=1;
  includeTest("16-add") or exit(-2);
  includeTest("26-ProcessMonitorOutput") or exit(-2);

  my $ALIEN_ROOT=$ENV{ALIEN_ROOT};
  
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
  $cat or exit (-1);
  $cat->execute("pwd") or exit (-2);
  $cat->execute("cd") or exit (-2);
  
  #put JDLs to LDAP      
  addFile($cat, "jdl/date.E.jdl","Executable=\"date\";
Price=\"30\";\n") or exit(-2);
  addFile($cat,"jdl/date.C.jdl","Executable=\"date\";
Price=\"3\";\n") or exit (-2); 
  print "Files added !\n";

  my $flag = 0;
  my $soap = AliEn::SOAP->new();

  # check if we have cluster monitor runing
  if ($soap->checkService("ClusterMonitor")){
    $flag = 1; 
  }

  #stop the monitor and submit
  $flag and system ("$ALIEN_ROOT/bin/alien StopMonitor ");
  sleep(3);
	
  #kill all waiting jobs
  print "kill all wating jobs\n";
  killAllWaitingJobs($cat);
  sleep(15);  
  
    
  # submit both jobs
  my ($id_cheap)=$cat->execute("submit", "jdl/date.C.jdl") or exit(-2);
  my ($id_expensive)=$cat->execute("submit", "jdl/date.E.jdl") or exit(-2);
  
  # sleep half a minute, to be sure, that job was inserted
  sleep(30);

  
  # restart job optimizers (to know that Priority optimizer worked) and sleep a bit
  system ("$ALIEN_ROOT/bin/alien StartJobOptimizer");
  sleep (100);

  # Start Cluster Monitor
  system ("$ALIEN_ROOT/bin/alien StartMonitor");
  print "Cluster monitor started\n";
  sleep(30); 
	
  #request a job 
  $cat->execute("request") or exit(-2);
  #wait 
  #print "done request, going to sleep ... \n";
  #sleep (45);
  
  my $res = 0;
  print "entering the loop, querying $id_cheap $id_expensive \n";
  
  
  my ($time_cheap, $time_expensive); 
  while (1){
    my ($str_cheap)     = $cat->execute("ps", "-A -T -id $id_cheap");
    my ($str_expensive) = $cat->execute("ps", "-A -T -id $id_expensive");
    print ("C: $str_cheap E: $str_expensive \n");	
    $time_cheap     = str2time ($str_cheap);
    $time_expensive = str2time ($str_expensive);	

    $time_expensive or (sleep(30) and next);
    $time_expensive <0   and exit(-2);
    $res=1;
    if ($time_cheap and $time_expensive > $time_cheap) {
      $res=0; #Not OK 
    }
    last; # else OK 
  }
  $cat->close();
  $flag or system("$ALIEN_ROOT/bin/alien StopMonitor");
  ok($res);
}


sub str2time{
  my $str=shift;
  my $i = 0;
  my @list = split (/\s+/,$str);
  $list[2] =~ /^E/ and return -1;
  ($list[8] eq "....") and return;
                 
  my $month = {  'Jan' => '0',
                 'Feb' => '1',
                 'Mar' => '2',
                 'Apr' => '3',
                 'May' => '4',
                 'Jun' => '5',
                 'Jul' => '6',
                 'Aug' => '7',
                 'Sep' => '8',
                 'Oct' => '9',
                 'Nov' => '10',
                 'Dec' => '11',
               };

  #      foreach (@list){
  #      print "$i: $_ \n";
  #      $i++;
  #      }

  my ($hour, $min, $sec) = split (":", $list[11]);
  my $mday  = $list[10];
  my $mon   = $month->{$list[9]};
  my $year  = $list[12];

  my $timestamp = timelocal($sec,$min,$hour,$mday,$mon,$year);
  my $d = sprintf ("%d", $timestamp);
  return $d;
}
