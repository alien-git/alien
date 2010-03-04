use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_JOBAGENT_RETRY}=1;
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);

  $cat->execute("cd") or exit (-2);
  my ($dir)=$cat->execute("pwd") or exit (-2);

  my $sename="$cat->{CONFIG}->{ORG_NAME}::cern::testse";

  addFile($cat, "bin/checkMemory.sh","#!/bin/bash
echo \"Checking how much free memory we have\"
free") or exit(-2);

  addFile($cat, "jdl/checkMemory.jdl","Executable=\"checkMemory.sh\";
Memory=20;
") or exit(-2);

  addFile($cat, "jdl/checkTooMuchMemory.jdl","Executable=\"checkMemory.sh\";
Memory=200000000;
") or exit(-2);

#  my $procDir=executeJDLFile($cat, "jdl/checkMemory.jdl") or exit(-2);#
#
#  print "JOB EXECUTED!!\nChecking if the archive is in the right place\n";#
#
#  my ($file)=$cat->execute("get", "$procDir/job-output/stdout");
  
#  open (FILE, "<$file") or exit(-2);
#  my @data=<FILE>;
#  close FILE;
#  print "The file contains \n @data\n";

  print "Let's submit a file that requests too much memory\n";
  my ($id)=$cat->execute("submit", "jdl/checkTooMuchMemory.jdl") or exit(-2);

  my $ready=0;
  for (my $i=0; $i<4; $i++) {
    sleep(5);
    my ($info)=$cat->execute("top", "-id", $id);
    $info->{status} eq "WAITING" and $ready=1 and last;
  }
  $ready or print "The job is not WAITING!!\n" and exit(-2);
  print "Let's try to execute the job...\n";
  system("alien", "proxy-destroy");
  my ($got)=$cat->execute("request") or exit(-2);
  system ("alien", "proxy-destroy");
  ($got eq "-2") or print "There was something to execute!\n" and exit(-2);
  print "Yuhuu!!\n";
  
}
