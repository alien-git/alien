use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $inputpfn="/tmp/test.94.inputfile.$$";
  open (FILE, ">$inputpfn") or print "Error opening $inputpfn $!\n" and exit(-2);
  print FILE "Hello world\n";
  close FILE;
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  
  $cat->execute("cd") or exit (-2);
  my ($dir)=$cat->execute("pwd") or exit (-2);
  
  $cat->execute("mkdir", "-p","bin", "jdl") or exit(-2);
  my $content="blablabla.".time;
  my $hostname=Net::Domain::hostfqdn();
  addFile($cat, "bin/CheckPFN.sh","#!/bin/bash
date
echo 'Starting the commnand'
echo \"I've been called with '\$*'\"
pwd
ls -al
#( -f test.94.inputfile.$$ ) && echo \"The file test.94.inputfile.$$ exists\"
#(-f test.94.inputfile.$$ ) || echo \"The file test.94.inputfile.$$ does not exist\"

date") or exit(-2);

  addFile($cat, "jdl/InputPFN.jdl","Executable=\"CheckPFN.sh\";
InputFile={\"LF:$dir/jdl/Input.jdl\",\"PF:file://$hostname:8092/$inputpfn\"};
","r") or exit(-2);
  
  my $procDir=executeJDLFile($cat, "jdl/InputPFN.jdl");
  unlink  $inputpfn;
  $procDir or exit(-2);
  my ($log)=getJobAgentLog($cat, $procDir) or exit(-2);
  open (LOG, "<$log" ) or exit(-2);
  my @log=grep (/Getting/, <LOG>);
  close LOG;
  print "We got the files @log\n";
  grep (m{Getting $procDir/job-log/execution.out}, @log)
    and print "We downloaded the execution log!!!\n" and exit(-2);
  grep (m{Getting .*/bin/}, @log) or
   print "We didn't download any executable!!!\n" and exit(-2);

  my ($output) = $cat->execute("get","$procDir/stdout") or exit(-2);
  open (OUT, "<$output");
  my @output = <OUT>;
  close OUT;
  grep(/test.94.inputfile.$$/,@output) or exit(-2);
  ok(1);
}
