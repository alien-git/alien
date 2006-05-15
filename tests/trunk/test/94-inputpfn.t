use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
  includeTest("26-ProcessMonitorOutput") or exit(-2);

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
InputFile={\"LF:$dir/jdl/Input.jdl\",\"PF:file://$hostname$inputpfn\"};
","r") or exit(-2);
  
  my $procDir=executeJDLFile($cat, "jdl/InputPFN.jdl");
  unlink  $inputpfn;
  $procDir or exit(-2);
  my ($log)=$cat->execute("get","$procDir/job-log/execution.out") or exit(-2);
  open (LOG, "<$log" ) or exit(-2);
  my @log=grep (/Getting/, <LOG>);
  close LOG;
  print "We got the files @log\n";
  grep (m{Getting /proc/.*/job-log/execution.out}, @log)
    and print "We downloaded the execution log!!!\n" and exit(-2);
  grep (m{Getting .*/bin/}, @log) or
   print "We didn't download any executable!!!\n" and exit(-2);

  my ($newfile)=$cat->execute("get", "$procDir/test.94.inputfile.$$") or print "Error getting the file'$procDir/test.94.inputfile.$$' from the catalogue\n" and exit(-2);
  ok(1);
}
