#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Data::Dumper;

BEGIN { plan tests => 1 }


{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);

  $cat->execute("cd") or exit (-2);
  my ($dir)=$cat->execute("pwd") or exit (-2);

  $cat->execute("mkdir", "-p","bin", "jdl") or exit(-2);
  my $content="blablabla.".time;
  addFile($cat, "bin/CheckInputOuptut.sh","#!/bin/bash
date
echo 'Starting the commnand'
echo \"I've been called with '\$*'\"
pwd
ls -al
echo 'Creating a temporaray file'
echo '$content'> file.out
date
","r") or exit(-2);

  addFile($cat, "jdl/Input.jdl","Executable=\"CheckInputOuptut.sh\";
InputFile=\"LF:$dir/jdl/Input.jdl\";
OutputFile={\"file.out\",\"stdout\",\"stderr\",\"resources\"}") or exit(-2);

  my $procDir=executeJDLFile($cat, "jdl/Input.jdl") or exit(-2);
 print "Job executed. Let's see if the output is what we want\n";
  my $files={"stdout"=>{}, "file.out"=>{}};
  my @tmp = keys %$files;
  foreach my $file (keys %$files) {
    my ($out)=$cat->execute("get","$procDir/$file") or print "Error getting $procDir/$file\n" and exit(-2);
    open (my $f, "<", $out) or print "Error opening $out" and exit(-2);
    my @data=<$f>;
    close $f;
    print "Got @data\n";
    $files->{$file}=join ("",@data);
  }
  $files->{stdout}=~ /Input\.jdl/ or print "Error the input data is not there!!!\n" and exit(-2);
  ($files->{"file.out"} eq "$content\n") or print "ERROR the content of the file '".$files->{"file.out"} ."' does not match with the original '$content\n'\n" and exit(-2);

  
  my $log=getJobAgentLog($cat, $procDir) or exit(-2);

  open (LOG, "<$log" ) or exit(-2);
  my @log=grep (/Getting/, <LOG>);
  close LOG;
  print "We got the files @log\n";
  grep (m{Getting $procDir/job-log/execution.out}, @log)
    and print "We downloaded the execution log!!!\n" and exit(-2);
  grep (m{Getting .*/bin/}, @log) or
    print "We didn't download any executable!!!\n" and exit(-2);

  print "Finally, let's take a look at the jdl, and make sure that it wasn't written in the requirements\n";

  $procDir=~ m{/alien-job-(\d+)$} or print "Error getting the jobid from $procDir!!\n" and return;
  my $id=$1;  
  my ($jdl)=$cat->execute("ps", "jdl", $id) or exit(-2);
  my $name="member\\(other.CloseSE,\"$cat->{CONFIG}->{SE_FULLNAME}\"\\)";
  $jdl =~ /$name/i and
    print "The requirement '${name}' is there!!\n" and exit(-2);

  ok(1);
}
