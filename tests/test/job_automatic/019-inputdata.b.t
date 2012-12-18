#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);
  my $id=shift;
  my $content=shift;
  print "THIS IS THE id $id (and this is $content)\n";

  $id or print "We didn't get the id of the job\n" and exit(-2);
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  my ($user)=$cat->execute("whoami") or exit(-2);
      
  my $procDir="~/alien-job-$id";

  my $files={"stdout"=>{}, "file.out"=>{}};
  foreach my $file (keys %$files) {
    my ($out)=$cat->execute("get","$procDir/$file") or exit(-2);
    open (FILE, "<$out") or print "Error opening $out" and exit(-2);
    my @data=<FILE>;
    close FILE;
    print "Got @data\n";
    $files->{$file}=join ("",@data);
  }
  $files->{stdout}=~ /Input\.jdl/ or print "Error the input data is not there!!!\n" and exit(-2);

  ($files->{"file.out"} eq "$content\n") or print "ERROR the content of the file '".$files->{"file.out"} ."' does not match with the original '$content\n'\n" and exit(-2);
  my ($log)=getJobAgentLog($cat, $procDir) or exit(-2);
  open (LOG, "<$log" ) or exit(-2);
  my @log=grep (/Getting/, <LOG>);
  close LOG;
  print "We got the files @log\n";
  grep (m{Getting alien-job-$id/.*/job-log/execution.out}, @log)
    and print "We downloaded the execution log!!!\n" and exit(-2);
  grep (m{Getting .*/bin/}, @log) or
    print "We didn't downloaded any executable!!!\n" and exit(-2);

  print "Finally, let's take a look at the jdl, and make sure that it was written in the requirements\n";

  my ($jdl)=$cat->execute("ps", "jdl", $id) or exit(-2);
  my $name="member\\(other.CloseSE,\"$cat->{CONFIG}->{SE_FULLNAME}\"\\)";
  $jdl =~ /$name/i or 
    print "The requirement '${name}' is not there!!\n" and exit(-2);

  ok(1);
}
