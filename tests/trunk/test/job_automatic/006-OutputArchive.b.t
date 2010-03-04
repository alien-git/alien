#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  my $id=shift or print "Error getting the job id\n" and exit(-2);
  my $procDir=checkOutput($cat, $id) or exit(-2);

  my $files={"stdout"=>{}, "file.out"=>{}};
  foreach my $file (keys %$files) {
    my ($out)=$cat->execute("get","$procDir/job-output/$file") or exit(-2);
    open (FILE, "<$out") or print "Error opening $out" and exit(-2);
    my @data=<FILE>;
    close FILE;
    print "Got @data\n";
    $files->{$file}=join ("",@data);
    my ($se, @pfn)=$cat->execute("whereis", "$procDir/job-output/$file") or exit(-2);
    my $found=0;
    foreach my $pfn (@pfn) {
      $pfn =~ /^guid:/ and $found=1;
      }
    $found or print "The pfns '@pfn' of $file doesn't look like a zip\n" and exit(-2);
  }
  $files->{stdout}=~ /Input\.jdl/ or print "Error the input data is not there!!!\n" and exit(-2);

  my ($log)=$cat->execute("get","$procDir/job-log/execution.out") or exit(-2);
  open (LOG, "<$log" ) or exit(-2);
  my @log=grep (/Getting/, <LOG>);
  close LOG;
  print "We got the files @log\n";
  grep (m{Getting /proc/.*/job-log/execution.out}, @log)
    and print "We downloaded the execution log!!!\n" and exit(-2);
  grep (m{Getting .*/bin/}, @log) or
    print "We didn't download any executable!!!\n" and exit(-2);
  
  ok(1);
}
