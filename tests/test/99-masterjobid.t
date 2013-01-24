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
  includeTest("job_automatic/008-split") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);

  addFile($cat, "bin/CheckMasterId.sh","#!/bin/bash
echo \"Checking if we can find the masterjobid\";
echo \"ALIEN_MASTERJOBID=\$ALIEN_MASTERJOBID\"
echo ok
") or exit(-2);

  addFile($cat, "jdl/SplitMaster.jdl","Executable=\"CheckMasterId.sh\";
Split=\"directory\";
InputData=\"${dir}split/*/*\";") or exit(-2);

  my ($ok,$procDir, $subjobs)=executeSplitJob($cat, "jdl/SplitMaster.jdl") 
    or exit(-2);

  $subjobs eq "2" or print "The job is not split in 2 subjobs\n" and exit(-2);
  my @dirs=$cat->execute("ls", "$procDir/");
  my $done=0;
  foreach my $entry (@dirs) {
    $entry =~ /job-log/ and next;
    $done=1;
    print "Checking the output of $entry\n";
    my ($file)=$cat->execute("get", "-silent", "$procDir/$entry/stdout") or exit(-2);
    open (FILE, "<$file") or print "Error opening $file\n" and exit(-2);
    my @content=<FILE>;
    close FILE;
    my ($line)=grep (s/^ALIEN_MASTERJOBID=//, @content);
    $line or print "There is no output in job $entry\n" and exit(-2);
    chomp $line;
    print "The masterjobid is $line\n";
    $line or print "The variable is not defined\n" and exit(-2);
  }
  $done or print "We haven't found the output!!\n" and exit(-2);
  print "ok\n";
}

