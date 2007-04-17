#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
  includeTest("26-ProcessMonitorOutput") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  $cat->execute("mkdir", "-p", "jdl");
  addFile($cat, "jdl/email.jdl", "Executable=\"date\";\nEmail=\"root\@localhost\";\n") or exit(-2);
#  my $status=jobWithEmail($cat,"jdl/email.jdl") or exit(-2);
#  $status eq "DONE" or exit(-2);
  $cat->execute("whereis", "-silent", "bin/dateWrong") or 
    $cat->execute("register", "bin/dateWrong", "file://wrongmachine/path/to/not/existant/file",22) 
      or exit(-2);
  addFile($cat, "jdl/emailWrong.jdl", "Executable=\"dateWrong\";\nEmail=\"root\@localhost\";\n") or exit(-2);

my  $status=jobWithEmail($cat, "jdl/emailWrong.jdl", "ERROR_IB") or exit(-2);
  $status =~ /ERROR_/ or exit(-2);

  ok(1);
}


sub jobWithEmail {
  my $cat=shift;
  my $jdl=shift;
  my $status=shift;

#  my ($procDir)=executeJDLFile($cat,$jdl, $status) or 
#    print "The job did not execute correctly!!\n" and return;
  my $procDir="/proc/newuser/55/";

  my ($file)=$cat->execute("get", "-f", "$procDir/job-log/execution.out");

  $file or return;
  print "The job finished!! Let's see the output of the ProcessMonitor\n";

  open (FILE, "<$file") or print  "THERE WAS NO OUTPUT\n" and return;

  my @file=<FILE>;
  close FILE;
  unlink $file;

  print "THE JOB WROTE: \n@file\n\n";

  my @mail=grep(/Sending an email to \S+ \(job \S+\)/, @file);
  if (! @mail){
    print "The job did not send any email\n";
    my $id=$procDir;
    $id=~ s{^.*/(\d+)/?}{$1};
    open (FILE, "find $cat->{CONFIG}->{TMP_DIR}/7200 -name proc.$id.out|") or print "Error finding the log file\n" and return;
    my $file=join("",<FILE>);
    close FILE or print "Error with the find\n" and return;
    chomp $file;
    $file or print "Error finding the temporary log file\n" and return;
    print "Let's check $file\n";
    open (FILE, "<$file") or print "Error checking $file\n" and return;
    my @file=<FILE>;
    close FILE;
    @mail=grep(/Sending an email to \S+ \(job \S+\)/, @file);
    if (@mail) {
      print "in the local file there was some trace\n";
    }else {
      return;
    }
  }
  join("", @mail) =~ /Sending an email to \S+ \(job (\S+)\)/;
  print "The email said that the job was $1\n";
  return $1;
}
