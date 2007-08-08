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
  my $status=jobWithEmail($cat,"jdl/email.jdl") or exit(-2);
  $status eq "DONE" or exit(-2);
  $cat->execute("whereis", "-silent", "bin/dateWrong") or 
    $cat->execute("register", "bin/dateWrong", "file://wrongmachine/path/to/not/existant/file",22) 
      or exit(-2);
  addFile($cat, "jdl/emailWrong.jdl", "Executable=\"dateWrong\";\nEmail=\"root\@localhost\";\n") or exit(-2);

  $status=jobWithEmail($cat, "jdl/emailWrong.jdl", "ERROR_IB") or exit(-2);
  $status =~ /ERROR_/ or exit(-2);

  ok(1);
}


sub jobWithEmail {
  my $cat=shift;
  my $jdl=shift;
  my $status=shift;

  my ($procDir)=executeJDLFile($cat,$jdl, $status) or 
    print "The job did not execute correctly!!\n" and return;
#my $procDir="/proc/newuser/81";
  my $id;
  $procDir=~ m{/(\d+)} or print "Error getting the job id from '$procDir'\n" and return;
  $id=$1;
  print "The job $id finished!! Let's wait 60 seconds before getting the trace\n";
  sleep(60);
  my ($trace)=$cat->execute("ps", "trace", $id, "all") or return;
  use Data::Dumper;
  foreach my $entry (@$trace){
    $entry->{trace}=~ /Sending an email to (\S+) \(job (\S+)\)/ or next;
    print "The job sent an email to $1, with status $2\n";
    return $2;
  }
  print "The job didn't send any emails\n";
  print Dumper($trace);
  return;
}
