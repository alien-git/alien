#!/bin/env alien-perl

use strict;
use Test;
use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  $ENV{ALIEN_JOBAGENT_RETRY}=1;
  includeTest("16-add") or exit(-2);
  includeTest("26-ProcessMonitorOutput") or exit(-2);




  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
  $cat or exit (-1);

  $cat->execute("pwd") or exit (-2);

  $cat->execute("cd") or exit (-2);
  addFile($cat, "jdl/SAVED_WARN.jdl","Executable=\"date\";\n"
             ."OutputArchive = \"myArchive.zip:stdout\@disk=7\";\n") or exit(-2);






  killAllWaitingJobs($cat);
  my ($id)=$cat->execute("submit", "jdl/SAVED_WARN.jdl") or exit(-2);



  print "Job submitted!! 
\#ALIEN_OUTPUT $id\n"


}



