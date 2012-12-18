#!/bin/env alien-perl

use strict;
use Test;
use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  $ENV{ALIEN_JOBAGENT_RETRY} = 1;
  includeTest("catalogue/003-add")                   or exit(-2);
  includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
  $cat or exit(-1);

  $cat->execute("cd") or exit(-2);

  addFile($cat, "jdl/DONE.jdl", "Executable=\"date\";\n" . "OutputArchive = \"myArchive.zip:stdout\@disk=2\";\n")
	or exit(-2);

  my ($idfine) = $cat->execute("submit", "jdl/DONE.jdl") or exit(-2);

  addFile($cat, "jdl/DONE_WARN.jdl", "Executable=\"date\";\n" . "OutputArchive = \"myArchive.zip:stdout\@disk=7\";\n")
	or exit(-2);

  my ($idwarn) = $cat->execute("submit", "jdl/DONE_WARN.jdl") or exit(-2);

  addFile($cat, "jdl/ERROR_SAVING.jdl",
	"Executable=\"date\";\n" . "OutputArchive = \"myArchive.zip:stdout\@ALICE::NONETHERE::SE,BOB::NEITHER::SE\";\n")
	or exit(-2);

  my ($iderror) = $cat->execute("submit", "jdl/ERROR_SAVING.jdl") or exit(-2);

  $cat->execute("pwd") or exit(-2);

  print "Job submitted!! 
\#ALIEN_OUTPUT $idfine,$idwarn,$iderror\n"

}

