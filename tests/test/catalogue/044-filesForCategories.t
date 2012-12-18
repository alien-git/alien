#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"role", "admin",});
  $cat or exit(-1);

  # LFNs of test files for categories 
  my @files = ("/alice/data/LHC10a/000108350/ESDs/pass1/10000108350018.10/test/AliESDs.root", 
  "/alice/data/LHC10a/000108350/raw/test/10000108350018.10.root",
  "/alice/data/LHC10a/000135780/ESDs/pass1/AOD014/0002/test/AliAOD.Dimuons.root",
  "/alice/data/2012/LHC12c/000180559/vpass1/AOD/003/test/AliAOD.Jets.root",
  "/alice/sim/2012/LHC12a12_bis/115401/037/test/AliESDfriends.root",
  "/alice/sim/2012/LHC12a12_bis/115401/AOD102/0016/test/AliAOD.Muons.root",
  "/alice/data/OCDB/test/testfile",
  "/alice/data/2012/OCDB/HLT/Calib/esdLayout/test/Run0_999999999_v1_s0.root",
  "/alice/cern.ch/user/a/aabramya/est.file"
  );
  
  for my $filename (@files)
  {
  	$filename =~ /^(.*)\/.*$/;
  	$cat->execute("mkdir", "-p", "$1") or exit(-2);
  	# Writing and reading test files for different categories
  	addFile($cat, "$filename", "test\n") or exit(-2);
        $cat->execute("cat", "$filename");
  }

  ok(1);
}
