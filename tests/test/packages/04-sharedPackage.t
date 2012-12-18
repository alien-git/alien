#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Service::PackMan; # needed for includeTest 76
use Cwd; # needed for includeTest 76

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("catalogue/003-add") or exit(-2);

  includeTest("packages/02-jobWithPackage") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);


  addFile($cat, "jdl/sharedPackage.jdl","Executable=\"JobWithPackage.sh\";
Packages=\"MySHAREDLS::1.0\"") or exit(-2);

  addPackage($cat, "MySHAREDLS", "/bin/ls") or exit(-2);
  addPackage($cat, "MySHAREDPS", "/bin/ps") or exit(-2);

  $cat->execute("removeTagValue", "packages/MySHAREDLS/1.0", "PackageDef");
  $cat->execute("addTagValue", "packages/MySHAREDLS/1.0", "PackageDef", "dependencies='MySHAREDPS::1.0'", "shared=1") or exit(-2);;

  $cat->execute("removeTagValue", "packages/MySHAREDPS/1.0", "PackageDef");
  $cat->execute("addTagValue", "packages/MySHAREDPS/1.0", "PackageDef", "config=''", "shared=1") or exit(-2);


  print "Let's submit the job\n";
  my ($id)=$cat->execute("submit", "jdl/sharedPackage.jdl") or exit(-2);

  print "We have submitted both jobs!!\n
\#ALIEN_OUTPUT $id \n";

  ok(1);
}
