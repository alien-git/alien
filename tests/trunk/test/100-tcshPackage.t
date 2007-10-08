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

  includeTest("16-add") or exit(-2);

  includeTest("76-jobWithPackage") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);


  addFile($cat, "bin/tcshPackage.csh","#!/bin/tcsh
echo \"This is a tcsh script\"
setenv MYVAR MYVALUE
echo \"Variable MYVAR defined to \$MYVAR\"
") or exit(-2);

  addFile($cat, "jdl/packageTCSH.jdl","Executable=\"tcshPackage.csh\";
Packages={\"MyTCSH::1.0\", \"MyPS::1.0\"}") or exit(-2);

  addPackage($cat, "MyTCSH", "/bin/ls") or exit(-2);

  print "\n\nLet's see if the package gets installed\n";
  my ($ok, $source)=installPackage("MyTCSH") or exit(-2);

  print "Let's submit the job\n";
  my ($id)=$cat->execute("submit", "jdl/packageTCSH.jdl") or exit(-2);

  print "We have submitted both jobs!!\n
\#ALIEN_OUTPUT $id\n";

  ok(1);
}
