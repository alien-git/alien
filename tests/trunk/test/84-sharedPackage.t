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
  includeTest("26-ProcessMonitorOutput") or exit(-2);
  includeTest("76-jobWithPackage") or exit(-2);

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
  my $procDir=executeJDLFile($cat,"jdl/sharedPackage.jdl") or exit(-2);

  my ($out)=$cat->execute("get","$procDir/job-output/stdout") or exit(-2);
  open (FILE, "<$out") or print "Error opening $out" and exit(-2);
  my @data=<FILE>;
  close FILE;
  print "Got @data\n";

  grep ( /Setting the environment to execute MySHAREDLS/, @data ) or print "Error the package MyLS is not  initialized!!\n" and exit(-2);
  grep ( /MyPS: command not found/, @data ) and print "Error the command MyPS is not in the PATH\n" and exit(-2);

  ok(1);
}
