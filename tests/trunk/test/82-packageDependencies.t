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

  addFile($cat, "jdl/packageDep.jdl","Executable=\"JobWithPackage.sh\";
Packages=\"MyLS::1.0\"") or exit(-2);

  addPackage($cat, "MyLS", "/bin/ls") or exit(-2);

  $cat->execute("removeTagValue", "packages/MyLS/1.0", "PackageDef");
  $cat->execute("addTagValue", "packages/MyLS/1.0", "PackageDef", "dependencies='MyPS::1.0'") or exit(-2);;

  print "\n\nLet's see if the package gets installed\n";
  my ($ok, $source)=installPackage("MyLS") or exit(-2);

  $source =~ /MyPS/ or print "The package does not depend on MyPS!!!\n" and exit(-2);

  print "Let's submit the job\n";

  my $procDir=executeJDLFile($cat,"jdl/packageDep.jdl") or exit(-2);

  my ($out)=$cat->execute("get","$procDir/job-output/stdout") or exit(-2);
  open (FILE, "<$out") or print "Error opening $out" and exit(-2);
  my @data=<FILE>;
  close FILE;
  print "Got @data\n";

  grep ( /Setting the environment to execute MyPS/, @data ) or print "Error the package MyPS is not  initialized!!\n" and exit(-2);
  grep ( /Setting the environment to execute MyLS/, @data ) or print "Error the package MyLS is not  initialized!!\n" and exit(-2);
  grep ( /MyPS: command not found/, @data ) and print "Error the command MyPS is not in the PATH\n" and exit(-2);

  ok(1);
}
