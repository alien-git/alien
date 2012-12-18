#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);
use AliEn::Service::PackMan;
use Cwd;
use AliEn::Util;

BEGIN { plan tests => 1 }


{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("catalogue/003-add") or exit(-2);


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);

  $cat->execute("mkdir", "-p","bin", "jdl") or exit(-2);

  addFile($cat, "bin/JobWithPackage.sh","#!/bin/bash
date
echo 'Starting the commnand'
MyPS
date
") or exit(-2);

  addFile($cat, "jdl/package.jdl","Executable=\"JobWithPackage.sh\";
Packages=\"MyPS::1.0\"\n") or exit(-2);

  addPackage($cat, "MyPS", "/bin/ps") or exit(-2);
  
  print "The package has been addedd!!!\n\n\n";

  my ($ok, $source)=installPackage("MyPS"); 
  $ok or print "Error installing the package!!\n" and  exit(-2);
  $source or print "Error: don't have anything to source\n" and exit(-2);
  print "\n\nLet's submit the job\n";

  my ($id)=$cat->execute("submit", "jdl/package.jdl") or exit(-2);

  print "We have submitted both jobs!!\n
\#ALIEN_OUTPUT $id\n";


  ok(1);
}
sub installPackage{
  use AliEn::SOAP;
  my $package=shift;
  my $soap=new AliEn::SOAP;
  print "Installing the package $package\n";
  my $result;
  while (1) {
    $result=$soap->CallSOAP("PACKMAN", "installPackage", "newuser", 
			    $package, "1.0") and last;
    my $message=$AliEn::Logger::ERROR_MSG;
    $soap->{LOGGER}->info("PackMan", 
			  "The reason it wasn't installed was $message");
    $message =~ /Package is being installed/ or last;
    sleep (30);
  }
  $result or return;
  my ($ok, $source)=$soap->GetOutput($result);

  print "Got $ok and $source\n";
  $ok or return;
  print "This is ok!!\n";
  return ($ok, $source);
}
sub addPackage{
  my $cat=shift;
  my $package=shift;
  my $file=shift;

  $cat->execute("mkdir", "-p","packages/$package/1.0") or return;
  $cat->execute("addTag", "packages/$package/", "PackageDef") or return;


  my $lfn="packages/$package/1.0/" . AliEn::Util::getPlatform();
  my $exists=$cat->execute("whereis", "-silent", $lfn);
  $exists and return 1;

  my $preserveDir = getcwd();

  print "Adding the package\n";
  my $dir="/tmp/alien-76.$$";
  mkdir $dir;
  chdir $dir or print "Error going to $dir\n" and exit(-2);
  print "Let's copy the file to $dir\n";
  if (!link  ($file, "$dir/$package")){
    system("cp", $file, "$dir/$package") and
      print "Error copying the file '$file' $!\n" and exit(-2);
  }
  print "Creating the environmnet file\n";
  open (FILE, ">$dir/.alienEnvironment") or exit(-2);

  print FILE "echo 'Setting the environment to execute $package'
DIR=\$1
shift
echo \"Package installed in \$DIR\"
export PATH=\$DIR:\$PATH
echo \"Executing \$*\"
\$*
";
  chmod 0755, "$dir/.alienEnvironment";
  system ("tar zcvf MyTar.tar .alienEnvironment $package") and print "Error doing the tar file " and return;
  my $host=Net::Domain::hostfqdn();
  my $done=$cat->execute("packman", "define", $package, "1.0", "$dir/MyTar.tar");
  system("rm", "-rf", $dir);
  chdir ($preserveDir) or die("Error returning to preserved dir");
  $done or return;
  
  #we delete the cache, so that the CE knows the new packages that have 
  #been defined
  $cat->execute("cleanCache");

  sleep(20);
  $cat->execute("packman", "list", "-force");
  return 1;

  


}
