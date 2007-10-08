#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);
use AliEn::Service::PackMan; # needed for includeTest 76
use Cwd;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("16-add") or exit(-2);
  includeTest("26-ProcessMonitorOutput") or exit(-2);
  includeTest("76-jobWithPackage") or exit(-2);
  includeTest("68-dbthreads") or exit(-2);

  my $checkProcess=createCheckProcess(50);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  my $host=Net::Domain::hostname();
  my ($homedir)=$cat->execute("pwd");
  my $packageDir="$homedir/packages/sourcePackage";
  my $lfnDir="$packageDir/1.0";
  my $lfn="$lfnDir/source";
  $cat->execute("rm", "-rf", $lfn);

  my $dir="/tmp/gcc.$$";
  my $preserveDir = getcwd();
  eval {
    mkdir $dir or die("Error creating $dir\n");
    chdir $dir or die("Error creating $dir\n");
    open (FILE, ">$dir/pre_install") or die ("Error opening the pre_install");
    print FILE "#!/bin/bash
echo 'HELLO WORLD'
echo \"I've been called as \$0\"
echo \"Have a nice day\"
";
    close FILE;

  };
  ($@) and print "ERROR $@ \n" and exit -2;
  chdir($preserveDir) or die("Error returning to old work dir");
  addPackage($cat, "sourcePackage", "$dir/pre_install") or exit(-2);;
  print getcwd() . "\n";
  system("rm -rf $dir");
  print getcwd() . "\n";

  $cat->execute("addTagValue", $lfnDir, "PackageDef", "post_install='$packageDir/post_install'") or exit(-2);

  addFile($cat, "$packageDir/post_install","#!/bin/bash
DIR=\$1
echo \"The software is installed in \$DIR\"
echo \"Let's copy pre_install into 'installed'\"
cp sourcePackage my_compiled
chmod +x my_compiled
","r") or exit(-2);

  print "Let's submit the job\n";

  addFile($cat, "jdl/compiled_package.jdl","executable=\"my_compiled\";
packages=\"sourcePackage::1.0\";
","r") or exit(-2);


  addFile($cat, "bin/my_compiled",
"#!/bin/bash
echo \"This command checks if the compiled package was installed\"
my_compiled && echo \"YUHUUU\";
") or exit(-2);

  my ($id)=$cat->execute("submit", "jdl/compiled_package.jdl") or exit(-2);

  print "We have submitted both jobs!!\n
\#ALIEN_OUTPUT $id \n";



  kill 9, $checkProcess;

  ok(1);
}
