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

  $cat->execute("cd") or exit (-2);
  my ($dir)=$cat->execute("pwd") or exit (-2);

  my $sename="$cat->{CONFIG}->{ORG_NAME}::cern::testse";

 addFile($cat, "jdl/UserArchive.jdl","Executable=\"CheckInputOuptut.sh\";
InputFile=\"LF:$dir/jdl/Input.jdl\";
OutputArchive={\"my_archive:stdout,stderr,file.out\@${sename}2\"}") or exit(-2);

  my $procDir=executeJDLFile($cat, "jdl/UserArchive.jdl") or exit(-2);#

  print "JOB EXECUTED!!\nChecking if the archive is in the right place\n";

  my (@out)=$cat->execute("whereis", "-r", "$procDir/job-output/stdout");
  
  print "IT IS IN @out\n";
  $out[0] or print "It isn't in any se!!!\n" and exit(-2);
  $out[0]=~ /^${sename}2$/i or print "It's in the wrong one!!\n" and exit(-2);
}
