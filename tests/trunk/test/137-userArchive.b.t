use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{

  my $id=shift or print "Error getting the id of the job\n" and exit(-2);
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("26-ProcessMonitorOutput") or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  $cat or exit (-1);
  my $sename="$cat->{CONFIG}->{ORG_NAME}::CERN::TESTSE";



  my $procDir=checkOutput($cat, $id) or exit(-2);#

  print "JOB EXECUTED!!\nChecking if the archive is in the right place\n";

  my (@out)=$cat->execute("whereis", "-r", "$procDir/job-output/stdout");
  
  my $resstatus=0;

  $out[0] or print "It isn't in any se!!!\n" and exit(-2);

  print "IT IS IN @out\n";
  for my $se (@out) {
     
     ($se=~ /^${sename}2$/i) and $resstatus=1; 
  }
  $resstatus or print "It's in the wrong one!!\n" and exit(-2);
  $resstatus and  ok(1);
}
