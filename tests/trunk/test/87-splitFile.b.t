use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{

  my $id=shift or print "No job to analyze!!\n" and exit(-2);

  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("86-split") or exit(-2);


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);


  my ($procDir)=checkSubJobs($cat, $id,3) or exit(-2); 

  print "ok\n";
}
