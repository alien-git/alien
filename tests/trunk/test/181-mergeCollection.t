use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }



{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",}) or 
    exit (-1);
  my ($dir)=$cat->execute("pwd") or exit(-2);
  my $collection="$dir/mergeCollection";
  $cat->execute("rm", "-silent", $collection);
  addFile($cat, "jdl/MergeCollection.jdl","Executable=\"CheckInputOuptut.sh\";
Split=\"production:1-5\";
OutputFile=\"file.out\";
MergeCollections={\"file.out:$collection\"}" ) or exit(-2);

  my ($id)=$cat->execute("submit", "jdl/MergeCollection.jdl") or exit(-2);
  $cat->close();
  print "ok!!\n
\#ALIEN_OUTPUT $id $collection\n";
}
