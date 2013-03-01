use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("catalogue/003-add") or exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
	or exit(-1);
  my ($dir) = $cat->execute("pwd") or exit(-2);

  addFile(
	$cat, "jdl/collectionSingle.jdl", "Executable=\"CheckInputOuptut.sh\";
InputData={\"$dir/collections/manual_collection\"};
"
  ) or exit(-2);
  print "And now, let's execute the job\n";

  #  executeJDLFile($cat,  "jdl/collectionSingle.jdl") or exit(-2);
  print "The first one went fine :)\nLet's try splitting it...\n";
  addFile(
	$cat, "jdl/collectionSplit.jdl", "Executable=\"CheckInputOuptut.sh\";
InputDataCollection={\"LF:$dir/collections/manual_collection\"};
Split=\"file\";
"
  ) or exit(-2);

  my ($id) = $cat->execute("submit", "jdl/collectionSplit.jdl") or exit(-2);

  print "Job submitted!!
\#ALIEN_OUTPUT $id\n";

}
