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
	$cat, "jdl/stage.jdl", "Executable=\"CheckInputOuptut.sh\";
InputData={\"$dir/jdl/stage.jdl\"};
InputDataCollection={\"LF:$dir/collections/manual_collection\"};
PreStage=1;
"
  ) or exit(-2);
  print "And now, let's execute the job\n";

  my ($id) = $cat->execute("submit", "jdl/stage.jdl") or exit(-2);

  print "Job submitted!!
\#ALIEN_OUTPUT $id";

}
