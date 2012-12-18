use strict;
use Test;
use AliEn::UI::Catalogue::LCM;

BEGIN { plan tests => 1 }
{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
 
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);

  print "Getting an instance of the catalogue";

  my $cat =AliEn::UI::Catalogue::LCM->new({"role", "newuser"});
  $cat or exit(-2);

  my $lfn="test.jdl";
  addFile($cat, $lfn,"Executable=\"date\";\n") or exit(-2);

  print "Doing the find";
  my @files=$cat->execute("find", ".", "*.jdl") or exit(-2);
  grep (/$lfn$/, @files) or print "The file is not there!!\n" and exit(-2);
  $cat->close;
  ok(1);

}
