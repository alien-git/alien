use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("catalogue/003-add") or exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM->new({"user", "newuser",})
	or exit(-1);
  my ($dir) = $cat->execute("pwd") or exit(-2);

  $cat->execute("rmdir", "-rf", "collections");
  $cat->execute("mkdir", "-p", "collections") or exit(-2);

  $cat->execute("cd", "collections") or exit(-2);

  addFile($cat, "file1", "First file of the collections")  or exit(-2);
  addFile($cat, "file2", "Second file of the collections") or exit(-2);
  my $f1 = getSize($cat, "file1");
  my $f2 = getSize($cat, "file2");

  print "First, let's do it by hand\n";

  $cat->execute("createCollection", "manual_collection") or exit(-2);

  $cat->execute("addFileToCollection", "file1", "manual_collection") or exit(-2);
  $cat->execute("addFileToCollection", "file2", "manual_collection") or exit(-2);
  my $colSize = getSize($cat, "manual_collection");
  if ($colSize != $f1 + $f2) {
	print "The collection is supposed to have size " . $f1 + $f2 . " , but it is $colSize\n";
	exit(-2);
  }
  print "Let's try to remove one of the files\n";
  $cat->execute("removeFileFromCollection", "file2", "manual_collection") or exit(-2);
  $colSize = getSize($cat, "manual_collection");
  if ($colSize != $f1) {
	print "The collection is supposed to have size $f1 , but it is $colSize\n";
	exit(-2);
  }

  $cat->execute("addFileToCollection", "file2", "manual_collection") or exit(-2);
  print "After removing the file, everything looks fine\n";
  my $size = getSize($cat, "manual_collection");
  print "And the size is $size\n";
  print "Ok, let's do an automatic collection\n";
  $cat->execute("find", "-c", "automatic_collection", ".", "file") or exit(-2);
  my $auto = getSize($cat, "automatic_collection");
  if ($auto != $f1 + $f2) {
	print "The automatic collection is supposed to have size " . ($f1 + $f2) . " , but it is $auto\n";
	exit(-2);
  }

  print "And the automatic collection has the right size\n";
  $dir = "/tmp/alien_col.$$";

  my ($files) = $cat->execute("get", "automatic_collection", $dir)
	or print "Error getting the files\n" and return;
  -d $dir or print "$dir is not a directory\n" and return;
  print "Got the files @$files\n";
  system("rm", "-rf", "$dir");
  print "OK!!";
}

sub getSize {
  my $cat  = shift;
  my $file = shift;
  my ($info) = $cat->execute("ls", "-silent", "-ilz", $file) or return;
  return $info->{size};
}
