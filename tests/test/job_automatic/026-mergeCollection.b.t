use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;
  includeTest("job_automatic/008-split") or exit(-2);

  my $id         = shift or print "No job to analyze!!\n"      and exit(-2);
  my $collection = shift or print "There is no collection!!\n" and exit(-2);

  my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
	or exit(-1);

  print "Let's check if it is in fact a collection\n";
  my ($type) = $cat->execute("type", $collection);
  $type eq "collection" or print "It is not a collection!! ($type)\n" and exit(-2);

  print "And let's see if it has any files inside\n";

  my ($list) = $cat->execute("listFilesFromCollection", $collection)
	or exit(-2);
  use Data::Dumper;
  print Dumper($list);
  print "It has $list ($#$list)\n";
  $#$list eq 4 or print "There aren't 5 files!!\n" and exit(-2);
  print "ok\n";
}
