use strict;
use AliEn::UI::Catalogue;

use AliEn::Service::ClusterMonitor; 

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("catalogue/013-cpdir") or exit(-2);

my $c=AliEn::UI::Catalogue->new ({user=>'newuser'}) or exit(-2);
$c->execute("rmdir", "-rf" ,"listDir");
$c->execute("mkdir", "-p", 'listDir') or exit(-1);

for(my $i=5; $i; $i--){
  $c->execute("touch", "listDir/file$i") or exit(-2);
}
$c->execute("mkdir", "listDir/subdir1") or exit(-2);
compareDirectory($c,"listDir", "file1","file2", "file3", "file4", "file5", "subdir1")
  or exit(-2);

compareDirectory($c,"listDir/", "file1","file2", "file3", "file4", "file5", "subdir1")
  or exit(-2);

print "DONE!!\nok\n";
