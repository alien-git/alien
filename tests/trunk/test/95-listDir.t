use strict;
use AliEn::UI::Catalogue;

eval "require AliEn::Service::ClusterMonitor" 
  or print "Error requiring the package\n $! $@\n" and exit(-2);

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

includeTest("93-cpdir") or exit(-2);

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
