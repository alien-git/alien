use strict;
use Time::HiRes qw (time);
use AliEn::UI::Catalogue;
do "$ENV{ALIEN_TESTDIR}/functions.pl";
includeTest("performance/106-performanceInsert") or exit(-2);
  
my $c=AliEn::UI::Catalogue->new({USER=>"admin", role=>"admin", 
				 USE_PROXY=>0, passwd=>"pass",
				}) or exit(-2);
my $dir="/test/performance.$$";
$c->execute("mkdir", "-p", $dir) or exit(-2); 

$c->execute("silent") or exit(-2); 

my $total=400;
my $step=100;
my $numberDirs=10;
my ($totalTime, $mean)=(0,0);
my $tempDir=$numberDirs;
while ($tempDir--) {
  my $subdir="$dir/$tempDir";
  print "Preparing dir $subdir ...";
  $c->execute("mkdir", $subdir) or exit(-2);
  $c->execute("moveDirectory", $subdir) or exit(-2);
  print "done\n";
  my ($partTime, $partMean)=insertEntries($c, $subdir, $total, $step);
  print "Inserting $total entries -> $partTime seconds ( $partMean ms/insert)\n";
  $totalTime+=$partTime;
  $mean+=$partMean;
}
$mean/=$numberDirs;
print "Inserting ". $total*$numberDirs." entries -> $totalTime seconds ( $mean ms/insert)\n";

$c->close();


