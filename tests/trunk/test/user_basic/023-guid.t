use strict;
use AliEn::GUID;
use AliEn::UI::Catalogue;
use Fcntl ':flock';

my $gen=new AliEn::GUID or exit(-2);

my $file="/tmp/alien.test.81.$$";

open (my $FILE, ">", $file);
my $list={};
my $clients=20;
my @processes;
my $times=1000;
print "Creating $clients*$times guid\n";
for (my $j=$clients;$j;$j--){
  my $pid=fork();
  if (!$pid) {
    for (my $i=$times;$i;$i--) {
      flock($FILE,LOCK_EX) or print "ERROR BLOCKING\n";
      print $FILE  $gen->CreateGuid()."\n";
      flock($FILE,LOCK_UN) or print "ERROR UNLOCKING\n";
    }
    exit;
  }
  push @processes, $pid;
}
foreach (@processes){
  waitpid($_,0);
}


close $FILE;
my $lines=`cat $file |wc -l`;
my $diflines=`cat $file |sort -u |wc -l`;
unlink $file;
print "GOT FILE $file\n$lines\n$diflines";
$lines eq $diflines or print "Error there were some repeated guids!!\n" and exit(-2);

print "Ok, let's put two entries in the catalogue\n";
my $cat=AliEn::UI::Catalogue->new({user=>"admin"}) or exit(-2);

my $binDir="$ENV{ALIEN_ROOT}/bin";
#system("su - alienmaster -c '$binDir/alien StopCatalogueOptimizer'"); 

$cat->execute("rmdir", "-rf","guid");
$cat->execute("mkdir", "-p", "guid") or exit (-2);
$cat->execute("debug", "GUID,Catalogue");
my $total=30;
my $j=$total;
#First, let's check that the guids are generated automatically
my $guids={};
my $guid;
while ($j) {
  $cat->execute("touch", "guid/$j") or exit(-2);
  ($guid)=$cat->execute("lfn2guid", "guid/$j") or exit(-2);
  $guids->{$guid} and print "GUID REPEATED!!\n" and exit(-2);
  $guids->{$guid}=1;
  $j--;
}

print "DONE\n";


