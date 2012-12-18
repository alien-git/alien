use strict;
use Time::HiRes qw (time);
use AliEn::UI::Catalogue;

my $c=AliEn::UI::Catalogue->new({USER=>"admin", role=>"admin",
				 USE_PROXY=>0, passwd=>"pass",
#				 debug=>4
				}) or exit(-2);
use Data::Dumper;

my $dir="/test/performance.$$";
$c->execute("mkdir", "-p", $dir) or exit(-2);
#print "Connected\n";
#exit;


$c->execute("silent") or exit(-2); 
my $total=5000;
my $step=500;
my ($totalTime, $mean)=insertEntries($c, $dir, $total, $step);

print "Inserting $total entries -> $totalTime seconds ( $mean ms/insert)\n";
$c->close();


sub insertEntries {
  my $c=shift;
  my $dir=shift;
  my $total=shift;
  my $step=shift;

  my $start=$total;
  print "Starting to insert\n";
  open (FILE, ">inserting.$total.dat") or print "Error opening the file\n" and exit(-2);
  my $before=time;
  while ($start) {
    $c->execute("touch", "$dir/file$start") or exit(-2);
#    $c->execute("add", "-r", "$dir/file$start", "/etc/passwd", 1700);
    $start--;
    if ( $start%$step eq "0") {
      my $intermediate=time();
      my $part=$total-$start;
      my $insert=(1000.0*($intermediate-$before))/$part; 
      print "\tSo far, $part -> ". ($intermediate -$before) . " seconds ( $insert ms/insert)\n";
      print FILE "$part $insert\n";
    }
  }

  my $after=time();
  my $time=$after-$before;
  my $mean=1000.0*$time/$total;
  close FILE;
  return ($time, $mean);
}
