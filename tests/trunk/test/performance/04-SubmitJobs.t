use strict;
use Time::HiRes qw (time);
use AliEn::UI::Catalogue::LCM::Computer;

my $c=AliEn::UI::Catalogue::LCM::Computer->new({USER=>"newuser"
				}) or exit(-2);
use Data::Dumper;

my $dir="/test/performance.$$";


$c->execute("silent") or exit(-2); 
my $total=500;
my $step=50;

system ("alien StopCE"); # necesario ?

my ($totalTime, $mean)=submitJobs($c, $dir, $total, $step);

print "Submitting $total entries -> $totalTime seconds ( $mean ms/submit)\n";
$c->close();


sub submitJobs {
  my $c=shift;
  my $dir=shift;
  my $total=shift;
  my $step=shift;

  my $start=$total;
  print "Starting to submit\n";
  open (FILE, ">submiting.$total.dat") or print "Error opening the file\n" and exit(-2);
  my $before=time;
  while ($start) {
    $c->execute("submit", "jdl/date.jdl") or exit(-2);
    $start--;
    if ( $start%$step eq "0") {
      my $intermediate=time();
      my $part=$total-$start;
      my $insert=(1000.0*($intermediate-$before))/$part; 
      print "\tSo far, $part -> ". ($intermediate -$before) . " seconds ( $insert ms/submit)\n";
      print FILE "$part $insert\n";
    }
  }

  my $after=time();
  my $time=$after-$before;
  my $mean=1000.0*$time/$total;
  close FILE;
  return ($time, $mean);
}
