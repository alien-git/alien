use strict;
use Time::HiRes qw (time);
use AliEn::UI::Catalogue::LCM::Computer;

my $c=AliEn::UI::Catalogue::LCM::Computer->new({USER=>"newuser"
				}) or exit(-2);
use Data::Dumper;

my $dir="performance/";

my ($user) = $c->execute("whoami");

$c->execute("silent") or exit(-2); 
my $step=50;

my @jobs = $c->execute("top", "-all_status", "-user", $user);
my $total=scalar(@jobs);

my ($totalTime, $mean)=topJobs($c, $dir, $total, $step, @jobs);

print "Doing top $total entries -> $totalTime seconds ( $mean ms/top)\n";
$c->close();


sub topJobs {
  my $c=shift;
  my $dir=shift;
  my $total=shift;
  my $step=shift;  
  my $jobs=shift;
  my $failed=0;
  
  my $start=0;
  print "Starting to top\n";
  open (FILE, ">".$dir."top.$total.$$.dat") or print "Error opening the file\n" and exit(-2);
  my $before=time;
  foreach my $job (@jobs) {
    $c->execute("top", "-id", $job->{queueId}) or $failed++;
    $start++;
    if ( $start%$step eq "0") {
      my $intermediate=time();
      my $insert=(1000.0*($intermediate-$before))/$start; 
      print "\tSo far, $start -> ". ($intermediate -$before) . " seconds ( $insert ms/top)\n";
      print FILE "$start $insert\n";
    }
  } 
  print FILE "Failed top: $failed \n";

  my $after=time();
  my $time=$after-$before;
  my $mean=1000.0*$time/$total;
  close FILE;
  return ($time, $mean);
}
