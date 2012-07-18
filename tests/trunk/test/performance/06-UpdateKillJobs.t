use strict;
use Time::HiRes qw (time);
use AliEn::UI::Catalogue::LCM::Computer;
use Data::Dumper;
use AliEn::Database::TaskQueue;
my $c=AliEn::UI::Catalogue::LCM::Computer->new({USER=>"newuser"
				}) or exit(-2);


my $dir="performance/";

my ($user) = $c->execute("whoami");

#$c->execute("silent") or exit(-2);
my $step=50;

my @jobs = $c->execute("top", "-all_status", "-user", $user);
my $total=scalar(@jobs);

my ($totalTime, $mean)=updateJobs($c, $dir, $total, $step, @jobs);
print "Updating $total entries -> $totalTime seconds ( $mean ms/update)\n";

($totalTime, $mean)=killJobs($c, $dir, $total, $step, @jobs);
print "Killing $total entries -> $totalTime seconds ( $mean ms/kill)\n";

$c->close();


sub updateJobs {
  my $c=shift;
  my $dir=shift;
  my $total=shift;
  my $step=shift;  
  my $jobs=shift;
my $host=Net::Domain::hostfqdn();
my $port=$ENV{ALIEN_MYSQL_PORT} ||3307;
my $d=AliEn::Database::TaskQueue->new({DRIVER=>"mysql", HOST=>"$host:$port", DB=>"processes", "ROLE", "admin", PASSWD=>"pass"}) 
  or print "Error connecting to the database\n" and exit(-2);
  
  my $start=0;
  print "Starting to update\n";
  open (FILE, ">".$dir."update.$total.$$.dat") or print "Error opening the file\n" and exit(-2);
  my $before=time;
  foreach my $job (@jobs) {
  	#$d->update("QUEUE", {statusId => 23}, "queueId=$job->{queueId}") or exit(-2);
    $d->updateStatus($job->{queueId}, "%", "UPDATING") or exit(-2);
    $start++;
    if ( $start%$step eq "0") {
      my $intermediate=time();
      my $insert=(1000.0*($intermediate-$before))/$start; 
      print "\tSo far, $start -> ". ($intermediate -$before) . " seconds ( $insert ms/update)\n";
      print FILE "$start $insert\n";
    }
    $start or last;
  }

  my $after=time();
  my $time=$after-$before;
  my $mean=1000.0*$time/$total;
  close FILE;
  $d->close();
  return ($time, $mean);
}

sub killJobs {
  my $c=shift;
  my $dir=shift;
  my $total=shift;
  my $step=shift;  
  my $jobs=shift;
  
  my $start=0;
  print "Starting to kill\n";
  open (FILE, ">".$dir."kill.$total.$$.dat") or print "Error opening the file\n" and exit(-2);
  my $before=time;
  foreach my $job (@jobs) {
    $c->execute("kill", $job->{queueId}) or exit(-2);
    $start++;
    if ( $start%$step eq "0") {
      my $intermediate=time();
      my $insert=(1000.0*($intermediate-$before))/$start; 
      print "\tSo far, $start -> ". ($intermediate -$before) . " seconds ( $insert ms/kill)\n";
      print FILE "$start $insert\n";
    }
    $start or last;
  }

  my $after=time();
  my $time=$after-$before;
  my $mean=1000.0*$time/$total;
  close FILE;
  return ($time, $mean);
}
