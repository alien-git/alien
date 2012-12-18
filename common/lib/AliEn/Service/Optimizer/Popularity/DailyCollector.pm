package AliEn::Service::Optimizer::Popularity::DailyCollector;
use strict;

use AliEn::Service::Optimizer::Popularity;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Popularity");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;
  my $interval = $AliEn::Service::Optimizer::Popularity::interval;
  my $hourlyCompletedTasks = 24/$interval; # the interval is 2 hours
  my $collectorName = "DailyCollector";
  my $method="info";
  
  $silent and $method="debug" and  @info=1;
  
  # $self->{SLEEP_PERIOD}=3600*24; # once in 24 hours 
  
  $self->{SLEEP_PERIOD}=30; 
  
  $self->$method(@info, "The $collectorName optimizer starts");

  my $tasks = $self->getCollectorTasks("$collectorName", $hourlyCompletedTasks);

  if ($tasks and $tasks->[0])
  {
    foreach my $f (@$tasks){
      $self->info ("=== Starting $collectorName for the day $f->{day} ===");
      my $result = $self->fillDailyPopTable($f->{day});
	  if ($result){ 
	     $self->removeCollectorTask("$collectorName", "$f->{day}");
	  }
      else{
      	$self->setCollectorTaskPending("$collectorName", "$f->{day}") or return 0;
      }
    }
    
    $self->$method(@info, "The $collectorName optimizer finished.");
    return; 
  }
  else {$self->info("There are not pending tasks for $collectorName");}
  return;
}

sub fillDailyPopTable
{
  my $self=shift;
  my $day = shift;
  
  my $fileNameQuery = "SELECT fileName, seId,  DATE(accessTime), SUM(nbReadOp), SUM(nbWriteOp),
  SUM(nbReadFailure), SUM(nbWriteFailure), SUM(nbUserSuccess), SUM(nbUserFailure) FROM filePopHourly 
  WHERE date(accessTime)='$day' GROUP BY fileName, seId";
  
  $self->{DB}->do("INSERT IGNORE INTO filePopDaily (fileName, seId, accessDate, nbReadOp, nbWriteOp, 
  nbReadFailure, nbWriteFailure, nbUserSuccess, nbUserFailure) $fileNameQuery") or $self->info("Could not do insert") and return 0;
   
  my $categoryQuery = "SELECT categoryId, userId, sum(nbReadOp), sum(nbWriteOp), DATE(accessTime) FROM categoryPopHourly 
  where DATE(accessTime) = '$day' GROUP BY categoryId, userId";

  $self->{DB}->do("INSERT IGNORE INTO categoryPopDaily (categoryId, userId, nbReadOp, nbWriteOp, accessDate) $categoryQuery") or $self->info("Could 
not do insert into categories table") and return 0;

  $self->info("The data is successfully inserted into daily popularity table!");
    
return 1;  
} 

1;
