package AliEn::Service::Optimizer::Popularity::HourlyCollector;
use strict;

use AliEn::Service::Optimizer::Popularity;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Popularity");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;
  my $interval  = $AliEn::Service::Optimizer::Popularity::interval;
  my $collectorName = "HourlyCollector";
  my $method="info";
  
  $silent and $method="debug" and  @info=1;
  
  # $self->{SLEEP_PERIOD}=3600*24; # once in 24 hours 
    $self->{SLEEP_PERIOD}=60*10; # once in 10 minutes
  
  $self->$method(@info, "The $collectorName optimizer starts");

  my $tasks = $self->getCollectorTasks("$collectorName");

  if ($tasks and $tasks->[0])
  {
    foreach my $f (@$tasks){
      my $result = $self->fillHourlyPopTable($f->{startTime}, $interval);
	  if ($result){
	  $self->increaseCompletedTasksNumber("DailyCollector", $f->{startTime}) and 
	  $self->removeCollectorTask("$collectorName", $f->{startTime}) and
	  	$self->$method(@info, "The $collectorName optimizer finished.") and return;
	  }
      $self->setCollectorTaskPending($collectorName, $f->{startTime}) or return;
    }
    
    $self->$method(@info, "The $collectorName optimizer finished.");
    return; 
  }
  else {$self->info("There are not pending tasks for $collectorName");}
  return;
}

sub fillHourlyPopTable
{
  my $self=shift;
  my $startTime = shift;
  my $interval  = shift; 
  my $buffer = "fileAccessInfo";
  my $catId = 0;

  my $fileNameQuery = "SELECT fileName, seId, DATE_ADD('$startTime', INTERVAL $interval HOUR), 
  SUM(case when success=1 and operation='read' then 1 else 0 end) AS nbReadSuccess,
  SUM(case when success=1 and operation='write' then 1 else 0 end)  AS nbWriteSuccess,
  SUM(case when success=0 and operation='read' then 1 else 0 end) AS nbReadFailure ,
  SUM(case when success=0 and operation='write' then 1 else 0 end) AS nbWriteFailure,
  COUNT(DISTINCT IF (success=1, userId, NULL )) AS nbUserSuccess,
  COUNT(DISTINCT IF (success=0, userId, NULL )) AS nbUserFailure  FROM $buffer WHERE accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR)
  GROUP BY fileName, seId";
  
  $self->{DB}->do("INSERT IGNORE INTO filePopHourly (fileName, seId, accessTime, nbReadOp, nbWriteOp, 
  nbReadFailure, nbWriteFailure, nbUserSuccess, nbUserFailure) $fileNameQuery") or $self->info("Could not do insert") and return 0;
   
  my $categoryQuery = "SELECT categoryId, userId, nbReadOp, nbWriteOp, accessTime FROM 
  (SELECT fileName, min(priority) as priority, userId, SUM(case when success=1 and operation='read' then 1 else 0 end) AS nbReadOp,
  SUM(case when success=1 and operation='write' then 1 else 0 end) AS nbWriteOp, DATE_ADD('$startTime', INTERVAL 2 HOUR) as accessTime
  from $buffer left join categoryPattern on (fileName rlike pattern) WHERE accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL 2 HOUR) GROUP BY fileName) bb join categoryPattern using (priority) GROUP BY categoryId, userId";
   
  $self->{DB}->do("INSERT IGNORE INTO categoryPopHourly (categoryId, userId, nbReadOp, nbWriteOp, accessTime) $categoryQuery") or $self->info("Could not do insert into categories table") and return 0;

  $self->debug(1, "The data is successfully inserted into hourly popularity table!");
  
  $self->info("Now we are going to clean $buffer table");
  $self->{DB}->delete("$buffer", "accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR)") or $self->info ("Could not clean the buffer!") and return 0;
  $self->info("The data is successfully deleted!");
  
return 1;  
} 

1;
