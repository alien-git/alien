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
    $self->{SLEEP_PERIOD}=60*15; # once in 15 minutes
  
  $self->$method(@info, "The $collectorName optimizer starts");

  my $tasks = $self->getCollectorTasks("$collectorName");

  if ($tasks and $tasks->[0])
  {
    foreach my $f (@$tasks){
      my $result = $self->fillHourlyPopTable($f->{startTime}, $interval);
	  if ($result){
	  	$self->sendSummeryToMonalisa ($f->{startTime}, $interval) and 
	  	$self->increaseCompletedTasksNumber("DailyCollector", $f->{startTime}) and 
		$self->removeCollectorTask("$collectorName", $f->{startTime});
	  }
      $self->setCollectorTaskPending($collectorName, $f->{startTime});
    }
    
    $self->$method(@info, "The $collectorName optimizer finished.");
    return; 
  }
  else {$self->info("There are not pending tasks for $collectorName");}
  return;
}

sub fillHourlyPopTable
{
  my $self = shift; 
  my $startTime = shift;
  my $interval  = shift; 
  my $buffer = "fileAccessInfo";
  
  my $fileNameQuery = "SELECT fileName, seId, DATE_ADD('$startTime', INTERVAL $interval HOUR), 
  SUM(case when success=1 and operation='read' then 1 else 0 end) AS nbReadSuccess,
  SUM(case when success=1 and operation='write' then 1 else 0 end)  AS nbWriteSuccess,
  SUM(case when success=0 and operation='read' then 1 else 0 end) AS nbReadFailure ,
  SUM(case when success=0 and operation='write' then 1 else 0 end) AS nbWriteFailure,
  COUNT(DISTINCT IF (success=1, userId, NULL )) AS nbUserSuccess,
  COUNT(DISTINCT IF (success=0, userId, NULL )) AS nbUserFailure  FROM $buffer WHERE accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR)
  GROUP BY fileName, seId";
  
  $self->{DB}->do("INSERT IGNORE INTO filePopHourly (fileName, seId, accessTime, nbReadOp, nbWriteOp, nbReadFailure, nbWriteFailure, nbUserSuccess, nbUserFailure) $fileNameQuery") 
  or $self->info("Could not do insert") and return 0;
  
  my $LFNs = $self->{DB}->queryColumn("SELECT fileName FROM $buffer WHERE accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR)
  GROUP BY fileName");
  
  if ($LFNs and $LFNs->[0])
  {
    foreach my $lfn (@$LFNs){
      if ($lfn =~ /^\/alice\/data\/.*\/*(LHC\d{2}[^\/]+)\/.+$/){
	  	my $period = $1;
	  	$self->{DB}->do("INSERT IGNORE INTO periods (periodName) VALUES (\"$period\")") and
	  	$self->{DB}->do("INSERT IGNORE INTO categoryPattern (perId, categoryName, pattern) 
	  	VALUES
	  	( (SELECT perId FROM periods WHERE periodName=\"$period\"), \"ESD\", \"\^\/alice\/data\/.*\/*$period\/.*\/*(ESDs\/\.+\$\|AliESDs\.*\\.root\$)\"), 
	  	( (SELECT perId FROM periods WHERE periodName=\"$period\"), \"AOD\", \"\^\/alice\/data\/.*\/*$period\/.*\/*(ESDs\/.*\/*AOD.*\$\|AliAOD\.*\\.root\$)\")") 
	  	or $self->info("Could not do insert") and return 0;
	  }
	  elsif ($lfn =~ /^\/alice\/sim\/.*\/*(LHC\d{2}[^\/]+)\/.+$/) {
	  	my $period = $1;
	  	$self->{DB}->do("INSERT IGNORE INTO periods (periodName) VALUES (\"$period\")") and
	  	$self->{DB}->do("INSERT IGNORE INTO categoryPattern (perId, categoryName, pattern) 
	  	VALUES 
	  	( (SELECT perId FROM periods WHERE periodName=\"$period\"), \"ESD\", \"\^\/alice\/sim\/.*\/*$period\/.*\/*AliESD\.*\\.root\$\"),
	  	( (SELECT perId FROM periods WHERE periodName=\"$period\"), \"AOD\", \"\^\/alice\/sim\/.*\/*$period\/.*\/*AliAOD\.*\\.root\$\")")
	  	or $self->info("Could not do insert") and return 0;
	  };
	};
  };
  
  my $categoryQuery = "SELECT categoryId, userId, count(*) AS nbReadOp, accessTime FROM 
  (SELECT fileName, max(categoryId) as categoryId, userId, DATE_ADD('$startTime', INTERVAL $interval HOUR) as accessTime
  from $buffer left join categoryPattern on (fileName rlike pattern) WHERE accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR) AND success=1 AND operation='read' GROUP BY fileName, userId) bb GROUP BY categoryId, userId";
  
  $self->{DB}->do("INSERT IGNORE INTO categoryPopHourly (categoryId, userId, nbReadOp, accessTime) $categoryQuery") or $self->info("Could not do insert into categories table") and return 0;

  $self->debug(1, "The data is successfully inserted into hourly popularity table!");
  
  $self->info("Now we are going to clean $buffer table");
  $self->{DB}->delete("$buffer", "accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR)") or $self->info ("Could not clean the buffer!") and return 0;
  $self->info("The data is successfully deleted!");
  
return 1;  
} 



sub sendSummeryToMonalisa
{
  my $self = shift;
  my $startTime = shift;
  my $interval  = shift; 
  my $method="info";
 
  # GETTING categoryName, nbAccesses, nbAccesses_R, nbUsers
  my $result =$self->{DB}->query("SELECT IF(periodName LIKE 'OTHER', categoryName, CONCAT(periodName, '_', categoryName)) as categoryName, sum(nbReadOp) as nbAccesses, sum(nbReadOp)/($interval*3600) as nbAccesses_R, count(DISTINCT userId) as nbUsers FROM categoryPopHourly 
  left join categoryPattern on (categoryPattern.categoryId = categoryPopHourly.categoryId) left join periods on (categoryPattern.perId = periods.perId) WHERE accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR) GROUP BY categoryPopHourly.categoryId") or return 0;
  if ($result)
  {
  	for my $href (@$result) {
      	my $categoryName = $href->{categoryName};
    	delete $href->{categoryName};
    	#send to MonALISA
    	$self->{MONITOR}->sendParameters("AliEn_PopularityService", "$categoryName", $href);
    }
  }
  
  # Corrupted files:
  # GETTING fileName, seName, nbReadFailures, nbReadFailures_R, nbUsers
  
  my $result2 =$self->{DB}->query("SELECT fileName, seName, SUM(nbReadFailure) as nbReadFailures, SUM(nbReadFailure)/($interval*3600) as nbReadFailures_R, SUM(nbUserFailure) as nbUsers FROM filePopHourly LEFT JOIN seInfo on (seInfo.seId = filePopHourly.seId)
  WHERE accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR) and nbReadOp = 0 and nbReadFailure !=0 GROUP BY fileName, seName") or return 0;
  if ($result2)
  {
  	for my $href (@$result2) {
      	#send to MonALISA
    	$self->{MONITOR}->sendParameters("AliEn_CorruptedFiles_Details", "Files", $href); 
    }
  }
   
  # Failures per-SE:
  # GETTING seName, nbReadFailures, nbReadFailures_R
  
  my $result3 =$self->{DB}->query("SELECT seName, SUM(nbReadFailure) as nbReadFailures, SUM(nbReadFailure)/($interval*3600) as nbReadFailures_R FROM filePopHourly LEFT JOIN seInfo on (seInfo.seId = filePopHourly.seId) 
WHERE accessTime BETWEEN '$startTime' AND DATE_ADD('$startTime', INTERVAL $interval HOUR) AND nbReadOp = 0 and nbReadFailure !=0 GROUP BY seName") or return 0;
  if ($result3)
  {
  	for my $href (@$result3) {
      	my $seName = $href->{seName};
    	delete $href->{seName};
    	#send to MonALISA
    	$self->{MONITOR}->sendParameters("AliEn_CorruptedFiles", "$seName", $href);
    }
  }

  $self->{LOGGER}->$method("MonALISA",  "Sent info to local MonALISA agent");

  return 1;
}

1;
