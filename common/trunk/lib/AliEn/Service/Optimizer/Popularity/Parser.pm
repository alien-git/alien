package AliEn::Service::Optimizer::Popularity::Parser;
use strict;

use AliEn::Service::Optimizer::Popularity;
use Time::HiRes;
use Date::Parse;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Popularity");
my $interval  = $AliEn::Service::Optimizer::Popularity::interval;
my $INTtime = $interval*3600;
my $name='Parser';
my $tableName = "collectors";

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;
  
  # $self->{SLEEP_PERIOD}=3600*24; # once in 24 hours 
  
  $self->{SLEEP_PERIOD}=60*10; # every 10 minute 
  
  
  $self->$method(@info, "The Parser optimizer starts");
  $self->getCollectorTask($name, $tableName);
  
 return; 
}

 sub getCollectorTask {
    my $self = shift;
    my $collectorName = shift;
    my $tableName = shift;
    my $currTime = time;
    my $task = $self->{DB}->query("select startTime from $tableName where name='$collectorName' and actions='0'");
    if ($task and $task->[0]) {
      my $startTime=$task->[0]->{startTime};
      my $epochSTARTtime = str2time($startTime);
      my $portion = $epochSTARTtime + $INTtime;
      my $LogFile;
      my $LogFileDate;
      if ( $startTime =~ m{^(\d{4})-(\d{2})-(\d{2})} ){
	 	  $LogFileDate = "$1/$2/$3";
    	  $LogFile = "$self->{CONFIG}->{LOG_DIR}/Authen_ops/" . $LogFileDate . "/operations";
      }
      if ($portion <= $currTime ) {
      	 if (-e $LogFile){
      	    my $result = $self->FILLfileAccessInfoTable($startTime, $currTime, $epochSTARTtime, $portion, $LogFile, $LogFileDate);
      	    if ($result){
      	
      	       $self->{DB}->do("update collectors set actions='0', startTime= DATE_ADD('$startTime',INTERVAL  $interval HOUR)  where name='$name'") and 
	          
	           $self->info("The $collectorName optimizer finished.") and return;
	        }
	        else {
	           $self->setCollectorTaskPending($collectorName, $startTime) and return; 
	        }
      	 }
      	 else {
      	 	$self->info("There were not file accesses at this time period!");
      	 	$self->{DB}->do("update collectors set actions='0', startTime= DATE_ADD('$startTime',INTERVAL  $interval HOUR)  where name='$name'") 
      	 	or $self->info("Could not update collectors table") and return;
      	 }
      }
    }
  	$self->info("The $collectorName optimizer finished");
 	return;
}

sub FILLfileAccessInfoTable 
{
	 my $self=shift;
	 my $startTime=shift;
	 
	 $self->{DB}->do("UPDATE collectors SET actions='1' WHERE startTime='$startTime' AND name='$name'");
	 my $currentTime=shift;
	 my $epochSTARTtime = shift;
	 my $portion = shift;
	 my $LogFile = shift;
	 my $LogFileDate = shift;
	 my $success;
	 my $accessTime;
	 my $operation;
	 my $fileName;
	 my $seName;
	 my $userName;
	 my $epochaccessTIME;
	 
     open my $pipe, "-|", "$ENV{ALIEN_ROOT}/bin/logtail", "$LogFile" or die "could not start logtail on $LogFile";

	 my @data = <$pipe>;

     
 if (@data) {

 # READFILE:
 	 foreach my $line (@data) {
	    if ($line =~ m/authorize/){
		 	if ( ($line !~ m/FAILED/) ){
		   		($accessTime, $userName, $operation, $fileName, $seName) = ($line =~ m{^(\d+:\d+:\d+).+\'(\w+)\s\w+\s(\w+)\s(.+)\s(.+)\'$});
		   		$success=1;
		  	}  
		  	else {
		  		($accessTime, $userName, $operation, $fileName, $seName) = ($line =~ m{^(\d+:\d+:\d+).+\'(\w+)\s\w+\s\w+\s(\w+)\s(.+)\s(.+)\'$});
		   		$success = 0;
		  	}
		  	if ($accessTime and $userName and $operation and $fileName and $seName)
		  	{
		  		$accessTime = $LogFileDate . " " . $accessTime;
		  		$epochaccessTIME=str2time($accessTime);
		  		$self->{DB}->do("INSERT IGNORE INTO userInfo (userName) values ('$userName')") or return;
		  		$self->{DB}->do("INSERT IGNORE INTO seInfo (seName) values ('$seName')") or return;
		  	

   		  		  my $userId = $self->{DB}->queryRow("SELECT userId from userInfo where userName='$userName'")->{userId} ;
		      	          my $seId = $self->{DB}->queryRow("SELECT seId from seInfo where seName='$seName'")->{seId}; 
		               
				  my $result=$self->{DB}->do("INSERT IGNORE INTO fileAccessInfo (fileName, success, userId, accessTime, seId, operation) values ('$fileName', $success, $userId, '$accessTime', $seId, '$operation')");

 				  if ($result) {
                                   $self->{DB}->do("insert ignore into collectors (name, actions, startTime) values ('HourlyCollector', '0' ,'$startTime')") 
                                   or $self->info("Could not insert a task for HourlyCollector") and return;
                                  }
                                  else {
                                   $self->info("Could not do insertions") and return;
                                  }
#                                  next READFILE;
#                                }
		  	}
	    }

	 }
  return 1;
 } 
 else {
	$self->info("The file is empty");
    if (($currentTime - $epochSTARTtime) >= 3600){
     $self->info("There were not file accesses at this time period!");
     $self->{DB}->do("update collectors set actions='0', startTime= DATE_ADD('$startTime',INTERVAL  $interval HOUR)  where name='$name'") 
      	or $self->info("Could not update collectors table") and return;	
	
    }
    my $acctime = $self->{DB}->queryColumn("select MIN(accessTime) from fileAccessInfo");
	if ($acctime and $acctime->[0]) {
    
     my $result2 = $self->{DB}->do("INSERT IGNORE INTO collectors (name,actions, startTime) VALUES ('HourlyCollector', '0', '$acctime->[0]')");
     $result2 or return;
	}
  return; 
 }
 
}
1;

