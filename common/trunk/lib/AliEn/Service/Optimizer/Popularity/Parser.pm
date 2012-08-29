package AliEn::Service::Optimizer::Popularity::Parser;
use strict;

use AliEn::Service::Optimizer::Popularity;
use Time::HiRes;
use Date::Parse;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Popularity");
my $interval  = $AliEn::Service::Optimizer::Popularity::interval;
my $INTtime = $interval*3600;
my $currTime = time;
my $name='Parser';
my $tableName = "collectors";

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  
  my @info;


  my $method="info";
  $silent and $method="debug" and  @info=1;
  
  # $self->{SLEEP_PERIOD}=3600*24; # once in 24 hours 
  
  $self->{SLEEP_PERIOD}=60*10; # every 10 minutes 
  
  $self->$method(@info, "The Parser optimizer starts");
  
 $self->getCollectorTask($name, $tableName);
  
 return; 
}

 sub getCollectorTask {
    my $self = shift;
    my $collectorName = shift;
    my $tableName = shift;
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
        	 $self->FILLfileAccessInfoTable($startTime, $epochSTARTtime, $portion, $LogFile, $LogFileDate);
      	}
      	my $result= $self->{DB}->do("update collectors set actions='0', startTime= (SELECT DATE_ADD(startTime,INTERVAL  $interval HOUR))  where name='$name'");
      	$result or $self->info("Could not update collectors table") and return;
     }
    }
  	# $self->$method(@info, "The Parser optimizer finished");
 	return;
}

sub FILLfileAccessInfoTable 
{
 my $self=shift;
 my $startTime=shift;
 
 $self->{DB}->do("UPDATE collectors SET actions='1' WHERE startTime='$startTime' AND name='$name'");
 my $epochSTARTtime = shift;
 my $portion = shift;
 my $LogFile = shift;
 my $LogFileDate = shift;
 my @time = localtime();
 my $success=1;
 my $fail=0;
 my $accessTime;
 my $operation;
 my $fileName;
 my $seName;
 my $userName;
 my $accTime;
 my $epochaccessTIME;
 # This is the interval of time that Parser exrtacts info from Authen
 open(FILE, "$LogFile") or die("Unable to open file");
  my @data = <FILE>;
  close(FILE);
  READFILE:
foreach my $line (@data) {
   if ( ($line =~ m/authorize/) && ($line !~ m/FAILED/) ){
   	 
      if ($line =~ m{operation: (.*)FileName:}){
          $operation = $1;
      }
      if ($line =~ m{FileName: (.*)seName}){
          $fileName = $1;
      }
      if ($line =~ m{seName: (.*)userName:}){
          $seName = $1;
          $self->{DB}->do("INSERT IGNORE INTO seInfo (seName) values ('$seName')");
      }
    
     if ($line =~ m{userName: (.*)}){
          $userName = $1;
          $self->{DB}->do("INSERT IGNORE INTO userInfo (userName) values ('$userName')");
     } 
 
     if ($line =~ m{^(\d{2}):(\d{2}):(\d{2})}) {
         $accTime = "$1:$2:$3 ";
         $accessTime = $LogFileDate . " " . $accTime;
         $epochaccessTIME=str2time($accessTime);
     	if ( ($epochSTARTtime <= $epochaccessTIME) && ($epochaccessTIME < $portion) ) {
  
     		my $userId = $self->{DB}->queryRow("SELECT userId from userInfo where userName='$userName'")->{userId} ;
      		my $seId = $self->{DB}->queryRow("SELECT seId from seInfo where seName='$seName'")->{seId}; 
               
		my $result=$self->{DB}->do("INSERT IGNORE INTO fileAccessInfo (fileName, success, userId, accessTime, seId, operation) values ('$fileName','$success', '$userId', '$accessTime', '$seId', $operation)");
		$result or $self->info("Can not do inserting") and return;
  	 	}
   	  }
	  next READFILE;
	}
	if ( ($line =~ m/authorize/) && ($line =~ m/FAILED/) ) {
  		if ($line =~ m{^(\d{2}):(\d{2}):(\d{2})}) {
       		$accTime = "$1:$2:$3 ";
       		$accessTime = $LogFileDate . " " . $accTime;
       		$epochaccessTIME=str2time($accessTime);
  		}
  		if ($line =~ m{operation: (.*)fileName:}){
          $operation = $1;
   		}
  		if ($line =~ m{fileName: (.*)seName:}){
         $fileName = $1;
  		}
  		if ($line =~ m{seName: (.*)}){
   			$seName = $1;
   			$self->{DB}->do("INSERT IGNORE INTO seInfo (seName) values ('$seName')");
  		}
  		if ($line =~ m/\'(.*)authorize\'/){
   			$userName = $1;
   			$self->{DB}->do("INSERT IGNORE INTO userInfo (userName) values ('$userName')");
  		}
		if ( ($epochSTARTtime <= $epochaccessTIME) && ($epochaccessTIME < $portion) ){

   			my $userId = $self->{DB}->queryRow("SELECT userId from userInfo where userName='$userName'")->{userId} ;
   			my $seId = $self->{DB}->queryRow("SELECT seId from seInfo where seName='$seName'")->{seId};
            
			my $result=$self->{DB}->do("INSERT IGNORE INTO fileAccessInfo (fileName, success, userId, accessTime, seId, operation) values ('$fileName','$fail', '$userId', '$accessTime', '$seId', $operation)");

			$result or $self->info("Can not do inserting") and return;
		}
    	next READFILE;
  	}
}
 
 
my $query = "select accessTime from fileAccessInfo where accessTime >= '$startTime'";

my $info2   = $self->{DB}->query("$query");
 
if ($info2 and $info2->[0]){
  $self->{DB}->do("insert into collectors (name,actions, startTime) values ('HourlyCollector', '0' ,'$startTime')") or return;
}

return 1;
} 

1;
