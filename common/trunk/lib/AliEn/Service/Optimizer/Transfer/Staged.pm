package AliEn::Service::Optimizer::Transfer::Staged;

use strict;

use vars qw (@ISA);
use AliEn::Service::Optimizer::Transfer;
use AliEn::Service;
use Classad;
use POSIX;

push (@ISA, "AliEn::Service::Optimizer::Transfer");
	
#this function is called when a transfer status is changed to STAGE or from time to time to try to retry the 
#transfer


sub checkWakesUp {
  my $self=shift;
  my $silent=(shift or 0);
  my $method="info";
  my @silentData=();
  $silent and $method="debug" and push @silentData, 1;
  $self->$method(@silentData, "Check STAGED transfers. Checking if there is anything to do");

  my $transfers =$self->{DB}->query("SELECT transferId,pfn,ctime,retrytime,maxtime FROM TRANSFERS_DIRECT where status='STAGED'");

  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkTransferRequirements in Staged.pm: error during execution of database query" )
      and return;

  $self->info( "There are ".($#{$transfers} +1)." transfers stuck in STAGED");

#   $self->{TRANSFERLOG}->putlog($id, "STATUS", 'checkStaged. Transfer STAGED. Optimizer trying to retry it');

  foreach my $transfer (@$transfers){
	my $pfn = $transfer->{pfn};
	my $id = $transfer->{transferId};
	my $maxTime = $transfer->{maxtime};
	my $retryTime;
	my @x = split(/\D/, $transfer->{ctime});
	$x[0] -= 1900;
	$x[1] -= 1;
	my $initTime = strftime("%s", reverse @x);
	$self->$method(@silentData, "cime is  $initTime ");
	my $actualTime = time ();
        
	if (defined($transfer->{retrytime}) and $transfer->{retrytime} ne 'NULL' and $transfer->{retrytime} != 0) {
		$retryTime = $transfer->{retrytime};
	} else {
		$retryTime = time()-1;
	}

	$self->{TRANSFERLOG}->putlog($id, "STATUS", 'checkStaged. Transfer STAGED. Optimizer trying to retry it');
	$self->$method(@silentData, "Retrying transfer $id, retrytime $retryTime, actual time $actualTime, maxtime $maxTime, init time ctime = $initTime");
	if ($retryTime < $maxTime && $retryTime<=$actualTime){
		my $stageQueryOut = system("stager_qry -M ".$pfn) ; # stage-query command here. Binary value
		$self->$method(@silentData, "retryTime less than Maxtime  $retryTime < $maxTime. StageQueryout stager_qry -M $pfn ".$stageQueryOut);
		my $stageQueryOut = 0;
		if ($stageQueryOut) {# stage-query does not succed
			$retryTime = $actualTime + ($retryTime - $initTime)* 2;
			$self->$method(@silentData, "retryTime is  $retryTime ");
			my $query = {ctime=>$actualTime};
			$query->{retrytime} = $retryTime;
			$query->{maxtime} = $maxTime;
		        $self->{LOGGER}->debug("FTD","Update Transfer $id with ".$query->{maxtime}.", retryTime ".$query->{retrytime}." and ".$query->{ctime});
			$self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, 'STAGED',"ALIEN_SOAP_RETRY",$query);
			$self->$method(@silentData, "Retrying transfer $id failed. Sleep until $retryTime");
		}
		else {
			$self->$method(@silentData, "Retrying transfer $id done, changing status to WAITING");
			$self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, 'WAITING',"ALIEN_SOAP_RETRY");
			#$self->{DB}->updateTransfer($id,{status=>"WAITING"});
			return 0;
		}
	}
	elsif ($retryTime > $maxTime ) {
        	$self->$method(@silentData, "Retrying transfer $id done, changing status to FAILED");
		$self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, 'FAILED',"ALIEN_SOAP_RETRY");
		#$self->{DB}->updateTransfer($id,{status=>"FAILED"});
	}
  }
	return;
}
