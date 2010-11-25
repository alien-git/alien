package AliEn::Service::Optimizer::Transfer::Staged;

use strict;

use vars qw (@ISA);
use AliEn::Service::Optimizer::Transfer;
use AliEn::Service;
use Classad;

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

  my $transfers =$self->{DB}->query("SELECT transferId,pfn,ctime FROM TRANSFERS_DIRECT where status='STAGED'");

  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkTransferRequirements in Staged.pm: error during execution of database query" )
      and return;

  $self->info( "There are ".($#{$transfers} +1)." transfers stuck in STAGED");
  #my $maxTime = 32578; #waiting as maximum for 8 hours
  my $maxTime = 100;

#  my $retryTime =10;


#   $self->{TRANSFERLOG}->putlog($id, "STATUS", 'checkStaged. Transfer STAGED. Optimizer trying to retry it');

  foreach my $transfer (@$transfers){
	my $pfn = $transfer->{pfn};
	my $id = $transfer->{transferId};
	my $retryTime = $transfer->{ctime};
	$self->{TRANSFERLOG}->putlog($id, "STATUS", 'checkStaged. Transfer STAGED. Optimizer trying to retry it');
	$self->$method(@silentData, "Retrying transfer $id, pfn = $pfn");
	if ($retryTime < $maxTime){
		#my $stageQueryOut = system("stager_qry -M ".$pfn) ; # stage-query command here. Binary value
		 $self->$method(@silentData, "retry Time less than Maxtime  $retryTime < $maxTime");
		my $stageQueryOut = 1;
		if ($stageQueryOut) {# stage-query does not succed
			$retryTime = $retryTime * 2;
		        $self->{DB}->updateTransfer($id,{ctime=>$retryTime});
			$self->$method(@silentData, "Retrying transfer $id failed. Sleep for $retryTime");
			sleep $retryTime;
		}
		else {
			$self->$method(@silentData, "Retrying transfer $id done, changing status to WAITING");
			$self->{DB}->updateTransfer($id,{status=>"WAITING"});
			return 0;
		}
	}
	else {
 # 	$self->{LOGGER}->error("Error trying to transfer '$transfer->{jdl}'");
        	$self->{TRANSFERLOG}->putlog($id, "STATUS", 'Transfer FAILED');
		$self->{DB}->updateTransfer($id,{status=>"FAILED"});
	}
  }
	return;
}
