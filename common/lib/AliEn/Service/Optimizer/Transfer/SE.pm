package AliEn::Service::Optimizer::Transfer::SE;

use strict;

use vars qw (@ISA);
use AliEn::Service::Optimizer::Transfer;

push (@ISA, "AliEn::Service::Optimizer::Transfer");



sub checkWakesUp {
  my $self=shift;
  my $silent=(shift or 0);
  my $method="info";
  my @silentData=();
  $silent and $method="debug" and push @silentData, 1;

  $self->$method(@silentData,"In checkTransferRequirements checking if we can put constraints in any transfer");
  
#  $self->{DB}->updateLocalCopyTransfers
#    or $self->{LOGGER}->warning("TransferOptimizer", "In checkTransferRequirements error updating local copy transfers");
  
  #Updating the transfers with status 'WAITING' and only one PFN
  my $transfers=$self->{DB}->query("SELECT entryid,requirements as jdl  FROM AGENT WHERE SE is NULL");
  
  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkTransferRequirements error during execution of database query" )
      and return;
  
  @$transfers
    or $self->$method(@silentData, "There is no transfer waiting" )
      and return ;

  $self->$method(@silentData, "In checkTransferRequirements here are ".($#{$transfers} +1)." transfers in WAITING to check");
  foreach my $data (@$transfers) {
    my @se=();
    while ( $data->{jdl}=~ s/member\(other.CloseSE,\"?([^\\")]*)\"?\)//){
      push @se, $1;
    }

    $self->debug(1, "In checkTransferRequirements possible SE: @se");
    if ($#se eq 0){
      my $dest=$se[0];
      $self->info( "Putting dest of $data->{entryid} as $dest");
      $self->{DB}->setSE($data->{entryid},$dest)
	or $self->info( "Error updating SE for transfer $data->{entryid}")
	  and next;
    }
  }

  return ;
}
