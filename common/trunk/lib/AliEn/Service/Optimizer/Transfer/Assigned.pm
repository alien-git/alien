package AliEn::Service::Optimizer::Transfer::Assigned;

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

  $self->$method(@silentData,"In checkTransferRequirements checking if we can archive some of the old transfers");

  #Updating the transfers with status 'WAITING' and only one PFN
  my $transfers=$self->{DB}->query("SELECT transferid FROM TRANSFERS_DIRECT where (status='ASSIGNED' and  ctime<SUBTIME(now(), SEC_TO_TIME(1800))) or (status='TRANSFERRING' and from_unixtime(started)<SUBTIME(now(), SEC_TO_TIME(14400)))");

  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkTransferRequirements error during execution of database query" )
      and return;

  $self->info( "There are ".($#{$transfers} +1)." transfers stuck in ASSIGNED for more than 30 minutes (or TRANSFERRING for 2 hours)");

  foreach my $transfer (@$transfers){
    $self->info("Putting the transfer $transfer->{transferid} back to 'INSERTING'");
    $self->{DB}->updateTransfer($transfer->{transferid}, {status=> "INSERTING"});
    $self->{TRANSFERLOG}->putlog($transfer->{transferid}, "STATUS", 'Transfer stalled. Moving it back to INSERTING');
  }
  $self->{DB}->do("UPDATE ACTIONS set todo=1 where action='INSERTING2'");

  return ;
}
