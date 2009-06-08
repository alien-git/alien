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
  my $transfers=$self->{DB}->query("SELECT transferid,jdl FROM TRANSFERS where status='ASSIGNED' and  ctime<SUBTIME(now(), SEC_TO_TIME(1800))");

  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkTransferRequirements error during execution of database query" )
      and return;


  $self->info( "There are ".($#{$transfers} +1)." transfers have been stuck in ASSIGNED for more than 30 minutes ");


  foreach my $transfer (@$transfers){
    $self->info("What can we do with '$transfer->{transferid}'?");
    my $ca = Classad::Classad->new($transfer->{jdl});
    $self->info( "Classad created");
    my ($ok, $action)=$ca->evaluateAttributeString("Action");
    $ok or $self->info("Error getting the action from the jdl!") and next;
    my $status='INSERTING';
    $self->info("Setting the transfer to $status");
    $self->{DB}->updateTransfer($transfer->{transferid}, {status=> "$status", jdl=>$transfer->{jdl}});
  }


  return ;
}
