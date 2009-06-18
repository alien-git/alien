package AliEn::Service::Optimizer::Transfer::Failed;

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
  $self->{SLEEP_PERIOD}=10;
  $self->$method(@silentData, "Checking if there is anything to do");
  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='FAILED_T'");
  $todo or return;
  $self->{DB}->updateActions({todo=>0}, "action='FAILED_T'");


  my $transfers=$self->{DB}->query("SELECT transferid from TRANSFERS_DIRECT where status='FAILED_T'");


  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkNewTransfers error during execution of database query" )
      and return;

  @$transfers or
    $self->$method(@silentData,"In checkNewTransfers no new transfers")
      and return;

  $self->info("For the time being, let's fail all those transfers");
  foreach my $t (@$transfers){
    $self->{DB}->updateTransfer($t->{transferid}, {status=>"FAILED"});
    $self->{TRANSFERLOG}->putlog($t->{transferid}, "STATUS", "Transfer changed to FAILED");

  }
}
return 1;
