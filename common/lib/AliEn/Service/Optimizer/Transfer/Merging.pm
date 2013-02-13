package AliEn::Service::Optimizer::Transfer::Merging;

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
  $self->$method(@silentData, "Checking if there is anything to do");
  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='MERGING'");
  $todo or return;
  my $transfers=$self->{DB}->getTransfersToMerge()
    or $self->info("Error getting the transfers") and return;

  foreach my $transfer (@$transfers){
    $transfer or next;
    $self->info("Checking for transfer $transfer");
    my $count=$self->{DB}->queryValue("SELECT count(*) from TRANSFERS_DIRECT where transfergroup=? and status != 'DONE'  and status !='FAILED'  and status !='KILLED'", undef, {bind_values=>[$transfer]});
    $self->info("There are $count transfers for it");
    $count and next;
    $self->info("The transfer finished!!!");


    $self->{DB}->updateTransfer($transfer,{status=>"DONE"})
      or $self->info("Error setting the master transfer to done");
  }
  return 1;
}

return 1;
