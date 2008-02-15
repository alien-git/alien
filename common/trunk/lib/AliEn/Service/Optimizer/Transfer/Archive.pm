package AliEn::Service::Optimizer::Transfer::Archive;

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
  my $now=time;
  my $expired=$now-7*48*60*60;
  my $transfers=$self->{DB}->query("SELECT transferid FROM TRANSFERS where (status='DONE' or status='FAILED' or status='EXPIRED') and finished<?",undef, {bind_values=>[$expired]});
  
  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkTransferRequirements error during execution of database query" )
      and return;
  
  my $table=$self->{DB}->getArchiveTable();

  $self->info( "There are ".($#{$transfers} +1)." transfers ready to be moved to $table (older than $expired)");


  foreach my $transfer (@$transfers){
    $self->{DB}->do("INSERT into $table select * from TRANSFERS where transferid=?", {bind_values=>[$transfer->{transferid}]});
    $self->{DB}->do("delete from TRANSFERS where transferid=?", {bind_values=>[$transfer->{transferid}]});

    print ".";
  }


  return ;
}
