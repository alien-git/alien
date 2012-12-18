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

  my $table=$self->{DB}->getArchiveTable();

  $self->{DB}->do("INSERT into $table select * from TRANSFERS where (status='DONE' or status='FAILED' or status='EXPIRED') and finished<?",{bind_values=>[$expired]});

  $self->{DB}->do("delete from TRANSFERS where (status='DONE' or status='FAILED' or status='EXPIRED') and finished<?", {bind_values=>[$expired]});


  return ;
}
