package AliEn::Service::Optimizer::Transfer;

use strict;

use vars qw (@ISA);

use AliEn::Database::Transfer;

use AliEn::Service::Optimizer;
use AliEn::UI::Catalogue::LCM;

use AliEn::TRANSFERLOG;
use POSIX qw(ceil);
@ISA=qw(AliEn::Service::Optimizer);


my $self;
sub initialize {
  $self=shift;
  my $options =(shift or {});

  $options->{role}="admin";

  $self->debug(1, "In initialize initializing service TransferManager" );

  $self->{SERVICE}="Transfer";

  $self->{CATALOGUE} = AliEn::UI::Catalogue::LCM->new($options);

  $self->debug(1, "In initialize creating AliEn::UI::Catalogue instance" );

  ( $self->{CATALOGUE} )
    or $self->{LOGGER}->error( "TransferOptimizer", "Error creating creating AliEn::UI::Catalogue instance" )
      and return;

  $self->{DB_MODULE}="AliEn::Database::Transfer";
  $self->SUPER::initialize(@_) or return;
  $self->{TRANSFERLOG}=AliEn::TRANSFERLOG->new() or return;
#  $self->StartChildren('Assigned', 'Archive', 'Inserting', 'Merging', 'SE', 'Agent') or return;
  $self->StartChildren('Inserting') or return;
#  $self->StartChildren('No_se') or return;


  return 1;

}

sub checkWakesUp {
  my $this=shift;
  my $silent=(shift or 0);
  my $method="info";
  $self->{SLEEP_PERIOD}=10;
  $silent and $method="debug";

  $self->{LOGGER}->$method("TransferOptimizer","In checkWakesUp checking if there is anything to do");

  $self->checkExpiredTransfers($silent);

  $self->checkTransferRequirements($silent);

  undef;
}

sub checkExpiredTransfers {
  my $self=shift;
  my $silent=(shift or 0);
  my $method="info";
  $silent and $method="debug";

  $self->{LOGGER}->$method("TransferOptimizer", "In checkExpiredTransfers checking transfers that are supposed to be expired");

  $self->{DB}->updateExpiredTransfers
    or $self->{LOGGER}->warning("TransferOptimizer", "In checkExpiredTransfers error updating expired transfers");
} 


return 1;
