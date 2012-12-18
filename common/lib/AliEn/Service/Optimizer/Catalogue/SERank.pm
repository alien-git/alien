
package AliEn::Service::Optimizer::Catalogue::SERank;
 
use strict;

require AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;
use LWP::UserAgent;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;
  $self->$method(@info, "The SE Rank optimizer starts");
  $self->{SLEEP_PERIOD}=1800;
  my $catalogue=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};
  
  $self->{CATALOGUE}->execute("refreshSERankCache");

  $self->info("Going back to sleep");
  return;
}

return 1;

