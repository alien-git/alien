package AliEn::Service::Optimizer::Catalogue::SE;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  $self->$method(@info, "The SE optimizer starts");
  return ;
}
1;
