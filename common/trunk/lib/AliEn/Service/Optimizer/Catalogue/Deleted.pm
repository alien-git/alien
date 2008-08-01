package AliEn::Service::Optimizer::Catalogue::Deleted;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");


sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  $self->{CATALOGUE}->execute("checkLFN");
  sleep (120);
  $self->{CATALOGUE}->execute("checkOrphanGUID");
  return ;
}

return 1;
