package AliEn::Service::Optimizer::Catalogue::Deleted;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");


sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  #Clean LFN_BOOKED, G#L, G#L_PFN
  $self->{CATALOGUE}->execute("removeExpiredFiles");

  
  sleep (120);
  
  return ;
}

return 1;
