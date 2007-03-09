package AliEn::Catalogue::GUID;

use strict;

sub getInfoFromGUID {
  my $self = shift;
  my $guid=shift;
  
  return $self->{DATABASE}->getAllInfoFromGUID({pfn=>1}, $guid);
}

return 1;
