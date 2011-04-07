package AliEn::Catalogue::GUID;

use strict;

sub getInfoFromGUID {
  my $self = shift;
  my $guid = shift;

  return $self->{DATABASE}->getAllInfoFromGUID({pfn => 1}, $guid);
}

sub checkPermission {
  my $self = shift;
  return $self->{DATABASE}->{GUID_DB}->checkPermission(@_);
}

return 1;
