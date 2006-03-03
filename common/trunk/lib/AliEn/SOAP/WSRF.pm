package AliEn::SOAP::WSRF;

use AliEn::SOAP;
#use Data::Dumper;

use strict;
use vars qw(@ISA);

@ISA = qw(AliEn::SOAP);

sub Connect {
  my $self=shift;
  my $options=shift;

  $self->SUPER::Connect($options, @_)
    or return;

  if ($options->{wsaddress}) {
    $self->debug(1, "Connecting using wsaddress $options->{wsaddress}");
    my $header = SOAP::Header->value($options->{wsaddress})->type('xml');
    $self->{WSADDRESSES}->{$options->{name}} = $header;
  }

  return 1;
}

sub _CallSOAPInternal {
  my $self = shift;
  my $service = shift;
  my $function = shift;

  my @params;
  push(@params, $self->{WSADDRESSES}->{$service}) if (exists $self->{WSADDRESSES}->{$service});
  push @params, @_;

  $self->debug(1, "_CallSOAPInternal: $service, calling $function, with params " . @params);

  return $self->SUPER::_CallSOAPInternal($service, $function, @params);
}
