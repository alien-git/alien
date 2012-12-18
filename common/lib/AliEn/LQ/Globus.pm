package AliEn::LQ::Globus;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use strict;

sub submit {
  my $self = shift;
  return;

}

sub getStatus {
    return 'QUEUED';
}

sub initialize() {
    my $self = shift;
    $self->{PATH} = "/tmp";
}

return 1;

