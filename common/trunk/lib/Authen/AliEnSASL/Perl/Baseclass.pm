package Authen::AliEnSASL::Perl::Baseclass;
use strict;
use vars qw ($DEBUG);

my $DEBUG=0;
sub _call {
    my ( $self, $name ) = @_;

    my $cb = $self->{callback}{$name};

    if ( ref($cb) eq 'ARRAY' ) {
        my @args = @$cb;
        $cb = shift @args;
        return $cb->( $self, @args );
    }
    elsif ( ref($cb) eq 'CODE' ) {
        return $cb->($self);
    }

    return $cb;
}

sub encode {
    my $self = shift;
    my $in   = shift;
    return $in;
}

sub decode {
    my $self = shift;
    my $in   = shift;
    return $in;
}

sub blocksize {
    my $self = shift;
    return ( 1 << 16 );
}


sub log {
  my $self = shift;
  $DEBUG or return;
  my $msg  = shift;
  print "SASL $msg\n";
  return;
}
1;
