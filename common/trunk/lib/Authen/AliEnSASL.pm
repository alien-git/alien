# This is the AliEn extension to Authen::SASL module
# created by Graham Barr <gbarr@pobox.com>

package Authen::AliEnSASL;

use strict;
use vars qw($VERSION @ISA @Plugins);
use Authen::AliEnSASL::Client;

use Carp;

@Plugins = qw(
  Authen::AliEnSASL::Perl
);

$VERSION = "1.00";

sub new {
    my $class    = shift;
    my $callback = shift;
    my $appname  = shift;

    if ( ref($callback) ne 'HASH' ) {
        print "The callbacks must be a hash\n";
        exit;
    }

    my $self = {};
    bless $self, $class;

    # Register the callbacks
    $self->callback( %{$callback} );

    return $self;
}

sub callback {
    my $self = shift;

    return $self->{callback}{ $_[0] } if @_ == 1;

    my %new = @_;
    @{ $self->{callback} }{ keys %new } = values %new;

    $self->{callback};
}

sub client_new {    # $self, $service, $host, $secflags
    my $self = shift;
    foreach my $pkg (@Plugins) {
        if ( eval "require $pkg" ) {
            my $class = "$pkg\:\:Client";

            #$self->{conn} = $class->new($self, @_);
            #return $self->{conn};
            return ( $self->{conn} = $class->new( $self, @_ ) );
        }
    }
    croak "Cannot find a SASL Connection library";
}

sub server_new {    # $self, $service, $host, $secflags
    my $self = shift;

    foreach my $pkg (@Plugins) {
        if ( eval "require $pkg" ) {
            my $class = "$pkg\:\:Server";
            return ( $self->{conn} = $class->new( $self, @_ ) );
        }
    }

    croak "Cannot find any SASL Connection library";
}

1;

