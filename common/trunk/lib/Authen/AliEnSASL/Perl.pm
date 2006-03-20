# The Perl implentation of SASL
# Uses Authen::SASL by Graham Barr <gbarr@pobox.com>
use strict;
use Authen::SASL::Perl;

package Authen::AliEnSASL::Perl;

use Authen::AliEnSASL::Perl::Client;
use Authen::AliEnSASL::Perl::Server;

use vars qw($VERSION @ISA);
use Carp;

#@ISA = qw(Authen::SASL::Perl);

my %secflags = (
    noplaintext  => 1,
    noanonymous  => 1,
    nodictionary => 1,
);

sub client_new {
    my ( $pkg, $parent, $service, $host, $secflags ) = @_;

    my @sec = grep { $secflags{$_} } split /\W+/, lc( $secflags || '' );

    my $self = {
        callback => { %{ $parent->callback } },
        service  => $service || '',
        host     => $host || '',
    };

    # Dumb selection;
    print "Test $pkg\n";
    my @mpkg = grep { eval "require $_;" && $_->_secflags(@sec) == @sec } map {
        ( my $mpkg = __PACKAGE__ . "::$_" ) =~ s/-/_/g;
        $mpkg;
      } split /[^-\w]+/, $parent->mechanism
      or croak "No SASL mechanism found\n";

    $mpkg[0]->_init($self);
}

1;

