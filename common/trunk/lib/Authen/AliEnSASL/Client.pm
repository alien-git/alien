# This is the AliEn extension to Authen::SASL module
# created by Graham Barr <gbarr@pobox.com>

# This is a sort of abstract class, and is only included for completenes

use strict;

package Authen::AliEnSASL::Client;
use Carp;

use vars qw($VERSION @ISA @Plugins);

$VERSION = "0.01";

my %secflags = (
    noplaintext  => 1,
    noanonymous  => 1,
    nodictionary => 1,
);

sub new {
    my $class  = shift;
    my $parent = shift;
    my ( $service, $host, $sec_flags ) = @_;

    my $self = {
        callback => { %{ $parent->callback } },
        service  => $service || '',
        host     => $host || '',
    };
    bless $self, $class;

    return $self;
}

# Methods to be implemented
sub start     { undef }
sub step      { undef }
sub mechanism { undef }

# Predefine methods, which can be overloaded if desired.
sub service { shift->{service} }
sub host    { shift->{host} }

1;

