# This is the AliEn extension to Authen::SASL module
# created by Graham Barr <gbarr@pobox.com>

# This is a sort of abstract class, and is only included for completenes

use strict;

package Authen::AliEnSASL::Server;
use Carp;

use vars qw($VERSION @ISA @Plugins);

$VERSION = "0.01";

#@ISA	 = qw(Authen::SASL);

sub new {
    my $class  = shift;
    my $parent = shift;
    my ( $service, $host, $sec_flags, $sec_level ) = @_;

    #By default set sec_level to 0.
    if ( !( defined($sec_level) ) ) {
        $sec_level = 0;
    }

    my @sec_levels = split ( " ", $sec_level );

    my $self = {
        callback  => { %{ $parent->callback } },
        service   => $service || '',
        host      => $host || '',
        sec_level => $sec_level || 0,
        sec_flags => $sec_flags || '',
    };
    bless $self, $class;

    return $self;
}

# Methods to be implemented
sub start        { undef }
sub step         { undef }
sub listmech     { undef }
sub mechanism    { undef }
sub user_exists  { undef }
sub user_setpass { undef }
1;

