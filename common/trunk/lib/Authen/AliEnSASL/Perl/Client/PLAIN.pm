# Author: Jan-Erik Revsbech
#
# This is the AliEn implentation of PLAIN method for SASL. 
#
#

package Authen::AliEnSASL::Perl::Client::PLAIN;

use strict;
use vars qw($VERSION @ISA);
use Authen::AliEnSASL::Perl::SASLCodes;
use Authen::AliEnSASL::Perl::Baseclass;

@ISA =
  qw(Authen::AliEnSASL::Perl::SASLCodes Authen::AliEnSASL::Perl::Baseclass);

$VERSION = "1.00";
my %secflags = ( noanonymous => 1, );

sub new {
    my $class = shift;
    my $self  = {};
    $self->{callback} = shift;
    $self->{service}  = shift;
    $self->{host}     = shift;
    bless $self, $class;
    return $self;
}

sub _secflags {
    shift;
    grep { $secflags{$_} } @_;
}

sub mechanism { 'PLAIN' }

sub start {
    my $self = shift;

    my @parts = map {
        my $v = $self->_call($_);
        defined($v) ? $v : ''
    } qw(user role pass);
    my $token = join ( "\0", @parts );
    my @retval = ( $self->SASL_CONTINUE, $token, length($token) );

}

sub step {
    my $self  = shift;
    my $in    = shift;
    my $inLen = shift;
    if ( $in eq "PLAINSASL OK" ) {
        return ( $self->SASL_OK, "", 0 );
    }
    else {
        return ( $self->SASL_BADAUTH, "Not allowed to authenticate.", 0 );
    }
}

sub DESTROY {
    my $self = shift;

    $self->log("Destroying PLAIN");
    return;
}

sub log {
    my $self = shift;
    my $msg  = shift;

    #print "Log: $msg\n";
    return;
}

1;

