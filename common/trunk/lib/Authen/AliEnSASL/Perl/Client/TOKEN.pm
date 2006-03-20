# Author: Jan-Erik Revsbech
#
# This is the AliEn implentation of TOKEN method for SASL. 
#
#

package Authen::AliEnSASL::Perl::Client::TOKEN;

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

sub mechanism { 'TOKEN' }

sub start {
    my $self = shift;

    my $user  = $self->_call('user');
    my $auth  = $self->_call('role');
    my $token = $self->_call('pass');

    if ( !($token) ) {

        #The token (secret) was not correctly retrieved
        my $output =
          "You have no token, run alien UpdateToken to get a new token";
        return ( $self->SASL_FAIL, $output, length($output) );
    }
    my $output = join ( "\0", ( $user, $auth, $token ) );
    my @retval = ( $self->SASL_CONTINUE, $output, length($output) );

}

sub step {
    my $self  = shift;
    my $in    = shift;
    my $inLen = shift;
    if ( $in eq "TOKENSASL OK" ) {
        return ( $self->SASL_OK, "", 0 );
    }
    else {
        return ( $self->SASL_BADAUTH, "Not allowed to authenticate.", 0 );
    }
}

sub DESTROY {
    my $self = shift;

    $self->log("Destroying TOKEN");
    return;
}

sub log {
    my $self = shift;
    my $msg  = shift;

    #print "Log: $msg\n";
    return;
}

1;

