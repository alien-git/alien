# Author: Jan-Erik Revsbech
#
# This is the AliEn implentation of SSH method for SASL. 
#
#

package Authen::AliEnSASL::Perl::Server::SSH;
use strict;
use vars qw($VERSION @ISA);

use Authen::AliEnSASL::Perl::SASLCodes;
use Crypt::OpenSSL::Random;
use Crypt::OpenSSL::RSA;
use MIME::Base64;
use Authen::AliEnSASL::Perl::Baseclass;
@ISA =
  qw(Authen::AliEnSASL::Perl::SASLCodes Authen::AliEnSASL::Perl::Baseclass);

$VERSION = "1.00";

my %secflags = ( noanonymous => 1, );

my $seclevel = 64;

sub new {
    my $class = shift;
    my $self  = {};
    $self->{callback} = shift;
    bless $self, $class;
    return $self;
}

sub _seclevel {
    shift;
    return $seclevel;
}

sub _secflags {
    shift;
    my $retval;
    grep { $secflags{$_} } @_;
}

sub start {
    my $self  = shift;
    my $in    = shift;
    my $inlen = shift;

    my ( $name, $auth ) = split "\0", $in;

    $self->{username} = $name;
    $self->{role}     = $auth;
    $self->{secret}   = "";

    $self->{publickey} = $self->_call("credential");

    Crypt::OpenSSL::RSA->import_random_seed();
    $self->{rsa} = Crypt::OpenSSL::RSA->new_public_key($self->{publickey});
    $self->{rsa}->use_pkcs1_oaep_padding();

    # Now generate random number.
    $self->{random} = Crypt::OpenSSL::Random::random_bytes(128);

    if ( !( $self->{random} ) ) {
        print "Error in creatinf random number\n";
        my $outstring = "Could not create random numver";
        return ( $self->SASL_FAIL, $outstring, length($outstring) );
    }
    my $challenge = $self->{rsa}->encrypt( $self->{random} );
    my $len       = length($challenge);

    return ( $self->SASL_CONTINUE, $challenge, $len );
}

sub step {
    my $self  = shift;
    my $in    = shift;
    my $inlen = shift;
    if ( $inlen > 0 ) {
        if ( $in eq $self->{random} ) {
            my $outstring = "SSHSASL OK";

            # Now check if user is allowed to authenticate as role
            if ( $self->_call("exists") ) {
                return ( $self->SASL_OK, $outstring, length($outstring) );
            }
            else {
                print
"Authentication failed. User not allowed to authticate as desired role\n";
                return ( $self->SASL_BADAUTH, "", 0 );
            }
        }
        else {
            print "Authentication failed. Challenge is not correct\n";
            return ( $self->SASL_BADAUTH, "", 0 );
        }
    }
    return ( $self->SASL_FAIL, "", 0 );
}
sub mechanism { 'SSH' }
1;

