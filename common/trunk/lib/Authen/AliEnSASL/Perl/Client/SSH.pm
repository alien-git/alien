# Author: Jan-Erik Revsbech
#
# This is the AliEn implentation of SSH method for SASL. 
#
#

package Authen::AliEnSASL::Perl::Client::SSH;

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

sub mechanism { 'SSH' }

sub start {
    my $self = shift;

    $self->{privatekey} = $self->_call('pass');
    if ( !( $self->{privatekey} ) ) {
        return ( $self->SASL_FAIL, "No SSH key, run alien create-keys first",
            100 );
    }

    Crypt::OpenSSL::RSA->import_random_seed();
    $self->{rsa} = Crypt::OpenSSL::RSA->new_private_key($self->{privatekey});
    $self->{rsa}->use_pkcs1_oaep_padding();

    my @parts = map {
        my $v = $self->_call($_);
        defined($v) ? $v : ''
    } qw(user role);
    my $token = join ( "\0", @parts );
    my @retval = ( $self->SASL_CONTINUE, $token, length($token) );
    return @retval;
}

sub step {
    my $self  = shift;
    my $in    = shift;
    my $inLen = shift;

    if ( $in eq "SSHSASL OK" ) {
        return ( $self->SASL_OK, "", 0 );
    }

    my $number = $self->{rsa}->decrypt($in);
    return ( $self->SASL_CONTINUE, $number, length($number) );
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

