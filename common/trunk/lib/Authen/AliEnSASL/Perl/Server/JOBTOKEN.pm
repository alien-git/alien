# Author: Jan-Erik Revsbech
#
# This is the AliEn implentation of TOKEN method for SASL. 
#
#

package Authen::AliEnSASL::Perl::Server::JOBTOKEN;
use strict;
use vars qw($VERSION @ISA);
use Authen::AliEnSASL::Perl::SASLCodes;
use Authen::AliEnSASL::Perl::Baseclass;
@ISA =
  qw(Authen::AliEnSASL::Perl::SASLCodes Authen::AliEnSASL::Perl::Baseclass);

$VERSION = "1.00";

my %secflags = ( noanonymous => 1, );

my $seclevel = 1;

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

    use Data::Dumper;
    print Dumper($in);
    print Dumper($inlen);

    my ( $name, $auth, $pass ) = split "\0", $in;

    $self->{username} = $name;
    $self->{role}     = $auth;
    $self->{secret}   = $pass;

    #Now check username by calling the callback

    my $username=$self->_call('exists') 
      or return ( $self->SASL_BADAUTH, "", 0 );

    $self->{ALL_DONE} = 1;
    my $output = "TOKENSASL OK";
    #the role is in fact the user who submitted the job
    $self->{role}=$username;
    $output.=" $username";
    return ( $self->SASL_OK, $output, length($output) );
}

sub step {
    my $self  = shift;
    my $in    = shift;
    my $inlen = shift;
}
sub mechanism { 'JOBTOKEN' }

1;
