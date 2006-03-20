# This is the Perl implementation of Authen::AliEnSASL::Client
#
#

package Authen::AliEnSASL::Perl::Client;

use vars qw($VERSION @ISA);
use Authen::AliEnSASL::Client;
use Authen::AliEnSASL::Perl::SASLCodes;
use strict;
use Carp;

$VERSION = 0.1;
@ISA     = qw(Authen::AliEnSASL::Client Authen::AliEnSASL::Perl::SASLCodes);

sub start {
  my $self  = shift;
  my $mechs = shift;

#  print " Test: @_ $mechs\n";
#  print " ******************* IN THE RIGHT START ***************\n";
  
  #print "The server can handle these auth-mechs:\n$mechs\n";
  # Go through list of all possible mechanism and choose the first one
  # I think this could be done simpler.
  my $name;
  my @mpkg =
    grep { eval "require $_;" && $_->_secflags(@SUPER::sec) == @SUPER::sec }
      map {
        ( my $mpkg = __PACKAGE__ . "::$_" ) =~ s/-/_/g;
        $mpkg;
      } split /[^-\w]+/, $mechs
	or croak "No SASL mechanism found\n";

  $self->{mechClass} =
    "$mpkg[0]"->new( $self->{callback}, $self->{service}, $self->{host} );

  # Now call start on the new class
  $self->{mechClass}->start(@_);
}

sub step {
    my $self = shift;
    $self->{mechClass}->step(@_);
}

sub mechanism {
    my $self = shift;
    $self->{mechClass}->mechanism(@_);
}

sub encode {
    my $self = shift;
    $self->{mechClass}->encode(@_);
}

sub decode {
    my $self = shift;
    $self->{mechClass}->decode(@_);
}

sub blocksize {
    my $self = shift;
    $self->{mechClass}->blocksize(@_);
}

sub DESTROY {
    my $self = shift;
    undef( $self->{mechClass} );
}
1;

