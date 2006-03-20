# Author: Jan-Erik Revsbech
#
# This is the AliEn implentation of GSSAPI method for SASL. 
#
#
$| = 1;

package Authen::AliEnSASL::Perl::Client::GSSAPI;

use strict;
use vars qw($VERSION @ISA);
use GSS;
use Authen::AliEnSASL::Perl::SASLCodes;
use Authen::AliEnSASL::Perl::Baseclass;

@ISA =
  qw(Authen::AliEnSASL::Perl::SASLCodes Authen::AliEnSASL::Perl::Baseclass);

$VERSION = "0.01";

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

sub mechanism { 'GSSAPI' }

sub start {
    my $self = shift;
#    $self->{CLIENT_COMPLETE} = 0;
    $self->{ALL_DONE}        = 0;
    $self->{GSS_DONE}        = 0;
    $self->{context}         = 0;    #new gssContext();
    $self->{credential}      = 0;    #new gssCredential("");

#   my $nam = "/O=Grid/O=CERN/OU=cern.ch/CN=" . $self->{service};
#    my $nam = "/C=ch/O=AliEn/O=Alice/OU=Host/CN=" . $self->{service};

    my $nam=$self->{service};
    $self->{targetname} = new gssName($nam);

    $self->step( "", 0 );
}

sub step {
  my $self   = shift;
  my $token  = shift;
  my $toklen = shift;
  $self->log("I got $toklen bytes of data.");
  my $inputbuffer = gssBuffer::create( $toklen, $token );
  my $outputbuffer = new gssBuffer("");
  
  my $maj_stat = 0;
  my $min_stat = 0;

  #Everything is done, this is the last step.
  $self->{ALL_DONE} and 
    return ( $self->SASL_OK, "", 0 );

  $self->{GSS_DONE} and
    return $self->secondStep($inputbuffer);
  $self->log("Ready to do a initSecContext");
  #gssBuffer::printHex($inputbuffer);
  $maj_stat = GSS::initSecContext(
				  $min_stat,           $self->{credential}, $self->{context},
				  $self->{targetname}, $inputbuffer,        $outputbuffer
				 );
  gssBuffer::DESTROY($inputbuffer);
  if ( $maj_stat != GSS_S_CONTINUE_NEEDED && $maj_stat != GSS_S_COMPLETE )  {
    $self->log("An error occured\n");
    $self->{GSS_DONE} = 1;

    #Destroy objects
    gssBuffer::DESTROY($outputbuffer);
    gssCredential::DESTROY( $self->{credential} );
    gssContext::DESTROY( $self->{context} );
    gssName::DESTROY( $self->{targetname} );
    my $output= "Unknown error.";
    ( $maj_stat == GSS_S_NO_CRED ) and 
      $output ="You have no credentials. Run alien proxy-init first";
    ( $maj_stat == GSS_S_CREDENTIALS_EXPIRED ) and
      $output = "Your proxy expired. Run alien proxy-init";
    $self->log($output);
    return ( $self->SASL_FAIL, $output, length($output) );

  }
  if ( $maj_stat == GSS_S_COMPLETE ) {
    $self->log("GSS complete\n");
    $self->{GSS_DONE} = 1;
  }
  if ( !( gssBuffer::isEmpty($outputbuffer) ) ) {
    my ( $len, $val ) = gssBuffer::get($outputbuffer);
    $self->log("Sending $len bytes.");
    return ( $self->SASL_CONTINUE, $val, $len );
  }
  if ( ( $maj_stat == GSS_S_COMPLETE )
       && gssBuffer::isEmpty($outputbuffer) )    {
      
    #my $retval = pack("Ia*",0,"");
    #return $retval;
    return ( $self->SASL_CONTINUE, "", 0 );
  }

  print "The buffer is empty but context is not complete.\nAborting\n";
  return ( $self->SASL_FAIL, "", 0 );

  #exit;
}

sub secondStep {
  my $self=shift;
  my $inputbuffer=shift;;

  $self->log("GSS is done, need to negotiate desired authid");

  #Okay GSS is done, need to negotiate desired authid
  my ( $len, $val ) = gssBuffer::get($inputbuffer);
  if ( $len == 0 ) {
    #The server returned a token of length zero, so respond with length zero
    #my $retval = pack("Ia*",0,"");
    #return $retval;
    return ( $self->SASL_CONTINUE, "", 0 );
  }

  #The server returned us a token, but we are done!!
  $self->log("The server says: $val");
  my $username;
  if ( $val eq "Auth OK send username" ) {
	
    #Get username from SASL Callback method
    $username = $self->_call('role');
    $self->log("Sending server $username\n");
    return ( $self->SASL_CONTINUE, $username, length($username) );
  }

  if ( $val eq "GSSSASL OK" ) {
    #We are all done!
    $self->log("Username is OK. All DONE!!");
    $self->{ALL_DONE} = 1;
    return ( $self->SASL_OK, "", 0 );
  }

  if ( $val eq "GSSSASL NOK" ) {
    #Desired username is not OK
    $self->log(
	       "You are not allowed to take $username credetials\n");
    return ( $self->SASL_BADAUTH, "", 0 );

  }
  #Unknown response
  $self->log("The server returned an unknown response.\n");

  #exit;
  return ( $self->SASL_BADPROT, "", 0 );
}

sub encode {
    my $self     = shift;
    my $in       = shift;
    my $maj_stat = 0;
    my $min_stat = 0;
    my $inbuf    = new gssBuffer($in);
    my $outbuf   = new gssBuffer("");

    $maj_stat =
      gssContext::wrap( $min_stat, $self->{context}, $inbuf, $outbuf );
    if ( $maj_stat == GSS_S_COMPLETE ) {
        print "encoded\n";
    }
    my ( $len, $val ) = gssBuffer::get($outbuf);
    print "Returning $len\n";
    return $val;
}

sub decode {
    my $self = shift;
    my $in   = shift;
    my ( $maj_stat, $min_stat );
    my $inbuf = gssBuffer::create( length($in), $in );
    my $outbuf = new gssBuffer("");
    $maj_stat =
      gssContext::unwrap( $min_stat, $self->{context}, $inbuf, $outbuf );
    if ( $maj_stat == GSS_S_COMPLETE ) {
        print "decoded\n";
    }
    my ( $len, $val ) = gssBuffer::get($outbuf);
    return $val;
}

sub DESTROY {
    my $self = shift;
    $self->log("Destroying GSSAPI mechs\n");
    defined ($self->{credential})  
             and gssCredential::DESTROY( $self->{credential} );
    defined ($self->{context}) 
             and gssContext::DESTROY( $self->{context} );
    defined ($self->{targetname}) 
             and gssName::DESTROY( $self->{targetname} );
}
1;

