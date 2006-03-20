# Author: Jan-Erik Revsbech
#
# This is the AliEn implentation of GSSAPI method for SASL. 
#
#
select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

package Authen::AliEnSASL::Perl::Server::GSSAPI;
use strict;
use vars qw($VERSION @ISA);
use GSS;
use Authen::AliEnSASL::Perl::SASLCodes;
use Authen::AliEnSASL::Perl::Baseclass;

@ISA =
  qw(Authen::AliEnSASL::Perl::SASLCodes Authen::AliEnSASL::Perl::Baseclass);
$VERSION = "1.00";

my %secflags = (
    noanonymous  => 1,
    noplaintext  => 1,
    nodictionary => 1,
);

my $seclevel = 112;

sub new {
    my $class = shift;
    my $self  = {};
    $self->{callback} = shift;
    bless $self, $class;
    return $self;
}

sub _secflags {
    shift;
    my @arr = @_;

    #  print "Test: @arr\n";
    grep { $secflags{$_} } @arr;
}

sub _seclevel {
    shift;
    return $seclevel;
}

sub start {
    my $self  = shift;
    my $in    = shift;
    my $inlen = shift;

    $self->{context}    = 0;    #new gssContext();
    $self->{credential} = 0;    #new gssCredential("");
    $self->{GSS_DONE}   = 0;
    $self->{GOT_AUTHID} = 0;
    $self->step( $in, $inlen );
}

sub step {
  my $self  = shift;
  my $in    = shift;
  my $inlen = shift;

  my ( $maj_stat, $min_stat )=(undef,0);
  my $input = gssBuffer::create( $inlen, $in );
  logmsg("Stepping with $inlen bytes");

  #gssBuffer::printHex($input);
  $self->{GSS_DONE} and return $self->secondStep($inlen, $in
);

  my $output = new gssBuffer("");
  $maj_stat =
    GSS::acceptSecContext( $min_stat, $self->{credential},
			   $self->{context}, $input, $output );
  if (   ( $maj_stat != GSS::GSS_S_COMPLETE )
	 && ( $maj_stat != GSS_S_CONTINUE_NEEDED ) )   {
    logmsg("An error occured during a call to GSS::acceptSecContext\n");
    gssBuffer::DESTROY($input);

    gssBuffer::DESTROY($output);
    gssCredential::DESTROY( $self->{credential} );
    gssContext::DESTROY( $self->{context} );

    # Here we should do some more extensive error checking in order to return the 
    # correct error corede.

    GSS::display_status( $maj_stat, $min_stat );
    return ( $self->SASL_FAIL, "", 0 );
  }
  if ( $maj_stat == GSS::GSS_S_COMPLETE ) {
    logmsg("Server (GSS) complete!!!\n");

    #Get information about connection
    my ( $maj_stat, $min_stat )=(undef,0);
    my $srcname  = new gssName("");
    my $targname = new gssName("");
    my $lifetime=0;
    my $mech = new gssOID();
    my $flags=0;
    my $local=0;
    my $open=0;

    $maj_stat = gssContext::inquire(
				    $min_stat, $self->{context}, $srcname,
				    $targname, $lifetime,        $mech,
				    $flags,    $local,           $open
				   );

    # Since GSS is now finished, register the username in the user callback
    my $tempbuffer = gssName::nameToBuffer($srcname);
    my ( $len, $val ) = gssBuffer::get($tempbuffer);
    gssBuffer::DESTROY($tempbuffer);

    #$self->{callback}{'user'} = $val;
    $self->{username} = $val;

    gssName::DESTROY($srcname);
    gssName::DESTROY($targname);
    gssOID::DESTROY($mech);

    $self->{GSS_DONE} = 1;
  }
  my ( $length, $val )= (0,"");

  if ( gssBuffer::isEmpty($output) ) {
    #The $output from accepSecContext is empty, so
    # send an empty token (Must be manually created!
    # Otherwise, just send $output
    gssBuffer::DESTROY($output); 
  }else {
    ( $length, $val ) = gssBuffer::get($output);
  }

  logmsg("Sending $length bytes back to user");
  return ( $self->SASL_CONTINUE, $val, $length );

}

sub secondStep {
  my $self=shift;
  my $inlen=shift;
  my $in=shift;
  logmsg("GSS IS DONE!!!\n");
  #Okay GSS is finished, but the client sent a token.
  # From now on we can assume that the buffer is clear text
  if ( ( $inlen == 0 ) ) {
    #User send empty string
    if ( $self->{ALL_DONE} ) {
      #If we are done, send ALL OK
      logmsg("Everything is ALL DONE AND OK\n");
      return ( $self->SASL_OK, "", 0 );
    }
    if ( !( $self->{ASKED_FOR_AUTHID} ) ) {
      # We need to ask the user to send username
      my $output = "Auth OK send username";
      logmsg("Asking user for desired role\n");
      $self->{ASKED_FOR_AUTHID} = 1;
      return ( $self->SASL_CONTINUE, $output, length($output) );
    }
    # Bad protocol
    return ( $self->SASL_BADPROT, "", 0 );
  }

  if ( $self->{ASKED_FOR_AUTHID} ) {

    # The user should be returning desired userid
    # Now register this name in the role callback
    #$self->{callback}{'role'} = $in;
    $self->{role} = $in;
    
    #Now check username by calling the callback
    my $allowed = $self->_call('exists');
    
    # If OK, send message and wait for empty string.
    if ($allowed) {
      $self->{ALL_DONE} = 1;
      logmsg("User is allowed\n");
      my $output = "GSSSASL OK";
      return ( $self->SASL_CONTINUE, $output, length($output) );
    }
    #User is not allowed to take this authid
    return ( $self->SASL_BADAUTH, "", 0 );
    
  }
  # The user returned an non empty string which we do not need.
  # ignore it and send back an empty string
  return ( $self->SASL_CONTINUE, "", 0 );

}

sub mechanism { 'GSSAPI' }

sub encode {
    my $self = shift;
    my $in   = shift;
    my ( $maj_stat, $min_stat );
    my $inbuf  = new gssBuffer($in);
    my $outbuf = new gssBuffer("");
    $maj_stat =
      gssContext::wrap( $min_stat, $self->{context}, $inbuf, $outbuf );
    if ( $maj_stat == GSS_S_COMPLETE ) {
        print "Encoded!!\n";
    }
    my ( $val, $len ) = gssBuffer::get($outbuf);
    return $val;
}

sub decode {
    my $self = shift;
    my $in   = shift;

    #my $inlen = shift;
    my ( $maj_stat, $min_stat );
    my $inlen = length($in);

    my $inbuf = gssBuffer::create( $inlen, $in );

    my $outbuf = new gssBuffer("");

    $maj_stat =
      gssContext::unwrap( $min_stat, $self->{context}, $inbuf, $outbuf );
    if ( $maj_stat == GSS_S_COMPLETE ) {

    }
    my ( $len, $val ) = gssBuffer::get($outbuf);
    return $val;
}

sub logmsg {
    my $msg = shift;

#    print "GSSAPI Server: $msg\n";
}
1;

