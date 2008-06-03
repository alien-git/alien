package AliEn::Authen::ClientVerifier;

use strict;

use Authen::AliEnSASL;
require IO::Socket;
require Storable;
require AliEn::Authen::Comm;
use AliEn::Config;
use AliEn::Logger;

use vars qw($VERSION @ISA);

push @ISA, 'AliEn::Logger::LogObject';


use Cache::MemoryCache;

my $CACHE = new Cache::MemoryCache();


# ***** For debugging only *************
use Authen::AliEnSASL::Perl;
use Authen::AliEnSASL;
use Authen::AliEnSASL::Perl::Client::PLAIN;
use Authen::AliEnSASL::Perl::Client::GSSAPI;
use Authen::AliEnSASL::Perl::Client::SSH;

# **************************************
my $Logger;

$VERSION = "1.00";

####################################################################
#
#  This method takes (It passed as a callback to server_new) should
# check what mechanism is used an take action thereafter. Not all
# methods will call this, but some will.
#
#  It will check what method is used, and return the appropirate 
# secret. For SSH it will return the private key, and for TOKEN
# it will return the TOKEN
#
####################################################################
sub get_secret {
  my $self = shift;
  
  my $mech     = $self->mechanism;
  my $username = $self->_call('user');
  
  my $config = AliEn::Config->new();
  
  my $org    = "\L$config->{ORG_NAME}\E";
  
  my $secret;
  
  $Logger->debug(1, "The mech is: $mech\n");
  if ( $mech eq "TOKEN" ) {
    my $filename = "$ENV{HOME}/.alien/identities.$org/token.$username";
    if ( open( TOKEN, "$filename" ) ) {
      my @lines = <TOKEN>;
      close(TOKEN);
      $secret = $lines[0];
    }
    else {
      return "";
    }
  }
  if ( $mech eq "SSH" ) {
    my $filename;
    if ( $ENV{PKI_KEY} ) {
      $filename = $ENV{PKI_KEY};
    }
    else {
      $filename = $ENV{HOME} . "/.alien/identities.$org/sshkey.$username";
    }
    $Logger->debug(1,"Using key from $filename\n");
    my @lines;
    if ( open( PRIVKEY, $filename ) ) {
			@lines = <PRIVKEY>;
			$secret = join ( "", @lines );
			close PRIVKEY;
		      }
    else {
      return "";
    }
  }
  return $secret;
}

sub new {
  my $proto = shift;
  my $self  = {};

  bless( $self, ( ref($proto) || $proto ) );
  $self->SUPER::new() or print "Error doing new\n" and return;


  $Logger=$self;
  my $ttl = "60 minutes";
	
  my $config = $self->{config} = $self->{CONFIG} =  AliEn::Config->new();

  my $socket = $self->{socket} = shift;

  $Logger->debug(1,"Creating a new ClientVerifier @_");

  ($socket->peerhost() and $socket->peerport()) 
    or print "Error: In ClientVerifier we got an empty socket\n" and 
      return;
  my $key = join("::",$socket->peerhost(),$socket->peerport(),@_);
	
  my ( $status, $token, $tokenlen ) =
    AliEn::Authen::Comm::read_buffer( $self->{socket} );
	
  my $auth = $CACHE->get($key);
	
  if ( not defined $auth ) {

    # username is who the user really is.
    # role is who the user wants to be.
    my $role = $auth->{ROLE} = shift;

    my $username = $auth->{USERNAME} = shift || $config->{LOCAL_USER};
    $auth->{FORCED_METHOD} = shift;

    # Ask server which authentication methods it support!
    AliEn::Authen::Comm::write_buffer( $self->{socket}, "REQUEST MECHS", "",0);
    # Wait for answer...
    ( $status, $token, $tokenlen ) =
      AliEn::Authen::Comm::read_buffer( $self->{socket} );
		
    if (! defined $token) {
      $token = "";
    }
    ;
		
    $auth->{methods} = $token;
    $Logger->debug(1, "The server supports $token");
		
    #  We now check which methods we have credentials for.
    $self->findUserCertificate( $auth);
    $self->findSSHKey($username, $auth);
    $self->findToken($username, $auth);

    $Logger->debug(1,"After sort: $auth->{methods}");
		
    if ( $auth->{FORCED_METHOD} ) {
      # We have a forced method, so use only that.
      $Logger->debug(1, "The forced method is  $auth->{FORCED_METHOD}  ");
      $auth->{methods} = $auth->{FORCED_METHOD};
    }
		
    $CACHE->set( $key, $auth, $ttl );
		
  }
	
  $self->{methods}  = $auth->{methods};
  $self->{ROLE}     = $auth->{ROLE};
  $self->{USERNAME} = $auth->{USERNAME};
  $self->{FORCED_METHOD} = $auth->{FORCED_METHOD};
	
  return $self;
}
sub findToken {
  my $self=shift;
  my $username=shift;
  my $auth=shift;

  my $org    = "\L$self->{CONFIG}->{ORG_NAME}\E";

  my $tokenfile = $ENV{HOME} . "/.alien/identities.$org/token.$username";
	
  if ( !( -e $tokenfile ) ) {
    #We do not have a token, so sort this one out.
    $Logger->debug(1, "NO TOKEN\n");
    $auth->{methods} =~ s/TOKEN ?//g;
  }
}
sub findSSHKey {
  my $self=shift;
  my $username=shift;
  my $auth=shift;

  my $org    = "\L$self->{CONFIG}->{ORG_NAME}\E";
	
  my $sshfile   = $ENV{HOME} . "/.alien/identities.$org/sshkey.$username";
	
  $Logger->debug(1, "Looking for the ssh key in $sshfile");
  $ENV{PKI_KEY} and $Logger->debug(1, "Environment $ENV{PKI_KEY}");
  my $exists=0;
  ( -e $sshfile ) and $exists=1;
  ( defined $ENV{PKI_KEY} and ( -e $ENV{PKI_KEY} ) ) and $exists=1;
  if (!$exists) {
    #We do not have a ssh-key, so sort this one out.
    $Logger->debug(1, "NO SSH");
    $auth->{methods} =~ s/SSH ?//g;
  }

}

sub findUserCertificate {
  my $self=shift;
  my $auth=shift;

  my $proxy= ($ENV{X509_USER_PROXY} || "/tmp/x509up_u$<");
  my $command="$ENV{GLOBUS_LOCATION}/bin/openssl x509 -checkend 1 -noout -in $proxy > /dev/null  2>&1";

  if (not -e $proxy or  system($command)) {
    print "Warning: No valid proxy. Trying SSH key...\n";
    $auth->{methods} =~ s/GSSAPI ?//g;
  }
  return;
}	

sub _new {
    my $proto = shift;
    my $self  = {};
    bless( $self, ( ref($proto) || $proto ) );
    $self->SUPER::new() or return;
    $self->{'socket'} = shift;
    my $config = AliEn::Config->new();
    my $org    = "\L$config->{ORG_NAME}\E";
#    $self->{config} = $config;

    # username is who the user really is.
    # role is who the user wants to be.
    my $role = $self->{ROLE} = shift;
    my $username = $self->{USERNAME} = shift || $config->{LOCAL_USER};
    $self->{FORCED_METHOD} = shift;

    my ( $status, $token, $tokenlen ) =
      AliEn::Authen::Comm::read_buffer( $self->{socket} );

    # Ask server which authentication methods it support!
    AliEn::Authen::Comm::write_buffer( $self->{socket}, "REQUEST MECHS", "",
        0 );

    # Wait for answer...
    ( $status, $token, $tokenlen ) =
      AliEn::Authen::Comm::read_buffer( $self->{socket} );

    $Logger->debug(1,"  The server supports $token\n");

    $self->{methods} = $token;

    #  We now check which methods we have credentials for.
		$self->findUserCertificate( $self);
		$self->findSSHKey($username, $self);
		$self->findToken($username, $self);


    $Logger->debug ("After sort: " . $self->{methods} . "\n");

    if ( $self->{FORCED_METHOD} ) {

        # We have a forced method, so use only that.
        $Logger->debug ("The forced method is " . $self->{FORCED_METHOD} . "\n");
        $self->{methods} = $self->{FORCED_METHOD};
    }

    return $self;
}

sub verify {
  my $self        = shift;
  my $secret      = shift;
  my $AFSPasswork = shift;

  # Create SASL object
  my $config = AliEn::Config->new();
  
  if ( !($secret) ) {
    $secret = \&get_secret;
  }
  
  # Currently we use the same user and role
  my $role     = $self->{ROLE};
  my $username = $self->{USERNAME};
  
  my $callbacks = {
		   user => $username,
		   pass => $secret,
		   role => $role,
		  };
  
  my $sasl = new Authen::AliEnSASL( $callbacks, "My application" );
  
  my $client=($config->{AUTHEN_SUBJECT} or 
	      "/C=ch/O=AliEn/O=Alice/OU=Host/CN=aliendb.cern.ch");
  $self->{SASLclient} = $sasl->client_new($client);
  
  # Initiate SASL Client object with desired mechs
  
  my ( $status, $token, $tokenlen ) =
    $self->{SASLclient}->start( $self->{methods} );
  
  $self->{mech} = $self->{SASLclient}->mechanism;
  $Logger->debug(1,"  The negotiated authentication mechanism is: $self->{mech} \n");

  # Now ask server to authenticate 
  $Logger->debug(1,"  Asking server for permission to authenticate...");
  
  AliEn::Authen::Comm::write_buffer(
				    $self->{socket}, "REQUEST AUTH",
				    $self->{mech},   length( $self->{mech} )
				   );
  
  # Wait for answer...
  my ( $statNEG, $tokenNEG, $tokenlenNEG ) =
    AliEn::Authen::Comm::read_buffer( $self->{socket} );
  if ( $statNEG ne "AUTH OK" ) {
    $Logger->debug(1,"FAILED\nThe server answered $statNEG\nServer did not allow you to authorize\n");
    
    return ( 0, "" );
  }
  $Logger->debug(1,"OK\n****************** Starting authentication proccess *************\n");

  if ( $status == $self->{SASLclient}->SASL_CONTINUE ) {

    #print "Continue needed\n";
    if (
	AliEn::Authen::Comm::write_buffer(
					  $self->{socket}, "AliEnClient TOKEN",
					  $token, $tokenlen
					 )
       )
      {
	$Logger->debug(1,"Sent $tokenlen bytes to server\nWaiting for answer...");
      }
    else {
      print "Communications error. Aborting\n";
      exit;
    }
  }
  else {
    $Logger->debug(1,"An error occured. Context not established. Aborting\n");
    return ( 0, $token );
  }

#  my $run = 1;
  my $saslstat;
#  do {
  while (1) {
    ( $status, $token, $tokenlen ) =
      AliEn::Authen::Comm::read_buffer( $self->{socket} );
    if ( !defined $token ) {
      print "Error: token does not exist\n";
      return ( 0, "" );
    }
    $Logger->debug(1,"Got $tokenlen bytes\n");
    if ( $tokenlen < 14  ) {
      $Logger->debug(1, "The server said: $token\nStatus: $status\n");
    }
    if ( $status eq "AliEnAuth OK" ) {
      #print "Server says OK\n";
      last;
      #$run = 0;
    }
    elsif ( $status eq "AliEnAuth CONTINUE" ) {
      ( $saslstat, $token, $tokenlen ) =
	$self->{SASLclient}->step( $token, $tokenlen );
      $Logger->debug(1,"Sent $tokenlen bytes to server.\nWaiting for answer...");
      AliEn::Authen::Comm::write_buffer( $self->{socket},
					 "AliEnClient TOKEN",
					 $token, $tokenlen );
    }
    else {
      $Logger->debug(1,"context not established\nWe got: $token and $tokenlen\n");
      return ( 0, $token );
    }
  }

  #Do the last client step
  
  ( $saslstat, $token, $tokenlen ) =
    $self->{SASLclient}->step( $token, $tokenlen );
  if ( $saslstat == $self->{SASLclient}->SASL_OK ) {
    $Logger->debug(1,"**************************************************************
                   auhtentication succesfull  
**************************************************************\n");
    }
  else {
    $Logger->debug(1,"the last step did not work...");
    return ( 0, "" );
  }

  my $pass = "";
  if ( $self->{mech} eq "TOKEN" ) {
    $pass = $self->{SASLclient}->_call('pass') || "";
  }elsif( $self->{mech} eq "JOBTOKEN"){
    #The token is the username that submitted this job
    $pass=$token;
  }

  return ( 1, $pass );
}

sub encrypt {
    my $self = shift;
    my $in   = shift;

    #Since the verifier dubles as a Cipher object, provide encode method
    my $enc = $self->{SASLclient}->encode($in);
    return $enc;
}

sub decrypt {
    my $self = shift;
    my $in   = shift;

    #Since the verifier dubles as a Cipher object, provide encode method
    my $dec = $self->{SASLclient}->decode($in);
    return $dec;
}

sub blocksize {
    my $self = shift;
    return $self->{SASLclient}->blocksize;
}

sub keysize {
    my $self = shift;
    return 64;
}

sub DESTROY {
    my $self = shift;

    #$self->{SASLclient}->DESTROY();
}
1;
__END__


=head1 NAME

Authen::ClientVerifier - A package used by DB-driver AlienProxy for authentication with AliEn system

=head1 SYNOPSIS

=over 4

=item Authen::ClientVerifier::new()

=item Authen::ClientVerifier::verify()

=item Authen::ClientVerifier::getNumberOfMethods()

=back

=head1 DESCRIPTION

This class specifies which methods the client can use for authentocation. It is specified in the anonymous array $AUTH_METHOD_NAME. The first thing this class does when instanciated is checking if the user has a AlienSSH key in his .alien/identities directory. If that is the case, the SSH is prefered over TOKEN methods. The class then send the entire list of prioritesed authentication methods it can handle, and it's up to ther server to choose which one is used. 

The server reurns the name of the method to use, and this class then creates and instance of Authen:Client:CHOSEN_METHOD. Where CHOSEN_METHOD is the entry is $AUTH_METHODS that coresponds to the chosen method.

When verify() is called, this class sends it to the instance of Authen:Client:CHOSEN_METHOD->verify() with username.

=head1  SEE ALSO

L<Authen::Client::SSH>, L<Authen::Client::Token>





