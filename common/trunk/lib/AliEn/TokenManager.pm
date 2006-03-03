=head1 NAME

AliEn::TokenManager

=head1 DESCRIPTION

The AliEn::TokenManager module is interface to AliEn authorization service.
It provides methods for token retrieval, updating and validating.

=head1 SYNOPSIS

  use AliEn::TokenManager;

  my $tm = AliEn::TokenManager->new;

  $res = $tm->validateUserToken( $user, $role, $token );

  $res = $tm->getUserToken( $user, $role, $password );

  $res = $tm->validateJobToken( $job, $jobToken );

  $res = $tm->updateToken( $user );

=cut

package AliEn::TokenManager;

use strict;
use AliEn::Authen::IIIkey;

use AliEn::SOAP;
use AliEn::Logger;

use Log::TraceMessages qw(t d);
use vars qw (@ISA);

push @ISA, 'AliEn::Logger::LogObject';

Log::TraceMessages::check_argv();

sub new{
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = {};
  #my $attr  = shift;

  my $ini = shift || new AliEn::Config();

  ($ini) or $self->{LOGGER}->error("DBCache","Error: Initial configuration not found.")
      and return;

  $self->{AUTH_HOST} = $ini->{'AUTH_HOST'};
  $self->{AUTH_PORT} = $ini->{'AUTH_PORT'};

  bless( $self, $class );
  $self->SUPER::new() or return;
  $self;
}

# sub validates user token.
# arguments:
#	1. user
#	2. role
#	3. token
# return value:
#	reference to hash with user and token
sub validateUserToken {
  my $self = shift;

  $self->debug(1, "In validateUserToken checking @_");

  my $user   = shift;
  my $role   = shift;
  my $TOKEN  = shift;
  my $done;

  $done =
    SOAP::Lite->uri('AliEn/Service/Authen')
	->proxy("http://$self->{AUTH_HOST}:$self->{AUTH_PORT}")
	  ->verifyToken( $user, $role, $TOKEN );
  
  ($done) and ( $done = $done->result );
  
  $done
    and $self->debug(1, "User $user validated .")
      and return $done;
  
  $self->{LOGGER}->error("TokenManager", "Error contacting the authentication server in $self->{AUTH_HOST}:$self->{AUTH_PORT}");
  
  undef;
}

# sub retrieves user token for given user and password.
# arguments:
#	1. user
#	2. role
#	3. password
# return value:
#	reference to hash with user and token
sub getUserToken{
  my $self = shift;

  $self->debug(1, "In getUserToken checking @_");

  my $user   = shift;
  my $role   = shift;
  my $passwd = shift;
  my $KEY    = "AliGatorMasterKey";
  my $done;

  my $y               = new AliEn::Authen::IIIkey();
  my $encryptedpasswd = $y->crypt( $passwd, $KEY );
  $done            =
    SOAP::Lite->uri('AliEn/Service/Authen')
	->proxy("http://$self->{AUTH_HOST}:$self->{AUTH_PORT}")
	  ->verify( $user, $role, $encryptedpasswd );
  
  ($done) and ( $done = $done->result );
  
  $done
    and $self->debug(1, "Password for user $user validated .")
      and return {token=>$done};
  
  $self->{LOGGER}->error("TokenManager", "Error contacting the authentication server in $self->{AUTH_HOST}:$self->{AUTH_PORT}");
  
  undef;
}

# sub validates job token.
# arguments:
#	1. job id
#	2. job token
# return value:
#	reference to hash with user and token
sub validateJobToken {
  my $self = shift;

  $self->debug(1, "In validateJobToken checking @_.");

  my $job      = shift;
  my $jobToken = shift;
  
  my $done =
    SOAP::Lite->uri('AliEn/Service/Authen')
	->proxy("http://$self->{AUTH_HOST}:$self->{AUTH_PORT}")
	  ->checkJobToken( $job, $jobToken );
  
  ($done) and ( $done = $done->result ) or $self->{LOGGER}->error("TokenManager", "The token has not been validated!") and return;
  
  $done
    and $self->debug(1, "Job validated. ")
      and return $done;
  
  $self->{LOGGER}->error("TokenManager", "Error contacting the authentication server in $self->{AUTH_HOST}:$self->{AUTH_PORT}");

	undef;
}

sub getJobToken{
  my $self = shift;
  
  my $jobID = shift;
  
  my $done =
    SOAP::Lite->uri('AliEn/Service/Authen')
	->proxy("http://$self->{AUTH_HOST}:$self->{AUTH_PORT}")
	  ->getJobToken($jobID);
  
  ($done) and ( $done = $done->result );
  
  $done
    and $self->debug(1, "Job token retrieved.")
      and return $done;
  
  $self->{LOGGER}->error("TokenManager", "Error contacting the authentication server in $self->{AUTH_HOST}:$self->{AUTH_PORT}");
  
  undef;
}


sub updateToken {
    my $self = shift;
    my $user = shift;
    my $passwd;
    my $KEY;

	$self->debug(1, "In updateToken updating token for $user");

	my $tokenfile = $ENV{HOME} . "/.alien/identities/token.$user";
    if ( open( TOKEN, "$tokenfile" ) ) {
        my @lines = <TOKEN>;
        close(TOKEN);
        $KEY = $lines[0];
    }
    else {
        #This is the overall hardcoded KEY, should be hidden a little better.
        $KEY = "AliGatorMasterKey";
    }
    print "User: $user\n";
    print "Enter password:";
    system("stty -echo");
    chomp( $passwd = <STDIN> );
    system("stty echo");
    print("\n");
    my $y               = new AliEn::Authen::IIIkey();
    my $encryptedpasswd = $y->crypt( $passwd, $KEY );

    my $done =
      SOAP::Lite->uri('AliEn/Service/Authen')
      ->proxy("http://$self->{AUTH_HOST}:$self->{AUTH_PORT}")
      ->verify( $user, $encryptedpasswd );

    if ( ( !$done ) or ( !$done->result ) ) {

		#Okay verification was not correct, so either the passowrd really is worng, or the secure serverkley has changed (ProxyServer restart).
        $KEY = "AliGatorMasterKey";
        my $encryptedpasswd = $y->crypt( $passwd, $KEY );
        $done =
          SOAP::Lite->uri('AliEn/Service/Authen')
          ->proxy("http://$self->{AUTH_HOST}:$self->{AUTH_PORT}")
          ->verify( $user, $encryptedpasswd );
    }
    ($done)
      or $self->{LOGGER}->error("TokenManager", "Error contacting the authentication server in $self->{AUTH_HOST}:$self->{AUTH_PORT}")
      and return;

    my $TOKEN = $done->result;

    if ( !$TOKEN ) {
        print STDERR "TokenManager: Your password was wrong, or account non-existent.\n";
    }
    else {
        ( -d "$ENV{HOME}/.alien.identities" )
          or mkdir "$ENV{HOME}/.alien.identities", 0700;
        open( TOKEN, ">$tokenfile" );
        print TOKEN $TOKEN;
        close(TOKEN);
        print STDERR "Your AliEn token has been updated.\n";

		$self->debug(1, "Token $TOKEN for $user updated.");
    }

    return $TOKEN;
}

=head1 METHODS

=over

=item C<new>

  $dbh = AliEn::TokenManager->new;

Creates new AliEn::TokenManager instance.

=item C<validateUserToken>

  $res = $tm->validateUserToken( $user, $role, $token );

Method validates token for stated user and role. If validation is successful
method returns result from AliEn service.

=item C<getUserToken>

  $res = $tm->getUserToken( $user, $role, $password );

Method retrieves token for stated user, role and user password. If authentication
is successful method returns result from AliEn service.

=item C<validateJobToken>

  $res = $tm->validateJobToken( $job, $jobToken );

Method validates token for stated job ID and token. If validation is successful
method returns result from AliEn service. 

=item C<updateToken>

  $res = $tm->updateToken( $user );

Method updates token for stated user. If update is successful, method will return
new token. 

=back

=cut

1;

