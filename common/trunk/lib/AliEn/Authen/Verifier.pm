#
# 19/03-2002 Now uses SASL with support for GSSAPI with Globus certs.
#
$| = 1;
use strict;

package AliEn::Authen::Verifier;

require IO::Socket;
require Storable;
require AliEn::Authen::Comm;

use AliEn::Logger::LogObject;
use AliEn::Config;
use Authen::AliEnSASL;
use AliEn::SOAP;

use vars qw($VERSION @ISA);

push @ISA, 'AliEn::Logger::LogObject';

$VERSION = "1.00";

my $AUTH_ERROR;
my $ADMINDBH;

# ***** For debugging only *************
use Authen::AliEnSASL::Perl;
use Authen::AliEnSASL;
use Authen::AliEnSASL::Perl::Server::PLAIN;
use Authen::AliEnSASL::Perl::Server::GSSAPI;
use GSS;

# **************************************
	 
####################################################################
#
#  This method takes (It passed as a callback to server_new) should
# check what mechanism is used an take action thereafter. Not all
# methods will call this, but some will.
#
#  For instance SSH will call this in order to retrive the users 
# public key.
#
####################################################################
sub get_sshkey {
    my $self = shift;

    my $mech = $self->mechanism;

    if ( $mech eq "SSH" ) {
        my $config = AliEn::Config->new();

        my $ldap = Net::LDAP->new( $config->{LDAPHOST} ) or die "$@";
        my $base = $config->{LDAPDN};

        #	my $organisation = $self->{CONFIG}->{ORG_NAME};

        my $username = $self->{username};

        $ldap->bind();
        my $mesg = $ldap->search(    # perform a search
            base   => "ou=People,$base",
            filter => "(&(objectclass=pkiUser)(uid=$username))"
        );
        if ( !$mesg->count ) {
            print STDERR "User $username does not exist in the LDAP\n";
            return 0;
        }

        my $entry = $mesg->entry(0);
        $ldap->unbind;
        print STDERR "Fetching public SSHkey for $username\n";
        return $entry->get_value('sshkey');

    }

    # If if was not one of these methods, return false.
    return 0;
}
##################### END GET_SSHKEY ###############################

################### BEGIN EXISTS_USER ##############################
#
#  This method takes (It passed as a callback to server_new) should
# check what mechanism is used an take action thereafter. Not all
# methods will call this, but some will.
#
#  It should possibly check if a password is correct (F.x. PLAIN)
# Using PAM or something else.
#
# It must also check if user $self->{username} is allowed to log in as
# $self->{role}. This goes for all methods.
#
# Return 0 if either fails.
#
sub exists_user {
  my $self = shift;
  
  my $mech = $self->mechanism;
  
  my $username = $self->{username};
  my $role     = $self->{role};
  
  print "Authmethod: $mech\n";
  print "Username  :   " . $self->{username} . "\n";
  print "Role      :   " . $self->{role} . "\n";
  
  if ( $mech eq "PLAIN" ) {
    #This one checks the password against masterpassword
    if ( $self->{secret} ne "ThePassword" ) {
      return 0;
    }
  }elsif ( $mech eq "TOKEN" ) {
    if ($username eq "root"){
      $username=$role;
    }
    my $DATA = $ADMINDBH->getFieldsFromTokens($role,"Token,(Expires-Now()) as validPeriod");
    
    defined $DATA
      or print "Error fetching token and password for user $role\n"
	and return 0;
    
    %$DATA
      or print "Token and password for user $role don't exist\n"
	and return 0;
    
    if ( ( $DATA->{Token} ne $self->{secret} ) || ( $DATA->{validPeriod} < 0 ) ) {
      print "Expires in $DATA->{validPeriod} TOKEN $DATA->{Token} and $self->{secret}\n";
      print "Either token was not correct, or it expired\n";
      return 0;
    }
    
# 		my $DATA = $ADMINDBH->getTokenValidPeriod($role);
# 
# 		$DATA
#  			or print "Token and password for user $role don't exist\n"
#  			and return 0;
# 
# 		my ( $dbTOKEN, $dbEXPIRED ) = split "###", $DATA;
#         if ( ( $dbTOKEN ne $self->{secret} ) || ( $dbEXPIRED < 0 ) ) {
#             print "Expires in $dbEXPIRED TOKEN $dbTOKEN and $self->{secret}\n";
#             print "Either token was not correct, or it expired\n";
#             return 0;
#         }
  } elsif ( $mech eq "JOBTOKEN"){
    print "CHECKING THE JOB TOKEN for $self->{username} ($self->{secret})\n";
    my $userName=$ADMINDBH->queryValue("SELECT userName from jobToken where jobId=? and jobToken=?", undef, {bind_values=>[$self->{role}, $self->{secret}]});
    if (!$userName){
      print "Job token is wrong!!\n";

      $AUTH_ERROR="The job token for job $self->{role} is not valid";
      return 0;
    }
    print "Job Token is valid (user $username)\n";
    $username=$role=$userName;
  }

  #Connect to LDAP host;
  my $config = AliEn::Config->new;
  my $ldap   = Net::LDAP->new( $config->{LDAPHOST} ) or die "$@";
  my $base   = $config->{LDAPDN};

  $ldap->bind();

  my $filter;
  
  # Search for different things depending on method
  
  if ( $role =~ /^\// ) {
    print "Translating subject into uid.\n";
    
    # The role is a subject, translate into UID
    $filter = "(&(objectclass=pkiUser)(subject=$role))";
    my $mesg = $ldap->search(
			     base   => "ou=People,$base",
			     filter => $filter
			    );
    my $total = $mesg->count;
    if ( $total == 0 ) {
      print "Failure in translating $role into uid\n";
      
      #No user registered with this subject:(
      $ldap->unbind;
      return 0;
    }
    my $entry = $mesg->entry(0);
    $role = $entry->get_value('uid');
    print "\"$self->{role}\" => $role\n";
    $self->{role} = $role;
    
    # Role is now translated into a uid!!
  }
  if ( $mech eq "GSSAPI" ) {
    
    # If method is GSSAPI, $username will contain the full subject of the 
    # clients certificate. Search for this.
    $filter = "(&(objectclass=pkiUser)(subject=$username))";
  }
  else {
      
    #All other methods will return the uid (string)
    $filter = "(&(objectclass=pkiUser)(uid=$username))";
  }
  my $mesg = $ldap->search(    # perform a search
			   base   => "ou=People,$base",
			   filter => $filter,
			  );
  my $total = $mesg->count;
  if ( $total < 1 ) {
    print "No entry with common name $username\n";
    $ldap->unbind;
    $AUTH_ERROR="No entry with common name $username";
    return 0;
  }
  
  if ($total > 1) {
    print "There are several entries with common name $username\n";
  }
  
  for (my $i=0; $i<$total; ++$i) {
    my $entry = $mesg->entry($i);
    
    my $uid   = $entry->get_value('uid');
    if ($role eq $uid) {
      print "Wants to be himself!!\n";
      $ldap->unbind;
      return $uid;
    }
    
    print "Checking if the user '$uid' can be '$role'\n";
    my $mesgRole = $ldap->search(    # perform a search
				 base   => "ou=Roles,$base",
				 filter => "(&(uid=$role)(|(public=yes)(users=$uid)))",
				);
    
    if ($mesgRole->count > 0) {
      $ldap->unbind;
      return $role;
    }
    print "User $uid not allowed to be $role\n";
  }

  $ldap->unbind;
  print "User $username not allowed to be $role\n";
  $AUTH_ERROR="user $username is not allowed to be $role";
  return 0;
}

sub new {
    my $proto = shift;
    my $self  = {};
    bless( $self, ( ref($proto) || $proto ) );
    my $adbh = shift;
    $ADMINDBH = $adbh;
    bless $self, $proto;
    $self->SUPER::new() or return;
    $self->{ADMINDBH} = $adbh;

#    if ($DEBUG) {
#        $self->{LOGGER}->debugOn();
#    }
    $self->debug(1, "Creating instance of Authen::Verifier" );

    # ## Set up globus variables (SHOULD BE DONE AT START IN SCRIPT Alien.sh) ##
    $ENV{X509_RUN_AS_SERVER} = 1;
     my $config = AliEn::Config->new();
    my $org=$config->{ORG_NAME};
    foreach ( "globus", "identities.$org" ) {
	my $dir="$ENV{ALIEN_HOME}/$_";
	if ( -e  "$dir/usercert.pem" ) { 
	    $ENV{X509_USER_CERT}     = "$dir/usercert.pem";
	    $ENV{X509_USER_KEY}      = "$dir/userkey.pem";
	    last;
	}
    }
    ###########################################################################

    my $callbacks = {
        credential => \&get_sshkey,
        exists     => \&exists_user,
    };

    #Create SASL object
    my $sasl =
      new Authen::AliEnSASL( $callbacks, "Alien Authentication server" );

    #  Fetch server object
    #
    # IMPORTANT: the last option specifies minimum sec. level. 
    #
    $self->{SASLserver} =
      $sasl->server_new( "ProxyServer", "hostname", "noanonymous", 0 );

    $self->info("I have these mechs installed: " . $self->{SASLserver}->listmech() );

    return $self;
}

sub verify {
  my $self   = shift;
  my $socket = shift;

  $self->{socket} = $socket;
  
  $self->debug(1, "Writing greeting" );
  AliEn::Authen::Comm::write_buffer( $self->{socket},
				     "ProxyServer $VERSION Ready Again",
				     "", 0 );
  $AUTH_ERROR="";
  my $done = 0;
  do {
    my ( $status, $inTok, $inToklen ) =
      AliEn::Authen::Comm::read_buffer( $self->{socket} );
    if ( $status eq "REQUEST AUTH" ) {
      $self->debug(1, "User wishes to authenticate" );
      
      AliEn::Authen::Comm::write_buffer( $self->{socket}, "AUTH OK", "",
				       );
      if ( $self->authenticate($inTok) ) {
	$self->info( "Context established\n" );
	$done = 1;
      }
      else {
	$AUTH_ERROR or $AUTH_ERROR="user did not authenticate";
	
	$self->info("Error context not established: $AUTH_ERROR" );
	return (undef, $AUTH_ERROR);
      }
    }
    elsif ( $status eq "REQUEST MECHS" ) {
      $self->debug(1, " Client wishes to retrive list of mecs" );
      my $mechs = $self->{SASLserver}->listmech();
      $self->debug(1, " Mechlist: $mechs" );
      AliEn::Authen::Comm::write_buffer( $self->{socket},"AliEnAUTH MECHS",
					 $mechs, length($mechs) );
    }
    else {
      $self->debug(1,"The command is not understood by this server\n" );
      return;
    }
  } while ( !$done );
  
  my $username = $self->{SASLserver}->getUsername;
  my $role     = $self->{SASLserver}->getRole;
#  print "On the server side, we should also know that it is $username\n";
#  $role='newuser';
  $self->info("Get password for $username (in $role)" );
  my $passwd = $self->{ADMINDBH}->getPassword($role);
  if (! $passwd ){ 
    $self->info("Couldn't get the password from the database!!!\nThe DBI error is $DBI::errstr");
    
    my $message= "User $role does not have the necessary privileges on the database";
    
    if ($DBI::errstr) {
      $DBI::errstr =~ /Can\'t connect/ and
	return (undef, "I think that the database is down... please connect later");
      $DBI::errstr =~ /Too many connections/ and 
	return (undef, "There are too many connections to the database... please connect later");
      $message.= "\n\t(DBI error: $DBI::errstr)";
    }
    #ok, maybe the user didn't exist... let's call the authen and create
    #the user

    $passwd=$self->createUser($role);
    $passwd and return ($role, $passwd);
    return (undef, "User $role does not have the necessary privileges on the database\n");
  }
  $self->info("Returning the password for $role)" );
  $self->{ADMINDBH}->disconnect();
  
  return ( $role, $passwd );

}

##
# This subroutine is called whenever a user is authenticated, but (s)he 
# doesn't have the privileges to talk to the database. Let's try to 
# connect to the Authen and create the entry
#
#
sub createUser{
  my $self=shift;
  my $role=shift;
  $self->info( "Checking if the user $role exists in the database");
  $self->{SOAP} or $self->{SOAP}=new AliEn::SOAP;

  $self->debug(1, "Let's do the soap call...");
  my $done=$self->{SOAP}->CallSOAP("Authen", "checkUserPassword", $role);

  $self->info("Got $done");
  $done or return;
  $self->info(1, "The user has just been created!!");
  return  $self->{ADMINDBH}->getPassword($role);
}

sub authenticate {
  my $self   = shift;
  my $method = shift;

  local $SIG{ALRM} =sub {
    print "$$ timeout in the authentication\n";
    die("timeout in disconnect");
  };
  alarm 60;

  my ( $status, $inTok, $inToklen ) =
    AliEn::Authen::Comm::read_buffer( $self->{socket} );

  $self->info("Method used is $method" );
  
  my ( $stat, $outTok, $outToklen ) =
      $self->{SASLserver}->start( $method, $inTok, $inToklen );

    $self->info("Method $method started" );

  my $outbuffer;
  
  while ( $stat == $self->{SASLserver}->SASL_CONTINUE ) {
    $self->debug(1,"Sending $outToklen bytes...\n");
    AliEn::Authen::Comm::write_buffer( $self->{socket},
				       "AliEnAuth CONTINUE",
				       $outTok, $outToklen );
    ( $status, $inTok, $inToklen ) =
      AliEn::Authen::Comm::read_buffer( $self->{socket} );
    ( $stat, $outTok, $outToklen ) =
      $self->{SASLserver}->step( $inTok, $inToklen );
    $self->debug(1, "Stepping with $inToklen bytes\n");
  }
  alarm 0;
  if ( $stat == $self->{SASLserver}->SASL_OK ) {
    $self->debug(1,"Server context is ok\n");
    $outbuffer = "AliEnAuth OK";
    AliEn::Authen::Comm::write_buffer( $self->{socket}, $outbuffer, $outTok,
				       $outToklen );
    return 1;
  }

  $outbuffer = "AliEnAuth NOK";
  $AUTH_ERROR or $AUTH_ERROR="user not authenticated";

  $self->debug(1,"Returning error: $AUTH_ERROR\n");

  AliEn::Authen::Comm::write_buffer( $self->{socket}, $outbuffer, 
				     $AUTH_ERROR, length($AUTH_ERROR) );
  return 0;
}

sub encrypt {
    my $self = shift;
    my $in   = shift;

    #Since the verifier dubles as a Cipher object, provide encode method
    my $enc = $self->{SASLserver}->encode($in);
    return $enc;
}

sub decrypt {
    my $self = shift;
    my $in   = shift;

    #Since the verifier dubles as a Cipher object, provide decode method
    my $dec = $self->{SASLserver}->decode($in);
    return $dec;
}

sub blocksize {
    my $self = shift;
    return $self->{SASLserver}->blocksize;
}

sub keysize {
    my $self = shift;
    return 64;
}
1;


__END__

=head1 NAME

Authen::Verifier - a PERL object for authentication in AliEn


=head1 SYNOPSIS

use AliEn::Authen::Verifier;

=head1 DESCRIPTION

=head1 AUTHOR

Jan-Erik Revsbech <revsbech@fys.ku.dk>

=cut





