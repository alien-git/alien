#
# 16/07/2007 Backward compatible Authen.pm with the use of cyrus-sasl
# 

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

$ENV{'SASL_PATH'} = $ENV{'ALIEN_ROOT'}."/lib/sasl2";
use Authen::SASL;

$VERSION = "1.00";

my $ALIEN_AUTH_ERROR;
my $CYRUS_AUTH_ERROR;
my $ADMINDBH;
my $CALLBACK_DATA;

# ***** For debugging only *************
use Authen::AliEnSASL::Perl;
use Authen::AliEnSASL;
use Authen::AliEnSASL::Perl::Server::PLAIN;
use Authen::AliEnSASL::Perl::Server::GSSAPI;
use GSS;

# **************************************
my $SubjectToUid = sub {
  my $subject = shift;

  my $uid=$ADMINDBH->queryValue("SELECT user from USERS_LDAP where dn=?",
			       undef, {bind_values=>[$subject]});
  if (! $uid){
    print STDERR "Failure in translating $subject into $uid\n";
    return 0;
  }
  print STDERR "***THE uid for $subject is $uid\n";
  return $uid;

    # Role is now translated into a uid!!
};	 

my  $checkUserRole = sub {
  my $user=shift;
  my $role=shift;

  if ( $role ne $user ) {
    print STDERR "Checking if the user '$user' can be '$role'\n";
    my $total=$ADMINDBH->queryValue("select count(*) from USERS_LDAP_ROLE where user=? and role=?", undef, 
					  {bind_values=>[$user, $role]});
    
    if ( !$total ) {
      print STDERR "User $user is not allowed to be $role\n";
      return "";
    }
  }
  return $role

};

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
    print STDERR "GETTING THE KEY FROM THE DATATBAT\n";
    my $username=$self->{username};
    my $ssh=$ADMINDBH->queryValue("select SSHKey from TOKENS where username=?",
				  undef, {bind_values=>[$username]});
    if ( ! $ssh ) {
      print STDERR "User $username does not exist in the LDAP\n";
      return 0;
    }
    print STDERR "Fetching public SSHkey for $username\n";
    return $ssh;
  }

  # If if was not one of these methods, return false.
  return 0;
}
##################### END GET_SSHKEY ###############################

###########      GET_SSHKEY_CYRUS_SASL  ############################
sub get_sshkey_cyrus_sasl {
  #my $self = shift;
  my $in = shift;
  
  #my $mech = $self->mechanism;
  
  my ( $mech, $username) = split "\0", $$in;


  if ( $mech eq "SSH" ) {
    print STDERR "GETTING THE KEY FROM THE DATATBAT\n";
    my $ssh=$ADMINDBH->queryValue("select SSHKey from TOKENS where username=?",
				  undef, {bind_values=>[$username]});
    if ( ! $ssh ) {
      print STDERR "User $username does not exist in the LDAP\n";
      return 0;
    }
    print STDERR "Fetching public SSHkey for $username\n";
    return $ssh;
  }

  # If if was not one of these methods, return false.
  return 0;
}
########### GET_SSHKEY_CYRUS_SASL  END ###########################

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
  } elsif ( $mech eq "JOBTOKEN"){
    print "CHECKING THE JOB TOKEN for $self->{username} ($self->{secret})\n";
    my $userName=$ADMINDBH->queryValue("SELECT userName from jobToken where jobId=? and jobToken=?", undef, {bind_values=>[$self->{role}, $self->{secret}]});
    if (!$userName){
      print "Job token is wrong!!\n";

      $ALIEN_AUTH_ERROR="The job token for job $self->{role} is not valid";
      return 0;
    }
    print "Job Token is valid (user $username)\n";
    $username=$role=$userName;
  }
  print "WE DON'T CONNECT TO LDAP ANYMORE\n";
  # Search for different things depending on method

  if ( $username =~ /^\// ) {
    my $tmprole=$username;
    $username=$SubjectToUid->( $username );
    if (! $username ) {
      print "Failure in translating $tmprole into uid\n";	
      #No user registered with this subject:(
      return 0;
    }
  }

  $checkUserRole->($username, $role) and return $role;
  print "User $username not allowed to be $role\n";

  $ALIEN_AUTH_ERROR="user $username is not allowed to be $role";
  return 0;
}

################### EXISTS_USER_CYRUS_SASL##########################
sub exists_user_cyrus_sasl {

  #my $self = shift;
  my $in = shift;

  my ( $mech, $username, $role, $secret) = split "\0", $$in;

  print "Authmethod: $mech\n";
  print "Username  :   " . $username . "\n";
  print "Role      :   " . $role . "\n";

  if ( $mech eq "PLAIN" ) {
    #This one checks the password against masterpassword
    if ( $secret ne "ThePassword" ) {
      return 0;
    }
  }
  elsif ( $mech eq "TOKEN" ) {
    if ($username eq "root") {
      $username=$role;
    }
    my $DATA = $ADMINDBH->getFieldsFromTokens($role,"Token,(Expires-Now()) as validPeriod");

    defined $DATA
      or print "Error fetching token and password for user $role\n"
	and return 0;

    %$DATA
      or print "Token and password for user $role don't exist\n"
	and return 0;
    
    if ( ( $DATA->{Token} ne $secret ) || ( $DATA->{validPeriod} < 0 ) ) {
      print "Expires in $DATA->{validPeriod} TOKEN $DATA->{Token} and $secret\n";
      print "Either token was not correct, or it expired\n";
      return 0;
    }
  }
  elsif ($mech eq "JOBTOKEN")  {
    print "CHECKING THE JOB TOKEN for $username ($secret)\n";

    my $userNameFromDB =
      $ADMINDBH->queryValue("SELECT userName from jobToken where jobId=? and jobToken=?", undef, {bind_values=>[$role, $secret]});
    
    if (!$userNameFromDB)      {
      print "Job token is wrong!!\n";
      $CYRUS_AUTH_ERROR="The job token for job $role is not valid";
      return 0;
    }

    print "Job Token is valid (user $username)\n";
    $username=$role=$userNameFromDB;
  }

  print "WE DON'T CONNECT TO LDAP ANYMORE\n";
  # Search for different things depending on method
  
  if ( $username =~ /^\// ) {
    my $tmprole=$username;
    $username=$SubjectToUid->( $username );
    if (! $username ) {
      print "Failure in translating $tmprole into uid\n";	
      #No user registered with this subject:(
      return 0;
    }
    # Role is now translated into a uid!!
  }

  $checkUserRole->($username, $role) and return $role;
  print "User $username not allowed to be $role\n";

  $CYRUS_AUTH_ERROR="user $username is not allowed to be $role";
  return 0;
}
################### END EXISTS_USER_CYRUS_SASL######################

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

    #Create AliEnSASL object
    my $sasl =
      new Authen::AliEnSASL( $callbacks, "Alien Authentication server" );

    #Create Cyrus SASL object
    
    my $cyrus_sasl = Authen::SASL->new (
                                            callback => {
                                                         pass => [\&get_sshkey_cyrus_sasl,  \$CALLBACK_DATA], # The only way to pass data
                                                         auth => [\&exists_user_cyrus_sasl, \$CALLBACK_DATA]  # to callback functions
                                                        }
                                       );

    #  Fetch server object
    #
    # IMPORTANT: the last option specifies minimum sec. level. 
    #
    $self->{AliEnSASLServer} = $sasl->server_new( "ProxyServer", "hostname", "noanonymous", 0 );

    
    #
    # Fetch Cyrus SASL Server object
    #     

    $self->{CyrusSASLServer} = $cyrus_sasl->server_new ("ProxyServer", "localhost");    

    $self->info("I have these AliEn SASL mechs installed: " . $self->{AliEnSASLServer}->listmech() );
    $self->info("I have these Cyrus SASL mechs installed: " . $self->{CyrusSASLServer}->listmech("", " ", "") );
    
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
  $ALIEN_AUTH_ERROR="";
  $CYRUS_AUTH_ERROR="";  
  my $authWithAliEnSASL;
  my $authOut;
  my $done = 0;
  do 
  {
    	my ( $status, $inTok, $inToklen ) = AliEn::Authen::Comm::read_buffer( $self->{socket} );

        if ( $status eq "REQUEST AUTH" ) 
        {
             $self->debug(1, "User wishes to authenticate" );
             AliEn::Authen::Comm::write_buffer( $self->{socket}, "AUTH OK", "", );

             if ( $self->authenticate_alien_sasl ($inTok) ) 
             {
	         $self->info( "Context established using AliEn SASL\n" );
                 $authWithAliEnSASL = "1";  
	         $done = 1;
             }
             else 
             {
	        $ALIEN_AUTH_ERROR or $ALIEN_AUTH_ERROR="user did not authenticate (AliEn SASL was used)"; 
                $self->info("Error context not established: $ALIEN_AUTH_ERROR" );
	        return (undef, $ALIEN_AUTH_ERROR);
             }
        }
        elsif ( $status eq "REQUEST MECHS" ) 
        {
             $self->debug(1, " Client wishes to retrive list of AliEn SASL mechs " );
             my $mechs = $self->{AliEnSASLServer}->listmech();
             $self->debug(1, " Mechlist: $mechs" );
             AliEn::Authen::Comm::write_buffer( $self->{socket},"AliEnAUTH MECHS", $mechs, length($mechs) );
        }
        elsif ($status eq "REQUEST CYRUS MECHS" )
        {
             $self->debug(1, " Client wishes to retrive list of Cyrus SASL mechs" );
             my $tmpmechs = $self->{CyrusSASLServer}->listmech("", " ", "");

                # we need to have mechanisms in the correct order
                my $mechs="";
                $tmpmechs =~ /GSSAPI ?/ and $mechs = "GSSAPI ";
                $tmpmechs =~ /SSH ?/ and $mechs .= "SSH ";
                $tmpmechs =~ /JOBTOKEN ?/ and $mechs .= "JOBTOKEN";
                $tmpmechs =~ /TOKEN ?/ and $mechs .= "TOKEN ";
               
	
 
             $self->debug(1, " Mechlist: $mechs" );
             AliEn::Authen::Comm::write_buffer( $self->{socket},"AliEnAUTH MECHS", $mechs, length($mechs) );
        }
        elsif ($status eq "REQUEST CYRUS AUTH" )
        {
            $self->debug(1, "User wishes to authenticate with Cyrus SASL " );
            AliEn::Authen::Comm::write_buffer( $self->{socket}, "AUTH OK", "",);

            $authOut = $self->authenticate_cyrus_sasl($inTok);
            if ( $authOut )
            {
               $self->info( "Context established using Cyrus SASL\n" );
               $authWithAliEnSASL = 0; 
               $done = 1;
            }
            else
            {
               $CYRUS_AUTH_ERROR or $CYRUS_AUTH_ERROR="user did not authenticate (Cyrus SASL was used)";
               $self->info("Error context not established: $CYRUS_AUTH_ERROR" );
               return (undef, $CYRUS_AUTH_ERROR);
            }
        }     
        else ## Command not understood
        {
             $self->debug(1,"The command is not understood by this server\n" );
             return;
        }
  } while ( !$done );
  

  my $username;
  my $role;
  my $mech;

   if ($authWithAliEnSASL eq "1")
  {
    $username = $self->{AliEnSASLServer}->getUsername;
    $role     = $self->{AliEnSASLServer}->getRole;
  }
  else ## User authenticated with Cyrus SASL
  {
    
    ($mech, $username, $role) = split "\0", $CALLBACK_DATA;
    ($mech eq "JOBTOKEN") and ($role = $authOut);
  }

  $self->info("Get password for $username (in $role)" );

  my $passwd = $self->{ADMINDBH}->getPassword($role);

  if (! $passwd )
  { 
    my $error = "";
    $DBI::errstr and $error = $DBI::errstr;

    $self->info("Couldn't get the password from the database!!!\nThe DBI error is $error");
    
    $error =~ /Can\'t connect/ and
      return (undef, "I think that the database is down... please connect later");
    $error =~ /Too many connections/ and 
      return (undef, "There are too many connections... please connect later");

    #ok, maybe the user didn't exist... let's call the authen and create
    #the user
    
    $passwd=$self->createUser($role);
    $passwd and return ($role, $passwd);

    return (undef, "User $role does not have the necessary privileges on the database\n\t(error $error)\n");
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

sub authenticate_alien_sasl {
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
      $self->{AliEnSASLServer}->start( $method, $inTok, $inToklen );

    $self->info("Method $method started" );

  my $outbuffer;
  
  while ( $stat == $self->{AliEnSASLServer}->SASL_CONTINUE ) {
    $self->debug(1,"Sending $outToklen bytes...\n");
    AliEn::Authen::Comm::write_buffer( $self->{socket},
				       "AliEnAuth CONTINUE",
				       $outTok, $outToklen );
    ( $status, $inTok, $inToklen ) =
      AliEn::Authen::Comm::read_buffer( $self->{socket} );
    ( $stat, $outTok, $outToklen ) =
      $self->{AliEnSASLServer}->step( $inTok, $inToklen );
    $self->debug(1, "Stepping with $inToklen bytes\n");
  }
  alarm 0;
  if ( $stat == $self->{AliEnSASLServer}->SASL_OK ) {
    $self->debug(1,"Server context is ok\n");
    $outbuffer = "AliEnAuth OK";
    AliEn::Authen::Comm::write_buffer( $self->{socket}, $outbuffer, $outTok,
				       $outToklen );
    return 1;
  }

  $outbuffer = "AliEnAuth NOK";
  $ALIEN_AUTH_ERROR or $ALIEN_AUTH_ERROR="user not authenticated";

  $self->debug(1,"Returning error: $ALIEN_AUTH_ERROR\n");

  AliEn::Authen::Comm::write_buffer( $self->{socket}, $outbuffer, 
				     $ALIEN_AUTH_ERROR, length($ALIEN_AUTH_ERROR) );
  return 0;
}

sub authenticate_cyrus_sasl
{
  my $self   = shift;
  my $method = shift;

  local $SIG{ALRM} = sub {
    print "$$ timeout in the authentication\n";
    die("timeout in disconnect");
  };
  alarm 60;

  my ( $status, $inTok, $inToklen ) = AliEn::Authen::Comm::read_buffer( $self->{socket} );

  $self->info("Method used is $method");
  $CALLBACK_DATA = join "\0", $method ,$inTok;

  

  my $outTok = $self->{CyrusSASLServer}->server_start( $inTok,$method);
  my $outToklen = length ($outTok);
  my $beforeLast;

  $self->info ("Method $method started");

  while ( $self->{CyrusSASLServer}->need_step() )
  {
     $beforeLast = $outTok;
     $outToklen = length ($outTok);
     $self->debug(1,"Sending $outToklen bytes...\n");
     AliEn::Authen::Comm::write_buffer( $self->{socket},
                                        "AliEnAuth CONTINUE",
                                        $outTok,
                                        $outToklen );

     ( $status, $inTok, $inToklen ) = AliEn::Authen::Comm::read_buffer( $self->{socket} );

    $outTok = $self->{CyrusSASLServer}->server_step( $inTok);
    $self->debug(1, "Stepping with $inToklen bytes\n");
  }

  alarm 0;

  if ( $self->{CyrusSASLServer}->code() == 0 ) #SASL OK
  {
    $self->debug(1,"Server context is ok\n");

    if ($method eq "JOBTOKEN")
    {
        $beforeLast =~ s/JOBTOKENSASL OK\s*//g;
        return $beforeLast;
    }
    else
    {
        return 1;
    }
  }

  $CYRUS_AUTH_ERROR =  $self->{CyrusSASLServer}->error()." Error code is ". $self->{CyrusSASLServer}->code()." Outis: $inToklen ";
  $self->debug(1,"Returning error: $CYRUS_AUTH_ERROR\n");

  AliEn::Authen::Comm::write_buffer( $self->{socket},
                                     "AliEnAuth NOK",
                                     $CYRUS_AUTH_ERROR,
                                     length($CYRUS_AUTH_ERROR) );
  return 0;
}


sub encrypt {
    my $self = shift;
    my $in   = shift;

    #Since the verifier dubles as a Cipher object, provide encode method
    my $enc = $self->{AliEnSASLServer}->encode($in);
    return $enc;
}

sub decrypt {
    my $self = shift;
    my $in   = shift;

    #Since the verifier dubles as a Cipher object, provide decode method
    my $dec = $self->{AliEnSASLServer}->decode($in);
    return $dec;
}

sub blocksize {
    my $self = shift;
    return $self->{AliEnSASLServer}->blocksize;
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





