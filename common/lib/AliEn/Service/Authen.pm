package AliEn::Service::Authen;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;

use AliEn::Service;

use AliEn::Authen::IIIkey;
use POSIX;
use Authen::PAM;
use AliEn::Database::Catalogue;

use Crypt::OpenSSL::RSA;
use Crypt::OpenSSL::Random;
use AliEn::Util;
use AliEn::UI::Catalogue::LCM::Computer;
use Time::HiRes;

use base qw(JSON::RPC::Legacy::Procedure);

use vars qw (@ISA $DEBUG);
push @ISA, "AliEn::Service";

$DEBUG = 0;
my $self = {};

# *******************************
# Needed for PAM authentification
my $VFusername = "";
my $VFpasswd   = "";

# *******************************
my $my_conv_func = sub {
    my @res;

    while (@_) {
        my $code = shift;
        my $msg  = shift;
        my $ans  = "";

        $ans = $VFusername if ( $code == PAM_PROMPT_ECHO_ON() );
        $ans = $VFpasswd   if ( $code == PAM_PROMPT_ECHO_OFF() );

        push @res, ( PAM_SUCCESS(), $ans );
    }
    push @res, PAM_SUCCESS();
    return @res;
};



#my $dbh;

my $ADMINPASSWD;
my $LDAP;

sub status2 :  Public()  {
  return "AliEn Service: $self->{CONFIG}->{VERSION}";
}  
sub initialize {
  $self = shift;
  my $options = (shift or {});
  $options->{role} = 'admin';
  $options->{ROLE} = 'admin';

  $self->{PORT}        = $self->{CONFIG}->{'AUTH_PORT'};
  $self->{HOST}        = $self->{CONFIG}->{'AUTH_HOST'};
  $self->{SERVICE}     = "Authen";
  $self->{SERVICENAME} = "Authen";
  $self->{LISTEN}      = 10;
  $self->{PREFORK}     = 5;

  #Delete the password from options. Not needed anymore. (MUST NOT BE SET)
  $options->{password} = '';

  $self->{options} = $options;
  $self->info("Initializing catalog daemon");

      
#  $self->{TASK_DB}
#    or $self->{TASK_DB} = AliEn::Database::TaskQueue->new({ROLE => 'admin', SKIP_CHECK_TABLES => 1});
#  $self->{TASK_DB} or $self->info("Error getting the instance of the taskDB!!") and return;

  $self->_ConnectToLDAP() or return;
  $self->{UI} = AliEn::UI::Catalogue::LCM::Computer->new($options) or $self->info("Error getting the ui") and return;
  $self->{UI}->{CATALOG}->{envelopeCipherEngine}
    or $self->info(
    "Error! We can't create the security envelopes!! Please, define the SEALED_ENVELOPE_ environment variables")
    and return;

  return $self;
}

#################################################################
# Create envelope, only for backward compability on < v2.19, see below
#
################################################################

sub createEnvelope {
  my $other = shift;
  my $user  = shift;

  my $before = Time::HiRes::time();
  $self->{LOGGER}->set_error_msg();
  $self->info("$$ Ready to create the envelope for user $user (and @_)");

  $self->{UI}->execute("user", "-", $user);

  $self->debug(1, "Executing access");
  my $options = shift;
  $options .= "v";
  my (@info) = $self->{UI}->execute("access", $options, @_);
  $self->info("$$ Everything is done for user $user (and @_)");
  grep(/^-debug(=\d+)?/, @_) and $self->info("Removing the debug sign") and $self->{UI}->execute("debug");

  my $time = Time::HiRes::time() - $before;
  $self->logEntry("$user envelope", $time);

  return @info;
}
sub doPackMan : Public {
	my $other=shift;
	
	#WITH RPC, all the arguments are passed in th first option. 
  my $ref=shift;
  @_=@$ref;
	
	my $user = shift;
	my $func = shift;
	$self->info("Authen is going to do a packman operation : $func");
	my $op={ 'getListPackagesFromDB'=>1,
		'findPackageLFNInternal'=>1,
		'registerPackageInDB'=>1,
		'deletePackageFromDB'=>1,
#		'recomputePackages'=>1,
	};
	
	if (not $op->{$func}){
		$self->info("Trying to do an invalid operation: '$func'");
		return {rcvalues=>[], rcmessages=>["'$func' is not a valid operation"]};
	}
	$self->info("Ready to do a packman operation: $func");
	$self->{LOGGER}->keepAllMessages();
	
	my @result=$self->{UI}->{PACKMAN}->$func(@_);
	my @messages = @{$self->{LOGGER}->getMessages()};
	$self->{LOGGER}->displayMessages();
	$self->info("DONE!");
	return {rcvalues=>\@result, rcmessages=>\@messages};
	
	
}
sub  CheckLocalPassword {
  my $self2=shift;
  my $user=shift;
  my $passwd=shift;

  my ( $pamh, $res );
  my $encrypter = new AliEn::Authen::IIIkey();

  my $tty_name = ttyname( fileno(STDIN) );

  $self->info("Checking password of $user" );

  $VFpasswd = $encrypter->decrypt( $passwd, "AliGatorMasterKey" );
  $self->debug(1, "Before PAM init" );
  $VFusername = $user;
  $pamh = new Authen::PAM( "login", $VFusername, \&$my_conv_func );
  $self->debug(1, "After PAM init" );
  $res = $pamh->pam_set_item( PAM_TTY(), $tty_name );
  $res = $pamh->pam_authenticate();

  if ($res) {
    $self->info("User passwd is not correct!" );
    return (0, "Password is not correct");
  }
  $self->info("User passwd is correct" );

  $self->checkUserDB($user) or
    return ( 0, "User $user does not exist in LDAP" );

  return 1;
}



sub insertCert : Public {
  print STDERR "HELLO WORLD IN INSERTCERT\n";
  $self->{LOGGER}->info("Do we have a logger");
  print STDERR "SIPE\n";
    my $self2         = shift;
    my $organisation = shift;
    my $user         = shift;
    my $passwd       = shift;
    my $subject      = shift;
print STDERR "BEFORE THE CALL\n";
    my ($ok, $message)=$self->CheckLocalPassword($user, $passwd);
    $ok or return (-1, $message);

    print STDERR "Modifying certificate subject for $user\n";
    $self->_checkLDAPConnection() or return (0, "Can't connect to the ldap");
    eval {
      my $mesg = $LDAP->search(
                               base   => "ou=People,$self->{CONFIG}->{LDAPDN}",
        filter => "(subject=$subject)"
                              );

      my $total = $mesg->count;

      if ( $total > 0 ) {

        #This certificate alredy exists one time.
        print STDERR "Certificate alredy exists\n";
        return ( -1,
            "A certificate with subject $subject is alredy in LDAP server" );

      }

      if ($LDAP->modify("uid=$user,ou=People,$self->{CONFIG}->{LDAPDN}",
                        replace => { 'subject' => $subject })){
        $self->info("$subject mapped to AliEn user $user" );

      }
      else {
        $self->{LOGGER}->warning( "Authen", "Error in updating subject for $user" );
        return ( -1,
                 "An LDAP error occured on serverside. Contact AliEn administrators" );
      }
    };
    if ($@){
      $self->info("Error doing the ldap query: $@");
      return (-1, "Error doing the ldap query: $@");
    }

    # DISABLING automatic user creation
    #($self->{addbh}->existsToken($user) )  or $self->addUser($user);
    ($self->{addbh}->existsToken($user) )  or return;

    return 1;
}


sub doOperation : Public  {
  my $other     = shift;

  #WITH RPC, all the arguments are passed in th first option. 
  my $ref=shift;
  @_=@$ref;
  my $user      = shift;
  my $directory = shift;
  my $op        = shift;
  $self->info("$$ Ready to do an operation for $user in $directory (and $op '@_')");

  my $jobID  = "0";
  my $before = Time::HiRes::time();
  if ($user =~ s/^alienid://) {
    $self->info("We are authenticating with a job token");
    my ($job, $token) = split(/ /, $user, 2);
    $self->info("ID: $job, TOKEN $token");
    $jobID = $job;
    my $role = $self->{UI}->{QUEUE}->{TASK_DB}->getUsername($job, $token);
    ($role)
      or $self->info("The job token is not valid")
      and return {rcvalues => [], rcmessages => ["The job token for job $job is not valid"]};
    $self->info("Doing the operation as $role");
    $user = $role;

  } elsif ($self->{HOST} =~ /^https/) {
    $self->info("Checking the authentication");
    $self->checkAuthentication($user)
      or return {    #rc=>1,
      rcvalues   => [],
      rcmessages => ["You are not authenticated as $user"]
      };
  }

  $self->{UI}->execute("user", "-", $user);
  my $mydebug = $self->{LOGGER}->getDebugLevel();
  my $params  = [];

  (my $debug, $params) = AliEn::Util::getDebugLevelFromParameters(@_);
  $debug and $self->{LOGGER}->debugOn($debug);
  @_ = @{$params};
  $self->{LOGGER}->keepAllMessages();
  $self->{UI}->{CATALOG}->{DISPPATH} = $directory;
  my @info;
  if ($op =~ /((authorize)|(copyDirectoryStructure))/) {
    @info = $self->{UI}->{CATALOG}->$1(@_, $jobID);
  } else {
    @info = $self->{UI}->execute($op, @_);
  }
  
  my @loglist = @{$self->{LOGGER}->getMessages()};

  $debug and $self->{LOGGER}->debugOn($mydebug);
  $self->{LOGGER}->displayMessages();
  $self->info("$$ doOperation DONE for user $user (and @_) result: @info, length:" . scalar(@info));
  
  my $time = Time::HiRes::time() - $before;

  my $infoNames = join('', @info);
  my @InfoArray=split('\&', $infoNames);
    
  if ($#InfoArray <= 0 and $InfoArray[0] !=1 ){
     $self->logEntry("$user $op", $time, undef, \@_);
  } else {
  	 $self->logEntry("$user $op", $time, \@InfoArray);
  }
  	
  return {rcvalues => \@info, rcmessages => \@loglist};

}

sub logEntry {
  my $self    = shift;
  my $message = shift;
  my $time    = shift;
  my @time    = localtime();
  my $envelope = shift;

  if ($envelope and ${$envelope}[1] ne ""){
  	my $access = ${$envelope}[1];
  	$access =~ s/\\//g;
    $access =~ s/access=//;
    my $lfn = ${$envelope}[2];
    $lfn =~ s/\\//g;
    $lfn =~ s/lfn=//;
    my $se = ${$envelope}[4];
    $se =~ s/\\//g;
    $se =~ s/se=//;
    $message .= " $access $lfn $se";
  } else{
  	my $failureDetails = shift;
  	my $operation = ${$failureDetails}[0];
  	my $var = ${$failureDetails}[1];
  	if (defined $var && ref($var) eq "HASH")
  	{
  		$message .= " FAILED $operation $var->{lfn} $var->{wishedSE}";
  	}
  }
  
my $month = (1+$time[4]);
my $day = $time[3];
if ($month < 10) {
 $month = "0$month";
}
if ($day < 10) {
 $day = "0$day";
}
 my $logDir  = "$self->{CONFIG}->{LOG_DIR}/Authen_ops/" . (1900 + $time[5]) . "/" . $month .  "/$day/";
# my $logDir  = "$self->{CONFIG}->{LOG_DIR}/Authen_ops/" . (1900 + $time[5]) . "/" . (1 + $time[4]) . "/$time[3]/";

  $self->info("GOING to $logDir");
  (-d $logDir) or system("mkdir", "-p", $logDir);
  open(FILE, ">> $logDir/operations") or return;
  print FILE "$time[2]:$time[1]:$time[0] $$ Took: $time seconds Done: '$message'\n";
    
  close FILE;
  return 1;

}



sub _ConnectToLDAP {
  my $self = shift;
  $LDAP and $LDAP->close();
  $LDAP = Net::LDAP->new($self->{CONFIG}->{LDAPHOST}, "onerror" => "warn") or print STDERR "$@" and return;
  print STDERR "Connecting to LDAP server .........";
  my $manager = ($self->{CONFIG}->{LDAPMANAGER} or "cn=Manager,dc=cern,dc=ch");    #

  #  my $result=  $LDAP->bind( $manager, password => $self->{LDAPpassword} );
  my $result = $LDAP->bind();
  $result->code && print STDERR "failed\nCould not bind to LDAP-Server: ", $result->error and return;
  print STDERR "OK\n";
  return 1;

}


sub _checkLDAPConnection {
  my $self = shift;
  $self->info("Checking if we have a connection to LDAP");
  eval {
    my $base = $self->{CONFIG}->{LDAPDN};
    my $mesg = $LDAP->search(               # perform a search
      base   => "ou=Config,$base",
      filter => "(ou=Config)",
    );
    $mesg->code && die("Error connecting to ldap: " . $mesg->());
    $self->debug(1, "The search worked");
    my $total = $mesg->count;
    $self->debug(1, "The total is $total");
    ($total) or die("The total is zero!!");

  };
  if ($@) {
    $self->info("Error connecting: $@\n Let's reconnect");
    $LDAP = Net::LDAP->new($self->{CONFIG}->{LDAPHOST}, "onerror" => "warn") or print STDERR "$@" and return;
    my $manager = ($self->{CONFIG}->{LDAPMANAGER} or "cn=Manager,dc=cern,dc=ch");
    my $result = $LDAP->bind($manager, password => $self->{LDAPpassword});
    $result->code && print STDERR "failed\nCould not bind to LDAP-Server: ", $result->error and return;
    $self->debug(1, "We are connected!!");

  }
  $self->debug(1, "The connection is up!!!");
  return 1;
}

##### added for apiservice to translate a subject into a role #####
#
#sub verifyRoleFromSubject {
#  my $self2   = shift;
#  my $subject = shift;
#  my $role    = shift;
#
#  my $UID = $SubjectToUid->($subject);
#  $UID or print STDERR "The subject '$subject' does not exist\n" and return;
#
#  ($role) or $role = $UID;
#
#  $self->info("Subject: $subject\nRole: $role\n UID: $UID");
#
#  $self->checkUserRole($UID, $role) or return;
#
#  return ($role);
#}
#
#sub checkUserRole {
#  my $self = shift;
#  my $user = shift;
#  my $role = shift;
#
#  if ($role ne $user) {
#    $self->info("Checking if the user $user can be $role");
#    my $total = $self->{addbh}->queryValue("select count(*) from USERS_LDAP_ROLE where user=? and role=?",
#      undef, {bind_values => [ $user, $role ]});
#    if (!$total) {
#      print STDERR "User $user is not allowed to be $role\n";
#      return "";
#    }
#  }
#  return $role
#
#}

1;

