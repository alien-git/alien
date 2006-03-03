package AliEn::Service::Authen;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;

use AliEn::Service;

#use AliEn::UI::Catalogue::Server;

use AliEn::Authen::IIIkey;
use POSIX;
use Authen::PAM;
use AliEn::Database::Catalogue;
use AliEn::Database::Admin;
use AliEn::Catalogue::Server;
use Crypt::OpenSSL::RSA;
use Crypt::OpenSSL::Random;
use AliEn::Util;


use vars qw (@ISA $DEBUG);
@ISA=("AliEn::Service");

$DEBUG=0;
my $self = {};
# *******************************
# Needed for PAM authentification
my $VFusername = "";
my $VFpasswd   = "";

# *******************************

#my $dbh;

my $ADMINPASSWD;
my $LDAP;

sub initialize {
	$self    = shift;
	my $options = (shift or {});
	$options->{role} = 'admin';
	$options->{ROLE} = 'admin';

	if ( !( $options->{password} ) ) {
		print "Please enter the password:\n";
		chomp( $options->{password} = <STDIN> );
	}
	my $LDAPpassword = $options->{password};

	#    $ADMINPASSWD = $password;


	$self->{PORT}=$self->{CONFIG}->{'AUTH_PORT'};
	$self->{HOST}=$self->{CONFIG}->{'AUTH_HOST'};
	$self->{SERVICE}="Authen";
	$self->{SERVICENAME}="Authen";
	$self->{LISTEN}=10;
	$self->{PREFORK}=1;

	#Delete the password from options. Not needed anymore. (MUST NOT BE SET)
	$options->{password} = '';
	# $options->{debug}=5;

	$self->{cat} = AliEn::Catalogue::Server->new($options);
	$self->{options} = $options;
	$self->{cat} or $self->{LOGGER}->error( "CatalogDaemon",
					"Could not create instance of ServerInterface. Daemon did not start" )
		and return;

	#	$self->{cat}->f_whoami();
	#	exit;
	$self->info( "Initializing catalog daemon" );

	$self->{addbh} = new AliEn::Database::Admin();    
	($self->{addbh})
		or $self->{LOGGER}->warning( "CatalogDaemon", "Error getting the Admin" )
		and return;

	$LDAP = Net::LDAP->new( $self->{CONFIG}->{LDAPHOST}, "onerror" => "warn" ) or print "$@" and return;
	print "Connecting to LDAP server .........";
	my $manager=($self->{CONFIG}->{LDAPMANAGER} or "cn=Manager,dc=cern,dc=ch");
	my $result=  $LDAP->bind( $manager, password => $LDAPpassword );
	$result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error and exit;
		print "OK\n";

		return $self;
}

# ***************************************************************
# Conversation function for PAM
# ***************************************************************
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

# ***************************************************************
# Creates a new token randomly. Alway 32 caracters long.
# ***************************************************************
my $createToken = sub {
    my $token = "";
    my @Array = (
        'X', 'Q', 't', '2', '!', '^', '9', '5', '3', '4', '5', 'o',
        'r', 't', '{', ')', '}', '[', ']', 'h', '9', '|', 'm', 'n', 'b', 'v',
        'c', 'x', 'z', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', ':', 'p',
        'o', 'i', 'u', 'y', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
        'A', 'S', 'D', 'F', 'G', 'H', 'J', 'Z', 'X', 'C', 'V', 'B', 'N', 'M'
    );
    my $i;
    for ( $i = 0 ; $i < 32 ; $i++ ) {
        $token .= $Array[ rand(@Array) ];
    }
    return $token;
};

# ***************************************************************
# Creates a random password.
# ***************************************************************
my $createPasswd = sub {
    my $passwd = "";
    my @Array  = (
        'X', 'Q',  'st', '2', '!', '^', '9', '5', "\$",
        '3', '4',  '5',  'po', 'r', 't', '{', ')', '}', '[',
        ']', 'gh', 'e9', '|',  'm', 'n', 'b', 'v', 'c', 'x',
        'z', 'a',  's',  'd',  'f', 'g', 'h', 'j', 'k', 'l',
        ':', 'p',  'o',  'i',  'u', 'y', 'Q', 'W', 'E', 'R',
        'T', 'Y',  'U',  'I',  'O', 'P', 'A', 'S', 'D', 'F',
        'G', 'H',  'J',  'Z',  'X', 'C', 'V', 'B', 'N', 'N',
        'M'
    );
    my $i;
    for ( $i = 0 ; $i < 10 ; $i++ ) {
      $passwd .= $Array[ rand(@Array) ];
    }
    return $passwd;
};

sub verifyToken {
    my $self2     = shift;
    my $username = shift;
    my $role     = shift;
    my $TOKEN    = shift;

    my $encrypter = new AliEn::Authen::IIIkey();

    #    if($username eq 'admin') {
    #	#This is admin. Return  true if password is the same as this one.
    #	my $passwd=$TOKEN;#$encrypter->decrypt($TOKEN,"AliGatorMasterKey");
    #	if($passwd eq $ADMINPASSWD) {
    #	    return $passwd;
    #	}
    #	else {
    #	    return;
    #	}
    #    }
    #my $dbTOKEN=$self->{addbh}->getEncToken($username);
    # No reason to encrypt token.
    my $dbTOKEN = $self->{addbh}->getToken($username);
    $self->info( "Verifing $username s token" );
    $self->debug(1, "  TOKEN: $TOKEN" );
    $self->debug(1, "DBTOKEN: $dbTOKEN\n" );
    if ( $TOKEN eq $dbTOKEN ) {
        $self->info("$username accepted $TOKEN" );

        return $TOKEN;
    }
    else {
        $self->info("Token not verified!!" );
        return;
    }
}

# ***************************************************************
# Is called when a user wishes to retrieve a new token. Checks
# if user is allowed a new token. Can check agains AFS password
# or anything else.
# ***************************************************************
sub verify {
    my $self2 = shift;
    $VFusername = shift;
    my $role        = shift;
    my $unencpasswd = shift;
    my $date = localtime;
    $date =~ s/^\S+\s(.*):[^:]*$/$1/	;
    my $tty_name = ttyname( fileno(STDIN) );
    my ( $pamh, $res );

    my $encrypter = new AliEn::Authen::IIIkey();
    $self->info("Token update request from $VFusername" );

    my $oldtoken = $self->{addbh}->getToken($VFusername);

    ($oldtoken) or $oldtoken = "a";

    my $PASSKEY = $oldtoken;

    $VFpasswd = $encrypter->decrypt( $unencpasswd, $PASSKEY );

    # *********************************************************************
    # This could in generel be any means of password checking.
    $self->debug(1, "Before PAM init" );

    $pamh = new Authen::PAM( "login", $VFusername, \&$my_conv_func );

    $self->debug(1, "After PAM init" );
    $res = $pamh->pam_set_item( PAM_TTY(), $tty_name );
    $res = $pamh->pam_authenticate();

    if ($res) {
        $self->info("Password not correct. Trying to decrypt with Global key" );
        $PASSKEY  = "AliGatorMasterKey";
        $VFpasswd = $encrypter->decrypt( $unencpasswd, $PASSKEY );

        $res = $pamh->pam_authenticate();

        $VFpasswd = "";
    }

    # *********************************************************************

    if ($res) {
        print "PASSWORD $unencpasswd\n";
        $self->info("Password not correct." );
        return;
    }

    ## Now check if user exists

    $self->info("Password correct." );

    #Checking if the user is in the LDAP

    checkUserLDAP($VFusername)   or return;

    #	my $newpasswd=$createPasswd->();

    #	my ($oldHost, $oldDB, $oldDriver)=
    #	    ($dbh->{DATABASE}->{HOST},$dbh->{DATABASE}->{DB},$dbh->{DATABASE}->{DRIVER});
    my $OLDTOKEN;
    $self->{addbh}->existsToken($VFusername) or return $self->addUser($VFusername);

    $self->info("User exists in database. Changing password." );
    
    #	    my (@hosts)=$dbh->{DATABASE}->query("SELECT hostIndex, address, db, driver FROM HOSTS");
    #	    my $tempHost;

    #WARNING NOW WE ARE NOT CHANGING THE TOKEN
    #	    foreach $tempHost (@hosts)
    #	    {
    #		my ($ind, $ho, $d, $driv)=split "###", $tempHost;
    #		$dbh->{DATABASE}->reconnect($ho, $d, $driv);
    #
    #		$dbh->{DATABASE}->insert("GRANT SELECT ON $d.*  TO $VFusername IDENTIFIED BY '$newpasswd'") or print STDERR "Problems with the GRANT (When updating tokens)!!\n";
    #
    #	    }
    #	    $self->info("Password updated in all databases.");
    #     	    $self->{addbh}->insert("UPDATE TOKENS SET password='$newpasswd',TOKEN='$token',Expires=Now() where Username='$VFusername'");
    #Change only pasword, not token
    #     	    $self->{addbh}->insert("UPDATE TOKENS SET password='$newpasswd',Expires=Now() where Username='$VFusername'");
    #     	    $dbh->{DATABASE}->reconnect($oldHost, $oldDB, $oldDriver);
    
    #	    my $encrypted=$encrypter->crypt($token,$SERVERKEY);
    
    #my $encrypted=$encrypter->crypt($oldtoken,$SERVERKEY);
    $self->info("Done." );
    
    my $tempuser = $VFusername;
    $role or $role = $VFusername;
    if ( $role ne $VFusername ) {
      print "Checking if the user $VFusername can be '$role'\n";
      
      #	    my $ldap= Net::LDAP->new($self->{CONFIG}->{LDAPHOST}) or print "$@" and return;
      my $base = $self->{CONFIG}->{LDAPDN};
      my $mesg = $LDAP->search(       # perform a search
			       base   => "ou=Roles,$base",
			       filter => "(&(uid=$role)(|(public=yes)(users=$VFusername)))",
			      );
      #	    $ldap->unbind;
      my $total = $mesg->count;
      if ( !$total ) {
	print "User is not allowed to be $role\n";
	return 0;
      }
      $tempuser = $role;
      $oldtoken = $self->{addbh}->getToken($role);

    }
    $self->{addbh}->addTime( $tempuser, 24 );
    print "User is allowed to be $role \n";
    return $oldtoken;
}

#Checks if a user is in the LDAP database
sub checkUserLDAP {

    my $user = shift;

    #    my $ldap= Net::LDAP->new($self->{CONFIG}->{LDAPHOST}) or print "$@" and return;
    my $base = $self->{CONFIG}->{LDAPDN};
    my $mesg = $LDAP->search(       # perform a search
        base   => "ou=People,$base",
        filter => "(uid=$user)",
    );

    #    $ldap->unbind;
    my $total = $mesg->count;
    if ( !$total ) {
        print
          "User $user does not exist in LDAP as $self->{CONFIG}->{ORG_NAME} user!!\n";
        return 0;
    }
    my $entry = $mesg->entry(0);
    my $UID   = $entry->get_value('cn');
    return $UID;
#    return 1;

}

my $SubjectToUid = sub {
    my $ldap    = shift;
    my $subject = shift;

    print "Tranlating subject into uid.\n";

    # The role is a subject, translate into UID
    my $filter = "(&(objectclass=pkiUser)(subject=$subject))";
    my $base   = $self->{CONFIG}->{LDAPDN};
    my $mesg   = $ldap->search(
        base   => "ou=People,$base",
        filter => $filter
    );
    my $total = $mesg->count;
    if ( $total == 0 ) {
        print "Failure in translating $subject into uid\n";

        #No user registered with this subject:(
        #	$ldap->unbind;
        return 0;
    }
    my $entry = $mesg->entry(0);
    my $UID   = $entry->get_value('uid');
    return $UID;

    # Role is now translated into a uid!!
};

########################################################################
#  This routine is a somewhat hack. It recieves a Certificate subject  #
#  and returns the token that is associated with the subject. The      #
#  token is encrypted with the webserver special key (public). This way#
#  only the webserver (Or anyone else who got acces to it) can de-crypt#
#  the key. Its nowhere near perfect, but it works.                    #
########################################################################
sub getTokenFromSubject {
    my $self2    = shift;
    my $subject = shift;
    my $role    = shift;

    my $UID = $SubjectToUid->( $LDAP, $subject );
    ($role) or $role = $UID;

    print "Subject: $subject\n";
    print "Role: $role\n";
    print "UID: $UID\n";

    Crypt::OpenSSL::RSA->import_random_seed();
    open PUB, "$ENV{ALIEN_HOME}/identities.web/webkey.public";
    my @lines = <PUB>;
    my $pubkey = join ( "", @lines );
    close PUB;
     my $rsa  = Crypt::OpenSSL::RSA->new_public_key($pubkey);

    my $oldtoken = $self->{addbh}->getToken($UID);
    $rsa->load_public_key($pubkey);
    my $challenge = $rsa->encrypt("AUTHOK::$oldtoken");

    if ( $role ne $UID ) {
        print "Checking if the user $UID can be $role\n";
        my $base = $self->{CONFIG}->{LDAPDN};
        my $mesg = $LDAP->search(       # perform a search
            base   => "ou=Roles,$base",
            filter => "(&(uid=$role)(|(public=yes)(users=$UID)))",
        );
        my $total = $mesg->count;
        if ( !$total ) {
            print "User $UID is not allowed to be $role\n";
            return 0;
        }
    }

    # I think we should change the token every time, but its not done now:(
    $self->{addbh}->addTime( $UID, 24 );
    return ( $UID, $role, $challenge );
}

sub createTable {
  my $self2       = shift;
  my $host       = shift;
  my $db         = shift;
  my $driver     = shift;
  my $user       = shift;
  my $table      = shift;
  my $definition = (
		    shift
          or "(type   CHAR(4),dir int(8),name  VARCHAR(255), 
         owner CHAR(8),ctime CHAR(16),comment VARCHAR(80) NOT NULL DEFAULT \"\", 
 pfn varchar(255) NOT NULL DEFAULT \"\", se VARCHAR(100), gowner char(8), size int)"
    );

  my $date = localtime;
  $date =~ s/^\S+\s(.*):[^:]*$/$1/	;


  $self->info(
			 "\n\tCreating new table $table for $user in $db $host" );
  $self->reconnect( $host, $db, $driver )
    or return (-1,"Error reconnecting to $db $host");

  $self->info("Checking the name of the table" );
  if ($table=~ /^T$/) {
    #We have to get a new table name;
    my $dir=$self->{cat}->{DATABASE}->getNewDirIndex();
    $dir or return (-1, "Error getting a new table name");
    $table="T$dir";
  }

  #    $dbh->{DATABASE}->reconnect($host, $db, $driver);
  $self->info("Creating the table $table" );
  my $errorMessage="Error creating the new table \n";

  $self->{cat}->{DATABASE}->createTable($table, $definition)
    or print STDERR "$errorMessage ($! $@)\n"
	and return (-1, "$errorMessage$@");

  $self->{cat}->{DATABASE}->grantAllPrivilegesToUser($user, $db, $table)
    or $self->{LOGGER}->warning( "Authen",
				 "Error granting privileges on table $table for $user\n($! $@ $DBI::errstr\n"
			       )
      and return (-1, "Error creating the new table");
  
  $self->info("Privileges changed for $user.. Returning $table" );

  return $table;
}

sub reconnect {
	my $self2   = shift;
	my $host   = shift;
	my $db     = shift;
	my $driver = shift;
	
#	(	$self->{OLD_DB}, $self->{OLD_DRIVER}, $self->{OLD_HOST})=
#($self->{cat}->{DATABASE}->{DB}, $self->{cat}->{DATABASE}->{DRIVER}, $self->{cat}->{DATABASE}->{HOST});
	my $index =	$self->{cat}->{DATABASE}->getHostIndex($host, $db, $driver);
#"SELECT hostIndex FROM HOSTS where address='$host' and db='$db' and  driver='$driver'"
	$self->info("Index is |$index|" );
	$index
		or print "ERROR database $db $host ($driver ) does not exist\n"
		
      and return;
	$self->info("Index $index" );
	if ( !$self->{cat}->{"DATABASE_$index"} ) {
		$self->info(
													 "Connecting for the first time to $index" );

		$self->{cat}->{"DATABASE_$index"} = AliEn::Database::Catalogue->new(
            {
                "DB"     => $db,
                "HOST"   => $host,
                "DRIVER" => $driver,
                "DEBUG"  => $self->{cat}->{DATABASE}->{DEBUG},
                "USER"   => $self->{cat}->{DATABASE}->{LOCAL_USER},
                "ROLE"   => $self->{cat}->{DATABASE}->{ROLE},
                "SILENT" => 0
            }
        );

		$self->{cat}->{"DATABASE_$index"}
			or print STDERR "ERROR GETTING THE NEW DATABASE\n"
				and return;

		$self->{cat}->{DATABASE} = $self->{cat}->{"DATABASE_$index"};
		$self->info("Connection done" );
		#$self->{cat}->validateDatabase() or print "NO VALIDATION\n" and return;
		
	}
	$self->{cat}->{DATABASE} = $self->{cat}->{"DATABASE_$index"};
	return 1;
}

sub changePrivileges {
    my $self2    = shift;
    my $host    = shift;
    my $db      = shift;
    my $driver  = shift;
    my $table   = shift;
    my $oldUser = shift;
    my $newUser = shift;

    my $date = localtime;
    $date =~ s/^\S+\s(.*):[^:]*$/$1/	;
    $self->info(
        "Changing owner of $table in $db $host from $oldUser to $newUser" );

    #    $dbh->{DATABASE}->reconnect($host, $db);
    #    $dbh->{DATABASE}->reconnect($host, $db, $driver);
    $self->reconnect( $host, $db, $driver ) or return;
    #$self->{cat}->{DATABASE}->
    #  ->insert("REVOKE ALL PRIVILEGES ON $db.T$table FROM  $oldUser");
	
	$self->{cat}->{DATABASE}->revokeAllPrivilegesFromUser($oldUser, $db,"T$table");
	
	$self->{cat}->{DATABASE}->grantAllPrivilegesToUser($newUser, $db,"T$table")

    #$self->{cat}->{DATABASE}->insert("GRANT ALL PRIVILEGES ON $db.T$table  TO $newUser")



      or $self->{LOGGER}->warning( "Authen",
        "Error changeing privilege on $db.T$table to $newUser" )
      and return;

    $self->info("New owner is $newUser" );
    return 1;
}

sub insertJob {
  my $self2   = shift;
  my $procid = shift;
  my $user   = shift;
 
  $self->info("Inserting job $procid from $user" );
  ($user)
    or $self->{LOGGER}->notice( "Authen",
				"Error: In insertJob not enough arguments" )
      and return;

  $self->{addbh}->insertJobToken($procid,$user,-1)
    or $self->{LOGGER}->error( "CatalogDaemon","Could not insert new jobToken" )
      and return ;

  $self->info( "Job $procid inserted\nMaking sure that the job is there");

  my @list=$self->{addbh}->query("SELECT * from jobToken where jobId=$procid");
  print Dumper(@list);

  return 1;
}

sub getJobToken {
  my $this  = shift;
  my $procid = shift;
  
  print "User is $ENV{SSL_CLIENT_SUBJECT}\n";

  $self->info("\nGetting  job $procid" );
  
  ($procid)
    or print $self->{LOGGER}->notice( "Authen",
				      "Error: In getJobToken not enough arguments" )
      and return;
  
  my ($data) =
    $self->{addbh}->getFieldsFromJobToken($procid,"jobToken, userName");
  
  ($data)
    or $self->{LOGGER}->error( "CatalogDaemon", "Database error fetching fields for $procid" )
      and return;
  
  my ( $token, $user ) = ( $data->{jobToken}, $data->{userName});
  
  ( $token eq '-1' )
    or $self->{LOGGER}->notice( "CatalogDaemon", "Job $procid already given.." )
      
      and return;
  
  $token = $createToken->();
  
  $self->{addbh}->setJobToken($procid,$token)
    or $self->{LOGGER}->warning( "CatalogDaemon","Error updating jobToken for user $user" ) 
      and return (-1, "error setting the job token");
  $self->info( "Making sure that the job is there...");
  my @result=$self->{addbh}->query("SELECT * from jobToken where jobId=$procid");
  use Data::Dumper;
  print Dumper(@result);
  
  $self->info("Changing the ownership of the directory" );

  my $procDir = AliEn::Util::getProcDir($user, undef, $procid);
  if (!($self->{cat}->f_chown("", $user, $procDir ))) {
    $self->{LOGGER}->warning("Broker",
			     "Error changing the privileges of the directory $procDir in the catalogue"
			    );
    $self->{LOGGER}->warning("Broker","Making a new database connection ");
    $self->{cat} = AliEn::Catalogue::Server->new($self->{options});
    $self->{LOGGER}->warning("Broker","Now I have a new database connection");
    if (!($self->{cat}->f_chown("",  $user, $procDir ))) {
      $self->{LOGGER}->critical(
				"Broker",
				"Error changing the privileges of the directory $procDir in the catalogue 2nd time"
			       );
      return ( -1, "changing the privileges" );
    }
  }
  
  
  $self->info("Sending job $procid to $user" );
  return { "token" => $token, "user" => $user };
}

sub checkJobToken {
  my $self2 = shift;
  my $job   = shift;
  my $token = shift;
  
  $self->info("In checkJobToken (job $job)..." );

  my ($user) = $self->{addbh}->getUsername($job,$token);
  
  if (!($user)) {
    $self->info("In checkJobToken (job $job)...reconnect the admin database" );
    $self->{addbh} = new AliEn::Database::Admin();    #$password);
    ($user) = $self->{addbh}->getUsername($job,$token);

    ($user) 
      or $self->info("Error: no user for proccess $job - failed" )
	and return;
  }
  
  $self->info("Getting the token of $user..." );
  
  ($token) = $self->{addbh}->getToken($user);
  
  $self->{addbh}->addTime( $user, 2 );
  
  $self->info( "Job $job authenticated (User: $user)" );
  return { "token" => $token, "user" => $user };
}

sub removeToken {
    my $self2 = shift;

    my $job = shift;
    $self->info("In removeJobToken, removing job $job token" );
    my $done = $self->{addbh}->deleteJobToken($job);
    ($done)
      or
      $self->{LOGGER}->warning( "Authen", "Error removing token $DBI::errstrs" );

    return 1;
}

sub insertKey {
    my $self2   = shift;
    my $user   = shift;
    my $passwd = shift;
    my $key    = shift;

    my ($ok, $message)=$self->CheckLocalPassword($user, $passwd);
    $ok or return (0, $message);

    # Remove the password from sourcecode (Put it in the database, or somthing)

    if ( $user eq "$self->{CONFIG}->{CLUSTER_MONITOR_USER}" ) {
      return $self->CheckProductionUser;
    }
    print "Modifying sshkey for $user\n";

    if ($LDAP->modify("uid=$user,ou=People,$self->{CONFIG}->{LDAPDN}",
            replace => { 'sshkey' => $key })){
      $self->info(
		     "$user has succesfully updated is SSHKEY" );
    }
    else {
      $self->{LOGGER}->warning( "Authen", "Error in updating SSHKEY for $user" );
    }

    ( $self->{addbh}->existsToken($user) )  or $self->addUser($user);

    $self->info(
            "$user requests insertion of public keys in database" );
    $self->{addbh}->setSSHKey( $user, $key );

    return 1;
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

  my $mesg = $LDAP->search(base   => "ou=People,$self->{CONFIG}->{LDAPDN}",
			   filter => "(uid=$user)");

  my $total = $mesg->count;

  if ( !$total ) {
    #This user doesn't exist!!
    print "User $user does not exist!!\n";
    return ( 0, "User $user does not exist in LDAP" );
  }

  return 1;
}
      
sub CheckProductionUser{
  my $this2=shift;


  my $encrypter = new AliEn::Authen::IIIkey();
#  my $encrypter=shift;
  
  my $username=$self->{CONFIG}->{CLUSTER_MONITOR_USER};
  # Okay, this is Aliprod, so return private key from database.

  $self->info("$username requests private key" );
  my $data =$self->{addbh}->getDBKey($username)
  	or $self->{LOGGER}->error("Authen","Error fetching private key for user $username") and return;

  $self->debug(1, "Encrypting $username key with $VFpasswd" );
  $VFpasswd or $VFpasswd = "AliGatorMasterKey";
  my $buffer = $encrypter->crypt( $data, $VFpasswd );
  $self->debug(1, "Base64 encoding key" );
  my $var = SOAP::Data->type( base64 => $buffer );
  $self->info("Sending back the key" );
  my $mesg = $LDAP->search(
			   base   => "ou=People,$self->{CONFIG}->{LDAPDN}",
			   filter => "(uid=$username)"
			  );
  
  my $total  = $mesg->count;
  my $public = "NO KEY";
  print
    "FOUND $total in ou=People,$self->{CONFIG}->{LDAPDN} with(uid=$username)\n";
  if ($total) {
    print "Giving back the public key\n";
    $public = $mesg->entry(0)->get_value("sshkey");
  }
  
  #	$ldap->unbind;
  $buffer = $encrypter->crypt( $public, $VFpasswd );
  $self->debug(1, "Base64 encoding key" );
  $public = SOAP::Data->type( base64 => $buffer );
  return ( 1, $var, $public );
}

sub insertCert {
    my $self2         = shift;
    my $organisation = shift;
    my $user         = shift;
    my $passwd       = shift;
    my $subject      = shift;

    my ($ok, $message)=$self->CheckLocalPassword($user, $passwd);
    $ok or return (-1, $message);

    print "Modifying certificate subject for $user\n";

    my $mesg = $LDAP->search(
        base   => "ou=People,$self->{CONFIG}->{LDAPDN}",
        filter => "(subject=$subject)"
    );

    my $total = $mesg->count;

    if ( $total > 0 ) {

        #This certificate alredy exists one time.
        print "Certificate alredy exists\n";
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

    ($self->{addbh}->existsToken($user) )  or $self->addUser($user);
    
    return 1;
}

sub addUser{
  my $this=shift;
  my $user=shift;

  $self->info(
		 "User $user does not exists in database. Creating user." );
#  my $newpasswd = $createPasswd->();
#  my $token     = $createToken->();
  
  $self->{cat}->f_addUser( $user,  );
#  $addbh->insertToken(undef,$user,$token,$newpasswd,'NOKEY');
	my $token=$self->{addbh}->getToken($user);
  $self->{addbh}->addTime( $user, 24 );
  return $token;
}

=item addSE (seName)

This function adds a new SE to the SE table of all the databases of the 
catalogue

=cut


sub addSE {
  my $this=shift;
  my $seName=shift;

  my $newnumber=1;
  $self->info("Trying to add a new se ( $seName)");
  my ($vo, $site, $se)=split (/::/, $seName);
  my $done=$self->{cat}->addSE($site, $se);
  $self->info("Adding the SE finished with $done");
  $done or return(-1,"Error inserting the entry in the database");

  my $seNumber=$self->{cat}->{DATABASE}->queryValue("SELECT seNumber from SE where seName='$seName'");
  
  return $seNumber;

}
sub requestCert {
    my $this         = shift;
    my $organisation = shift;
    my $user         = shift;
    my $passwd       = shift;
    my $request      = shift;

   
    print "Creating certificate for $user\n";

    my ($ok, $message)=$this->CheckLocalPassword($user, $passwd);
    $ok or return (0, $message);

    my $username=checkUserLDAP($user);

    ($username) or print "User $user does not exist in LDAP \n" 
	and return (0, "User $user does not exist in LDAP \n");

    print "Usename $username\n";


    my $date=time;
    my $file="$self->{CONFIG}->{TMP_DIR}/Cert.$user.$date";
    
    open (FILE, ">$file") or print "Error opening the file $file" 
	and return(0, "Error opening the file $file");
    print FILE  "$request";

    close FILE;
    print "Request $file\n";

    open SAVEOUT, ">&STDERR";
    if ( !open( STDERR, ">$file.subject" ) ) {
	print "Error opening the file $file.subject\n";
	return (0,"Error opening the file $file.subject\n");
    }
    

    #This part should be moved to AliEn::X509
    system("$ENV{ALIEN_ROOT}/bin/openssl","x509","-in","$file","-noout","-req","-signkey","$ENV{ALIEN_CA}/private/key.pem");

    close STDERR;
    open STDERR, ">&SAVEOUT";
    open (FILE, "$file.subject" );
    my @subject=<FILE>;
    close FILE;
    my ($subject) =grep (/^subject=/, @subject);
       
    print "SUBJECT $subject\n";

    $subject =~ /subject=\/C=ch\/ST=Switzerland\/L=Geneva\/O=AliEn\/O=$organisation\/OU=People\/CN=$username/i or print "Error $subject does not match with $username\n" and return (0,"Error $subject does not match with $username\n");



    my $config = "$ENV{ALIEN_ROOT}/ssl/alien-user-ssl.conf";
 
    my $outfile="$file.out";

    my $path="$ENV{ALIEN_HOME}/.startup/.passwd";
    my $command = "$ENV{ALIEN_ROOT}/bin/openssl ca -batch -passin file:$path -config $config -policy policy_anything -out $outfile -in $file";

    $ENV{ALIEN_USER}="$user";
    system($command);
    
    my $pem;

    open (FILE, "$outfile") or print "Error opening the file $outfile\n" 
	and return (0, "Error opening the file $outfile\n");
    my @file=<FILE>;
    close FILE;

    $pem = join ("", @file);
 
    print "Returning the certificate\n";
    return $pem;
} 
# This function is called if the role is authenticated, but the user doesn't
# have the right privileges
#
sub checkUserPassword {
  shift;
  my $role=shift;
  $self->info( "Let's see if the user '$role' has the right privileges");
  my $privileges=$self->{cat}->{DATABASE}->query("show grants for $role");
  $privileges and $self->info( "Let's return") and return 1;
  $self->info("The user didn't have any privileges... let's create them");
  $self->addUser($role);

  return 1;
}
sub recreateJobToken {
  shift;
  my $jobid=shift;
  $self->info( "Recreating the token for job $jobid");
  my $user=$self->{addbh}->getFieldFromJobToken("user", $jobid);
  $user or $self->info( "Error getting the user of that token");

  $self->{addbh}->deleteJobToken($jobid);

  $self->{addbh}->insertJobToken($jobid,$user,-1)
    or $self->{LOGGER}->error( "CatalogDaemon","Could not insert new jobToken" )
      and return ;

  return 1;

}
##### added for apiservice to translate a subject into a role #####

sub verifyRoleFromSubject {
    my $self2    = shift;
    my $subject = shift;
    my $role    = shift;

    my $UID = $SubjectToUid->( $LDAP, $subject );
    ($role) or $role = $UID;

    $self->info("Subject: $subject\n");
    $self->info("Role: $role\n");
    $self->info("UID: $UID\n");

    if ( $role ne $UID ) {
        $self->info("Checking if the user $UID can be $role\n");
        my $base = $self->{CONFIG}->{LDAPDN};
        my $mesg = $LDAP->search(       # perform a search
            base   => "ou=Roles,$base",
            filter => "(&(uid=$role)(|(public=yes)(users=$UID)))",
                                        );
        my $total = $mesg->count;
        if ( !$total ) {
            print "User $UID is not allowed to be $role\n";
            return "";
        }
    }

    return ( $role );
}


return 1;

