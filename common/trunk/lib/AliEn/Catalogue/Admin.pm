package AliEn::Catalogue::Admin;

use strict;

use AliEn::Database::Admin;

# This package contains the functions that can only be called by the 
# administrator
#
#

# ***************************************************************
# Creates a new token randomly. Alway 32 caracters long.
# ***************************************************************
my $createToken = sub {
    my $token = "";
    my @Array = (
        'X', 'Q', 't', '2', '!', '^', '9', '5', "\$", '3', '4', '5', 'o',
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

sub f_mount {
  my $self = shift;
  my $mountpoint = shift;
  my $organisation = shift;

  if ( $self->{ROLE} !~  /^admin(ssl)?$/ ) {
    $self->info("Error: only the administrator can add new hosts");
    return;
  }
  $self->info( "Mounting another VO");
  
  my $message="";
  
  $mountpoint or $message="missing mount point";
  $organisation or $message.=" missing organisation name";

  $message and $self->info( "Error: $message\nUsage: mount <mountdir> <V.O name>") and return;

  $mountpoint = $self->GetAbsolutePath($mountpoint);


  my $org=$self->{CONFIG}->{ORG_NAME};
  my $t=$self->{CONFIG}->Reload({organisation=>$organisation});
  if (!$t) {
    #		$self->{CONFIG}=$self->{CONFIG}->Reload({organisation=>$org});
    $self->info( "Error: not possible to get the configuration of $organisation");
    return 0;
  }
  #	$self->{CONFIG}=$t;
  my $address=$t->{CATALOGUE_DATABASE};
  if (!$address) {
    $self->info( "Error getting the address of the catalogue of $organisation");
    return 0;
  }
  $self->debug(1,"In Admin Interface, ready to mount");
  $self->f_touch($mountpoint) or return 0;
  
  $self->debug(1,"File inserted");
  
  my ($host, $driver, $db)=split ("/", $address);
  $self->info( "Adding the host to the hosts table ($address)");
  
  if (!$self->f_addHost($host, $driver, $db, $organisation)){
    $self->info( "Error adding the new host");
    $self->f_removeFile("s", $mountpoint);
    return 0;
  }
  my ($hostIndex) = $self->{DATABASE}->getHostIndex ($host, $db, $driver);
  
  $self->debug(1, "Now we just have to modify D0");
  my $dir=$self->f_dir($mountpoint);
  my $basename=$self->f_basename($mountpoint);
  $self->debug(1, "We have to change the entry $basename in $dir");
  my $VOpath=$self->getVOPath($mountpoint);
  
  my $newVOPath=$VOpath;
  $newVOPath=~ s/\/?$/\//;
  
  #my $done = $self->{DATABASE}->insert("UPDATE D0 set hostIndex=$hostIndex, path='$newVOPath' where path='$VOpath'");
  $self->{DATABASE}->updateD0Entry($VOpath,{hostIndex=>$hostIndex, path=>$newVOPath})
    or $self->{LOGGER}->error("Error setting new path and hostIndex for path $VOpath")
		and return;

  #$done = $self->{DATABASE}->insert("UPDATE T$dir set dir=1000, type='d7555' where name='$basename'");
  $self->{DATABASE}->updateDirEntry($dir,$basename, {dir=>1000, type=>'d7555'})
    or $self->{LOGGER}->error("Error setting new dir and type for name $basename")
      and return;
  
  $self->debug(1, "Organisation $organisation mounted under $mountpoint");
  return 1;
}

sub f_addHost {
  my $self   = shift;
  my $host   = shift;
  my $driver = shift;
  my $db     = shift;
  my $org    =(shift or "");

  if ( !$db ) {
    print STDERR
      "Error: not enough arguments in addHost\nUsage addHost <host> <driver> <database> [<organisation>]\n";
    return;
  }

  if ( $self->{ROLE} !~ /^admin(ssl)?/ ) {
    print STDERR "Error: only the administrator can add new hosts\n";
    return;
  }

  my $hostIndex = $self->{DATABASE}->getHostIndex ($host, $db, $driver);

  if ($hostIndex) {
		print STDERR "Error: $db in $host already exists!!\n";
		return;
	      }

  $hostIndex = $self->{DATABASE}->getMaxHostIndex + 1;

  $self->info( "Trying to connect to $db in $host...");
  my ( $oldHost, $oldDB, $oldDriver ) = (
					 $self->{DATABASE}->{HOST},
					 $self->{DATABASE}->{DB},
					 $self->{DATABASE}->{DRIVER}
					);
  
  my $rhosts = $self->{DATABASE}->getAllHosts();
  defined $rhosts
    or $self->{LOGGER}->error("Admin", "Error: not possible to get all hosts")
      and return;
  my $rusers = $self->{DATABASE}->getAllFromGroups("Username,Groupname,PrimaryGroup");
  defined $rusers
    or $self->{LOGGER}->error("Admin", "Error: not possible to get all users")
      and return;

  my $rindexes =$self->{DATABASE}->getAllIndexes()  
    or $self->{LOGGER}->error("Admin", "Error: not possible to get mount points")
      and return;

  my $rses=$self->{DATABASE}->query("SELECT * from SE");

  $self->debug(1, "Connecting to new database ($host $db $driver)");
  my $oldConfig=$self->{CONFIG};
  my $newConfig;
  if ($org) {
    $newConfig=$self->{CONFIG}->Reload({"organisation", $org});
    $newConfig or $self->info( "Error gettting the new configuration") and return;
    $self->{CONFIG}=$newConfig;
  }

  if ( !$self->{DATABASE}->reconnect( $host, $db, $driver ) ) {
    $self->{LOGGER}->error("Admin", "Error: not possible to connect to $driver $db in $host");
    $self->{DATABASE}->reconnect( $oldHost, $oldDB, $oldDriver );
    $newConfig and $self->{CONFIG}=$oldConfig;
    return;
  }
  if (!$org) {
    $self->{DATABASE}->createCatalogueTables();
    my  $addbh = new AliEn::Database::Admin();
    ($addbh)
      or $self->{LOGGER}->warning( "Admin", "Error getting the Admin" )
    	and return;

    my $rusertokens = $addbh->getAllFromTokens("Username, password");
    $addbh->destroy();

    #also, grant the privileges for all the users
    foreach my $rtempUser (@$rusertokens) {
      $self->{DATABASE}->grantBasicPrivilegesToUser($self->{DATABASE}->{DB}, $rtempUser->{Username}, $rtempUser->{password});
    }
    #Now, we have to fill in the tables
    #First, all the hosts in HOSTS
    foreach my $rtempHost (@$rhosts) {
      $self->{DATABASE}->insertHost($rtempHost->{hostIndex}, $rtempHost->{address}, $rtempHost->{db}, $rtempHost->{driver});
    }
    $self->{DATABASE}->insertHost($hostIndex, $host, $db, $driver);
    
    #Now, we should enter the data of D0
    foreach my $rdir (@$rindexes) {
      $self->debug(1, "Inserting an entry in INDEXES");
      $self->{DATABASE}->do("INSERT INTO INDEXTABLE (hostIndex, tableName, lfn) values('$rdir->{hostIndex}', '$rdir->{tableName}', '$rdir->{lfn}')");
    }
    
    #Also, GROUPS table;
    foreach my $ruser (@$rusers) {
      $self->debug(1, "Adding a new user");
      $self->{DATABASE}->insertIntoGroups($ruser->{Username}, $ruser->{Groupname}, $ruser->{PrimaryGroup});
    }

    #and finally, the SE
    foreach my $se (@$rses) {
      $self->debug(1, "Adding a new user");
      $self->{DATABASE}->insert("SE", $se);
    }
  }
  
  #in the old nodes, add the new link
  foreach my $rtempHost (@$rhosts) {
    $self->debug(1, "Connecting to database ($rtempHost->{address} $rtempHost->{db} $rtempHost->{driver})");
    $self->{DATABASE}->reconnect( $rtempHost->{address}, $rtempHost->{db}, $rtempHost->{driver} );
    $self->{DATABASE}->insertHost($hostIndex, $host, $db, $driver, $org);
  }

  $self->debug(1, "Connecting to old database ($oldHost $oldDB $oldDriver)");
  $self->{DATABASE}->reconnect( $oldHost, $oldDB, $oldDriver );
  $self->info( "Host added!!");
  return 1;
}

sub f_addUser {
  my $self = shift;

  $self->debug(1, "Adding a user");
  if ( $self->{ROLE} !~ /^admin(ssl)?/ ) {
    print STDERR "Only the administrator can add a user\n";
    return;
  }
  my $user = shift;

	
  if (!$user) {
    $self->{LOGGER}->error("Catalogue/Admin",
			   "Error: not enough arguments\nUsage: addUser <username> \n");
    return;
  }
  my $passwd =  $createPasswd->();
  
  my $homedir ="$self->{CONFIG}->{USER_DIR}/" . substr( $user, 0, 1 ) . "/$user/";
  $homedir =~ s{//}{/};
  $self->debug(1, "Creating a password");
	
	
  #If called with option $noHomedir the homedir is not created

  #	if ( !$noHomedir ) {
  #	}


  my ($oldDB, $oldDriver, $oldHost)=($self->{DATABASE}->{DB}, $self->{DATABASE}->{DRIVER}, $self->{DATABASE}->{HOST});

  my $rhosts = $self->{DATABASE}->getAllHosts();

  my $group = $self->getUserGroup($user);

  foreach my $rtempHost (@$rhosts) {
    print "Granting privileges for $user in $rtempHost->{db}\n";
    $self->{DATABASE}->reconnect($rtempHost->{address}, $rtempHost->{db}, $rtempHost->{driver});

    $self->{DATABASE}->grantExtendedPrivilegesToUser($self->{DATABASE}->{DB}, $user, $passwd);

    $self->{DATABASE}->insertIntoGroups($user, $group, 1);
  }
	
  #	my $centralServer   = $self->{CONFIG}->getValue('AUTHEN_HOST');
  #	my $centralDB = $self->{CONFIG}->getValue('AUTHEN_DATABASE');
  #	my $centralDriver   = $self->{CONFIG}->getValue('AUTHEN_DRIVER');

  $self->{DATABASE}->reconnect( $oldHost, $oldDB, $oldDriver );

  my $token= $createToken->();
  $self->debug(1, "Deleting user from token");

  my  $addbh = new AliEn::Database::Admin();

  $addbh or $self->debug(1, "Error creating Admin instance") and return;

  $addbh->deleteToken($user);

  $self->debug(1, "Inserting values into token");
  $addbh->insertToken("", $user, $token, $passwd, "");
  $addbh->destroy();

  #We have to grant select privileges on the transfer and IS databases
  my @transfers=split('/', $self->{CONFIG}->{TRANSFER_DATABASE});
  my @privileges = ("SELECT ON $self->{CONFIG}->{QUEUE_DATABASE}.*",
		    "SELECT ON $transfers[2].*",
		    "SELECT ON $self->{CONFIG}->{IS_DATABASE}.*", 
		   "INSERT,SELECT,UPDATE,DELETE ON $self->{CONFIG}->{CATALOG_DATABASE}.GUID");

  $self->{DATABASE}->grantPrivilegesToUser(\@privileges,$user);
  my $procdir="/proc/$user/";
  $self->info( "Creating new homedir for  $user and $procdir");
  $self->f_mkdir( "p", $homedir ) or 
    $self->info( "Error creating $homedir") and return ;

  $self->{DATABASE}->moveEntries($homedir) or 
    $self->info( "Error moving the directory $homedir",1100) and
      return;
#  my $table=$self->{DATABASE}->getIndexHost($homedir) or 
#    $self->info( "Error getting the table of $homedir") and return;

  $self->f_mkdir( "p", $procdir ) or 
    $self->info( "Error creating $procdir") and return ;

  $self->{DATABASE}->moveEntries($procdir) or 
    $self->info( "Error moving the directory $procdir ") and return;
  $self->info("Changing privileges for  $user");
  $self->f_chown("", $user, $homedir ) or return;
  $self->f_chown("", $user, $procdir ) or return;

  $self->info(  "User $user added");
  return 1;
}

sub getUserGroup {
    my $self = shift;
    my $oldusername = shift || return;

    my (
        $username,    $userpasswd, $uid,     $gid,       $userquota,
        $usercomment, $usergcos,   $userdir, $usershell, $userexpire
      )
      = getpwnam($oldusername);

    ($gid) or return $oldusername;

    my ( $gname, $passwd, $newgid, $members ) = getgrgid($gid);

    return $gname;
}

sub f_host {
    my $self = shift;

    $self->{SILENT}
      or print
"Current database:\t $self->{DATABASE}->{DB} in $self->{DATABASE}->{HOST}\n";

    return $self->{DATABASE}->{HOST};
}

sub f_chgroup {
    my $self   = shift;
    my $user   = shift;
    my @groups = @_;
    if ( !@groups ) {
        print STDERR
"Error: not enough arguments in chgroup\nUsage chgroup <user> <initial_group> [<group> [...]]\n";
        return;
    }
    if ( $self->{ROLE} !~ /^admin(ssl)?$/ ) {
        print STDERR "Error: only the administrator can change users groups\n";
        return;
    }

    my $rhosts = $self->{DATABASE}->getAllHosts();

    foreach my $rtempHost (@$rhosts) {
        $self->{DATABASE}->reconnect($rtempHost->{address}, $rtempHost->{db}, $rtempHost->{driver});

        $self->{DATABASE}->deleteUser($user)
          or print STDERR ("Error in changing groups!\n")
          and return;

        $self->{DATABASE}->insertIntoGroups($user, $groups[0], 1)
          or print STDERR ("Error in changing groups!\n")
          and return;

        shift @groups;
        for (@groups) {
          $self->{DATABASE}->insertIntoGroups($user, $_, 0)
            or print STDERR ("Error in changing groups!\n")
            and return;
        }
    }
    return 1;
}
#sub f_createNewIndexTable {
#  my $self=shift;
#  if ( $self->{ROLE} !~ /^admin(ssl)?$/ ) {
#    $self->info("Error: only the administrator can add new table");
#    return;
#  }
#  my $entry=$self->{DATABASE}->getNewDirIndex();
#  $self->{DATABASE}->checkDLTable($entry) or return;
#  $self->info( "Directory table D$entry created");
#  return "D${entry}L";
#}
#sub makeNewIndex{
#  my $self=shift;
#  my $lfn=shift;
#  my $index=$self->{DATABASE}->getNewDirIndex() or return;
#
#  $self->f_mkdir("s", $lfn) or return;
#  my $done=$self->moveDirectoryToIndex($lfn, $index);
#  $done and return 1;
#  $self->info( "Moving the directory to the new index did not work");
#  $self->f_rmdir("s", $lfn);
#  return;
#}

sub moveDirectoryToIndex {
  my $self=shift;
  my $lfn=shift;


  if ( $self->{ROLE} !~ /^admin(ssl)?$/ ) {
    $self->info("Error: only the administrator can add new table (you are '$self->{ROLE}')");
    return;
  }

  $lfn or $self->info( "Error: not enough arguments in moveDirectoryToIndex\nUsage: moveDirectory <lfn> ") and return;

  $lfn = $self->GetAbsolutePath($lfn, 1);

  $self->isDirectory($lfn) or $self->info( "The entry $lfn does not exist or it is not a directory") and return;
  $self->info( "Moving the directory $lfn");
  $lfn =~ s{/?$}{/};
  $self->checkPermissions("w", $lfn) or return;

  return $self->{DATABASE}->moveEntries($lfn);

}
sub expungeTables {
  my $self=shift;
  ( $self->{ROLE} =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can add new hosts") and return;

  $self->info("Dropping the empty tables");
  return $self->{DATABASE}->DropEmptyDLTables();


}

sub addSE_HELP {
  return "addSE: creates a new database for an SE, and inserts it in the table of all the catalogues
\tUsage:
\t\taddSE [-pd] <site_name> <se_name>
\tOptions:
\t\t -p: do not give an error if the SE already exists
\t\t -d: copy the data from the existing catalogue (this is only needed after a migration from AliEn to AliEn2).
The command will create a database called se_<vo>_<site>_<se_name>, where the se will put all its entries
";
}

sub addSE {
  my $self=shift;

  ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can add new sites") and return;

  $self->debug(1, "Adding a new SE");
  (my $options, @_)=$self->Getopts(@_);
  my $site=shift;
  my $name=shift;

  ($site and $name) or $self->info($self->addSE_HELP()) and return;

  my $addToTables=1;
  my $SEName="$self->{CONFIG}->{ORG_NAME}::${site}::$name";
  my $SEnumber=$self->{DATABASE}->queryValue("SELECT seNumber from SE where seName='$SEName'");

  #Check that the SE doesn't exist;
  if ($SEnumber){
    if ($options =~ /p/) {
      $addToTables=0;
    } else {
      $self->info("The se $SEName already exists!!", 1);
      return;
    }
  }
  my $dbName="se_".lc($SEName);
  $dbName =~ s{::}{_}g;

  if ($addToTables) {
    #First, let's create the database
    $SEnumber=1;
    my $max=$self->{DATABASE}->queryValue("SELECT max(seNumber)+1 FROM SE");
    ($max) and $SEnumber=$max;
    
    $self->info("Adding the new SE $SEName with $SEnumber");
    
    if (!$self->{DATABASE}->executeInAllDB("insert", "SE", {seName=>$SEName, seNumber=>$SEnumber})) {
      $self->info("Error adding the entry");
      $self->{DATABASE}->executeInAllDB("delete", "SE", "seName='$SEName' and seNumber=$SEnumber");
      return;
    }
  }
  my $done=$self->{DATABASE_FIRST}->do("CREATE DATABASE IF NOT EXISTS $dbName")
    or return;
  print "The done returned $done\n";
  if ($done ne "0E0"){
    $self->{DATABASE_FIRST}->do("create function $dbName.string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))") or return;
    $self->{DATABASE_FIRST}->do("create function $dbName.binary2string (my_uuid binary(16)) returns varchar(36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')");
    
    $self->{DATABASE_FIRST}->do("grant all on $dbName.* to $self->{CONFIG}->{CLUSTER_MONITOR_USER}");
  }

  $self->debug(2, "Let's create the tables");
  require AliEn::Database::SE;
  my ($host, $driver, $db)=split ( m{/}, $self->{CONFIG}->{CATALOGUE_DATABASE});

  my $s=AliEn::Database::SE->new({DB=>$dbName, DRIVER=>$driver, HOST=>$host})
    or $self->info("Error connecting to the database $dbName",3) and return;

  if ($options=~ /d/){
    $self->info("Copying the data");
    $self->{DATABASE}->executeInAllDB("do", "insert into $dbName.FILES (pfn, size, guid)  select pfn, size, guid from FILES2 where se='$SEName'")
  }
  $self->info("Entry Added!!!");

  return $SEnumber;
}

#############################################################################
#
# Banking functions 


sub addFunds_HELP{
return "addFunds adds given amount of funds to the given group account
\t Usage: 
\t\t addFunds <account> <amount>";
}

sub addFunds{
  my $self=shift;
  
  #
  # Check if the role is admin
  #
  ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can add funds to accounts") and return;

  my $account = shift || "";
  my $amount  = shift || "";
  my $silent  = shift || "";
  
  ($account and $amount) or return;
  
  my $currency=$self->{CONFIG}->{BANK_CURRENCY} || "unit";

  $silent or $self->info("Adding $amount $currency(s) to $account");
  my $done = $self->{SOAP}->CallSOAP("LBSG", "addFunds", $account, $amount);
  
  $done or $self->info("Error: Can not add funds, SOAP call to LBSG
	                  service failed") and return; 
  
  my $result = $done->result;
  ($result eq 1) and return 1; 	 
  $self->info ("Error: $result "); 

    
  return 1;      
}

sub createBankAccount_HELP{
return "createBankAccount creates a new bank account with the given amount of funds
\t Usage: 
\t\t createBankAccount <account> <amount>";
}



sub createBankAccount {
	
  my $self = shift;
  #
  # Check if the role is admin
  #
   ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or 
     $self->info("Error: only admin can create new bank account") and return;
       
  
  my $account = shift || "";
  my $amount  = shift || "";
  my $silent  = shift || "";

  ($account ) or return;
   $amount or $amount=0;
  
  my $currency=$self->{CONFIG}->{BANK_CURRENCY} || "unit";
 
  $silent or $self->info("Creating new bank account: $account with the initial amount of $amount $currency(s)");
	 
    my $done = $self->{SOAP}->CallSOAP("LBSG", "createBankAccount", $account, $amount);
    $done or $self->info("Error: Can not create account, SOAP call to LBSG
	                  service failed") and return;    

    my $result = $done->result;
     ($result eq 1) and return 1; 	 
      $self->info ("Error: $result "); 
 
	  
    return 1;
}


sub transactFunds_HELP{
return "transactFunds Makes the fund transaction between two accounts 
\t Usage: 
\t\t transactFunds <from account> <to account> <amount>";
}

sub transactFunds {
	
  my $self = shift;

  #
  # Check if the role is admin
  #
   ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or 
     $self->info("Error: only admin can create new bank account") and return;
       
  
  my $fromAccount = shift || "";
  my $toAccount   = shift || "";
  my $amount      = shift || "";
  my $silent      = shift || "";
  
  ($fromAccount and $toAccount and $amount) or return;
   
  my $currency=$self->{CONFIG}->{BANK_CURRENCY} || "unit";
	  
  $silent or $self->info("Making transaction of $amount $currency(s) from $fromAccount to $toAccount");
	 
    my $done = $self->{SOAP}->CallSOAP("LBSG", "transactFunds",   $fromAccount,$toAccount, $amount);
    $done or $self->info("Error: Can not transact funds, SOAP call to LBSG
	                  service failed") and return; 

     my $result = $done->result;
     ($result eq 1) and return 1; 	 
    $self->info ("Error: $result "); 
    return 0;
}
return 1;
