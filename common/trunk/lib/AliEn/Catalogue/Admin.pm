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
  return $self->{DATABASE}->addHost($host,$driver, $db, $org);
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

  my $group = $self->getUserGroup($user);

  $self->{DATABASE}->addUser($user, $group, $passwd)
    or return;

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

);

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

sub moveGUIDToIndex_HELP{
  return "moveGUID: moves all the GUID to a different table in the catalogue
This command can only be executed by admin

Usage:
    moveGUID [<guid>]

If the guid is not specified, a new one will be created
";
}

sub moveGUIDToIndex {
  my $self=shift;
  my $guid=shift;


  if ( $self->{ROLE} !~ /^admin(ssl)?$/ ) {
    $self->info("Error: only the administrator can add new table (you are '$self->{ROLE}')");
    return;
  }
  $self->{GUID} or $self->{GUID}=AliEn::GUID->new();
  $guid or $guid=$self->{GUID}->CreateGuid();

  $self->info( "All the guids newer than '$guid' will be in a different table");

  return $self->{DATABASE}->moveGUIDs($guid);

}
sub expungeTables {
  my $self=shift;
  ( $self->{ROLE} =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can add new hosts") and return;

  $self->info("Dropping the empty tables");
  return $self->{DATABASE}->DropEmptyDLTables();


}

sub setSEio_HELP {
    return "setSEio: allows you to set the se io methods and storage path in all databases
\t Usage:
\t\tsetSEio <site_name> <se_name> <se_iodaemons> <se_storagepath>
\t\t (se_iodaemons like root://cl4.ujf.cas.cz:1094 )
\t\t (se_storagepath like /raidRA/aliprod/SE )
";
}

sub setSEio {
    my $self =shift;
    ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can add new sites") and return;

    (my $options, @_)=$self->Getopts(@_);
    my $site=shift;
    my $name=shift;
    my $seio=shift;
    my $sesp=shift;

    ($site and $name and $seio and $sesp) or $self->info($self->setSEio_HELP()) and return;
    return $self->{DATABASE}->setSEio($options,$site,$name,$seio,$sesp);
}

sub getSEio_HELP {
    return "getSEio: allows you to get the se io methods and storage path of an SE
\t Usage:
\t\tgetSEio <site_name> <se_name> 
";
}
sub getSEio {
    my $self =shift;
    ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
	$self->info("Error: only the administrator can add new sites") and return;
    
    (my $options, @_)=$self->Getopts(@_);
    my $site=shift;
    my $name=shift;
    
    ($site and $name) or $self->info($self->getSEio_HELP()) and return;
    my $seio = $self->{DATABASE}->getSEio($options,$site,$name);
    foreach (keys %$seio) {
	printf "%32s\t",$_;
	if (defined $seio->{$_}) {
	    print "$seio->{$_}\n";
	} else {
	    print "NULL\n";
	}
    }
    print "\n";
    return $seio;
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

  my ($dbName, $SEnumber)=$self->{DATABASE}->addSE($options, $site, $name) or return;

  my $done=$self->{DATABASE_FIRST}->do("CREATE DATABASE IF NOT EXISTS $dbName")
    or return;
  print "The done returned $done\n";
  if ($done ne "0E0"){
    require AliEn::Database::SE;
    my ($host, $driver, $db)=split ( m{/}, $self->{CONFIG}->{CATALOGUE_DATABASE});
    $self->{DATABASE_FIRST}->do("grant all on $dbName.* to $self->{CONFIG}->{CLUSTER_MONITOR_USER}");

    my $s=AliEn::Database::SE->new({DB=>$dbName, DRIVER=>$driver, HOST=>$host, ROLE=>'admin'})
    or $self->info("Error connecting to the database $dbName",3) and return;

    $self->{DATABASE_FIRST}->do("create function $dbName.string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))") or return;
    $self->{DATABASE_FIRST}->do("create function $dbName.binary2string (my_uuid binary(16)) returns varchar(36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')");
    
  }


  return $SEnumber;
}


sub checkSEVolumes_HELP {
  my $self=shift;
  return "checkSEVolumes: checks the volumes defined in ldap for an se
Syntax:
     checkSEVolumes <site> <se>
";
}

sub checkSEVolumes {
  my $self=shift;
  my $site=shift;
  my $se=shift;

  ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can check the databse") and return;

  ($site and $se) or $self->info("Error: not enough arguments in checkSEVolumes. ". $self->checkSEVolumes_HELP()) and return;
  my $oldInfo=$self->{CONFIG}->{SE_LVMDATABASE};
  require AliEn::Database::SE;
  $self->{CONFIG}->{SE_LVMDATABASE}=$self->{CONFIG}->{CATALOGUE_DATABASE};

  $self->{CONFIG}->{SE_LVMDATABASE}=~ s{/[^/]*$}{/\Lse_$self->{CONFIG}->{ORG_NAME}_${site}_${se}\E};
  my $db=AliEn::Database::SE->new();
  if (!$db){
    $self->info("Error getting the database");
    $self->{CONFIG}->{SE_LVMDATABASE}=$oldInfo;
    return;
  }
  $self->info("Got the database");
  $db->checkVolumes($site, $se);
  $db->close();

  $self->{CONFIG}->{SE_LVMDATABASE}=$oldInfo;
  return 1;
}

return 1;
