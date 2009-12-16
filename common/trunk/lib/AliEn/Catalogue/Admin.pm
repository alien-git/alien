package AliEn::Catalogue::Admin;

use strict;

use AliEn::Database::Admin;
use AliEn::Database::Transfer;
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

  $self->resyncLDAP();
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


sub moveDirectory_HELP {
  return "Usage:
moveDirectory [-b] <lfn>
where lfn is a directory

Only 'admin' is allowed to excecute moveDirectory. It changes the structure of the database, putting the directory <lfn> into a new table

Options:
  -b: Go back to the previous table
"

}
sub moveDirectory {
  my $self=shift;

  @ARGV=@_;
  my $opt={};
  
  Getopt::Long::GetOptions($opt,  "b") or
     $self->info("Error: unkown options:'@_'\n" . $self->moveDirectory_HELP()) and return;
  @_=@ARGV;

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

  return $self->{DATABASE}->moveEntries($lfn, $opt);

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

  return $self->{DATABASE}->moveGUIDs($guid, @_);

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
\t\taddSE [-p] <site_name> <se_name>
\tOptions:
\t\t -p: do not give an error if the SE already exists
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

  my $SEnumber=$self->{DATABASE}->addSE($options, $site, $name) or return;

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

sub f_showStructure_HELP{
  return "showStructure: returns the database tables that are used from within a directory in the catalogue. Usage

  showStructure [-csgf] <directory>

Options: 
-c: count the number of entries in each of the tables
-s: summary at the end 
-g: count the guid instead of the lfns
-f: count only files (ignore directories)
\n";
}
sub f_showStructure {
  my $self=shift;
  my $options=shift;
  my $dir=shift;
  
  my $lfn;
  my $info;
  if ($options=~ /g/){
    if ($dir){
      $info=$self->{DATABASE}->getIndexHostFromGUID($dir);
      if ($info){
	$info->{guidTime}=$self->{DATABASE}->{GUID_DB}->queryValue("select string2date(?)", undef, {bind_values=>[$dir]});
	$info->{tableName}=~ s/^G(.*)L$/$1/;
	$info=[$info];
      }
    }else{
      $info=$self->{DATABASE}->{GUID_DB}->query("SELECT * FROM GUIDINDEX order by guidTime");
    }
  }else{
    $lfn= $self->GetAbsolutePath($dir);
    $self->info("Checking the directories under $lfn");
    $info=$self->{DATABASE}->getHostsForLFN($lfn);
  }
  if ($options=~ /(c|s)/  ){
    $self->info("Let's print the number of entries");
    my $total=0;
    foreach my $dir (@$info){
      my $s=$self->{DATABASE}->getNumberOfEntries($dir, $options);
      my $entryName=$dir->{lfn} || $dir->{guidTime};

      $options=~ /c/ and $self->info("Under $entryName ($dir->{tableName}): $s entries");
      $s>0 and $total+=$s;
    }
    my $field= $lfn || $dir || "the guid catalogue";
    $options=~ /s/ and $self->info("In total, under $field: $total entries");
  }
  return $info;
}

sub f_renumber {
  my $self=shift;
  my $dir=shift;
  my $options=shift || "";
 ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can check the databse") and return;
  if ($options =~ /g/i){
    $self->info("Renumbering a guid table");
    return $self->{DATABASE}->renumberGUIDtable($dir, $options);
  }
  my $lfn = $self->GetAbsolutePath($dir);
  $self->checkPermissions("w", $lfn) or return;
  $self->info("Ready to renumber the entries in $lfn");


  return $self->{DATABASE}->renumberLFNtable($lfn, $options);
}

sub resyncLDAP {
  my $self=shift;

  $self->info("Let's syncrhonize the DB users with ldap");
  eval {
    my  $addbh = new AliEn::Database::Admin()
      or die("Error getting the admin database");
    $self->info("Got the database");
    my $ldap = Net::LDAP->new($self->{CONFIG}->{LDAPHOST}) or die "Error contacting LDAP in $self->{CONFIG}->{LDAPHOST}\n $@\n";

    $ldap->bind;      # an anonymous bind
    $self->info("Got the ldap");
    $addbh->update("USERS_LDAP", {up=>0});
    $addbh->update("USERS_LDAP_ROLE", {up=>0});
    my $mesg = $ldap->search( base   => "ou=People,$self->{CONFIG}->{LDAPDN}",
			     filter => "(objectclass=pkiUser)", );
    foreach my $entry ( $mesg->entries()) {
      my $user=$entry->get_value('uid');
      my @dn=$entry->get_value('subject');
      foreach my $dn (@dn){
	$addbh->do("insert into USERS_LDAP(user,dn, up)  values (?,?,1)", {bind_values=>[$user, $dn]});
      }
      my $ssh=$entry->get_value('sshkey');
      $addbh->do("update TOKENS set SSHkey=? where username=?", {bind_values=>[$ssh, $user]});

    }
    $self->info("And now, the roles");
    $mesg   = $ldap->search( base   => "ou=Roles,$self->{CONFIG}->{LDAPDN}",
			     filter => "(objectclass=AliEnRole)",
			  );
    my $total = $mesg->count;
    for (my $i=0; $i<$total; $i++){
      my $entry=$mesg->entry($i);
      my $user=$entry->get_value('uid');
      my @dn=$entry->get_value('users');
      $self->debug(1,"user: $user => @dn");
      foreach my $dn (@dn){
	$addbh->do("insert into USERS_LDAP_ROLE(user,role, up)  values (?,?,1)", {bind_values=>[$dn, $user]});
      }
    }
    $self->info("And let's add the new users");
    my $newUsers=$addbh->queryColumn("select a.user from USERS_LDAP a left join USERS_LDAP b on b.up=0 and a.user=b.user where a.up=1 and b.user is null");
    foreach my $u (@$newUsers){
      $self->info("Adding the user $u");
      $self->f_addUser($u);
    }
    $addbh->delete("USERS_LDAP", "up=0");
    $addbh->delete("USERS_LDAP_ROLE", "up=0");

    $addbh->close();
  };
  if ($@){
    $self->info("Error doing the sync: $@"); 
    return;
  }
  $self->info("ok!!");
  $self->resyncLDAPSE();

  return 1;
}

sub getLDAP {
  my $self=shift;
  my $ldap;
  eval {
    $ldap=Net::LDAP->new($self->{CONFIG}->{LDAPHOST}) or die("Error contacting $self->{CONFIG}->{LDAPHOST}");
    $ldap->bind();
  };
  if ($@){
    $self->info("Error connecting to ldap!: $@");
    return;
  }
  return $ldap;
}


sub checkFTDProtocol{
  my $self=shift;
  my $entry=shift;
  my $sename=shift;
  my $db=shift;
  # $self->info("WHAT SHALL WE DO HERE????");
  my @protocols=$entry->get_value('ftdprotocol');
  foreach my $p (@protocols){
    my ($name, $options)=split('\s+',$p,2);
    $self->info("Inserting $name and $sename");
    my $info={sename=>$sename, protocol=>$name};
    if ($options){
      ($options=~ s/\s*transfers=(\d+)\s*//) and
	$info->{max_transfers}=$1;
      $options!~ /^\s*$/ and $info->{options}=$options;
    }
    $db->insertProtocol($info);
  }
  foreach my $p ($entry->get_value('deleteprotocol')){
    my ($name, $options)=split('\s+',$p,2);
    $db->insertProtocol({sename=>$sename, protocol=>$name, 
			 deleteprotocol=>1});

  }
  return 1;
}
sub resyncLDAPSE {
  my $self=shift;

  $self->info("Let's resync the SE and volumes from LDAP");
  my $ldap=$self->getLDAP() or return;

  my $transfers=AliEn::Database::Transfer->new({ROLE=>'admin'});
  if (! $transfers){
    $self->info("Error getting the transfer database");
    $ldap->unbind();
    return;
  }
  $transfers->do("UPDATE PROTOCOLS set updated=0");

  my $mesg=$ldap->search(base=>$self->{CONFIG}->{LDAPDN},
			 filter=>"(objectClass=AliEnMSS)");
  my $total=$mesg->count;
  $self->info("There are $total entries under AliEnMSS");

  my $db=$self->{DATABASE}->{LFN_DB}->{FIRST_DB};

  my $new_SEs={};

  foreach my $entry ($mesg->entries){
    my $name=uc($entry->get_value("name"));
    my $dn=$entry->dn();
    $dn=~ /ou=Services,ou=([^,]*),ou=Sites,o=([^,]*),/i or $self->info("Error getting the site name of '$dn'") and next;
    $dn=~ /disabled/ and 
      $self->debug(1, "Skipping '$dn' (it is disabled)") and next;
    my ($site, $vo)=($1,$2);
    my $sename="${vo}::${site}::$name";
    $self->info("Doing the SE $sename");
    $self->checkSEDescription($entry, $site, $name, $sename);
    $self->checkIODaemons($entry, $site, $name, $sename);
    $self->checkFTDProtocol($entry,$sename, $transfers);
    my @paths=$entry->get_value('savedir');

    my $info=  $db->query("select * from SE_VOLUMES where sename=?",
			 undef, {bind_values=>[$sename]});

    my @existingPath=@$info;
    my $t=$entry->get_value("mss");
    my $host=$entry->get_value("host");
    foreach my $path (@paths){
      my $found=0;
      my $size=-1;
      $path =~ s/,(\d+)$// and $size=$1;
      $self->info("  Checking the path of $path");
      for my $e (@existingPath){
        $e->{mountpoint} eq $path or next;
        $self->debug(1, "The path already existed");
        $e->{FOUND}=1;
        $found=1;
	$new_SEs->{$sename}=1;
        ( $size eq $e->{size}) and next;
        $self->info("**THE SIZE IS DIFFERENT ($size and $e->{size})");
        $db->do("update SE_VOLUMES set size=? where mountpoint=? and sename=?", {bind_values=>[$size, $e->{mountpoint}, $sename]});
      }
      $found and next;
      $self->info("**WE HAVE TO ADD THE SE '$path'");

      if (! $host) {
	$self->info("***The host didn't exist. Maybe we have to get it from the father??");
	my $base=$dn;
	$base =~ s/^[^,]*,(name=([^,]*),)/$1/;
	$self->info("looking in $base (and $2");
	my $mesg2=$ldap->search(base=>$base,
				filter=>"(&(name=$2)(objectClass=AliEnMSS))");
	my @entry2= $mesg2->entries();
	$host=$entry2[0]->get_value('host');
	
      }
      my $method= lc($t)."://$host";

      $db->do("insert into SE_VOLUMES(sename, volume,method, mountpoint,size) values (?,?,?,?,?)", {bind_values=>[$sename, $path, $method, $path, $size]});
      $new_SEs->{$sename} or  $new_SEs->{$sename}=0;
    }
    foreach my $oldEntry (@existingPath){
      $oldEntry->{FOUND} and next;
      $self->info("**The path $oldEntry->{mountpoint} is not used anymore");
      $db->do("update SE_VOLUMES set size=usedspace where mountpoint=? and sename=?", {bind_values=>[$oldEntry->{mountpoint}, $sename]});
      $new_SEs->{$sename}=1;
    }
  }

  foreach my $item (keys %$new_SEs){
    $new_SEs->{$item} and next;
    $self->info("The se '$item' is new. We have to add it");
    my ($vo, $site, $name)=split(/::/, $item, 3);
    $self->addSE("-p", $site, $name);
  }
  $db->do("update SE_VOLUMES set usedspace=0 where usedspace is null");

  $db->do("update SE_VOLUMES set freespace=size-usedspace where size!= -1");
  $db->do("update SE_VOLUMES set freespace=2000000000 where size=-1");

  $transfers->do("delete from PROTOCOLS where updated=0");
  $transfers->do("insert into PROTOCOLS(sename,max_transfers) values ('no_se',10)");

  $ldap->unbind();
  $transfers->close();
  return;
}
sub getBrokenLFN{
  my $self=shift;
  return $self->{DATABASE}->getBrokenLFN(@_);
}

sub checkSEDescription {
  my $self=shift;
  my $entry=shift;
  my $site=shift;
  my $name=shift;
  my $sename=shift;


  my $db=$self->{DATABASE}->{LFN_DB}->{FIRST_DB};

  my $min_size=0;
  foreach my $d ($entry->get_value('options')){
    $self->info("Checking $d");
    $d=~ /min_size\s*=\s*(\d+)/ and $min_size=$1;
  }
  my $type=$entry->get_value("mss");
  my @qos=$entry->get_value("QoS");

  my $qos="," . join(",",@qos). ",";
  $self->info("The se $sename has $min_size and $type");

  my $exists=$db->queryValue("select count(*) from SE where sename=? and seminsize=? and setype=? and seqos=?", undef, {bind_values=>[$sename, $min_size, $type, $qos]});
  if (not $exists){
    $self->info("We have to update the entry!!!");
    $db->do("update SE set seminsize=?, setype=?, seqos=? where sename=?", {bind_values=>[$min_size,$type, $qos, $sename]});
  }
  return 1;

}


sub checkIODaemons {
  my $self=shift;
  my $entry=shift;
  my $site=shift;
  my $name=shift;
  my $sename=shift;

  my (@io)=$entry->get_value('iodaemons');
  my $found= join("", grep (/xrootd/, @io)) or return;
  $self->debug(1, "This se has $found");

  $found =~ /port=(\d+)/i or $self->info("Error getting the port from '$found' for $sename") and return;
  my $port=$1;
  my $host=$entry->get_value('host');
  $found =~ /host=([^:]+)(:.*)?$/i and $host=$1;
  $self->info("Using host=$host and port=$port for $sename");

  my $path=$entry->get_value('savedir') or return;

  $path=~ s/,.*$//;
  my $seioDaemons="root://$host:$port";
  $self->debug(1, "And the update should $sename be: $seioDaemons, $path");
  my $e=$self->{DATABASE}->{LFN_DB}->query("SELECT sename,seioDaemons,sestoragepath from SE where seName='$sename'");
  my $path2=$path;
  $path2 =~ s/\/$// or $path2.="/";
  my $total=$self->{DATABASE}->{LFN_DB}->queryValue("SELECT count(*) from SE where seName='$sename' and seioDaemons='$seioDaemons' and ( seStoragePath='$path' or sestoragepath='$path2')");
  
  if ($total<1){
    $self->info("***Updating the information of $site, $name ($seioDaemons and $path");
    $self->setSEio($site, $name, $seioDaemons, $path);
  }
  
  return 1;
}


sub checkLFN_HELP {
  return "checkLFN. Updates all the guids that are referenced by lfn
Usage:
       checkLFN [<db> [<table>]] ";
}

sub checkLFN {
  my $self=shift;

 ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can check the databse") and return;

  $self->info("Ready to check that the lfns are ok");

  return  $self->{DATABASE}->checkLFN(@_);
}

sub checkOrphanGUID{
  my $self=shift;

 ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can check the databse") and return;

  $self->info("And now, let's see if there are any orphan guids");
  return $self->{DATABASE}->checkOrphanGUID(@_);
}

sub optimizeGUIDtables{
  my $self=shift;
  
  $self->info("Let's optimize the guid tables");
  return $self->{DATABASE}->optimizeGUIDtables(@_);
}

sub masterSE_list {
  my $self=shift;
  my $sename=shift;
  $self->info("Counting the number of entries in $sename");
  return $self->{DATABASE}->masterSE_list($sename);
}

sub masterSE_getFiles{
  my $self=shift;
  return $self->{DATABASE}->masterSE_getFiles(@_);
}

sub calculateJobQuota {
	my $self = shift;
	my $silent = shift;

 ( $self->{ROLE}  =~ /^admin(ssl)?$/ ) or
    $self->info("Error: only the administrator can check the databse") and return;

  my $method="info";
  my @data;
  $silent and $method="debug" and push @data, 1;
  
  $self->$method(@data, "Calculate Job Quota");

  $self->$method(@data, "Compute the number of unfinished jobs in last 24 hours per user");
  $self->{TASK_DB}->do("update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, count(1) as unfinishedJobsLast24h from QUEUE q where (status='INSERTING' or status='WAITING' or status='STARTED' or status='RUNNING' or status='SAVING' or status='OVER_WAITING') and (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user collate latin1_general_cs set pr.unfinishedJobsLast24h=IFNULL(C.unfinishedJobsLast24h, 0)") or $self->$method(@data, "Failed");

  $self->$method(@data, "Compute the total runnning time of jobs in last 24 hours per user");
  $self->{TASK_DB}->do("update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, sum(p.runtimes) as totalRunningTimeLast24h from QUEUE q join QUEUEPROC p using(queueId) where (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user collate latin1_general_cs set pr.totalRunningTimeLast24h=IFNULL(C.totalRunningTimeLast24h, 0)");

  $self->$method(@data, "Compute the total cpu cost of jobs in last 24 hours per user");
  $self->{TASK_DB}->do("update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, sum(p.cost) as totalCpuCostLast24h from QUEUE q join QUEUEPROC p using(queueId) where (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user collate latin1_general_cs set pr.totalCpuCostLast24h=IFNULL(C.totalCpuCostLast24h, 0)") or $self->$method(@data, "Failed");

  $self->$method(@data, "Change job status from OVER_WAITING to WAITING");
	$self->{TASK_DB}->do("update QUEUE q join PRIORITY pr on pr.user=SUBSTRING( q.submitHost, 1, POSITION('\@' in q.submitHost)-1 ) collate latin1_general_cs set q.status='WAITING' where (pr.totalRunningTimeLast24h<pr.maxTotalRunningTime and pr.totalCpuCostLast24h<pr.maxTotalCpuCost) and q.status='OVER_WAITING'") or $self->$method(@data, "Failed");

  $self->$method(@data, "Change job status from WAITING to OVER_WAITING");
	$self->{TASK_DB}->do("update QUEUE q join PRIORITY pr on pr.user=SUBSTRING( q.submitHost, 1, POSITION('\@' in q.submitHost)-1 ) collate latin1_general_cs set q.status='OVER_WAITING' where (pr.totalRunningTimeLast24h>=pr.maxTotalRunningTime or pr.totalCpuCostLast24h>=pr.maxTotalCpuCost) and q.status='WAITING'") or $self->$method(@data, "Failed");

  $self->$method(@data, "Synchronize with SITEQUEUES");
  foreach (qw(OVER_WAITING WAITING)) {
    $self->{TASK_DB}->do("update SITEQUEUES s set $_=(select count(1) from QUEUE q where status='$_' and s.site=q.site)") or $self->$method(@data, "$_ Failed");
    $self->{TASK_DB}->do("update SITEQUEUES s set $_=(select count(1) from QUEUE q where status='$_' and q.site is null) where s.site='UNASSIGNED::SITE'") or $self->$method(@data, "$_ UNASSIGNED::SITE Failed");
  }

  return;
}

return 1;
