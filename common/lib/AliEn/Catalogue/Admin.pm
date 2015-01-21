package AliEn::Catalogue::Admin;

use strict;
use Data::Dumper;

use AliEn::Database::Transfer;
use AliEn::Database::TaskQueue;

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
    'X', 'Q', 't', '2', '!', '^', '9', '5', "\$", '3', '4', '5', 'o', 'r', 't', '{', ')', '}',
    '[', ']', 'h', '9', '|', 'm', 'n', 'b', 'v',  'c', 'x', 'z', 'a', 's', 'd', 'f', 'g', 'h',
    'j', 'k', 'l', ':', 'p', 'o', 'i', 'u', 'y',  'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O',
    'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'Z',  'X', 'C', 'V', 'B', 'N', 'M'
  );
  my $i;
  for ($i = 0 ; $i < 32 ; $i++) {
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
    'X', 'Q', 'st', '2',  '!', '^', '9', '5', "\$", '3', '4', '5', 'po', 'r', 't', '{', ')', '}',
    '[', ']', 'gh', 'e9', '|', 'm', 'n', 'b', 'v',  'c', 'x', 'z', 'a',  's', 'd', 'f', 'g', 'h',
    'j', 'k', 'l',  ':',  'p', 'o', 'i', 'u', 'y',  'Q', 'W', 'E', 'R',  'T', 'Y', 'U', 'I', 'O',
    'P', 'A', 'S',  'D',  'F', 'G', 'H', 'J', 'Z',  'X', 'C', 'V', 'B',  'N', 'N', 'M'
  );
  my $i;
  for ($i = 0 ; $i < 10 ; $i++) {
    $passwd .= $Array[ rand(@Array) ];
  }
  return $passwd;

};


sub f_addUser {
  my $self = shift;

  $self->debug(1, "Adding a user");
  if ($self->{ROLE} !~ /^admin(ssl)?/) {
    print STDERR "Only the administrator can add a user\n";
    return;
  }
  my $user = shift;

  if (!$user) {
    $self->{LOGGER}->error("Catalogue/Admin", "Error: not enough arguments\nUsage: addUser <username> \n");
    return;
  }

  $self->{TASK_DB}
    or $self->{TASK_DB} = AliEn::Database::TaskQueue->new({ROLE => 'admin', SKIP_CHECK_TABLES => 1});
  $self->{TASK_DB} or $self->info("Error getting the instance of the taskDB!!") and return;

  my $homedir = "$self->{CONFIG}->{USER_DIR}/" . substr($user, 0, 1) . "/$user/";

  $homedir =~ s{//}{/};

  my $group = $self->getUserGroup($user);

  $self->{DATABASE}->addUser($user, $group)
    or return;

  $self->info("Creating new homedir for  $user");
  $self->f_mkdir("p", $homedir)
    or $self->info("Error creating $homedir")
    and return;

  $self->info("Changing privileges for  $user");
  $self->f_chown("", $user, $homedir) or return;
  $self->info("Adding the FQUOTAS");
  my $exists =
    $self->{DATABASE}->queryValue("select Username from FQUOTAS join USERS on userId=uId where Username like ?", undef, {bind_values => [$user]});

  if (defined($exists) and $exists eq $user) {
    $self->debug(1, "$user entry for FQUOTAS exists!");
  } else {
    $self->debug(1, "$user entry for FQUOTAS does not exist!");
    ##File Quota
    my $nbFiles               = 0;
    my $totalSize             = 0;
    my $tmpIncreasedNbFiles   = 0;
    my $tmpIncreasedTotalSize = 0;
    my $maxNbFiles            = 10000;
    my $maxTotalSize          = 10000000000;
    my $db                    = $self->{DATABASE};
    if($user =~ /^admin$/) {
      $maxNbFiles = -1;
      $maxTotalSize = -1;
    }
    $db->do(
      "insert into FQUOTAS ( "
        . "userId, nbFiles, totalSize, tmpIncreasedNbFiles, tmpIncreasedTotalSize, maxNbFiles, maxTotalSize ) select uId," 
		. "?,?,?,?,?,? from USERS where Username like ?",
      { bind_values =>
          [ $nbFiles, $totalSize, $tmpIncreasedNbFiles, $tmpIncreasedTotalSize, $maxNbFiles, $maxTotalSize, $user ]
      }
    );
    $self->info("User $user added");
  }

  $self->info("Adding the jobquotas");
  $self->{TASK_DB}->checkPriorityValue($user) or return;
  $self->info("User $user added");

  $self->resyncLDAP();
  return 1;
}

sub getUserGroup {
  my $self = shift;
  my $oldusername = shift || return;

  my ($username, $userpasswd, $uid, $gid, $userquota, $usercomment, $usergcos, $userdir, $usershell, $userexpire) =
    getpwnam($oldusername);

  ($gid) or return $oldusername;

  my ($gname, $passwd, $newgid, $members) = getgrgid($gid);

  return $gname;
}

sub f_host {
  my $self = shift;

  $self->{SILENT}
    or print "Current database:\t $self->{DATABASE}->{DB} in $self->{DATABASE}->{HOST}\n";

  return $self->{DATABASE}->{HOST};
}

sub f_chgroup {
  my $self   = shift;
  my $user   = shift;
  my @groups = @_;
  if (!@groups) {
    print STDERR "Error: not enough arguments in chgroup\nUsage chgroup <user> <initial_group> [<group> [...]]\n";
    return;
  }
  if ($self->{ROLE} !~ /^admin(ssl)?$/) {
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
  my $self = shift;

  @ARGV = @_;
  my $opt = {};

  Getopt::Long::GetOptions($opt, "b")
    or $self->info("Error: unkown options:'@_'\n" . $self->moveDirectory_HELP())
    and return;
  @_ = @ARGV;

  my $lfn = shift;

  if ($self->{ROLE} !~ /^admin(ssl)?$/) {
    $self->info("Error: only the administrator can add new table (you are '$self->{ROLE}')");
    return;
  }

  $lfn or $self->info("Error: not enough arguments in moveDirectoryToIndex\nUsage: moveDirectory <lfn> ") and return;

  $lfn = $self->GetAbsolutePath($lfn, 1);

  $self->isDirectory($lfn) or $self->info("The entry $lfn does not exist or it is not a directory") and return;
  $self->info("Moving the directory $lfn");
  $lfn =~ s{/?$}{/};
  $self->checkPermissions("w", $lfn) or return;

  return $self->{DATABASE}->moveEntries($lfn, $opt);

}

sub moveGUID_HELP {
  return "moveGUID: moves all the GUID to a different table in the catalogue
This command can only be executed by admin

Usage:
    moveGUID [<guid>]

If the guid is not specified, a new one will be created
";
}

sub moveGUID {
  my $self = shift;
  my $guid = shift;

  if ($self->{ROLE} !~ /^admin(ssl)?$/) {
    $self->info("Error: only the administrator can add new table (you are '$self->{ROLE}')");
    return;
  }
  $self->{GUID} or $self->{GUID} = AliEn::GUID->new();
  if (!$guid) {
    $guid = $self->{GUID}->CreateGuid();
  }

  $self->info("All the guids newer than '$guid' will be in a different table");

  return $self->{DATABASE}->moveGUIDs($guid, @_);

}

sub expungeTables {
  my $self = shift;
  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can add new hosts")
    and return;

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
  my $self = shift;
  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can add new sites")
    and return;

  (my $options, @_) = $self->Getopts(@_);
  my $site = shift;
  my $name = shift;
  my $seio = shift;
  my $sesp = shift;

  ($site and $name and $seio and $sesp) or $self->info($self->setSEio_HELP()) and return;
  return $self->{DATABASE}->setSEio($options, $site, $name, $seio, $sesp);
}

sub getSEio_HELP {
  return "getSEio: allows you to get the se io methods and storage path of an SE
\t Usage:
\t\tgetSEio <site_name> <se_name> 
";
}

sub getSEio {
  my $self = shift;
  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can add new sites")
    and return;

  (my $options, @_) = $self->Getopts(@_);
  my $site = shift;
  my $name = shift;

  ($site and $name) or $self->info($self->getSEio_HELP()) and return;
  my $seio = $self->{DATABASE}->getSEio($options, $site, $name);
  foreach (keys %$seio) {
    printf "%32s\t", $_;
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
  my $self = shift;

  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can add new sites")
    and return;

  $self->debug(1, "Adding a new SE");
  (my $options, @_) = $self->Getopts(@_);
  my $site = shift;
  my $name = shift;

  ($site and $name) or $self->info($self->addSE_HELP()) and return;

  my $SEnumber = $self->{DATABASE}->addSE($options, $site, $name) or return;

  return $SEnumber;
}

sub f_showStructure_HELP {
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
  my $self    = shift;
  my $options = shift;
  my $dir     = shift;

  my $lfn;
  my $info;
  if ($options =~ /g/) {
    if ($dir) {
      my $table = $self->{DATABASE}->getIndexTableFromGUID($dir);
      if ($info) {
        $info->{guidTime} =
          $self->{DATABASE}->queryValue("select string2date(?)", undef, {bind_values => [$dir]});
        $info->{tableName} =~ s/^G(.*)L$/$1/;
        $info = [$info];
      }
    } else {
      $info = $self->{DATABASE}->query("SELECT * FROM GUIDINDEX order by guidTime");
    }
  } else {
    $lfn = $self->GetAbsolutePath($dir);
    $self->info("Checking the directories under $lfn");
    $info = $self->{DATABASE}->getHostsForLFN($lfn);
  }
  if ($options =~ /(c|s)/) {
    $self->info("Let's print the number of entries");
    my $total = 0;
    foreach my $dir (@$info) {
      my $s = $self->{DATABASE}->getNumberOfEntries($dir, $options);
      my $entryName = $dir->{lfn} || $dir->{guidTime};

      $options =~ /c/ and $self->info("Under $entryName ($dir->{tableName}): $s entries");
      $s > 0 and $total += $s;
    }
    my $field = $lfn || $dir || "the guid catalogue";
    $options =~ /s/ and $self->info("In total, under $field: $total entries");
  }
  return $info;
}

sub f_renumber {
  my $self    = shift;
  my $dir     = shift;
  my $options = shift || "";
  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can check the databse")
    and return;
  if ($options =~ /g/i) {
    $self->info("Renumbering a guid table");
    return $self->{DATABASE}->renumberGUIDtable($dir, $options);
  }
  my $lfn = $self->GetAbsolutePath($dir);
  $self->checkPermissions("w", $lfn) or return;
  $self->info("Ready to renumber the entries in $lfn");
  my $entry = $self->{DATABASE}->getIndexHost($lfn);
  $entry or $self->info("The path $lfn is not in the catalogue ") and return;

  my $table = "L$entry->{tableName}L";
  $self->info("DOING $table");
  return $self->{DATABASE}->renumberLFNtable($table, $options);
}

sub resyncLDAP {
  my $self = shift;

  $self->info("Let's synchronize the DB users with ldap");
  eval {
    $self->info("Got the database");
    my $ldap = Net::LDAP->new($self->{CONFIG}->{LDAPHOST})
      or die "Error contacting LDAP in $self->{CONFIG}->{LDAPHOST}\n $@\n";

    $ldap->bind;    # an anonymous bind
    my $userColumn=$self->{DATABASE}->reservedWord("user");
    $self->info("Got the ldap");
    $self->{DATABASE}->update("USERS_LDAP",      {up => 0});
    $self->{DATABASE}->update("USERS_LDAP_ROLE", {up => 0});
    my $mesg = $ldap->search(
      base   => "ou=People,$self->{CONFIG}->{LDAPDN}",
      filter => "(objectclass=pkiUser)",
    );
    foreach my $entry ($mesg->entries()) {
      my $user = $entry->get_value('uid');
      my @dn   = $entry->get_value('subject');
      foreach my $dn (@dn) {
        $self->{DATABASE}->do("insert into USERS_LDAP( $userColumn ,dn, up)  values (?,?,1)",
          {bind_values => [ $user, $dn ]});
      }
    }
    $self->info("And now, the roles");
    $mesg = $ldap->search(
      base   => "ou=Roles,$self->{CONFIG}->{LDAPDN}",
      filter => "(objectclass=AliEnRole)",
    );
    my $total = $mesg->count;
    for (my $i = 0 ; $i < $total ; $i++) {
      my $entry = $mesg->entry($i);
      my $user  = $entry->get_value('uid');
      my @dn    = $entry->get_value('users');
      $self->debug(1, "user: $user => @dn");
      foreach my $dn (@dn) {
        $self->{DATABASE}->do("insert into USERS_LDAP_ROLE ($userColumn,role, up)  values (?,?,1)",
          {bind_values => [ $dn, $user ]});
      }
    }
    $self->info("And let's add the new users");
      
    my ($newUsers) = $self->{DATABASE}->queryColumn("select $userColumn from USERS_LDAP      where up=1 and user not in (select Username from USERS)");  
    my ($newRoles) = $self->{DATABASE}->queryColumn("select role        from USERS_LDAP_ROLE where up=1 and role not in (select Username from USERS)");
    
    foreach my $u (@$newUsers, @$newRoles) {
      $self->info("Adding the user $u");
      $self->f_addUser($u);
    }
    $self->{DATABASE}->delete("USERS_LDAP",      "up=0");
    $self->{DATABASE}->delete("USERS_LDAP_ROLE", "up=0");

    $self->{DATABASE}->close();
  };
  if ($@) {
    $self->info("Error doing the sync: $@");
    return;
  }
  $self->info("ok!!");
  $self->resyncLDAPSE();

  return 1;
}
sub listSEDistance_HELP {
  return "listSEDistance: Returns the closest working SE for a particular site. Usage
  
 listSEDistance [<site>] [read|write]
 
 
 Options: 
   <site>: site name. Default: current site
   [read|write]: action. Default write
  "
}



sub listSEDistance {
	my $self=shift;
	my $options=shift;

	my $sitename = (shift || $self->{CONFIG}->{SITE});
	my $action  = (shift || 'write');

	$self->info("Displaying the list of SE from the site $sitename");
	$action =~ /^(read)|(write)$/i or $self->info("Error in listSEDistance: action '$action' not understood ") and return;


	my $se=$self->{DATABASE}->query("select sename, sedemotewrite, sedemoteread, sitedistance, sitedistance + sedemote$action weight
	from SE join SEDistance using (seNumber)  where sitename=? order by 4 asc ", undef, {bind_values=>[$sitename]});	
	
	$se or $self->info("Error doing the query in listSEDistance") and return;
	
	$self->info("The ordered list $action is:");
	foreach my $e (@$se){
	  $self->info("$e->{weight}     $e->{sename} (distance: $e->{sitedistance} read: $e->{sedemoteread} write: $e->{sedemotewrite}) \n",undef, 0)		
	}
	
	return $se;
}

sub setSEStatus {
	my $self=shift;
	my $options=shift;
	my $sename =shift;
	
	($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can check the databse")
    and return;
  my $set={};
	foreach my $e (@_){
		$self->info("CHECKING $e");
		$e =~ /^((read)|(write))=(\d+.?\d*)$/ or $self->info("Ignoring: $e") and next;
		my $action=$1;
		my $value=$4;
		($value >=0 and $value<=1 ) or $self->info("The value for $action has to be between 1 and 0") and next;
		$self->info("WE will put the $action to $value");
		$set->{"sedemote$action"}=$value;  		
	}
	if (keys(%$set)){
		$self->info("Updating the status of $sename");
		$self->{DATABASE}->update('SE', $set, "sename=?", {bind_values=>[$sename]});
		
	}
	
	return 1;
}


sub refreshSEDistance {
  my $self = shift;
  my $options =shift;
  my $sitename = shift;
	
	my @todo;
	if ($sitename){
		$self->info("Doing only the site '$sitename'");
		@todo=$sitename;		
	} else {
		my $e=$self->{DATABASE}->queryColumn("select distinct sitename from SEDistance");
		@todo=@$e;		
	}

  foreach my $site (@todo) {
  	my @selist;
#  	$self->{CONFIG}->{SEDETECTMONALISAURL}
#	    and @selist = $self->getListOfSEFromMonaLisa($site);

	if (!@selist) {
  	  $self->info("We couldn't get the info from ML. Putting all the ses");
      @selist = @{$self->{DATABASE}->query("select distinct seName, if(locate(?,sename)>1, 0, 0.5) distance  from SE", undef,
    		{bind_values=>[$site]})};
  	}
  	$site and (scalar(@selist) gt 0) or $self->info("No site or selist\n") and return 0;
	$self->info("Going to lock SE(r) and SEDistance(w)");
  	$self->{DATABASE}->lock("SE read, SEDistance");
  	$self->info("Locked SE(r) and SEDistance(w)");
  	eval{
  		$self->{DATABASE}->do("delete from SEDistance where sitename=?", {bind_values => [$site]});
  	};
  	$@ and $self->info("Could not delete from SEDistance: $@\n");
  	  	
  	foreach my $entry (@selist) {
    	eval {
    		$self->{DATABASE}->do(
      		"insert into SEDistance (sitename,seNumber,sitedistance)
        		      select ?, seNumber,  ?  from SE where upper( seName) LIKE upper(?)  ",
      		{bind_values => [ $site, $entry->{distance}, $entry->{seName} ]}
    		);
  		};
  		$@ and $self->info("Could not insert into SEDistance, reason $@\n");
  	}
  	$self->info("Going to unlock SE(r) and SEDistance(w)");
  	$self->{DATABASE}->unlock();
  	$self->info("Unlocked SE(r) and SEDistance(w)");
  }

  return 1;
}

sub getListOfSEFromMonaLisa {
  my $self = shift;
  my $site = shift;

  my $url = "$self->{CONFIG}->{SEDETECTMONALISAURL}?site=$site&dumpall=true";

  $self->info("Getting the list from $url");
  my @selist;
  my $monua = LWP::UserAgent->new();
  $monua->timeout(120);
  $monua->agent("AgentName/0.1 " . $monua->agent);
  my $monreq = HTTP::Request->new("GET" => $url);
  $monreq->header("Accept" => "text/html");
  my $monres    = $monua->request($monreq);
  my $monoutput = $monres->content;
  $monres->is_success() and push @selist, split(/\n/, $monoutput);
  $self->info("SE Rank Optimizer, MonAlisa replied for site $site with se list: @selist");
  return @selist;

}

sub getLDAP {
  my $self = shift;
  my $ldap;
  eval {
    $ldap = Net::LDAP->new($self->{CONFIG}->{LDAPHOST}) or die("Error contacting $self->{CONFIG}->{LDAPHOST}");
    $ldap->bind();
  };
  if ($@) {
    $self->info("Error connecting to ldap!: $@");
    return;
  }
  return $ldap;
}

sub checkFTDProtocol {
  my $self   = shift;
  my $entry  = shift;
  my $sename = shift;
  my $db     = shift;

  # $self->info("WHAT SHALL WE DO HERE????");
  my @protocols = $entry->get_value('ftdprotocol');
  foreach my $p (@protocols) {
    my ($name, $options) = split('\s+', $p, 2);
    $self->info("Inserting $name and $sename");
    my $info = {sename => $sename, protocol => $name};
    if ($options) {
      ($options =~ s/\s*transfers=(\d+)\s*//)
        and $info->{max_transfers} = $1;
      $options !~ /^\s*$/ and $info->{options} = $options;
    }
    $db->insertProtocol($info);
  }
  foreach my $p ($entry->get_value('deleteprotocol')) {
    my ($name, $options) = split('\s+', $p, 2);
    $db->insertProtocol(
      { sename         => $sename,
        protocol       => $name,
        deleteprotocol => 1
      }
    );

  }
  return 1;
}

sub resyncLDAPSE {
  my $self = shift;

  $self->info("Let's resync the SE and volumes from LDAP");
  my $ldap = $self->getLDAP() or return;

  my $transfers = AliEn::Database::Transfer->new({ROLE => 'admin'});
  if (!$transfers) {
    $self->info("Error getting the transfer database");
    $ldap->unbind();
    return;
  }
  $transfers->do("UPDATE PROTOCOLS set updated=0");

  my $mesg = $ldap->search(
    base   => $self->{CONFIG}->{LDAPDN},
    filter => "(objectClass=AliEnMSS)"
  );
  my $total = $mesg->count;
  $self->info("There are $total entries under AliEnMSS");

  my $db = $self->{DATABASE};

  my $new_SEs = {};

  foreach my $entry ($mesg->entries) {
    my $name = uc($entry->get_value("name"));
    my $dn   = $entry->dn();
    $dn =~ /ou=Services,ou=([^,]*),ou=Sites,o=([^,]*),/i
      or $self->info("Error getting the site name of '$dn'")
      and next;
    $dn =~ /disabled/
      and $self->debug(1, "Skipping '$dn' (it is disabled)")
      and next;
    my ($site, $vo) = ($1, $2);
    my $sename = "${vo}::${site}::$name";
    $self->info("Doing the SE $sename");
    $self->checkSEDescription($entry, $site, $name, $sename);
    $self->checkIODaemons($entry, $site, $name, $sename);
    $self->checkFTDProtocol($entry, $sename, $transfers);
    my @paths = $entry->get_value('savedir');

    my $info = $db->query("select * from SE_VOLUMES where seNumber in (select seNumber from SE where upper(sename)=upper(?))", undef, {bind_values => [$sename]});

    my @existingPath = @$info;
    my $t            = $entry->get_value("mss");
    my $host         = $entry->get_value("host");
    foreach my $path (@paths) {
      my $found = 0;
      my $size  = -1;
      $path =~ s/,(\d+)$// and $size = $1;
      $self->info("  Checking the path of $path");
      for my $e (@existingPath) {
        $e->{mountpoint} eq $path or next;
        $self->debug(1, "The path already existed");
        $e->{FOUND}         = 1;
        $found              = 1;
        $new_SEs->{$sename} = 1;
        ($size eq $e->{size}) and next;
        $self->info("**THE SIZE IS DIFFERENT ($size and $e->{size})");
        $db->do(
          "update SE_VOLUMES set " . $self->{DATABASE}->reservedWord("size") . "=? where mountpoint=? and seNumber in (select seNumber from SE where seName=?)",
          {bind_values => [ $size, $e->{mountpoint}, $sename ]});
      }
      $found and next;
      $self->info("**WE HAVE TO ADD THE SE '$path'");

      if (!$host) {
        $self->info("***The host didn't exist. Maybe we have to get it from the father??");
        my $base = $dn;
        $base =~ s/^[^,]*,(name=([^,]*),)/$1/;
        $self->info("looking in $base (and $2");
        my $mesg2 = $ldap->search(
          base   => $base,
          filter => "(&(name=$2)(objectClass=AliEnMSS))"
        );
        my @entry2 = $mesg2->entries();
        $host = $entry2[0]->get_value('host');

      }
      my $method = lc($t) . "://$host";

      $db->do(
        "insert into SE_VOLUMES(seNumber, volume,method, mountpoint, "
          . $db->reservedWord("size")
          . ") select seNumber,?,?,?,? from SE where seName like ?",
        {bind_values => [ $path, $method, $path, $size, $sename ]}
      );
      $new_SEs->{$sename} or $new_SEs->{$sename} = 0;
    }
    foreach my $oldEntry (@existingPath) {
      $oldEntry->{FOUND} and next;
      $self->info("**The path $oldEntry->{mountpoint} is not used anymore");
      $db->do(
        "update SE_VOLUMES set "
          . $self->{DATABASE}->reservedWord("size")
          . "=usedspace where mountpoint=? and seNumber in (select seNumber from SE where upper(seName)=upper(?))",
        {bind_values => [ $oldEntry->{mountpoint}, $sename ]}
      );
      $new_SEs->{$sename} = 1;
    }
  }

  foreach my $item (keys %$new_SEs) {
    $new_SEs->{$item} and next;
    $self->info("The se '$item' is new. We have to add it");
    my ($vo, $site, $name) = split(/::/, $item, 3);
    $self->addSE("-p", $site, $name);
  }
  $db->do("update SE_VOLUMES set usedspace=0 where usedspace is null");

  $db->do("update SE_VOLUMES set freespace='size-usedspace' where " . $db->reservedWord("size") . "<> -1");
  $db->do("update SE_VOLUMES set freespace=2000000000 where " . $db->reservedWord("size") . "=-1");

  $transfers->do("delete from PROTOCOLS where updated=0");
  $transfers->do("insert into PROTOCOLS(sename,max_transfers) values ('no_se',10)");

  $ldap->unbind();
  $transfers->close();
  return;
}

sub getBrokenLFN {
  my $self = shift;
  return $self->{DATABASE}->getBrokenLFN(@_);
}

sub checkSEDescription {
  my $self   = shift;
  my $entry  = shift;
  my $site   = shift;
  my $name   = shift;
  my $sename = shift;

  my $db = $self->{DATABASE};

  my $min_size = 0;
  foreach my $d ($entry->get_value('options')) {
    $self->info("Checking $d");
    $d =~ /min_size\s*=\s*(\d+)/ and $min_size = $1;
  }
  my $type             = $entry->get_value("mss");
  my @qos              = $entry->get_value("QoS");
  my @seExclusiveWrite = $entry->get_value("seExclusiveWrite");
  my @seExclusiveRead  = $entry->get_value("seExclusiveRead");
  my $seVersion        = $entry->get_value("seVersion") || "";

  my $qos = "";
  scalar(@qos) > 0 and $qos = "," . join(",", @qos) . ",";

  my $seExclusiveWrite = "";
  scalar(@seExclusiveWrite) > 0 and $seExclusiveWrite = "," . join(",", @seExclusiveWrite) . ",";
  my $seExclusiveRead = "";
  scalar(@seExclusiveRead) > 0 and $seExclusiveRead = "," . join(",", @seExclusiveRead) . ",";
  $self->info(
    "The se $sename has $min_size and $type and $qos and ex-write: $seExclusiveWrite and  ex-read: $seExclusiveRead");

  my $exists = $db->queryValue(
"select count(*) from SE where upper(sename)=upper(?) and seminsize=? and setype=? and seqos=? and seExclusiveWrite=? and seExclusiveRead=? and seVersion=?",
    undef,
    {bind_values => [ $sename, $min_size, $type, $qos, $seExclusiveWrite, $seExclusiveRead, $seVersion ]}
  );

  if (not $exists) {
    $self->info("We have to update the entry!!!");
    if($db->{DRIVER}=~/Oracle/i){
      my $e=$db->queryValue("SELECT COUNT(*) FROM SE WHERE UPPER(sename)=UPPER(?)", undef,{bind_values => [$sename]});
      if(not $e){
        $db->do("insert into SE  (sename,seminsize,setype,seqos) values (?,?,?,?)", {bind_values => [$sename,$min_size, $type, $qos]});
      }
    }

    $db->do(
"update SE set seminsize=?, setype=?, seqos=?, seExclusiveWrite=?, seExclusiveRead=? , seVersion=? where sename=?",
      {bind_values => [ $min_size, $type, $qos, $seExclusiveWrite, $seExclusiveRead, $seVersion, $sename ]}
    );
  }
  return 1;

}

sub checkIODaemons {
  my $self   = shift;
  my $entry  = shift;
  my $site   = shift;
  my $name   = shift;
  my $sename = shift;

  $self->info("Doing checkIODaemons for $sename");

  my (@io) = $entry->get_value('iodaemons');
  my ($proto, $host, $port, $olb_host) = split(/:/, join("", @io));

  $host =~ /host=([^:]+)(:.*)?$/i and $host = $1 or $self->info("Error getting the host name from $sename") and return;
  $port =~ /port=(\d+)/i or $self->info("Error getting the port for $sename") and return;
  $port = $1;
  $self->info("Using proto=$proto host=$host and port=$port for $sename");
  $proto =~ s/xrootd/root/i;

  my $path = $entry->get_value('savedir') or $self->info("Error getting the savedir from $sename") and return;

  $path =~ s/,.*$//;
  my $seioDaemons = "$proto://$host:$port";
  $self->debug(1, "And the update should $sename be: $seioDaemons, $path");
  my $e =
    $self->{DATABASE}
    ->query("SELECT sename,seioDaemons,sestoragepath from SE where upper(seName)=upper('$sename')");
  my $path2 = $path;
  $path2 =~ s/\/$// or $path2 .= "/";
  my $total =
    $self->{DATABASE}->queryValue(
"SELECT count(*) from SE where upper(seName)=upper('$sename') and seioDaemons='$seioDaemons' and ( seStoragePath='$path' or sestoragepath='$path2')"
    );

  if ($total < 1) {
    $self->info("***Updating the information of $site, $name ( $seioDaemons and $path )");
    $self->setSEio($site, $name, $seioDaemons, $path);
  }

  return 1;
}

sub checkLFN_HELP {
  return "checkLFN. Updates all the guids that are referenced by lfn
Usage:
       checkLFN [-force] [<table>] ";
}

sub checkLFN {
  my $self = shift;

  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can check the databse")
    and return;

  $self->info("Ready to check that the lfns are ok");

  return $self->{DATABASE}->checkLFN(@_);
}

# ***************************************************************
# Remove expired files from LFN_BOOKED and SE.
# ***************************************************************

sub removeExpiredFiles {
  my $self = shift;
  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: Only the administrator can remove entries from LFN_BOOKED")
    and return;
  $self->info("Removing expired entries from LFN_BOOKED");

  my $db = $self->{DATABASE};

  #Get files
  use Time::HiRes qw (time);
  my $currentTime = time();

  #delete directories
  $db->do("DELETE FROM LFN_BOOKED WHERE lfn LIKE '%/' ");
  my $files = $db->query(
        "SELECT expiretime, lfn, "
      . $db->reservedWord("size")
      . ", gownerId, binary2string(guid) as guid,pfn, Username as user"
      . " FROM LFN_BOOKED join USERS on ownerId=uId 
    WHERE expiretime<?", undef, {bind_values => [$currentTime]}
  );
  $files or return;

  foreach my $file (@$files) {
    my @pfns           = $self->cleanupGUIDCatalogue($db, $file);
    my $count          = $#pfns;
    my $physicalDelete = $self->physicalDeleteEntries($db, @pfns);

    $db->do("DELETE FROM LFN_BOOKED WHERE lfn=? and expiretime=?",
      {bind_values => [ $file->{lfn}, $file->{expiretime} ]}); 
    $self->{DATABASE}->fquota_update(-1 * $file->{size} * $count, -1 * $count, $file->{user});
    $self->info("$file->{lfn}($file->{guid}) was deleted($physicalDelete physical files) and quotas were rolled back ("
        . -1 * $file->{size} * $count . ", "
        . -1 * $count
        . ") times for $file->{user}");
  }
  $self->{DATABASE}->{VIRTUAL_ROLE} = "admin";
}

sub physicalDeleteEntries {
  my $self = shift;
  my $db   = shift;

  foreach my $data (@_) {
    my $pfn          = $data->{pfn};
    my $seNumber     = $data->{seNumber};
    my $seName       = $data->{seName};
    my @transfers    = split('/', $self->{CONFIG}->{TRANSFER_DATABASE});
    my $transferName = $transfers[2];
    $transferName =~ s/(.)*://i;
    my $list = $db->queryColumn(
      "SELECT protocol FROM $transferName.PROTOCOLS
              WHERE upper(sename)=upper(?) AND deleteprotocol=1", undef, {bind_values => [$seName]}
    );
    my @protocols = @$list;
    my $pD        = 0;
    @protocols or push @protocols, "rm";
    foreach my $protocol (@protocols) {
      my $protName = "AliEn::FTP::" . lc($protocol);
      unless ($self->{DELETE}->{lc($protocol)}) {
        eval "require $protName";
        $self->{DELETE}->{lc($protocol)} = $protName->new();
      }
      $self->{DELETE} or $self->info("Error creating the interface to $protocol") and last;
      if ($self->{DELETE}->{lc($protocol)}->delete($pfn)) {
        $self->info("File $pfn deleted!!");
        $pD = 1;
        last;
      }
    }
    if (!$pD) {
      $self->info("The file couldn't be deleted");
      $db->do("insert into DELETE_FAILED (pfn) values (?)", {bind_values => [$pfn]});
    }
  }
  return 1;
}

sub cleanupGUIDCatalogue {
  my $self           = shift;
  my $db             = shift;
  my $file           = shift;
  my $count          = 0;
  my @pfns           = ();
  my $physicalDelete = 0;

  my $guiddb = $self->{DATABASE};
  my $dbinfo = $self->{DATABASE}->getIndexTableFromGUID($file->{guid});
  $dbinfo or return;

  $guiddb or $self->info("Error connecting to the database") and return;
  $self->{DATABASE}->{VIRTUAL_ROLE} = "$file->{user}";
  $self->info("Deleting $file->{lfn} as $self->{DATABASE}->{VIRTUAL_ROLE}");
  if ($self->{DATABASE}->checkPermission("w", $file->{guid})) {
    $self->info("Have Permission on GUID");

    #Delete file
    my $table = $dbinfo;
    if ($file->{pfn} =~ m{\*}) {
      #Delete all the pfns of that file
      my $ref = $guiddb->query(
        "SELECT pfn, senumber
            FROM ${table}_PFN  join  $table g using (guidId)
            WHERE guid=string2binary(?)",
        undef, {bind_values => [ $file->{guid} ]}
      );
      $ref and @pfns = @$ref;

      $self->info("Removing expired entries from $table _PFN");
      $self->info($file->{guid});
      $guiddb->do("delete from  ${table}_PFN where guidid in (select guidid from  $table where guid=string2binary(?))",
        {bind_values => [ $file->{guid} ]});
      $self->info("Removing expired entries from $table ");
      $guiddb->do("delete from $table  where guid=string2binary(?)", {bind_values => [ $file->{guid} ]});

    } else {
      #Just delete the entry...
      @pfns = {pfn => $file->{pfn}, senumber => $file->{senumber}};
      $self->info("Just delete the entry...");
      $guiddb->do(
        "delete from g using $table join ${table}_PFN g using (guidid ) where guid=string2binary(?) and pfn=?",
        {bind_values => [ $file->{guid}, $file->{pfn} ]});
    }
  }
  return @pfns;
}

sub checkOrphanGUID {
  my $self = shift;

  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can check the databse")
    and return;

  $self->info("And now, let's see if there are any orphan guids");
  return $self->{DATABASE}->checkOrphanGUID(@_);
}

sub optimizeGUIDtables {
  my $self = shift;

  $self->info("Let's optimize the guid tables");
  return $self->{DATABASE}->optimizeGUIDtables(@_);
}

sub masterSE_list {
  my $self   = shift;
  my $sename = shift;
  $self->info("Counting the number of entries in $sename");
  my $info= $self->{DATABASE}->masterSE_list($sename, @_) or return;
  $self->info(
      "The SE $sename has:
  $info->{referenced} entries in the catalogue.
  $info->{replicated} of those entries are replicated
  $info->{broken} entries not pointed by any LFN"
    );
  if ($info->{guids}) {
    $self->info("And the guids are:" . Dumper($info->{guids}));
  }

  return $info;
  
}

sub masterSE_getFiles {
  my $self = shift;
  return $self->{DATABASE}->masterSE_getFiles(@_);
}

sub calculateFileQuota {
  my $self   = shift;
  my $silent = shift;

  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can check the database")
    and return;

  my $method = "info";
  my @data;
  $silent and $method = "debug" and push @data, 1;

  $self->$method(@data, "Calculate File Quota");
  my $lfndb = $self->{DATABASE};

  my $calculate = 0;
  my $rtables   = $lfndb->getAllLFNTables();

  foreach my $h (@$rtables) {
    my $LTableIdx = $h->{tableName};
    my $LTableName = "L${LTableIdx}L";

    #check if all tables exist for $LTableIdx
    $lfndb->checkLFNTable("${LTableIdx}");

    $self->$method(@data, "Checking if the table ${LTableName} is up to date");
    $lfndb->queryValue(
"select 1 from (select max(ctime) ctime, count(1) counter from $LTableName) a left join LL_ACTIONS on tableName=? and action='QUOTA' where extra is null or extra<>counter or time is null or time<ctime",
      undef,
      {bind_values => [$LTableIdx]}
    ) or next;

    $self->$method(@data, "Updating the table ${LTableName}");
    $lfndb->do("delete from LL_ACTIONS where action='QUOTA' and tableName=?", {bind_values => [$LTableIdx]});
    $lfndb->do(
"insert into LL_ACTIONS(tableName, time, action, extra) select ?, max(ctime), 'QUOTA', count(1) from $LTableName",
      {bind_values => [$LTableIdx]}
    );

    my %sizeInfo;
    $lfndb->do("delete from ${LTableName}_QUOTA");

    my $fquotaL = $lfndb->query("select USERS.uId as \"user\", count(l.lfn) as nbFiles, sum(l."
        . $lfndb->reservedWord("size")
        . ") as totSize from ${LTableName} l 
        JOIN USERS ON l.ownerId=USERS.uId 
        where l.type='f' group by l.ownerId order by l.ownerId");

    if ( scalar(@$fquotaL) ){
	    my $quotaInsert = "insert into ${LTableName}_QUOTA ("
	        . "userId, nbFiles, totalSize) values ";
	        
	    foreach my $u (@$fquotaL) {
	    	$quotaInsert .= "($u->{user},$u->{nbFiles},$u->{totSize}),";
	    }
	    $quotaInsert =~ s/,$//;
	        
	    $lfndb->do("$quotaInsert");
    }

    $calculate = 1;
  }

  $calculate or $self->$method(@data, "No need to calculate") and return;

  my %infoLFN;
  foreach my $h (@$rtables) {
    my $tableIdx = $h->{tableName};
    my $tableName = "L${tableIdx}L";
    $self->$method(@data, "Getting from Table ${tableName}_QUOTA ");
    my $userinfo = $lfndb->query("select userId, nbFiles, totalSize from ${tableName}_QUOTA");
    foreach my $u (@$userinfo) {
      my $userid = $u->{userId};
      if (exists $infoLFN{$userid}) {
        $infoLFN{$userid} = {
          nbfiles   => $infoLFN{$userid}{nbfiles} + $u->{nbFiles},
          totalsize => $infoLFN{$userid}{totalsize} + $u->{totalSize}
        };
      } else {
        $infoLFN{$userid} = {
          nbfiles   => $u->{nbFiles},
          totalsize => $u->{totalSize}
        };
      }
    }
  }

  $self->$method(@data, "Updating FQUOTAS table");
  $self->{DATABASE}->lock("FQUOTAS write, USERS");
  $self->{DATABASE}
    ->do("update FQUOTAS set nbFiles=0, totalSize=0, tmpIncreasedNbFiles=0, tmpIncreasedTotalSize=0")
    or $self->$method(@data, "initialization failure for all users");
  foreach my $user (keys %infoLFN) {
    $self->{DATABASE}->do(
      "update FQUOTAS set nbFiles=?, totalSize=? where userid=?", {bind_values=>[ $infoLFN{$user}{nbfiles}, $infoLFN{$user}{totalsize},$user]})
      or $self->$method(@data, "update failure for user $user");
  }
  $self->{DATABASE}->unlock();
}

return 1;
