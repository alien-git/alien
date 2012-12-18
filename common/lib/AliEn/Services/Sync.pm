package AliEn::Services::Sync;

use strict;
use AliEn::Catalogue;

sub updateDatabase {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = {};

    my $database = shift;

    if ( !$database ) {
        print STDERR
"Error in updateDatabase. Not enough arguments\nUsage updateDatabase <database>\n";
        return;
    }

    $self->{SOURCEDB}     = $database->{DB};
    $self->{SOURCEHOST}   = $database->{HOST};
    $self->{SOURCEDRIVER} = $database->{DRIVER};

    $self->{NOPROXY} = 0;    #($database->{noproxy} or 0);

    $self->{DATABASE} = $database;

    #CONTACTING THE DATABASE WITHOUT GOING THROUGH THE PROXY
    if ( $self->{NOPROXY} ) {
        my $dsn =
"dbi:$database->{DRIVER}:database=$self->{DATABASE}->{DB};host=$self->{DATABASE}->{HOST}";
        print "Conecting to $dsn\n";

        #	$self->{DATABASE}->{DBH} and
        #	$self->{DATABASE}->{DBH}->disconnect();
        #	$self->{DATABASE}->{DBH}=DBI->connect($dsn, $self->{DATABASE}->{USER}, $self->{DATABASE}->{PASSWD},  {PrintError => 0}) or print "Error Updating\n" and return;
    }

    $self->{DEBUG} = $database->{DEBUG};

    ( $self->{DEBUG} > 0 )
      and print
"DEBUG LEVEL 1\t\tStarting update of $self->{SOURCEDB} in $self->{SOURCEHOST}!!\n";

    my @targets =
      $self->{DATABASE}->query(
"SELECT hostIndex, address, db, driver, lastUpdate, lastDelete, lastTagUpdate, lastTagDelete  from HOSTS where address<>'$self->{SOURCEHOST}' or db<>'$self->{SOURCEDB}'"
      );

    my $target;
    bless( $self, $class );

    foreach $target (@targets) {
        $self->updateHost($target);
    }
    print "Deleting entries from the DELETED table ";

    my ($minDel) =
      $self->{DATABASE}->query(
"SELECT min(lastDelete)  from HOSTS where address<>'$self->{SOURCEHOST}' or db<>'$self->{SOURCEDB}'"
      );
    print " (smaller than $minDel)...";
    $self->{DATABASE}->insert("DELETE FROM DELETED where entryId<$minDel");
    print "ok!\n";
    ( $self->{DEBUG} > 0 ) and print "DEBUG LEVEL 1\t\tUpdate done!!\n";

    return 1;
}

sub updateHost {
    my $self = shift;
    my $link = shift;

    ($link) or print STDERR "ERROR not enough arguments\n" and return;
    my ( $hostIndex, $host, $db, $driver, $lastUpdate, $lastDelete,
        $lastTagUpdate, $lastTagDelete )
      = split "###", $link;
    ($lastDelete)    or ( $lastDelete    = 0 );
    ($lastTagDelete) or ( $lastTagDelete = 0 );
    ($lastTagUpdate) or ( $lastTagUpdate = 0 );

    $self->{HOSTINDEX} = $hostIndex;

    ( $self->{DEBUG} > 0 )
      and print "DEBUG LEVEL 1\t\tUpdating $db in $host!!\n";

    $self->{TARGET} = AliEn::Database->new(
        {
            "DB"     => "$db",
            "HOST"   => "$host",
            "DRIVER" => "$driver",
            "ROLE"   => $self->{DATABASE}->{ROLE}
        }
    );
    $self->{TARGET}
      or print "ERROR GETTING THE DATABASE $db at $host\n"
      and exit;
    $self->{TARGET}->validate()
      or print "ERROR validating $db at $host\n"
      and exit;

    print "SO FAR SO GOOD\n";

    #User($self->{DATABASE}->{USER},
    #				  $self->{DATABASE}->{PASSWD});

    if ( $self->{NOPROXY} ) {

        #CONTACTING THE DATABASE WITHOUT GOING THROUGH THE PROXY
        #	my $dsn="dbi:$self->{TARGET}->{DRIVER}:database=$self->{TARGET}->{DB};host=$self->{TARGET}->{HOST}";
        #	
        #	$self->{TARGET}->{DBH}->disconnect();
        #	$self->{TARGET}->{DBH}=DBI->connect($dsn, $self->{TARGET}->{USER},$self->{TARGET}->{PASSWD},  {PrintError => 0});
    }

    ( $self->{TARGET} )
      or print STDERR "ERROR: GETTING DATABASE $db $host \n"
      and return;

    ( $self->{DEBUG} > 0 )
      and print
      "DEBUG LEVEL 1\t\tUpdating $db in $host  $lastUpdate, $lastDelete!!\n";

    $self->UpdateTable( "D0", "path, dir, hostIndex",
        "DELETED", "lastUpdate", $lastUpdate, "lastDelete", $lastDelete );
    $self->UpdateTable(
        "TAG0",         "path, tagName", "TAGDELETED", "lastTagUpdate",
        $lastTagUpdate, "lastTagDelete", $lastTagDelete
    );
    $self->{TARGET}->destroy;
}

sub UpdateTable {
    my $self = shift;

    $self->{TABLE}        = shift;
    $self->{TABLECOLUMNS} = shift;

    $self->{DELTABLE} = shift;

    $self->{COLLASTUPDATE} = shift;
    my $lastUpdate = shift;
    $self->{COLLASTDELETE} = shift;
    my $lastDelete = shift;

    print
"Updating table $self->{TABLE} ($self->{TABLECOLUMNS}), using  $self->{DELTABLE} and $self->{COLLASTUPDATE}=$lastUpdate and $self->{COLLASTDELETE}=$lastDelete\n";

    my ($data) =
      $self->{TARGET}->query(
"SELECT $self->{COLLASTUPDATE}, $self->{COLLASTDELETE}  from HOSTS where address='$self->{SOURCEHOST}' and db='$self->{SOURCEDB}'"
      );

    my ( $targetLastUpdate, $targetLastDelete ) = split "###", $data;

    ($targetLastDelete) or ( $targetLastDelete = 0 );

    my $repeat = 1;

    while ($repeat) {
        my $update =
          $self->getNewFiles( $self->{DATABASE}, $lastUpdate, $lastDelete );

        $self->updateEntries( $self->{TARGET}, $update );

        ( $self->{DEBUG} > 1 )
          and print
"\nFirst half!!!\nSELECT $self->{COLLASTUPDATE}, $self->{COLLASTDELETE}  from HOSTS where address='$self->{SOURCEHOST}' and db='$self->{SOURCEDB}'\n";

        $self->{DATABASE}
          ->insert( $update->{update} . " where hostIndex=$self->{HOSTINDEX}" );
        ( $lastUpdate, $lastDelete ) =
          ( $update->{newLastUpdate}, $update->{newLastDelete} );

        my $update2 =
          $self->getNewFiles( $self->{TARGET}, $targetLastUpdate,
            $targetLastDelete );

        $self->updateEntries( $self->{DATABASE}, $update2 );

        ( $targetLastUpdate, $targetLastDelete ) =
          ( $update2->{newLastUpdate}, $update2->{newLastDelete} );

        my $query =
          $update2->{update}
          . " where address='$self->{SOURCEHOST}' and db='$self->{SOURCEDB}'";

        $self->{TARGET}->insert($query);

        ( $self->{DEBUG} > 1 ) and print "\nDONE!!!\n";

        $repeat = $update->{repeat} + $update2->{repeat};
    }

    ( $self->{DEBUG} > 1 ) and print "Done!!....\n";
}

sub updateEntries {
    my $self     = shift;
    my $database = shift;
    my $update   = shift;

    if ( $update->{delete} ) {
        ( $self->{DEBUG} > 1 ) and print "Deleting \n";
        ( $self->{DEBUG} > 2 ) and print $update->{delete} . "\n";

        #	$self->updateCommand($database, "","","DELETE FROM $self->{TABLE} where ", " or ", split ("###", $update->{delete}));
        $self->updateCommand( $database, "DELETE FROM $self->{TABLE} where ",
            " or ", split ( "###", $update->{delete} ) );

        if ( $update->{insertDelete} ) {

            #	    $self->updateCommand($database, "","","INSERT IGNORE INTO $self->{DELTABLE} (path, firsttime) VALUES ", ", ", split ("###", $update->{insertDelete}));
            $self->updateCommand(
                $database,
"INSERT IGNORE INTO $self->{DELTABLE} (path, firsttime) VALUES ",
                ", ",
                split ( "###", $update->{insertDelete} )
            );
        }
    }

    if ( $update->{newEntries} ) {

        ( $self->{DEBUG} > 1 ) and print "\nInserting";
        ( $self->{DEBUG} > 2 ) and print $update->{newEntries} . "\n";

        #	$self->updateCommand($database, "","","INSERT IGNORE INTO $self->{TABLE} ($self->{TABLECOLUMNS}) VALUES ", ", ", split ("###", $update->{newEntries}));	
        $self->updateCommand(
            $database,
            "INSERT IGNORE INTO $self->{TABLE} ($self->{TABLECOLUMNS}) VALUES ",
            ", ",
            split ( "###", $update->{newEntries} )
        );
    }

}

sub updateCommand {
    my $self     = shift;
    my $database = shift;

    #    my $lmask=shift;
    #    my $rmask=shift;
    my $beginning = shift;
    my $join      = shift;
    my @dir       = @_;

    @dir or return;

    #    @dir=grep {s/^(.*)$/$lmask$1$rmask/} @dir; 
    my $command = "$beginning" . join "$join", @dir;

    ( $self->{DEBUG} > 3 ) and print "HACIENDO $command\n";
    $database->insert("$command");

    ($DBI::errstr) and print "Error in Update: $DBI::errstr\n$command\n";

    ( $self->{DEBUG} > 1 ) and print ".";

    return 1;
}

sub getNewFiles {
    my $self = shift;

    my $database   = shift;
    my $lastUpdate = shift;
    my $lastDelete = shift;

    my $limit  = 500;
    my $repeat = 0;

    my @directories =
      $database->query(
"SELECT entryId, $self->{TABLECOLUMNS} from $self->{TABLE} where entryId>$lastUpdate order by entryId limit $limit "
      );

    my ($newLastUpdate) =
      $database->query("SELECT MAX(entryId) from $self->{TABLE}");
    if ( $#directories eq $limit - 1 ) {
        $repeat = 1;
        my ( $entryId, $path, $dir, $index ) = split "###",
          $directories[$#directories];
        $newLastUpdate = $entryId;
        ( $self->{DEBUG} > 1 )
          and print
"Updated entries in $database->{HOST} $database->{DB} $newLastUpdate\n";
    }

    my ($directory);
    my @new = ();
    foreach $directory (@directories) {
        my ( $entryId, @list ) = split "###", $directory;
        @new = ( @new, "('" . join ( "', '", @list ) . "') " );
    }

    @directories =
      $database->query(
"SELECT path,entryId, firsttime from $self->{DELTABLE} where entryId>$lastDelete limit $limit "
      );

    my ($newLastDelete) =
      $database->query("SELECT MAX(entryId) from $self->{DELTABLE}");
    ($newLastDelete) or $newLastDelete = 0;
    my @deleted       = ();
    my @insertDeleted = ();
    foreach $directory (@directories) {
        my ( $path, $entryId, $firsttime ) = split "###", $directory;
        @deleted = ( @deleted, "path='$path' " );
        ($firsttime) and @insertDeleted = ( @insertDeleted, "('$path', 0) " );
    }

    if ( $#directories eq $limit - 1 ) {
        $repeat = 1;
        my ( $path, $entryId ) = split "###", $directories[$#directories];
        $newLastDelete = $entryId;
        ( $self->{DEBUG} > 1 ) and print "Updated entries $newLastDelete\n";
    }

    return {
        "update" =>
"UPDATE HOSTS set $self->{COLLASTUPDATE}=$newLastUpdate , $self->{COLLASTDELETE}=$newLastDelete",
        "newLastUpdate" => "$newLastUpdate",
        "newLastDelete" => "$newLastDelete",
        "newEntries"    => join ( "###", @new ),
        "delete"        => join ( "###", @deleted ),
        "insertDelete"  => join ( "###", @insertDeleted ),
        "repeat"        => $repeat,
    };
}

return 1;

