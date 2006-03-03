package AliEn::Catalogue::Server;

use AliEn::Catalogue;
@ISA = ('AliEn::Catalogue');

use strict;

sub createRemoteTable {
    my $self   = shift;
    my $host   = shift;
    my $db     = shift;
    my $driver = shift;
    my $user   = shift;
    my $table  = shift;

    my $date = localtime;
    $date =~ s/^\S+\s(.*):[^:]*$/$1/	;
    print STDERR "\n\n$date New table $table created by $user in $db $host\n";

    $self->{DATABASE}->reconnect( $host, $db, $driver );
    $self->{LOGGER}->info( "CatalogDaemon","Checking the name of the table" );
    if ($table=~ /^T$/) {
      #We have to get a new table name;
      my $dir=$self->{DATABASE}->getNewDirIndex();
      $dir or return (-1, "Error getting a new table name");
      $table="T$dir";
    }

    #    $dbh->{DATABASE}->reconnect($host, $db, $driver);
    $self->{LOGGER}->info( "CatalogDaemon", "Creating the table $table" );

    my $definition = "(type   CHAR(4),
                   dir int(8),name  CHAR(64), owner CHAR(8),ctime CHAR(16),
                   comment CHAR(80) NOT NULL DEFAULT \"\",pfn CHAR(255) NOT NULL DEFAULT \"\", se char(100), gowner char(8), size int)";

	$self->{DATABASE}->createTable($table, $definition)
      or $self->{LOGGER}->error( "CatalogDaemon","Error creating table $table" )
	  and return;
	  
	$self->debug(1,"Table $table created" );

    $self->{DATABASE}->grantAllPrivilegesToUser($user, $db, $table)
	  or $self->{LOGGER}->error( "CatalogDaemon","Error granting privileges to user $user on $table" )
      and return;

    print STDERR "$date\t\tPrivileges  changed for $user\n";
    return $table;
}

sub changePriv {
    my $self = shift;

    my $host    = shift;
    my $db      = shift;
    my $driver  = shift;
    my $oldUser = shift;
    my $newUser = shift;
    my $table   = shift;

    $self->{DATABASE}->revokeAllPrivilegesFromUser($oldUser, $db, "T$table");
    $self->{DATABASE}->grantAllPrivilegesToUser($newUser, $db, "T$table");
}

return 1;
