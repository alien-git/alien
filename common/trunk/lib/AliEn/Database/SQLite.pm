#/**************************************************************************
# * Copyright(c) 2001-2002, ALICE Experiment at CERN, All rights reserved. *
# *                                                                        *
# * Author: The ALICE Off-line Project / AliEn Team                        *
# * Contributors are mentioned in the code where appropriate.              *
# *                                                                        *
# * Permission to use, copy, modify and distribute this software and its   *
# * documentation strictly for non-commercial purposes is hereby granted   *
# * without fee, provided that the above copyright notice appears in all   *
# * copies and that both the copyright notice and this permission notice   *
# * appear in the supporting documentation. The authors make no claims     *
# * about the suitability of this software for any purpose. It is          *
# * provided "as is" without express or implied warranty.                  *
# **************************************************************************/
package AliEn::Database::SQLite;

use strict;
use DBI;

use AliEn::Config;
use AliEn::Logger;
use AliEn::Database;

use vars qw(@ISA);

@ISA=('AliEn::Database');

sub new {
  my $proto = shift;
  my $self  = (shift or {} );
  $self->{HOST}="localhost";
  $self->{DB}="SQL";
  $self->{DRIVER}="SQLite";
  $self->{TABLES}={};

  $self->{CONFIG}=new AliEn::Config();
  $self->{LOGGER}=new AliEn::Logger();
  $self->{DIRECTORY}=$self->{CONFIG}->{TMP_DIR};
  $self->debug(1, "Using the SQLite database in $self->{DIRECTORY}");
  return AliEn::Database::new($proto, $self, @_);
}

sub initialize {
  my $self=shift;

  if ( !( -d $self->{DIRECTORY} ) ) {
    $self->debug(1, "Creating directory $self->{DIRECTORY}");
    my $dir = "";
    foreach ( split ( "/", $self->{DIRECTORY} ) ) {
      $dir .= "/$_";
      mkdir $dir, 0777;
      }
  }

  $self->reconnect() or 
    $self->{LOGGER}->info("SQLite", "Error connecting to the Database TXT") and
      return;
  map {$self->createTable($_) or return;} keys %{$self->{TABLES}};

  return $self->SUPER::initialize();

}
sub _do {
  my $self=shift;
  my $query=shift;
  ($query) or return $self->SUPER::_do($query, @_);
  $self->debug(1, "In SQLite, trying to do $query");
  $query =~ s/IF +NOT +EXISTS//i or return $self->SUPER::_do($query, @_);

  $self->debug(1, "Still in sqlite...");

 #it is a query to create a table...
  $self->{LOGGER}->silentOn();
  my $done=$self->SUPER::_do($query, @_);
  $self->{LOGGER}->silentOff();
  $self->debug(1, "Back in SQLite with ". $self->{LOGGER}->error_msg());
  $done and return $done;

  #if there was an error, let's check if it was that the table already existed
  if ($self->{LOGGER}->error_msg() =~ /table \S+ already exists/i) {
    $self->{LOGGER}->set_error_msg();
    return 1;
  }
  $self->{LOGGER}->info("SQLite", "There was an SQL error: ".$self->{LOGGER}->error_msg());
  return;
}

sub describeTable {
  my $self = shift;
  my $table = shift;

  undef;
}

sub getDatabaseDSN{
  my $self=shift;
  $self->debug(1, "Returning the dsn of a SQLite database");
  return "DBI:SQLite:dbname=$self->{DIRECTORY}/SQLite.db";
}

sub createTable{
  my $self=shift;
  my $table=shift;

  $self->debug(1, "Checking table $table");

  my $file="$self->{DIRECTORY}/$table";
  my $description=$self->{TABLES}->{$table};

  if ( !( -e $file ) ) {

    # Create the table again.
    $self->debug(1, "Creating CSV-table $table" );
    
    $self->{DBH}->do("CREATE TABLE $table ($description)")
      or $self->{LOGGER}->error( "SQLite",
				 "Cannot create table $table ($description). Error: " . $self->{DBH}->errstr() )
	and return;

    chmod 0777, $file;
  }
  return 1;

}


1;

