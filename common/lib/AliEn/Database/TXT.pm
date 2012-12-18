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
package AliEn::Database::TXT;

use strict;
use DBI;

use AliEn::Config;
use AliEn::Logger;
use AliEn::Database;
use LockFile::Simple;
use vars qw(@ISA);

use DBD::SQLite;

@ISA=('AliEn::Database');

sub new {
  my $proto = shift;
  my $self  = (shift or {} );
  $self->{HOST}="localhost";
  $self->{DB}="TXT";
  $self->{DRIVER}="SQLite333";
  $self->{TABLES}={};

  $self->{CONFIG}=new AliEn::Config();
  $self->{DIRECTORY}=$self->{CONFIG}->{TMP_DIR};

  
  $self=AliEn::Database::new($proto, $self, @_);
  $self or return;
  $self->{DBH}->{'RaiseError'} = 1;
  
  return $self;
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
    $self->{LOGGER}->info("TXT", "Error connecting to the Database TXT") and
      return;
  map {$self->createTable($_) or return;} keys %{$self->{TABLES}};
  return $self->SUPER::initialize();

}
sub describeTable {
  my $self = shift;
  my $table = shift;

  undef;
}

sub getDatabaseDSN{
  my $self=shift;
  $self->debug(1, "Returning the dsn of a text database");
  return "DBI:SQLite:dbname=$self->{DIRECTORY}/file.mss";
}

sub createTable{
  my $self=shift;
  #my $table=lc shift;
  my $table= shift;


  $self->debug(1, "Checking table $table");

  #my $file="$self->{DIRECTORY}/".lc($table);
  my $description=$self->{TABLES}->{$table};

#  if ( !( -e $file ) ) {

    # Create the table again.
    $self->debug(1, "Creating CSV-table $table " );
    
  $self->{DBH}->do("CREATE TABLE if not exists $table ($description)")
      or $self->{LOGGER}->error( "TXT",
				 "Cannot create table $table ($description). Error: " . $self->{DBH}->errstr() )
	and return;

#    chmod 0770, $file;
#  }
  return 1;

}

sub lock {
  my $self=shift;
  my $table=shift;
  $self->info("Ready to lock $table");
  LockFile::Simple::lock("$self->{DIRECTORY}/$table.lck");

  return 1;
}

sub unlock {
  my $self=shift;
  my $table=shift;
  LockFile::Simple::unlock("$self->{DIRECTORY}/$table.lck");

  return 1;
}

1;

