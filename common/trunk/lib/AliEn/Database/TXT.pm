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

use vars qw(@ISA);

@ISA=('AliEn::Database');

sub new {
  my $proto = shift;
  my $self  = (shift or {} );
  $self->{HOST}="localhost";
  $self->{DB}="TXT";
  $self->{DRIVER}="CSV";
  $self->{TABLES}={};

  $self->{CONFIG}=new AliEn::Config();
  $self->{DIRECTORY}=$self->{CONFIG}->{TMP_DIR};

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
  return "DBI:CSV:f_dir=$self->{DIRECTORY}";
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
      or $self->{LOGGER}->error( "TXT",
				 "Cannot create table $table ($description). Error: " . $self->{DBH}->errstr() )
	and return;

    chmod 0777, $file;
  }
  return 1;

}

1;

