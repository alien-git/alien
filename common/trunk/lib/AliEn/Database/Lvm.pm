#/**************************************************************************
# * Copyright(c) 2001-2003, ALICE Experiment at CERN, All rights reserved. *
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

package AliEn::Database::Lvm;

use AliEn::Database;

use strict;

use vars qw(@ISA);
@ISA=("AliEn::Database");

sub initialize {
  my $self     = shift;

  $self->SUPER::initialize() or return;

  return 1;
}


sub setLvmTable{
  my $self = shift;
  $self->{FILETABLE} = (shift or "FILES");
  $self->{VOLUMETABLE} = (shift or "VOLUMES");
}

sub checkVolumeTable{
  my $self = shift;
  defined $self->{VOLUMETABLE} or $self->{VOLUMETABLE} = (shift or "VOLUMES");

  my %columns = (
      'volume'     => "varchar(255) not null",
      'mountpoint' => "char(255)",
      'size'       => "int(20)",
      'freespace'  => "int(20)",
      'usedspace'  => "int(20)",
                );

  $self->checkTable($self->{VOLUMETABLE}, "volume", \%columns, 'volume');
}



sub checkFileTable{
  my $self = shift;
  defined $self->{FILETABLE} or $self->{FILETABLE} = (shift or "FILES");

  my %columns = (	file=>"varchar(255) not null",
			pfn  =>"varchar(255)",
	                volume =>"varchar(255)",
	                expires =>"int",
			guid =>"varchar(40)",
			size =>"int(20)",
	                ttl  =>"int",
		);

  $self->checkTable($self->{FILETABLE}, "file", \%columns, 'file');
}


#FILES database
sub updateFile{
    my $self = shift;
    $self->update("$self->{FILETABLE}",@_);
}

sub deleteFromFile{
    my $self = shift;
    $self->delete("$self->{FILETABLE}",@_);
}

sub getFieldsFromFileEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$self->debug(1,"In getFieldsFromFileEx fetching attributes $attr with condition $addsql from table $self->{FILETABLE}");
	$self->query("SELECT $attr FROM $self->{FILETABLE} $addsql");
}

sub getFieldFromFileEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$self->debug(1,"In getFieldFromFileEx fetching attributes $attr with condition $addsql from table $self->{FILETABLE}");
	$self->queryColumn("SELECT $attr FROM $self->{FILETABLE} $addsql");
}

#VOLUMES database

sub updateVolume{
    my $self = shift;
    $self->update("$self->{VOLUMETABLE}",@_);
}

sub deleteFromVolume{
    my $self = shift;
    $self->delete("$self->{VOLUMETABLE}",@_);
}

sub getFieldsFromVolumeEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$self->debug(1,"In getFieldsFromVolumeEx fetching attributes $attr with condition $addsql from table $self->{VOLUMETABLE}");
	$self->query("SELECT $attr FROM $self->{VOLUMETABLE} $addsql");
}

sub getFieldFromVolumeEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$self->debug(1,"In getFieldFromVolumeEx fetching attributes $attr with condition $addsql from table $self->{VOLUMETABLE}");
	$self->queryColumn("SELECT $attr FROM $self->{VOLUMETABLE} $addsql");
}

