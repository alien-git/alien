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

package AliEn::Catalogue::Basic;
use vars qw($DEBUG);

$DEBUG=0;
use strict;
#
#  Given a path, and an operation (r,w,x), checks if allowed.
#
# options: hash  
#               If RETURN_HASH is set, it will return all the info
#               of the selected entry, instead or returning only the lfn
sub checkPermissions {
  my $self      = shift;
  my $operation = shift;
  my $file      = shift;
  my $silent    = ( shift or $self->{SILENT} );
  my $options   =(shift or {});

  my $mode="info";
  $silent and $mode="debug";

  $DEBUG and $self->debug(1, "Checking the permission $operation on  $file" );

  $file = $self->GetAbsolutePath($file,2);

  $self->selectDatabase($file) or return;

  my $realfile = $self->getVOPath($file);

  my $isfile=0;
  my $basename=".";
  my $temp=$realfile;
  $temp=~ s{(.)/$}{$1};

  my $parentdir=$self->f_dirname($temp);
  my $dbOptions={retrieve=>"lfn,perm,owner,gowner"};

  $options->{RETURN_HASH} and $dbOptions={};

  my $entries=$self->{DATABASE}->getAllInfoFromLFN($dbOptions, $temp,
						   "$temp/", $parentdir)
      or $self->{LOGGER}->info("Basic", "Error looking for $realfile") and
	return;
  my @entries=@{$entries};
  $DEBUG and $self->debug(1, "There are $#entries +1 with that pattern");
  my $entry=shift @entries;
  $entry or $self->{LOGGER}->info("Basic", "Entry $realfile does not exist in the catalogue") and return;

  foreach my $test (@entries){
    length($test->{lfn})>length($entry->{lfn}) and $entry=$test;
  }

  my ($lfn, $perm, $owner, $gowner)=($entry->{lfn}, $entry->{perm}, $entry->{owner}, $entry->{gowner});

  ($perm) or print STDERR "Error selecting permissions of $lfn" and return;
  my $returnValue=$entry;
  $options->{RETURN_HASH} or  $returnValue=$entry->{lfn};

  my $role= $options->{ROLE} || $self->{ROLE};

  if ( $role =~  /^admin(ssl)?$/ ) {
    #admin has superuseracces.
    return $returnValue;
  }

  $DEBUG and $self->debug(1, "Checking the file $lfn");
  my $subperm;
  if ( $role eq $owner ) {
    $DEBUG and $self->debug(1, "Checking ownership" );
    $subperm=substr( $perm, 0, 1 );
  } elsif ($self->{DATABASE}->checkUserGroup($role, $gowner)){
    $DEBUG and $self->debug(1, "Checking same group" );
    $subperm=substr( $perm, 1, 1 );
  }
  else {
    $DEBUG and $self->debug(1, "Checking rest of the world" );
    $subperm=substr( $perm, 2, 1 );
  }
  $self->checkOnePerm( $operation, $subperm) and return $returnValue;

  $self->info("You don't have enough privileges on $file", 10003);
  return 0;
}

sub checkOnePerm {
  my $self      = shift;
  my $operation = shift;
  my $perm      = shift;

  if ( $operation eq 'r' ) {
    $perm>3 and return 1;
  }elsif ( $operation eq 'w' ) {
    ($perm%4)>1 and return 1;
  } elsif ( $operation eq 'x' ) {
    ($perm%2) and return 1;
  }
  return;

}


#
#This subroutine receives a directory, and connects to the
#database that has that directory.
#
sub selectDatabase {
  my $self = shift;
  $DEBUG and $self->debug(1, "SelectDatabase start @_");

  my $path = shift;

  if ( !$path ) {
    print STDERR "Error in selectDatabase: Not enough arguments\n";
    return;
  }

  if ( $self->{MOUNT} and ($path !~ /^$self->{MOUNT}/ )) {
    $self->info( "We are going out of another VO");
    $self->{MOUNT}="";
    $self->{DATABASE}=$self->{"DATABASE_FIRST"};
    $self->{CONFIG}=$self->{CONFIG}->Reload({organisation=>$self->{FIRSTORG}});
    $self->{CONFIG} or $self->info("Error getting the new configuration") and return;

    return $self->selectDatabase($path);
  }

  my $real_path=$self->getVOPath($path);
  return $self->{DATABASE}->selectDatabase($real_path);
}

#
# Given the path, returns the index of the table
#
#sub GetDirIdx {
#  my $self = shift;
#  my $path = shift;
#  $path or return;
#  if (@_) {
#    printf STDERR "Too many arguments in f_dir!!\n";
#    return;
#  }
#
#  $path = $self->getVOPath($path);
#
#  return $self->{DATABASE}->getFieldFromD0($path,"dir");
#}

#sub f_dir {
#  return shift->GetDirIdx(@_);
#}


#sub f_dirs {
# # my $self=shift;
#  @_ or return;
#  $DEBUG and $self->debug(1, "Getting the paths of @_");#
#
#  my @list=();
#  foreach my $e (@_) {
#    push @list, "path='$e'";#".$self->getVOPath($e)."'";
#  }
#  my $query="SELECT dir,path from D0 where ". join(" or ", @list);
#
#  $DEBUG and $self->debug(1, "Doing query $query");
#  return $self->{DATABASE}->query($query);
#}

sub f_complete_path {
  return shift->GetAbsolutePath(@_);
}

sub getVOPath {
  my ( $self, $path ) = @_;

  $path=$self->f_complete_path($path);

  $DEBUG and $self->debug(3, "Checking if we have something mounted ($self->{MOUNT} and $path");
  $self->{MOUNT} and $path=~ s/^$self->{MOUNT}//;
  $DEBUG and $self->debug(1, "VOPath is $path");

  return $path;
}


sub GetHomeDirectory {
  my $self = shift;
  return ("$self->{CONFIG}->{USER_DIR}/"
	  . substr( $self->{ROLE}, 0, 1 )
	  . "/$self->{ROLE}" );
}

#
#Returns the name of the file of a path
#
sub f_basename {
  my $self = shift;
  my $arg = shift;

  my $pos = rindex( $arg, "/" );

  if ( $pos < 0 ) {
    return ("$arg");
  }
  else {
    return ( substr( $arg, $pos + 1 ) );
  }
}

#
#Returns the table of the father
#

#sub f_parent_dir {
#    my ( $self, $path ) = @_;#
#
#    $path = $self->f_complete_path($path);
#    $path = $self->f_dirname($path);
#    if ( $path eq "" ) {
#        return ( $self->{CURDIR} );
#    }
#    else {
#        return $self->{DATABASE}->getFieldFromD0($path,"dir");
#    }
#}

#
#Given a partial or complete path, it returns the equivalent complete path
#
sub GetAbsolutePath {
  my $self = shift;
  my $path = shift || "";
  my $trailingslash = (shift or 0);
  $DEBUG and $self->debug(3, "Getting the full path of $path");
  ($path) or return $self->{CURPATH};

  # replace ~
  $path =~ s/^~/$self->GetHomeDirectory()/e;
  $path = $self->{CURPATH} . $path if (index( $path, '/' ) != 0);
  $DEBUG and $self->debug(4, "Starting with $path");
  while (
	 $path =~ s{//+}{/}g or
	 $path =~ s{/\./}{/}g or
	 $path =~ s{/[^/]+/\.\.}{} or
	 $path =~ s{^/\.\./}{/}g ) {};

  $path =~ s/\/\.{1,2}$/\//;

  $path =~ s/\/$// if ($trailingslash == 1);
  $path =~ s /\/?$/\// if ($trailingslash == 2);
  $DEBUG and $self->debug(1, "Full path is $path");
  return $path;
}


return 1;
