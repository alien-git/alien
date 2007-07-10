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

package AliEn::Catalogue::Collection;

use strict;
use AliEn::SOAP;
use AliEn::MD5;
use vars qw ($DEBUG);

$DEBUG=0;
#
#This function adds an entry to the catalog
#
# Possible options:
#          -f: create the entry even if the pfn is empty
#
sub f_createCollection {
  my $self = shift;
  $DEBUG and $self->debug(1, "In the catalogue, doing addCollection(".join(",", map {defined $_ ? $_ : ""} @_ ).")");
  my $file = shift;
  my $guid = shift ;
  my $perm = (shift or $self->{UMASK});

  if (! $guid){
    $DEBUG and $self->debug(2, "Getting a new GUID for this file");
    eval {
      require AliEn::GUID;
      $self->{GUID} or $self->{GUID}=AliEn::GUID->new();
      $guid=$self->{GUID}->CreateGuid();
      $guid and $DEBUG and $self->debug(2,"Got $guid");
    }
  }

  $file = $self->f_complete_path($file);
  $DEBUG and $self->debug(2, "file is $file");

  my $permLFN=$self->checkPermissions( 'w', $file ) or  return;
  $self->existsEntry($file, $permLFN) and
    $self->{LOGGER}->error("File", "file $file already exists!!",1) 
      and return;

  # Now, insert it into D0, and in the table
  my $basename   = $self->f_basename($file);
  my $insert={lfn=>$file,  perm=>$perm,  owner=>$self->{ROLE},
	      gowner=>$self->{MAINGROUP}, guid=>$guid,  };

  $self->{DATABASE}->createCollection( $insert)
    or $self->info("Error inserting entry into directory")
      and return;

  $self->info("File $file inserted in the catalog");
  return 1;
}

sub f_addFileToCollection_HELP{
  return "addFileToCollection: inserts a file into a collection of files
Usage:
\taddFileToCollection <lfn> <collection>
";
}
sub f_addFileToCollection {
  my $self=shift;

  my ($permFile, $permColl)=$self->_checkFileAndCollection(shift,shift) or return;

  $self->{DATABASE}->addFileToCollection($permFile, $permColl) or return;
  $self->info( "File '$permFile->{lfn}' added to the collection!");
  return $self->updateCollection("",$permColl);
}
sub updateCollection_HELP{
  return "updateCollection: Check the consistency of a collection. 

Usage:
\tupdateCollection [<options>] <collection_name

Possible options:

By default, it checks the SE that contains all the files of the collection
";
}
sub updateCollection {
  my $self=shift;
  my $options=shift;
  my $coll=shift;
  my $permColl;
  if ( UNIVERSAL::isa( $coll, "HASH" )) {
    $permColl=$coll;
    $coll=$coll->{lfn};
  }
  else{
    $coll = $self->f_complete_path($coll);

    $permColl=$self->checkPermissions( 'w', $coll, undef, {RETURN_HASH=>1} ) 
       or  return;
  }
  if (! $self->isCollection($coll, $permColl)){
    $self->info("$coll is not a collection of files");
    return
  }
  $self->info("At some point we should update the SE's of a collection");

  my $info=$self->{DATABASE}->getInfoFromCollection($permColl->{guid})
    or $self->info("Error getting the info for $coll") and return;

  my @se;
  my $first=1;
  foreach my $file (@$info){
    my @tempSe=$self->f_whereis("slrg", $file->{guid});
    if ($first){
      @se=@tempSe;
      $first=0;
    }else {
      my @andse;
      $self->info("at the beginning, old (@se) new (@tempSe)");
      foreach my $oldName (@se){
	print "Checking $oldName in @tempSe\n";
	grep (/^$oldName$/, @tempSe) 
	  and $self->info("Putting $oldName") and push @andse, $oldName;
      }
      @se=@andse;
    }
    @se or $self->info("So far, there are no SE that contain all the files") and last;
  }
  $self->info("All the files of this collection are in the SE: @se");
  $self->{DATABASE}->updateFile($coll, {se=>join(",", @se)}, {autose=>1});

  return 1;
}

sub isCollection{
  my $self=shift;
  my $name=shift;
  my $perm=shift;
  
  if (!$perm){
    $perm=$self->checkPermissions( 'r', $name, undef, {RETURN_HASH=>1} )  or  return;
  }
  
  $perm and $perm->{type} and $perm->{type} =~ /^c$/ and return 1;
  return ;
}

sub f_listFilesFromCollection{
  my $self=shift;
  my $coll=shift;
  $coll = $self->f_complete_path($coll);
  my $perm=$self->checkPermissions( 'r', $coll, undef, {RETURN_HASH=>1} )  or  return;
  if (! $self->isCollection($coll, $perm)){
    $self->info("'$coll' is not a collection of files");
    return;
  }
  my $info=$self->{DATABASE}->getInfoFromCollection($perm->{guid})
    or $self->info("Error getting the info for $coll") and return;

  my $message="";
  foreach my $file (@{$info}){
    $message.="\t$file->{guid} (from the file $file->{origLFN})\n";
  }
  $self->info($message,undef, 0);
  return $info;

}

sub _checkFileAndCollection{
  my $self=shift;
  my $lfn=shift;
  my $collection=shift;
 
  $lfn = $self->f_complete_path($lfn);
  $collection = $self->f_complete_path($collection);

  my $permFile=$self->checkPermissions( 'r', $lfn, undef, {RETURN_HASH=>1} )  or  return;
  if (! $self->isFile($lfn, $permFile->{lfn}) ) {
    $self->info("file $lfn doesn't exist!!",1);
    return;
  }

  my $permColl=$self->checkPermissions( 'w', $collection, undef, {RETURN_HASH=>1} )  or  return;
  if (! $self->isCollection($collection, $permColl)){
    $self->info("$collection is not a collection of files");
    return
  }
  return ($permFile, $permColl);
}

sub f_removeFileFromCollection{
  my $self=shift;

  my ($permFile, $permColl)=$self->_checkFileAndCollection(shift,shift) or return;
  return $self->{DATABASE}->removeFileFromCollection($permFile, $permColl);
}
1;
