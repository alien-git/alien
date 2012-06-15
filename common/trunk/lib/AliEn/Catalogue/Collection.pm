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

use AliEn::MD5;
use vars qw ($DEBUG);

$DEBUG = 0;

#
sub f_createCollection {
  my $self = shift;
  $DEBUG
    and $self->debug(1, "In the catalogue, doing addCollection(" . join(",", map { defined $_ ? $_ : "" } @_) . ")");
  my $file = shift;
  my $guid = shift;

  if (!$guid) {
    $DEBUG and $self->debug(2, "Getting a new GUID for this file");
    eval {
      require AliEn::GUID;
      $self->{GUID} or $self->{GUID} = AliEn::GUID->new();
      $guid = $self->{GUID}->CreateGuid();
      $guid and $DEBUG and $self->debug(2, "Got $guid");
    };
  }

  $file = $self->f_complete_path($file);
  $DEBUG and $self->debug(2, "file is $file");

  my $permLFN = $self->checkPermissions('w', $file) or return;
  $self->existsEntry($file, $permLFN)
    and $self->{LOGGER}->error("File", "file $file already exists!!", 1)
    and return;

  my $ownerId = $self->{DATABASE}->getOwnerId($self->{ROLE});
  my $gownerId = $self->{DATABASE}->getGownerId($self->{MAINGROUP});
  # Now, insert it into D0, and in the table
  my $basename = $self->f_basename($file);
  my $insert   = {
    lfn      => $file,
    perm     => $self->{UMASK},
    ownerId  => $ownerId,
    gownerId => $gownerId,
    guid     => $guid,
    type     => 'c'
  };

  $self->{DATABASE}->createCollection($insert)
    or $self->info("Error inserting entry into directory")
    and return;

  $self->info("File $file inserted in the catalog");
  return $file;
}

sub f_addFileToCollection_HELP {
  return "addFileToCollection: inserts a file into a collection of files
Usage:
\taddFileToCollection [-gn] [-name <name>]  <file> <collection> [<extra>]
Options:
\t\t-g: use the file as guid instead of lfn
\t\t-n: do not update the collection after adding the file

";
}

sub f_addFileToCollection {
  my $self = shift;
  my $opt  = {};

  @ARGV = @_;
  Getopt::Long::GetOptions($opt, "g", "n", "name=s",)
    or $self->info("Error parsing the arguments to addFileToCollection")
    and return;
  @_ = @ARGV;
  my $options = join("", keys %$opt);


  my ($permFile, $permColl) = $self->_checkFileAndCollection(shift, shift, $options) or return;
  my $extra = join(" ", @_);

  $self->{DATABASE}->addFileToCollection($permFile, $permColl, {localName => $opt->{name}, data => $extra}) or return;
  my $name = $permFile->{lfn};
  $opt->{g} and $name = $permFile->{guid};
  $self->info("File '$name' added to the collection!");
  $opt->{n} and return 1;
  return $self->updateCollection("s", $permColl);
}

#
#  The help for 'updateCollection' is in UI/Catalogue/LCM.pm, since there are
#  some options that require interaction with the LCM
#
#
#
sub updateCollection {
  my $self    = shift;
  my $options = shift;
  my $coll    = shift;
  my $permColl;
 
 
  if (UNIVERSAL::isa($coll, "HASH")) {
    $permColl = $coll;
    $coll     = $coll->{lfn};
  } else {
    $coll = $self->f_complete_path($coll);

    $permColl = $self->checkPermissions('w', $coll, 0, 1)
      or return;
  }
  if (!$self->isCollection($coll, $permColl)) {
    $self->info("$coll is not a collection of files");
    return;
  }
  $self->debug(1, "Ready to update the collection");

  my $info = $self->{DATABASE}->getInfoFromCollection($permColl->{guid})
    or $self->info("Error getting the info for $coll")
    and return;

  my @se;
  my $first   = 1;
  my $size    = 0;
  my $silent  = "";
  my $summary = {total => 0, collection => $coll};
  foreach my $file (@$info) {
    $summary->{total}++;
    #my ($info) = $self->f_whereis("slrgi", $file->{guid});
    my $info;
    if( $self->{DATABASE}->{DRIVER}=~ /Oracle/i) {
      my @info=$self->f_whereis("slrgi", $file->{guid});
      $info = $info[0];
    }else{
      ($info) = $self->f_whereis("slrgi", $file->{guid});
    }

    my @tempSe;
    map { push @tempSe, $_->{seName} } @{$info->{pfn}};
    $size += $info->{size};
    my @done;
    foreach (@tempSe) {
      grep(/^$_$/, @done) and next;
      $summary->{$_} or $summary->{$_} = 0;
      $summary->{$_}++;
    }
    if ($first) {
      @se    = @tempSe;
      $first = 0;
    } else {
      my @andse;
      $self->debug(1, "at the beginning, old (@se) new (@tempSe)");
      foreach my $oldName (@se) {
        $self->debug(2, "Checking $oldName in @tempSe");
        grep (/^$oldName$/, @tempSe)
          and $self->debug(1, "Putting $oldName")
          and push @andse, $oldName;
      }
      @se = @andse;
    }
    if (!@se) {
      $options =~ /s/ or $self->info("So far, there are no SE that contain all the files");
      last;
    }
  }
  $options =~ /s/
    or $self->info("All the files of this collection are in the SE: @se");

  $self->{DATABASE}->updateFile(
    $coll,
    { size => $size,
      se   => join(",", @se)
    },
    {autose => 1}
  );
  ($options =~ /c/) and return $summary;
  return 1;
}

sub isCollection {
  my $self = shift;
  my $name = shift;
  my $perm = shift;

  if (!$perm) {
    $perm = $self->checkPermissions('r', $name, 0, 1) or return;
  }

  $perm and $perm->{type} and $perm->{type} =~ /^c$/ and return 1;
  return;
}

sub f_listFilesFromCollection_HELP {
  return "Usage: listFiles [options] <collection>

Possible options:
\t-g: The collection is a GUID instead of an lfn
\t-v: Return the extended information for each entry in the collection (it might take some time if there are many entries in the collection)
";
}

sub f_listFilesFromCollection {
  my $self = shift;
  my $opt  = {};
  @ARGV = @_;

  Getopt::Long::GetOptions($opt, "g", "v", "z")
    or $self->info("Error parsing the arguments to addFileToCollection")
    and return;
  @_ = @ARGV;
  my $coll = shift;
  my $guid;
  if ($opt->{g}) {
    my $info = $self->{DATABASE}->checkPermission('r', $coll, "type")
      or return;
    $info->{type} eq "c" or $self->info("The guid $coll is not a collection") and return;
    $guid = $coll;
  } else {
    $coll = $self->f_complete_path($coll);
    my $perm = $self->checkPermissions('r', $coll, 0, 1) or return;
    if (!$self->isCollection($coll, $perm)) {
      $self->info("'$coll' is not a collection of files");
      return;
    }
    $guid = $perm->{guid};
  }
  my $info = $self->{DATABASE}->getInfoFromCollection($guid)
    or $self->info("Error getting the info for $coll")
    and return;

  my $message = "";
  foreach my $file (@{$info}) {
    $message .= "\t$file->{guid}";
    $file->{origLFN}   and $message .= "  (from the file $file->{origLFN})";
    $file->{localName} and $message .= " (will save as '$file->{localName}')";
    $file->{data}      and $message .= " (extra info '$file->{data}')";

    if ($opt->{v}) {
      my $extra = $self->{DATABASE}->getAllInfoFromGUID({}, $file->{guid});
      for my $entry ("size", "md5") {
        $message .= "( $entry = $extra->{$entry})";
        $file->{$entry} = $extra->{$entry};
      }
    }
    $message .= "\n";
  }

  $self->info($message, undef, 0);
  if ($opt->{z}) {
    return @$info;
  }
  return $info;
}

sub _checkFileAndCollection {
  my $self       = shift;
  my $lfn        = shift;
  my $collection = shift;
  my $opt        = shift;
  my $permFile;
  if ($opt =~ /g/) {
    $self->info("The file '$lfn' is in fact the guid");
    $permFile = $self->getInfoFromGUID($lfn)
      or return;
  } else {
    $lfn = $self->f_complete_path($lfn);
    $permFile = $self->checkPermissions('r', $lfn, 0, 1) or return;
    if (!$self->isFile($lfn, $permFile->{lfn})) {
      $self->info("file $lfn doesn't exist!!", 1);
      return;
    }
  }
  $self->debug(1, "And now, let's check the collection $collection");
  $collection = $self->f_complete_path($collection);

  my $permColl = $self->checkPermissions('w', $collection, 0, 1) or return;
  if (!$self->isCollection($collection, $permColl)) {
    $self->info("$collection is not a collection of files");
    return;
  }
  return ($permFile, $permColl);
}

sub f_removeFileFromCollection_HELP {
  return "removeFileFromCollection: removes a file from a collection
Usage:
\tremoveFileFromCollection [-g] <file> <collection>

Options:
\t\t-g:\t use the file as guid instead of lfn
";
}

sub f_removeFileFromCollection {
  my $self = shift;
  my $opt  = shift;
  my ($permFile, $permColl) = $self->_checkFileAndCollection(shift, shift, $opt) or return;
  my $done = $self->{DATABASE}->removeFileFromCollection($permFile, $permColl) or return;
  return $self->updateCollection("", $permColl);
}
1;
