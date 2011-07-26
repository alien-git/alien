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

package AliEn::Catalogue::File;

use strict;
use AliEn::SE::Methods;
use AliEn::URL;
use AliEn::SOAP;
use AliEn::MD5;
use Data::Dumper;
use vars qw ($DEBUG);

$DEBUG = 0;

#
#This function adds an entry to the catalog
#
# Possible options:
#          -f: create the entry even if the pfn is empty
#
sub f_registerFile {
  my $self = shift;
  $DEBUG
    and $self->debug(1, "In the catalogue, doing registerFile(" . join(",", map { defined $_ ? $_ : "" } @_) . ")");
  my $opt    = shift;
  my $file   = shift;
  my $size   = shift;
  my $se     = shift;
  my $guid   = shift;
  my $perm   = (shift or $self->{UMASK});
  my $selist = shift || 0;
  my $md5    = shift;
  my $pfn    = shift || "";
  my $jobid  = shift || 0;

  if (!defined $size) {
    $self->info(
"Error in add: not possible to register the file in the catalogue. Not enough arguments. \nUsage register <lfn> <size> <storage element>",
      1
    );
    return;
  }

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

  # First, we check that we have permission in that directory
  #  my $tempname = $self->f_dirname($file);

  my $permLFN = $self->checkPermissions('w', $file, 0, 1) or return;
  if ($self->existsEntry($file, $permLFN->{lfn})) {
    if (($permLFN->{owner} eq $self->{ROLE}) and ($permLFN->{gowner} eq $self->{MAINGROUP})) {

    } else {
      $self->info(
        "file $file already exists. Overwrite not allowed, file owner: $permLFN->{owner}, gowner: $permLFN->{gowner}!!",
        1
      );
      return;
    }
  }

  # Now, insert it into D0, and in the table
  my $basename = $self->f_basename($file);
  my $insert   = {
    lfn    => $file,
    perm   => $perm,
    owner  => $self->{ROLE},
    gowner => $self->{MAINGROUP},
    size   => $size,
    guid   => $guid,
    jobid  => $jobid
  };
  $se     and $insert->{se}           = $se;
  $md5    and $insert->{md5}          = $md5;
  $pfn    and $insert->{pfn}          = $pfn;
  $selist and $insert->{seStringList} = $selist;

  $self->{DATABASE}->createFile($opt, $insert)
    or $self->info("Error inserting entry into directory")
    and return;

  $self->info("File $file inserted in the catalog");
  return 1;
}

#
#This function adds several entries to the catalog
#
# Possible options:
#          -f: create the entry even if the pfn is empty
#
sub f_bulkRegisterFile {
  my $self = shift;
  $DEBUG
    and $self->debug(1, "In the catalogue, doing registerFile(" . join(",", map { defined $_ ? $_ : "" } @_) . ")");

  #  my $opt = shift;
  my $options   = shift;
  my $directory = shift;
  my $files     = shift;

  $directory = $self->f_complete_path($directory);

  # First, we check that we have permission in that directory
  #  my $tempname = $self->f_dirname($file);

  my $permLFN = $self->checkPermissions('w', $directory) or return;
  $self->isDirectory($directory, $permLFN)
    or $self->info("$directory is not a directory!!", 1)
    and return;

  my $ok = 1;
  my @insert;
  my $list = "";
  foreach my $entry (@$files) {
    if (!$entry->{guid}) {
      $DEBUG and $self->debug(2, "Getting a new GUID for this file");
      $entry->{guid} = $self->{GUID}->CreateGuid();
      $entry->{guid} and $DEBUG and $self->debug(2, "Got $entry->{guid}");
    }
    $entry->{lfn} =~ m{/}
      and $self->info("The entry $entry->{lfn} cannot be inserted in bulk (there can't be any directories")
      and return;
    $entry->{lfn} = "$directory/$entry->{lfn}";

    # Now, insert it into D0, and in the table
    my $insert = {
      lfn    => $entry->{lfn},
      perm   => $self->{UMASK},
      owner  => $self->{ROLE},
      gowner => $self->{MAINGROUP},
      size   => $entry->{size},
      guid   => $entry->{guid},
    };

    for my $field ("se", "md5", "selist", "seStringlist", "pfn", "pfns", "type") {
      $entry->{$field} and $insert->{$field} = $entry->{$field};
    }
    $entry->{user} and $insert->{owner} = $insert->{gowner} = $entry->{user};

    $list .= "$entry->{lfn} ";
    push @insert, $insert;
  }
  $self->{DATABASE}->createFile($options, @insert)
    or print STDERR "Error inserting entry into directory\n" and return;

  $self->info("Files $list inserted in the catalog");
  return 1;
}

# Returns the list of SE that have this file
# Possible options:   -l Give only the list of SE (not pfn)
#                     -g return also the file info
#
#
#sub f_whereisFile {
#  my $self    = shift;
#  my $options = shift;
#  my $lfn     = shift;
#
#  $lfn = $self->f_complete_path($lfn);
#
#  my $permFile = $self->checkPermissions('r', $lfn, 0, 1) or return;
#  if (!$self->isFile($lfn, $permFile->{lfn})) {
#    $self->{LOGGER}->error("File", "file $lfn doesn't exist!!", 1);
#    return;
#  }
#  if ($options =~ /g/) {
#    my $ret = {};
#    $ret->{selist} = $self->{DATABASE}->getSEListFromFile($lfn, $permFile->{seStringlist});
#    $ret->{fileinfo} = $permFile;
#
#    return $ret;
#  } else {
#    my $ret=$self->{DATABASE}->getSEListFromFile($lfn, $permFile->{seStringlist});
#
#    return $ret;
#  }
#}

#
#
#

sub f_updateFile_HELP {
  return "Updates the information concerning a file in the catalogue
Usage:
  update <lfn> [-se <se>] [-guid <guid>] [-size <size>] [-md5 <md5>]
\n";

}

sub f_updateFile {
  my $self = shift;
  $self->debug(2, "In File Interface, f_updateFile @_");
  my $file = shift;
  my $args = join(" ", @_);

  my $size   = "";
  my $se     = "";
  my $update = {};

  $args =~ s/-?-s(ize)?[\s=]+(\S+)// and $update->{size} = $2;
  $args =~ s/-?-s(e)?[\s=]+(\S+)//   and $update->{se}   = $2;
  $args =~ s/-?-g(uid)?[\s=]+(\S+)// and $update->{guid} = $2;
  $args =~ s/-?-m(d5)?[\s=]+(\S+)//  and $update->{md5}  = $2;
  $args =~ s/-?-p(fn)?[\s=]+(\S+)//  and $update->{pfn}  = $2;

  my $message = "";

  (keys %$update) or $message = "You should update at least one of the fields";
  $args =~ /^\s*$/ or $message = "Argument $args not known";

  $message
    and print STDERR "Error:$message\n" . $self->f_updateFile_HELP()
    and return;

  $file = $self->f_complete_path($file);

  my $permLFN = $self->checkPermissions('w', $file)
    or return;
  if (!$self->isFile($file, $permLFN)) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!", 1);
    return;
  }

  $DEBUG and $self->debug(2, "Ready to do the update");

  $self->{DATABASE}->updateFile($file, $update,)
    or $self->info("Error doing the update of $file", 11)
    and return;

  $self->info("File $file updated in the catalog");
  return 1;
}

#
#This function returns the GUID of a file in the catalog
#
#

sub f_getGuid {
  my $self = shift;
  $DEBUG and $self->debug(2, "In FileInterface getGuid @_");
  my $options = shift || "";
  my $file = shift;

  ($file)
    or print STDERR "Error: not enough arguments in f_getGuid!\n" and return;

  $file = $self->f_complete_path($file);
  my $info = $self->checkPermissions('r', $file, 0, 1)
    or return;
  $info->{guid} or $self->info("The file '$file' doesn't exist") and return;
  return $info->{guid};
}

#
#
#
sub f_getMD5 {
  my $self = shift;
  $DEBUG and $self->debug(2, "In FileInterface getMD5 @_");
  my $options = shift || "";
  my $file = shift;

  ($file)
    or print STDERR "Error: not enough arguments in f_getGuid!\n" and return;

  $file = $self->f_complete_path($file);
  my $permLFN = $self->checkPermissions('r', $file, 0, 1)
    or return;
  if (!$self->isFile($file, $permLFN->{lfn})) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!", 1);
    return;
  }
  my $guid = $permLFN->{guid};
  my $md5  = $permLFN->{md5};

  if ($options =~ /g/) {
    $options =~ /s/
      or $self->info("$md5\t$file (guid $guid)", undef, 0);
    return {md5 => $md5, guid => $guid};
  }
  $options =~ /s/ or $self->info("$md5\t$file", undef, 0);
  return $md5;
}

#
#This function retrieves a file from the catalog.
#
#
sub f_showMirror {
  my $self = shift;
  $DEBUG and $self->debug(2, "In FileInterface getFile @_");
  my $options = shift || "";
  my $file = shift;

  my $silent   = $options =~ /s/;
  my $original = $options =~ /o/;
  $self->{SILENT} and $silent = 1;

  my $logger = "error";
  $silent and $logger = "debug";

  ($file)
    or print STDERR "Error: not enough arguments in whereis\nUsage: whereis [-o] <file>\n" and return;

  $file = $self->f_complete_path($file);

  my $permLFN = $self->checkPermissions('r', $file) or return;
  if (!$self->isFile($file, $permLFN)) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!", 1);
    return;
  }

  $DEBUG and $self->debug(2, "Ready to get the list of SE");
  my $ref = $self->{DATABASE}->getSEListFromFile($file) or return;
  $self->info("Getting the SE list @$ref");
  return $ref;
}

sub f_addMirror_HELP {
  return "addMirror: adds a new PFN to an entry in the catalogue
Usage:
\taddMirror [-gc] <lfn> <se> [<pfn>] [-md5=<md5>]

Options:
\t-g: Use the lfn as guid
\t-c: Check the md5 of the replica
-md5: Specify the md5 of the file. 
";

}

sub f_addMirror {
  my $self = shift;

  $self->info("Adding a mirror @_");

  my $file = shift;
  my $se   = shift || $self->{CONFIG}->{SE_FULLNAME};
  my $pfn  = shift || "";

  my $opt = {};
  @ARGV = @_;
  Getopt::Long::GetOptions($opt, "g", "md5=s", "c")
    or $self->info("Error parsing the arguments to addMirror")
    and return;
  @_ = @ARGV;

  # my $md5 =shift;
  $file or $self->info("Error not enough arguments in addMirror" . $self->f_addMirror_HELP(), 1) and return;
  $file = $self->f_complete_path($file);

  my $permLFN = $self->checkPermissions('w', $file, 0, 1) or return;

  if (!$self->isFile($file, $permLFN->{lfn})) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!", 1);
    return;
  }
  if ($opt->{c} and !$opt->{md5}) {
    $opt->{md5} = AliEn::MD5->new($pfn);
    $opt->{md5} or $self->info("Error getting the md5sum of '$pfn'") and return;
  }
  $self->{DATABASE}->insertMirrorFromFile($file, $se, $pfn, $opt->{md5}) or return;
  $self->deleteEntryFromBookingTableAndOptionalExistingFlagTrigger(($self->{ROLE} || $self->{CONFIG}->{ROLE}),
    {lfn => $file, turl => $pfn, se => $se, guid => $permLFN->{guid}}, 0);

  $self->info("File '$file' has a mirror in '${se}'");
  return 1;
}

sub f_deleteMirror_HELP {
  return "deleteMirror: 
Removes a replica of a file from the catalogue
Uage:
\tdeleteMirror [-g] <lfn> <se> [<pfn>]

Options:
   -g: the lfn is a guid
"

}

sub f_deleteMirror {
  my $self    = shift;
  my $options = shift;
  $self->info("Deleting a mirror @_");

  my $file = shift;
  my $se   = shift;

  $file
    or $self->info("Error not enough arguments in deleteMirror\n" . $self->f_deleteMirror_HELP())
    and return;

  if ($options =~ /g/) {
    $self->info("Removing the replica from the guid directly");
    $self->{DATABASE}->deleteMirrorFromGUID($file, $se, @_)
      or $self->info("Error removing the mirror of $file in $se")
      and return;
    return 1;
  }
  $file = $self->f_complete_path($file);

  my $permLFN = $self->checkPermissions('w', $file) or return;
  if (!$self->isFile($file, $permLFN)) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!", 1);
    return;
  }

  $self->{DATABASE}->deleteMirrorFromLFN($file, $se, @_)
    or $self->info("Error removing the mirror of $file in $se")
    and return;
  $self->info("Mirror from ${se} removed");
  return 1;
}

sub f_setExpired_HELP {
  return
"setExpired: Sets the expire date for an entry in the catalogue. When that date arrives, all the entries in 'replica' SE will be deleted. If there are no entries in long term SE, the lfn will be renamed to 'lfn.expired'. 
Usage:
\t\tsetExpired <seconds> <lfn> [<lfn>+]

To see the expire date of a file, do 'ls -e'
";
}

sub f_setExpired {
  my $self    = shift;
  my $seconds = shift;

  @_ or $self->info("Error: not enough arguments" . $self->f_setExpired_HELP()) and return;

  while (@_) {
    my $file = shift;

    $file = $self->GetAbsolutePath($file);

    my $permLFN = $self->checkPermissions("w", $file)
      or return;

    if (!$self->isFile($file, $permLFN)) {
      $self->{LOGGER}->error("File", "file $file doesn't exist!!", 1);
      return;
    }
    $self->debug(2, "Let's put the expiration time of $file");
    $self->{DATABASE}->setExpire($file, $seconds) or return;

    $self->info("The file $file will expire in $seconds seconds");
  }
  return 1;

}

sub f_ln {
  my $self   = shift;
  my $source = shift;
  my $target = shift;

  ($source and $target)
    or $self->{LOGGER}->error("File", "Error: not enough arguments in ln!!\nUsage: ln <source> <target>\n")
    and return;

  $source = $self->GetAbsolutePath($source);
  $target = $self->GetAbsolutePath($target);

  (!$self->isDirectory($source))
    or $self->{LOGGER}->error("File", "$source cannot be a directory\n")
    and return;
  (!$self->isDirectory($target))
    or $self->{LOGGER}->error("File", "$target cannot be a directory\n")
    and return;
  ($self->{DATABASE}->existsEntry($target))
    and $self->{LOGGER}->error("File", "$target exists!\n")
    and return;

  #Check permissions
  my $filehash = $self->checkPermissions("r", $source, 0, 1);
  $filehash
    or $self->{LOGGER}->error("File", "ERROR: checkPermission failed for $source")
    and return;
  $filehash = $self->checkPermissions("w", $target, 0, 1);
  $filehash
    or $self->{LOGGER}->error("File", "ERROR: checkPermission failed for $target")
    and return;
  return $self->{DATABASE}->softLink($source, $target);
}

# This subroutine is used to find all the metadata of a file that is going
# to be copied. The result is giving back in the variable $todoMetadata
#
sub getCPMetadata {
  my $self         = shift;
  my $source       = shift;
  my $targetDir    = shift;
  my $targetName   = shift;
  my $todoMetadata = shift;

  $targetDir =~ s{/?$}{/};
  $self->info("We are supposed to copy also the metadata of $source to $targetDir");
  my $sourceDir = $source;
  $sourceDir =~ s{/[^/]*$}{/};
  my $tags = $self->f_showTags("allr", $sourceDir);
  my $entries = {};
  foreach my $tag (@$tags) {

    #making sure that the destination has all the tags#
    $self->debug(1, "We should add the tag $tag->{tagName}");
    $self->info("Adding tag $tag->{tagName}....");
    $self->f_addTag($targetDir, $tag->{tagName}, $tag->{tagName}, $tag->{path})
      or $self->info("Error defining the metadata $tag->{tagName}")
      and return;
    my $tableName = $self->{DATABASE}->getTagTableName($targetDir, $tag->{tagName});
    $tableName
      or $self->info("Error getting the name of the table")
      and next;

    #let's put the entries
    my @list = ();
    $entries->{$tag->{tagName}} and push @list, @{$entries->{$tag->{tagName}}};
    $self->info("Getting the metadata for $sourceDir and $tag->{tagName}");
    my ($columns, $info) = $self->f_showTagValue("", $sourceDir, $tag->{tagName});
    $self->info("Getting the extra metadata for $sourceDir and $tag->{tagName}");
    my ($columns2, $info2) = $self->f_showTagValue("r", $sourceDir, $tag->{tagName});

    $self->info("Processing metadata values for $tag");
    foreach my $entry (@$info, @$info2) {
      my $toInsert = {file => "$targetName"};
      if (!$targetName) {
        $toInsert->{file} = $entry->{file};
        if ($toInsert->{file} =~ s/^$source//) {
          $toInsert->{file} = "$targetDir$toInsert->{file}";
        } else {
          $self->info("The file doesn't start with $source");
          $toInsert->{file} = $targetDir;
        }
        $self->info(
"Since we are copying a directory, the info is from $toInsert->{file} (from $entry->{file}, $source and $targetDir"
        );
        my $tempDir = $toInsert->{file};
        $tempDir =~ s/[^\/]*$//;
        $self->f_addTag($tempDir, $tag->{tagName}, $tag->{tagName}, $tag->{path})
          or $self->info("Error creating the tag $tag->{tagName} in $tempDir")
          and return;
      }
      foreach my $key (keys %$entry) {
        $key =~ /^(entryId)|(file)$/ and next;
        $toInsert->{$key} = $entry->{$key};
      }
      push @list, $toInsert;
    }
    @list or $self->info("For the tag $tag->{tagName}, there wasn't any metadata. Ignoring it") and next;
    $entries->{$tableName} = \@list;
  }

  $self->info("Adding metadata values for $targetName....");
  foreach my $key (keys %$entries) {
    my @list = ();
    $todoMetadata->{$key} and push @list, @{$todoMetadata->{$key}};
    push @list, @{$entries->{$key}};
    $todoMetadata->{$key} = \@list;
  }

  foreach my $key (keys %$entries) {

    #Insert data into table
    my $tagName = $key;
    $tagName =~ s/^TadminV//;
    my @data = ();
    foreach my $val (keys %{${$entries->{$key}}[0]}) {
      $val =~ /^(offset)|(file)/ and next;
      push @data, "$val='${$entries->{$key}}[0]->{$val}'";
    }
    $self->info("@data");
    $self->f_addTagValue($targetName, $tagName, @data)
      or $self->{LOGGER}->error("Catalogue::File", "Could not add tag value @data for tag $tagName on tag $targetName");
  }

  return $entries;
}

#
#Move files
#
sub f_mv {
  my $self    = shift;
  my $options = shift;
  my $source  = shift;
  my $target  = shift;
  $source
    or $target
    or $self->{LOGGER}->error("File", "ERROR: Source and/or target not specified")
    and return;

  my $fullSource = $self->GetAbsolutePath($source);
  my $fullTarget = $self->GetAbsolutePath($target);
  $self->info("$fullSource ($source) --> $fullTarget ($target)");

  #Check quotas
  my $filehash = $self->checkPermissions("w", $fullTarget, 0, 1);
  $filehash
    or $self->{LOGGER}->error("File", "ERROR: checkPermission failed for $fullTarget")
    and return;
  $filehash = $self->checkPermissions("w", $fullSource, 0, 1);
  $filehash
    or $self->{LOGGER}->error("File", "ERROR: checkPermission failed for $fullSource")
    and return;

  #Do move
  my @returnVal = ();
  if($self->isDirectory($fullTarget)) {
    my $tmp = 0;
    $fullTarget =~ m{/$} and $tmp = 1;
    if($fullSource =~ m{([^/]+)/?$}) {
      $tmp or $fullTarget.="/".$1;
      $tmp and $fullTarget.=$1;
    }
  }
  if ($self->isDirectory($fullSource)) {
    @returnVal = $self->{DATABASE}->moveFolder($fullSource, $fullTarget);
  } else {
    if ($self->isDirectory($fullTarget)) {
      my $file = "";
      $fullSource =~ m{([^/]*)/?$} and $file = $1;
      $fullTarget .= $file;
    }
    @returnVal = $self->{DATABASE}->moveFile($fullSource, $fullTarget);
  }

  #Manage metadata if option specified
  if ($options =~ /m/) {
    my $todoMetadata = {};
    my $targetDir    = "$fullTarget";
    $targetDir =~ s{/[^/]*$}{/};
    $self->getCPMetadata($fullSource, $targetDir, $fullTarget, $todoMetadata);
  }
  return @returnVal;
}

#
#Delete file from catalogue
#
sub f_removeFile {
  my $self    = shift;
  my $options = shift;
  my $file    = shift;
  my $silent  = ($options =~ /s/);
  if (!$file) {
    ($options =~ /s/)
      or $self->{LOGGER}->error(
      "File", "Error in remove: not enough arguments\nUsage remove [-s] <path>\n
                                Options: -s : silent. Do not print error messages\n"
      ) and return;
  }

  #Check if file specified is a directory
  my $fullPath = $self->GetAbsolutePath($file);
  $self->isDirectory($fullPath)
    and $self->{LOGGER}->error("File", "ERROR: $fullPath is a directory")
    and return;

  #Check permissions
  my $filehash = $self->checkPermissions("w", $fullPath, 0, 1);
  if (!$filehash) {
    $self->{LOGGER}->error("File", "Check permission on $fullPath failed");
    return;
  }
  $self->existsEntry($fullPath, $filehash->{lfn})
    or $self->error("file $fullPath does not exists!!", 1)
    and return;

  return $self->{DATABASE}->removeFile($fullPath, $filehash, $self->{ROLE});
}

#
#Delete directory and all associated files from catalogue
#
sub f_rmdir {
  my $self = shift;
  my ($options, $path) = @_;
  my $deleteall = (($options =~ /r/) ? 1 : 0);
  my $message = "";
  ($path) or $message = "no directory specified";
  ($path and $path eq ".")  and $message = "Cannot remove current directory";
  ($path and $path eq "..") and $message = "Cannot remove parent directory.";
  $message
    and $self->info("Error $message\nUsage: rmdir [-r] <directory>", 1)
    and return;

  #Check if path specifed is a file
  $path = $self->GetAbsolutePath($path, 1);
  unless ($self->isDirectory($path)) {
    $self->info("ERROR: $path is not a directory", 1);
    return;
  }

  #Check permissions
  my $parentdir = $self->GetParentDir($path);
  my $filehash = $self->checkPermissions("w", $parentdir, 0, 1);
  $filehash
    or $self->info("ERROR: checkPermissions failed on $parentdir", 1)
    and return;
  $filehash = $self->checkPermissions("w", $path, 0, 1);
  $filehash
    or $self->info("ERROR: checkPermsissions failed on $path", 1)
    and return;
  return $self->{DATABASE}->removeDirectory($path, $parentdir,$self->{ROLE});
}

#
#returns the flags and the files of the input line
# (
sub Getopts {
  my $self = shift;
  my ($word, @files, $flags);

  $flags = "";
  @files = ();

  foreach $word (@_) {
    if ($word =~ /^-.*/) {
      $flags = substr($word, 1) . $flags;
    } else {
      @files = (@files, $word);
    }
  }
  return ($flags, @files);
}

#
#touch file in catalogue
#
sub f_touch {
  my $self    = shift;
  my $options = shift;
  my $lfn     = shift
    or $self->info( "Error missing the name of the file to touch", 1)
    and return;
  $lfn = $self->GetAbsolutePath($lfn);
  if($self->existsEntry($lfn)) {
    $self->info("The  lfn $lfn already exists in the file catalogue");
    return;
  }
  my ($ok, $message) = $self->checkFileQuota($self->{CONFIG}->{ROLE}, 0);
  if ($ok eq -1) {
    $self->info($message,1)
      or return;
  }

  #Insert file in catalogue
  $self->info("Inserting file $lfn");
  $self->f_registerFile($options, $lfn, 0)
    or $self->info( "Could not touch file", 1)
    and return;
  $self->info("$lfn successfully created")
    and return 1;
}

sub f_du_HELP {
  return "Gives the disk space usge of a directory
Usage:
\tdu [-hf] <dir>

Options:
\t\t-h: Give the output in human readable format
\t\t-f: Count only files (ignore the size of collections)
";
}

sub f_du {
  my $self    = shift;
  my $options = shift;
  my $path    = $self->GetAbsolutePath(shift);
  my $entry   = $self->{DATABASE}->existsEntry($path);
  $entry or $self->info("du: `$path': No such file or directory", 11, 1) and return;
  $self->info("Checking the disk space usage of $path");
  my $space = $self->{DATABASE}->getDiskUsage($entry, $options);
  my $unit = "";
  if ($options =~ /h/) {
    my @possible = ("K", "M", "G", "T", "P", "H");
    while (@possible and $space > 1024) {
      $space = sprintf("%.2f", $space / 1024);
      $unit = shift @possible;
    }

  }
  $self->info("$path uses $space ${unit}bytes");

  return $space;
}


sub f_di_HELP {
  return "Gives the number of entries in the L#L tables and optimizes them
Usage:
\tdi <options> <max_lim> <min_lim> <dir>

Options:
\t\t
\t\toptimize: Optimizes the the L#L LFN tables wrt number of entries in the table (all the L#L tables)
\t\toptimize_dir: Optimizes the the L#L LFN tables wrt number of entries in the table in the path specified (current directory by default)
\t\tmax_lim: Maximum limit of number of entries to be present in a table
\t\tmin_lim: Maximum limit of number of entries to be present in a table
";
}

sub f_di {
  my $self    = shift;
  my $options = shift;
# $self->moveDirectory("ZX","-b");
# $self->info("Did it work");
# my $c = $self->{DATABASE}->query("SELECT * FROM G0L");
# foreach my $row(@$c) {
#       $self->info("====>>>> $row->{owner}");
#       #map { $self->info("$_ ====>>>> $row->{$_}") } keys %$row;
#       $self->info("\n\n\n\n");
# }
# return 1;
  if ($options eq "optimize") {
     
      my $max_lim = shift;
      my $min_lim = shift;
      $self->info("Trying to optimiz...");
      $self->info("maxLim:: $max_lim");
      $self->info("minLim:: $min_lim");

      my @stack_final = ();
      my (@LFN) = $self->{DATABASE}->getNumEntryIndexes();
      
      my $num_tables = @LFN/2;
      for (my $i=0; $i<$num_tables; $i++)
      {
         $self->info("L$LFN[$i]L has $LFN[$i+$num_tables] number of entries.");
         my $table_name = "L".$LFN[$i]."L";
         my $tq = "SELECT lfn FROM INDEXTABLE WHERE tableName=$LFN[$i]";
         my $base = $self->{DATABASE}->queryValue($tq);
         if ($LFN[$i+$num_tables] > $max_lim)
         {
            #getting the depth of the directory tree 
            #following the bottom -> top approach
            my $qt = "SELECT MAX( LENGTH(lfn) - LENGTH(REPLACE(lfn,'/','')) ) AS depth FROM ".$table_name."";
            my $depth = $self->{DATABASE}->queryValue($qt);
            $self->info("depth ::: $depth");
            #here optimization loop will be written
            my @stack_dir=();  #stack of directories which will be moved usinf moveDirectory() 
            for(my $j=$depth; $j>0 ;$j--) 
            {
                my $q1 = "SUBSTRING_INDEX(lfn,'/',".$j.")";
                my $q2 = "SELECT ".$q1." AS DIR , COUNT( ".$q1.") AS CNT FROM ".$table_name." WHERE ".$q1." IN ";
                my $q3 = $q2." ( SELECT DISTINCT ".$q1." FROM ".$table_name." WHERE type LIKE \"d\") GROUP BY ".$q1."";
                $self->info("Here in Optimize");
                my $q = $self->{DATABASE}->query($q3);
                foreach my $row(@$q)
                {
                    my $dir = $row->{DIR};
                    my $cnt = $row->{CNT};
                    #optimization part 
                    if($cnt >$max_lim) 
                    {
                        $self->info("Inside condition ... => exceeding");
                        #pushing the directory to be removed into the stack
                        #the actual removal of the directories will take place in the calling function
                        push @stack_dir,$dir;
                        $dir = $base.$dir;
                        $self->moveDirectory($dir);
                        $self->info("Dir moved n pushed :: $dir ");
                    }
                }
            }
            #my @stack_temp = $self->{DATABASE}->optimizeTables($max_lim,$min_lim,$table_name);
            my @stack_temp = @stack_dir;
            push @stack_final,@stack_temp;
            use Data::Dumper;
            $self->info(Dumper(@stack_final));
	       }
         if ($LFN[$i+$num_tables] < $min_lim)
         {
           my $q1 = "SELECT lfn FROM INDEXTABLE WHERE tableName=".$LFN[$i]."";
           $self->info("Query::::: $q1");
           my $q = $self->{DATABASE}->queryValue($q1);
           $self->info(": $q");
           if($q =~ m/\// and length($q)==1 ) 
           {
             $self->info("Warning:: Can't move moveDirectory -b ROOT /  directory");
           } 
           else 
           {
            my $q2="SELECT tableName FROM INDEXTABLE WHERE \"".$q."\" RLIKE CONCAT(\"^\",lfn) AND lfn NOT LIKE \"".$q."\" ORDER BY LENGTH(lfn) DESC limit 1"; 
            $self->info("Query2 :: $q2");
            my $tname = $self->{DATABASE}->queryValue($q2); 
            my $q3 = "SELECT COUNT(*) FROM L".$tname."L ";
            $self->info("Query3 :: $q3");
            my $IsFull = $self->{DATABASE}->queryValue($q3);
            if($IsFull+$LFN[$i+$num_tables] >$max_lim )
            {
              $self->info("Warning:: Trying to moveDirectory -b into L".$tname."L ");
              $self->info("Warning:: Can't move back i.e. it will possibly exceede max_lim=$max_lim");
            }
            else
            {
              $self->moveDirectory($q,"-b");
              $self->info("Dir moved back :: $q ");
            }
           }
         }
      }
      return @LFN;
  }
  elsif ($options eq "optimize_dir") {
      #optimizes the directory specified otherwise the current directory
      my $max_lim = shift;
      my $min_lim = shift;
      my $path    = $self->GetAbsolutePath(shift);
      my $entry   = $self->{DATABASE}->existsEntry($path);
      $entry or $self->info("di: optimize_dir `$path': No such file or directory", 11, 1) and return;
      $self->info("Trying to optimiz the dir :: $path");
      $self->info("maxLim:: $max_lim");
      $self->info("minLim:: $min_lim");
      my @stack_final = ();
      my $rtables = $self->{DATABASE}->getTablesForEntry($path)
        or $self->info("Error getting the tables for '$path'")
        and return;
      use Data::Dumper;  $self->info(Dumper($rtables)); $self->info("CHK :: 1");
      foreach my $rtable (@$rtables)
      {
        $self->info("CHK :: 2 :: inside::: ");
        my $table_name = "L".$rtable->{tableName}."L";
        my $base = $rtable->{lfn};
        my $q1 = "SELECT COUNT(*) FROM ".$table_name.""; 
        my $num_entries = $self->{DATABASE}->queryValue($q1);
        if ($num_entries > $max_lim)
        {
            #getting the depth of the directory tree 
            #following the bottom -> top approach
            my $qt = "SELECT MAX(LENGTH(lfn) - LENGTH(REPLACE(lfn,'/','')) ) AS depth FROM ".$table_name."";
            my $depth = $self->{DATABASE}->queryValue($qt);
            $self->info("depth ::: $depth");
            #here optimization loop will be written
            my @stack_dir=();  #stack of directories which will be moved usinf moveDirectory() 
            for(my $j=$depth; $j>0 ;$j--) 
            {
                my $q1 = "SUBSTRING_INDEX(lfn,'/',".$j.")";
                my $q2 = "SELECT ".$q1." AS DIR , COUNT( ".$q1.") AS CNT FROM ".$table_name." WHERE ".$q1." IN ";
                my $q3 = $q2." ( SELECT DISTINCT ".$q1." FROM ".$table_name." WHERE type LIKE \"d\") GROUP BY ".$q1."";
                $self->info("Here in Optimize");
                my $q = $self->{DATABASE}->query($q3);
                foreach my $row(@$q)
                {
                    my $dir = $row->{DIR};
                    my $cnt = $row->{CNT};
                    #optimization part 
                    if($cnt >$max_lim) 
                    {
                        $self->info("Inside condition ... => exceeding");
                        #pushing the directory to be removed into the stack
                        #the actual removal of the directories will take place in the calling function
                        push @stack_dir,$dir;
                        $dir = $base.$dir;
                        if ($dir =~ m/^$path/)
                        {
                           $self->moveDirectory($dir);
                           $self->info("Dir moved n pushed :: $dir ");
                        }
                    }
                }
            }
            #my @stack_temp = $self->{DATABASE}->optimizeTables($max_lim,$min_lim,$table_name);
            my @stack_temp = @stack_dir;
            push @stack_final,@stack_temp;
            use Data::Dumper;
            $self->info(Dumper(@stack_final));
	      }
        if ($num_entries < $min_lim)
        {
           my $q1 = "SELECT lfn FROM INDEXTABLE WHERE tableName=".$rtable->{tableName}."";
           $self->info("Query::::: $q1");
           my $q = $self->{DATABASE}->queryValue($q1);
           $self->info(": $q");
           if($q =~ m/\// and length($q)==1 ) 
           {
             $self->info("Warning:: Can't move moveDirectory -b ROOT /  directory");
           } 
           else 
           {
            my $q2="SELECT tableName FROM INDEXTABLE WHERE \"".$q."\" RLIKE CONCAT(\"^\",lfn) AND lfn NOT LIKE \"".$q."\" ORDER BY LENGTH(lfn) DESC limit 1"; 
            $self->info("Query2 :: $q2");
            my $tname = $self->{DATABASE}->queryValue($q2); 
            my $q3 = "SELECT COUNT(*) FROM L".$tname."L ";
            $self->info("Query3 :: $q3");
            my $IsFull = $self->{DATABASE}->queryValue($q3);
            if($IsFull+$num_entries >$max_lim )
            {
              $self->info("Warning:: Trying to moveDirectory -b into L".$tname."L ");
              $self->info("Warning:: Can't move back i.e. it will possibly exceede max_lim=$max_lim");
            }
            else
            {
              $self->moveDirectory($q,"-b");
              $self->info("Dir moved back :: $q ");
            }
           }
         }
      }
      #return @LFN;
  }
  else {
    #my $path    = $self->GetAbsolutePath(shift);
    #my $entry   = $self->{DATABASE}->existsEntry($path);
    #$entry or $self->info("di: `$path': No such file or directory", 11, 1) and return;
    #$self->info("It works from the TRUNK also .. yipeeeee!!!! ");
    #$self->info("Checking the number of entries of $path");
    my (@LFN) = $self->{DATABASE}->getNumEntryIndexes();
    my $num_tables = @LFN/2;
    for (my $i=0; $i<$num_tables; $i++)
    {
      $self->info("L$LFN[$i]L has $LFN[$i+$num_tables] number of entries.");
    }
    return @LFN;
  }
}


sub f_populate_HELP {
  return "Populates a given directory with sub-directories and finally with files
Usage:
\t populate <dir> num_subdirs1 num_subdirs2 ..... num_files <dir>
";
}

sub f_populate {
  my $self    = shift;
  my $path    = $self->GetAbsolutePath($_[0]);
  $self->info("PATH::: `$path'");
  my $entry   = $self->{DATABASE}->existsEntry($path);
  $entry or $self->info("Directory doesnt exist .. Creating One .. !! '@_' \n". $self->f_populate_HELP()) 
         and $self->f_mkdir("sp", $path);
  my @stack_list= ();
  my $len = 0;
  my $num_files = 0;
  my $total_files = 0;
  my $num_dirs = 0;
  my $base = 0;
  push @stack_list, [@_];

  while(@stack_list) {
    my $temp_ref = shift @stack_list;
    my @temp = @$temp_ref;
    $self->{LOGGER}->silentOn();
    $self->info(Dumper(@temp));
    $self->info("Extracting the elements from stack :: @temp");
    $self->info("Some values :: @temp");
    $len = scalar(@temp); 
    
    if($len ==2) {
      #touch files
      $base = $temp[0];
      $num_files = $temp[1];
      if ($num_files =~ s/^\+//) {
          $num_files = int(rand($num_files))+1;
      }
      for(my $i=0; $i < $num_files; $i++)
      {
          my $fn = $base."/File".$i; 
          $self->f_touch(-1,$fn);
          $self->info("Touched File:: $fn");
          $total_files++;
	    }
    } elsif ($len>2) {
      #make directories 
      my $dir_name = "A";
      $base = shift @temp;
      $num_dirs = shift @temp;
      if ($num_dirs =~ s/^\+//) {
          $num_dirs = int(rand($num_dirs))+1;
      }
      for(my $i=0; $i < $num_dirs; $i++)
      {
          my @temp1=@temp;
          my $base_dir = $base."/".$dir_name; 
          $self->f_mkdir("", $base_dir);
          $self->info("Directory created :: $base_dir");
          $dir_name++;
          unshift @temp1,$base_dir;
          push @stack_list, [ @temp1 ];
	    }
      #Recurse through the directories
    } else {
      $self->info("Warning ... length=1..Not possible");
      }
      
    $self->{LOGGER}->silentOff();
  }
  $self->info("Summary ::");
  $self->info("Populating in the directory :: $path");
  $self->info("Total number of files populated. :: $total_files");
  return 1;
}

=usingRecursion
sub f_populate {
  my $self    = shift;
  my $path    = $self->GetAbsolutePath(shift);
  $self->info("I! ::: @_ ");
  my @param=@_;
  my $nparam=@_;
  $self->info("PATH::: `$path'");
  my $entry   = $self->{DATABASE}->existsEntry($path);
  $entry or $self->info("Error: unknown options: '@_' \n". $self->f_populate_HELP()) 
         and return;
  my $base = $path;
  my $num_dirs = $param[0];
  my $num_files = $param[$nparam-1];
  my $depth = $nparam-1;
  my $depth_fix = $depth;
  sub rmkdir()
  {
    my($base, $num_dirs, $depth,$num_files,$depth_fix) = @_;
    $self->f_mkdir("", $base);
    #if depth = 0, no more subdirectories need to be created
    if($depth == 0)
    {
        for(my $i=0; $i < $num_files; $i++)
        {
          my $fn = $base."/File".$i; 
          $self->f_touch(-1,$fn);
          $self->info("Touched File:: $fn");
	      }
        return 0;
    }
    #Recurse through the directories
    my $dir_name = "A";
    for(my $i = 0; $i < int(rand($num_dirs))+1; $i++)
    {
        &rmkdir("$base/$dir_name", $param[$depth_fix - $depth +1], $depth - 1,$num_files,$depth_fix);
        $dir_name++;
    }
  }
  &rmkdir( $path , $param[0], $depth, $num_files, $depth_fix );
  $self->info("Populating in the directory of $path");
  return 1;
}
=cut

sub f_nEntries_HELP {
  return "Gives the number of entries in the current directory (total no. of files + directories)
Usage:
\tnEntries <dir>
";
}

sub f_nEntries {
  my $self    = shift;
  my $path    = $self->GetAbsolutePath(shift);
  my $entry   = $self->{DATABASE}->existsEntry($path);
  $self->info("nEntries");
  $entry or $self->info("nEntries: `$path': No such file or directory", 11, 1) and return;
  my $rtables = $self->{DATABASE}->getTablesForEntry($path)
     or $self->info("Error getting the tables for '$path'")
     and return;
  my $num_entries =0;
  foreach my $rtable (@$rtables)
  {
        my $table_name = "L".$rtable->{tableName}."L";
        my $base = $rtable->{lfn};
        my $q1 = "SELECT COUNT(*) FROM ".$table_name.""; 
        $num_entries = $num_entries + $self->{DATABASE}->queryValue($q1);
  }
  $self->info("Total number of Entries in $path : $num_entries");
  return $num_entries;
}



=item C<whereis($options, $lfn)>

This subroutine returns the list of SE that have a copy of an lfn
Possible options:

=over


=item -l do not get the pfns (return only the list of SE)


=item -s tell the SE to stage the files


=item -r resolve links


=item -i return as well the information of the file


=back


=cut

sub f_whereisReadCache {
  my $self    = shift;
  my $options = shift;
  my $lfn     = shift;

  my $cache;

  if ($options =~ /c/) {
    if (!$self->{CONFIG}->{CACHE_SERVICE_ADDRESS}) {
      $self->info("Warning: we want to ask the cache service, but we don't know its address...");
      return;
    }
    $cache = "$self->{CONFIG}->{CACHE_SERVICE_ADDRESS}?ns=whereis&key=${options}_$lfn";
    my ($ok, @value) = AliEn::Util::getURLandEvaluate($cache, 1);
    if ($ok) {
      $self->info("Returning the value from the cache '@value'");
      return 1, @value;
    }
    $self->info("The cache didn't have the value");
  }
  return $cache;
}

sub f_whereisWriteCache {
  my $self  = shift;
  my $cache = shift;

  if ($cache) {
    $self->info("Setting the cache for $cache");
    my ($ok, $value) = AliEn::Util::getURLandEvaluate("$cache&value=" . Dumper([@_]));
  }
  return 1;
}

sub f_whereis_HELP {
  return "whereis: gives the PFN of a LFN or GUID.
Usage:
\twhereis [-lg] lfn

Options:
\t-l: Get only the list of SE (not the pfn)
\t-g: Use the lfn as guid
\t-r: Resolve links (do not give back pointers to zip archives)
\t-s: Silent
\t-c: Keep it in the cache
"
}

sub f_whereis {
  my $self    = shift;
  my $options = shift;
  my $lfn     = shift;
  my @failurereturn;
  my $failure;
  my $returnval;
  $failure->{"__result__"} = 0;

  push @failurereturn, $failure;
  my $silent = $self->{SILENT};
  $options =~ /s/ and $silent = 1;

  if (!$lfn) {
    $self->info("Error not enough arguments in whereis. " . $self->f_whereis_HELP());
    if   ($options =~ /z/) { return @failurereturn; }
    else                   { return }
  }

  my ($cache, @rest) = $self->f_whereisReadCache($options, $lfn);
  @rest and return @rest;

  my $guidInfo;
  my $info;

  if ($options =~ /g/) {
    $DEBUG and $self->debug(2, "Let's get the info from the guid");
    $guidInfo = $self->{DATABASE}->getAllInfoFromGUID({pfn => 1}, $lfn)
      or $self->info("Error getting the info of the guid '$lfn'")
      and return;
    $info = $guidInfo;
  } else {
    $lfn = $self->GetAbsolutePath($lfn);
    my $permFile = $self->checkPermissions('r', $lfn) or return;
    $info = $self->{DATABASE}->getAllExtendedInfoFromLFN($lfn)
      or $self->info("Error getting the info from '$lfn'")
      and return;
    $info->{guidInfo}
      or $self->info("That lfn is not associated with a guid")
      and return;
    $guidInfo = $info->{guidInfo};
  }

  ($guidInfo and $guidInfo->{pfn})
    or $self->info("Error getting the data from $lfn")
    and return;
  my @SElist = @{$guidInfo->{pfn}};
  $silent or $self->info("The file $lfn is in");
  use Data::Dumper;

  if ($options =~ /r/) {
    $DEBUG and $self->debug(2, "We are supposed to resolve links");

    my @realSE = ();
    my @pfns;
    my @allReal;
    foreach my $entry (@SElist) {
      $self->debug(1, "What do we do with $entry  ($entry->{pfn} and $entry->{seName} }??");
      if ($entry->{pfn} =~ m{^guid://[^/]*/([^\?]*)(\?.*)?$}) {
        my $anchor = $2 || "";

        $DEBUG and $self->debug(2, "We should check the link $1!!");
        my @done = $self->f_whereis("grs", $1)
          or $self->info("Error doing the where is of guid '$1'")
          and return;
        while (@done) {
          my ($se, $pfn) = (shift @done, shift @done);
          grep (/^$se$/, @realSE) or push @realSE, $se;
          $pfn =~ /^auto$/ or push @pfns, "$pfn$anchor";
          push @allReal, {seName => $se, pfn => "$pfn$anchor"};
        }
      } else {
        my $seName = $entry->{seName} || $entry->{sename};
        grep (/^$seName$/, @realSE) or push @realSE, $seName;
        push @allReal, $entry;
      }
    }
    $info->{REAL_SE}  = \@realSE;
    $info->{REAL_PFN} = \@pfns;
    @SElist           = @allReal;
    $silent
      or $self->info("The file is really in these SE: @{$info->{REAL_SE}}");

  }

  if ($options =~ /t/) {
    $DEBUG and $self->debug(2, "Let's take a look at the transfer methods");
    my @newlist;
    foreach my $entry (@SElist) {
      my $seName = $entry->{seName} || $entry->{sename};
      if ($seName eq "no_se") {

        # zip files have 'no_se' set, so we need to add this 'virtual' SE anyway
        push @newlist, $entry;
      } else {

        # non-zip files have to be checked for the required protocols
        push @newlist, $self->checkIOmethods($entry, @_);
      }
    }
    @SElist = @newlist;
  }

  my @return = ();
  foreach my $entry (@SElist) {
    my $seName = $entry->{seName} || $entry->{sename};
    my ($se, $pfn) = ($seName, $entry->{pfn} || "auto");
    $silent or $self->info("\t\t SE => $se  pfn =>$pfn\n", undef, 0);
    if ($options !~ /l/) {
      if ($options =~ /z/) {
        push @return, {se => $se, guid => $guidInfo->{guid}, pfn => $pfn};
      } else {
        push @return, $se, $pfn;
      }
    } else {
      if ($options =~ /z/) {
        push @return, {se => $se};
      } else {
        push @return, $se;
      }
    }
  }
  $options =~ /i/ and @return = $info;
  $self->f_whereisWriteCache($cache, @return);

  return @return;
}

sub getIOProtocols {
  my $self   = shift;
  my $seName = shift;

  my $cache = AliEn::Util::returnCacheValue($self, "io-$seName");
  if ($cache) {
    $DEBUG and $self->debug(2, "$$ Returning the value from the cache (@$cache)");
    return $cache;
  }
  my $protocols =
    $self->{DATABASE}
    ->queryValue("select seiodaemons from SE where upper(seName)=upper(?)", undef, {bind_values => [$seName]});
  my @protocols = split(/,/, $protocols);
  AliEn::Util::setCacheValue($self, "io-$seName", [@protocols]);
  $DEBUG and $self->debug(2, "Giving back the protocols supported by $seName (@protocols)");
  return \@protocols;
}

sub getStoragePath {
  my $self   = shift;
  my $seName = shift;

  my $cache = AliEn::Util::returnCacheValue($self, "prefix-$seName");
  if ($cache) {
    $DEBUG and $self->debug(2, "$$ Returning the value from the cache ($cache)");
    return $cache;
  }
  my $storagepath =
    $self->{DATABASE}
    ->queryValue("select seStoragePath from SE where seName=?", undef, {bind_values => [$seName]});
  if ((!defined $storagepath) || ($storagepath eq "")) {
    $storagepath = "/";
  }
  AliEn::Util::setCacheValue($self, "prefix-$seName", $storagepath);
  $DEBUG and $self->debug(2, "Returning the storagepath supported by $seName ($storagepath)");
  return $storagepath;
}

sub createFileName {
  my $self   = shift;
  my $seName = shift or return;
  my $guid   = (shift or 0);
  my $prefix = shift || $self->getStoragePath($seName);
  my $filename;
  if (!$guid) {
    $guid = $self->{GUID}->CreateGuid();
    if (!$guid) {
      $self->{LOGGER}->error("File", "cannot create new guid");
      return;
    }
  }
  $filename = sprintf "%s/%02.2d/%05.5d/%s", $prefix, $self->{GUID}->GetCHash($guid), $self->{GUID}->GetHash($guid),
    $guid;
  $filename =~ s{/+}{/}g;
  return ($filename, $guid);
}

sub createTURLforSE {
  my $self = shift;
  my $se   = shift;
  my $guid = (shift or 0);

  my $protocols = $self->getIOProtocols($se)
    or $self->info("Error getting the IO protocols of $se")
    and return;

  my ($newpath, $newguid) = $self->createFileName($se, $guid)
    or return;
  return ("$$protocols[0]/$newpath", $newpath);
}

sub createDefaultUrl {
  my $self = shift;
  my $se   = shift;
  my $guid = shift;
  my $size = shift;
  my $prefix =
    $self->{DATABASE}->queryValue(
    'select concat(method,concat(\'/\',mountpoint)) from SE_VOLUMES where freespace>? and upper(sename)=upper(?)',
    undef, {bind_values => [ $size, $se ]});
  if (!$prefix) {
    $self->info("There is no space in '$se' to put that file (size $size)!!", 1);
    return;
  }
  $self->info("So far so good: $prefix (and $guid)");
  my ($filename, $nguid) = $self->createFileName($se, $guid, "/");
  return ("$prefix$filename", $nguid);

}

sub createFileUrl {
  my $self       = shift;
  my $se         = shift;
  my $clientprot = shift;
  my $guid       = (shift or 0);

  my $protocols = $self->getIOProtocols($se)
    or $self->info("Error getting the IO protocols of $se")
    and return;
  my $selectedprotocol = 0;

  foreach (@$protocols) {
    if ($_ =~ /^$clientprot/) { $selectedprotocol = $_; last; }
  }

  $selectedprotocol
    or $self->info("The client protocol '$clientprot' could not be found in the list of supported protocols of se $se")
    and return;

  my ($newpath, $newguid) = $self->createFileName($se, $guid)
    or return;
  return ("$selectedprotocol/$newpath", $newguid, $se);
}

sub checkIOmethods {
  my $self    = shift;
  my $entry   = shift;
  my @methods = @_;

  my $protocols = $self->getIOProtocols($entry->{seName})
    or $self->info("Error getting the IO protocols of $entry->{seName}")
    and return;

  if (@methods) {
    $DEBUG
      and
      $self->debug(2, "The client supports @methods. Let's remove from @$protocols the ones that are not supported");
    my @newProtocols;
    foreach my $method (@methods) {
      push @newProtocols, grep (/^$method:/i, @$protocols);
    }
    $DEBUG and $self->debug(2, "Now we have @newProtocols");
    $protocols = \@newProtocols;
  }
  my @list;
  foreach my $method (@$protocols) {
    $DEBUG and $self->debug(2, "Putting $method");
    my $item = {};
    foreach (keys %$entry) {
      $item->{$_} = $entry->{$_};
    }
    if (!($item->{pfn} =~ /guid:\/\//)) {
      if ($method !~ /\/$/) {
        $item->{pfn} =~ s{^[^:]*://[^/]*}{$method/}i;
      } else {
        $item->{pfn} =~ s{^[^:]*://[^/]*}{$method}i;
      }
    }

    push @list, $item;
  }
  return @list;
}

sub checkFileQuota {
#######
## return (0,message) for normal error
## return (-1,message) for error that should throw access exception. Consequence is all
##                     remaining write accesses will be dropped, as they will fail anyway.
##
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("In checkFileQuota user is not specified.\n")
    and return (-1, "user is not specified.");
  my $size = shift;
  (defined $size) and ($size ge 0)
    or $self->{LOGGER}->error("In checkFileQuota invalid file size (undefined or negative).\n")
    and return (-1, "size is not specified.");
  my $count = shift || 1;

  my $db    = $self->{DATABASE};
  my $array = $db->queryRow(
"SELECT nbFiles, totalSize, maxNbFiles, maxTotalSize, tmpIncreasedNbFiles, tmpIncreasedTotalSize FROM FQUOTAS WHERE "
      . $db->reservedWord("user") . "=?",
    undef,
    {bind_values => [$user]})
    or $self->{LOGGER}->error("Failed to get data from the FQUOTAS quota table.")
    and return (0, "Failed to get data from the FQUOTAS quota table. ");
  $array
    or $self->{LOGGER}->error("There's no entry for user $user in the FQUOTAS quota table.")
    and return (-1, "There's no entry for user $user in the FQUOTAS quota table.");

  my $nbFiles               = ($array->{'nbFiles'}               || 0);
  my $maxNbFiles            = ($array->{'maxNbFiles'}            || 0);
  my $tmpIncreasedNbFiles   = ($array->{'tmpIncreasedNbFiles'}   || 0);
  my $totalSize             = ($array->{'totalSize'}             || 0);
  my $maxTotalSize          = ($array->{'maxTotalSize'}          || 0);
  my $tmpIncreasedTotalSize = ($array->{'tmpIncreasedTotalSize'} || 0);
  
  $self->info("In checkFileQuota for user: $user, request file size:$size request file count:$count -- (nF = $nbFiles/$maxNbFiles and nS = $totalSize/$maxTotalSize)");

  $DEBUG
    and $self->debug(
    1, "size: $size nbFile: $nbFiles/$tmpIncreasedNbFiles/$maxNbFiles 
              nbFile: $nbFiles/$tmpIncreasedNbFiles/$maxNbFiles
       totalSize: $totalSize/$tmpIncreasedTotalSize/$maxTotalSize"
    );

  #Unlimited number of files
  if ($maxNbFiles == -1) {
    $self->info("Unlimited number of files allowed for user ($user)");
  } else {
    if ($nbFiles + $tmpIncreasedNbFiles + $count > $maxNbFiles) {
      $self->info("Uploading file for user ($user) is denied - number of files quota exceeded ($maxNbFiles).");
      return (-1, "Uploading file for user ($user) is denied - number of files quota exceeded ($maxNbFiles).");
    }
  }

  #Unlimited size for files
  if ($maxTotalSize == -1) {
    $self->info("Unlimited file size allowed for user ($user)");
  } else {
    if ($size + $totalSize + $tmpIncreasedTotalSize > $maxTotalSize) {
      $self->info("Uploading file for user ($user) is denied, file size ($size) - total file size quota exceeded.");
      return (-1, "Uploading file for user ($user) is denied, file size ($size) - total file size quota exceeded.");
    }
  }

#$self->{PRIORITY_DB}->do("update PRIORITY set tmpIncreasedNbFiles=tmpIncreasedNbFiles+1, tmpIncreasedTotalSize=tmpIncreasedTotalSize+$size where user LIKE  '$user'") or $self->info("failed to increase tmpIncreasedNbFile and tmpIncreasedTotalSize");

  $self->info("In checkFileQuota $user: Allowed");
  return (
    1, undef,
    ($size + $totalSize + $tmpIncreasedTotalSize) / $maxTotalSize,
    ($nbFiles + $tmpIncreasedNbFiles) / $maxNbFiles
  );
}

sub fquota_list {
  my $self    = shift;
  my $options = {};
  @ARGV = @_;
  Getopt::Long::GetOptions($options, "silent", "unit=s")
    or $self->info("Error checking the options of fquota list", 1)
    and return;
  @_ = @ARGV;

  #Default unit - Megabyte
  my $unit  = "M";
  my $unitV = 1024 * 1024;

  $options->{unit} and $unit = $options->{unit};
  ($unit !~ /[BKMG]/)
    and $self->info("unknown unit. use default unit: Mega Byte")
    and $unit = "M";
  ($unit eq "B") and $unitV = 1;
  ($unit eq "K") and $unitV = 1024;
  ($unit eq "M") and $unitV = 1024 * 1024;
  ($unit eq "G") and $unitV = 1024 * 1024 * 1024;

  my $user = (shift || "%");
  my $whoami = $self->{ROLE};

  # normal users can see their own information
  if (($whoami !~ /^admin(ssl)?$/) and ($user eq '%')) {
    $user = $whoami;
  }

  my $usersuffix = $self->{DATABASE}->reservedWord("user") . " = '$user' ";
  ($user eq '%') and $usersuffix = $self->{DATABASE}->reservedWord("user") . " like '%'";

  if (($whoami !~ /^admin(ssl)?$/) and ($user ne $whoami)) {
    $self->info("Not allowed to see other users' quota information", 1);
    return;
  }

  my $result =
    $self->{DATABASE}->query("SELECT "
      . $self->{DATABASE}->reservedWord("user")
      . ", nbFiles, maxNbFiles, totalSize, maxTotalSize, tmpIncreasedNbFiles, tmpIncreasedTotalSize FROM FQUOTAS where $usersuffix"
    )
    or $self->info("Failed to getting data from FQUOTAS table", 1)
    and return -1;
  $result->[0]
    or $self->info("User $user does not exist in the FQQUOTAS table", 1)
    and return -1;

  my $cnt = 0;
  my $printout =
    sprintf "\n------------------------------------------------------------------------------------------\n";
  $printout .= sprintf "            %12s    %12s    %42s\n", "user", "nbFiles", "totalSize($unit)";
  $printout .= sprintf "------------------------------------------------------------------------------------------\n";
  foreach (@$result) {
    $cnt++;
    my $totalSize    = ($_->{'totalSize'} + $_->{'tmpIncreasedTotalSize'}) / $unitV;
    my $maxTotalSize = $_->{'maxTotalSize'} / $unitV;
    ##Changes for unlimited file size
    if ($_->{'maxTotalSize'} == -1) {
      $maxTotalSize = -1;
    }
    $printout .= sprintf " [%04d. ]   %12s     %5s/%5s           \t %.4f/%.4f\n", $cnt, $_->{'user'},
      ($_->{'nbFiles'} + $_->{'tmpIncreasedNbFiles'}), $_->{'maxNbFiles'}, $totalSize, $maxTotalSize;
  }
  $printout .= sprintf "------------------------------------------------------------------------------------------\n";
  $self->info($printout);
}

sub fquota_set_HELP {
  return "Usage:
  fquota set <user> <field> <value> - set the user quota
                                      (maxNbFiles, maxTotalSize(Byte))
                                      use <user>=% for all users\n";
}

sub fquota_set {
  my $self  = shift;
  my $user  = shift or $self->info($self->fquota_set_HELP(), 1) and return;
  my $field = shift or $self->info($self->fquota_set_HELP(), 1) and return;
  my $value = shift;
  (defined $value) or $self->info($self->fquota_set_HELP(), 1) and return;

  if ($field !~ /(maxNbFiles)|(maxTotalSize)/) {
    $self->info("Wrong field name! Choose one of them: maxNbFiles, maxTotalSize", 1);
    return;
  }

  my $set = {};
  $set->{$field} = $value;
  my $db = $self->{DATABASE};
  my $done = $db->update("FQUOTAS", $set, $db->reservedWord("user") . "= ?", {bind_values => [$user]});

  $done or $self->info("Failed to set the value in the FQUOTAS table", 1) and return -1;

  if ($done eq '0E0') {
    ($user ne "%") and $self->info("User '$user' not exist.", 1) and return -1;
  }

  $done and $self->fquota_list("$user");
}

return 1;
