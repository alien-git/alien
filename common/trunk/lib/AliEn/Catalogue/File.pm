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
use vars qw ($DEBUG);

$DEBUG=0;



#
#This function adds an entry to the catalog
#
# Possible options:
#          -f: create the entry even if the pfn is empty
#
sub f_registerFile {
  my $self = shift;
  $DEBUG and $self->debug(1, "In the catalogue, doing registerFile(".join(",", map {defined $_ ? $_ : ""} @_ ).")");
  my $opt = shift;
  my $file = shift;
  my $size = shift;
  my $se   = shift;
  my $guid = shift ;
  my $perm = (shift or $self->{UMASK});
  my $selist= shift || 0;
  my $md5  = shift;
  my $pfn =shift ||"";



  if (! defined $size ) {
    print STDERR
      "Error in add: not possible to register the file in the catalogue. Not enough arguments. \nUsage register <lfn> <size> <storage element>\n";
    return;
  }

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

  # First, we check that we have permission in that directory
#  my $tempname = $self->f_dirname($file);

  my $permLFN=$self->checkPermissions( 'w', $file ) or  return;
  $self->existsEntry($file, $permLFN) and
    $self->{LOGGER}->error("File", "file $file already exists!!",1) 
      and return;

  # Now, insert it into D0, and in the table
  my $basename   = $self->f_basename($file);
  my $insert={lfn=>$file,  perm=>$perm,  owner=>$self->{ROLE},
	      gowner=>$self->{MAINGROUP}, size =>$size,guid=>$guid,  };
  $se and $insert->{se}=$se;
  $md5 and $insert->{md5}=$md5;
  $pfn and $insert->{pfn}=$pfn;
  $selist and $insert->{seStringList}=$selist;

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
  $DEBUG and $self->debug(1, "In the catalogue, doing registerFile(".join(",", map {defined $_ ? $_ : ""} @_ ).")");
#  my $opt = shift;
  my $options=shift;
  my $directory=shift;
  my $files = shift;
  
  $directory=$self->f_complete_path($directory);

  # First, we check that we have permission in that directory
#  my $tempname = $self->f_dirname($file);

  my $permLFN=$self->checkPermissions( 'w', $directory ) or  return;
  $self->isDirectory($directory, $permLFN) or
    $self->info("$directory is not a directory!!",1) 
      and return;

  my $ok=1;
  my @insert;
  my $list="";
  foreach my $entry (@$files){
    if (! $entry->{guid}){
      $DEBUG and $self->debug(2, "Getting a new GUID for this file");
      $entry->{guid}=$self->{GUID}->CreateGuid();
      $entry->{guid} and $DEBUG and $self->debug(2,"Got $entry->{guid}");
    }
    $entry->{lfn}=~ m{/} and
      $self->info("The entry $entry->{lfn} cannot be inserted in bulk (there can't be any directories") and return;
    $entry->{lfn}="$directory/$entry->{lfn}";
    # Now, insert it into D0, and in the table
    my $insert={lfn=>$entry->{lfn},  perm=>$self->{UMASK}, 
		owner=>$self->{ROLE}, gowner=>$self->{MAINGROUP},
		size =>$entry->{size},	guid=>$entry->{guid},  };

    for my $field ("se", "md5", "selist", "seStringlist", "pfn", "pfns", "type"){
      $entry->{$field} and $insert->{$field}=$entry->{$field};
    }
    $entry->{user} and $insert->{owner}=$insert->{gowner}=$entry->{user};

    $list.="$entry->{lfn} ";
    push @insert, $insert;
  }
  $self->{DATABASE}->createFile($options, @insert)
    or print STDERR "Error inserting entry into directory\n"
      and return;

  $self->info("Files $list inserted in the catalog");
  return 1;
}
# Returns the list of SE that have this file
# Possible options:   -l Give only the list of SE (not pfn)
#                     -g return also the file info
#
#
sub f_whereisFile {
  my $self=shift;
  my $options=shift;
  my $lfn=shift;

  $lfn = $self->f_complete_path($lfn);

  my $permFile=$self->checkPermissions( 'r', $lfn, 0, 1 )  or  return;
  if (! $self->isFile($lfn, $permFile->{lfn}) ) {
    $self->{LOGGER}->error("File", "file $lfn doesn't exist!!",1);
    return;
  }
  if ($options =~ /g/){
    my $ret={};
    $ret->{selist}   = $self->{DATABASE}->getSEListFromFile($lfn, $permFile->{seStringlist});
    $ret->{fileinfo} = $permFile;
    return $ret;
  }else {
    return $self->{DATABASE}->getSEListFromFile($lfn, $permFile->{seStringlist});
  }
}
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
  my $args=join(" ", @_);

  my $size="";
  my $se="";
  my $update = {};

  $args =~ s/-?-s(ize)?[\s=]+(\S+)// and $update->{size}=$2;
  $args =~ s/-?-s(e)?[\s=]+(\S+)// and $update->{se}=$2;
  $args =~ s/-?-g(uid)?[\s=]+(\S+)// and $update->{guid}=$2;
  $args =~ s/-?-m(d5)?[\s=]+(\S+)// and $update->{md5}=$2;
  $args =~ s/-?-p(fn)?[\s=]+(\S+)// and $update->{pfn}=$2;

  my $message="";

  (keys %$update) or $message="You should update at least one of the fields";
  $args =~ /^\s*$/ or $message="Argument $args not known";

  $message
    and print STDERR "Error:$message\n".$self->f_updateFile_HELP()
      and return;

  $file = $self->f_complete_path($file);

  my $permLFN= $self->checkPermissions( 'w', $file )
    or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }

  $DEBUG and $self->debug(2, "Ready to do the update");

  $self->{DATABASE}->updateFile($file, $update, )
    or $self->info( "Error doing the update of $file",11)
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
    or print STDERR "Error: not enough arguments in f_getGuid!\n"
      and return;

  $file = $self->f_complete_path($file);
  my $info=$self->checkPermissions( 'r', $file,0, 1 )
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
    or print STDERR "Error: not enough arguments in f_getGuid!\n"
      and return;

  $file = $self->f_complete_path($file);
  my $permLFN=$self->checkPermissions( 'r', $file, 0, 1 )
    or return;
  if (! $self->isFile($file, $permLFN->{lfn}) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }
  my $guid=$permLFN->{guid};
  my $md5=$permLFN->{md5};

  if ($options=~ /g/ ){
    $options =~ /s/ or 
      $self->info("$md5\t$file (guid $guid)", undef,0);
    return {md5=>$md5, guid=>$guid};
  }
  $options =~ /s/ or $self->info("$md5\t$file", undef,0);
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

  my $silent = $options =~ /s/;
  my $original = $options =~ /o/;
  $self->{SILENT} and $silent = 1;

  my $logger="error";
  $silent and $logger="debug";

  ($file)
    or print STDERR "Error: not enough arguments in whereis\nUsage: whereis [-o] <file>\n"
      and return;

  $file = $self->f_complete_path($file);

  my $permLFN=$self->checkPermissions( 'r', $file )  or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }

  $DEBUG and $self->debug(2, "Ready to get the list of SE");
  my $ref=$self->{DATABASE}->getSEListFromFile( $file) or return;
  $self->info( "Getting the SE list @$ref");
  return $ref;
}

sub f_addMirror_HELP{
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
  my $pfn  =shift || "";

  my $opt={};
  @ARGV=@_;
  Getopt::Long::GetOptions($opt,  "g", "md5=s", "c") or 
      $self->info("Error parsing the arguments to addMirror") and return;;
  @_=@ARGV;

 # my $md5 =shift;
  $file or $self->info( "Error not enough arguments in addMirror".$self->f_addMirror_HELP(),1) and return;
  $file = $self->f_complete_path($file);

  my $permLFN=$self->checkPermissions( 'w', $file )  or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }
  if ($opt->{c} and !$opt->{md5}){
    $opt->{md5}=AliEn::MD5->new($pfn);
    $opt->{md5} or $self->info("Error getting the md5sum of '$pfn'") and return;
  }
  $self->{DATABASE}->insertMirrorFromFile($file, $se, $pfn, $opt->{md5}) or return;

  $self->info("File '$file' has a mirror in '${se}'");
  return 1;
}

sub f_deleteMirror_HELP{
  return "deleteMirror: 
Removes a replica of a file from the catalogue
Uage:
\tdeleteMirror [-g] <lfn> <se> [<pfn>]

Options:
   -g: the lfn is a guid
"

}


sub f_deleteMirror {
  my $self = shift;
  my $options=shift;
  $self->info("Deleting a mirror @_");

  my $file = shift;
  my $se   = shift;

  $file or $self->info( "Error not enough arguments in deleteMirror\n" . $self->f_deleteMirror_HELP())
    and return ;

  if ($options =~ /g/){
    $self->info("Removing the replica from the guid directly");
    $self->{DATABASE}->deleteMirrorFromGUID( $file, $se,@_) or 
      $self->info( "Error removing the mirror of $file in $se") and return;
    return 1;
  }
  $file = $self->f_complete_path($file);

  my $permLFN= $self->checkPermissions( 'w', $file ) or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }

  $self->{DATABASE}->deleteMirrorFromLFN( $file, $se,@_) or 
    $self->info( "Error removing the mirror of $file in $se") and return;
  $self->info("Mirror from ${se} removed");
  return 1;
}

sub f_setExpired_HELP{
  return "setExpired: Sets the expire date for an entry in the catalogue. When that date arrives, all the entries in 'replica' SE will be deleted. If there are no entries in long term SE, the lfn will be renamed to 'lfn.expired'. 
Usage:
\t\tsetExpired <seconds> <lfn> [<lfn>+]

To see the expire date of a file, do 'ls -e'
";
}
sub f_setExpired{
  my $self=shift;
  my $seconds=shift;

  
  @_ or $self->info("Error: not enough arguments". $self->f_setExpired_HELP()) and return;

  while (@_){
    my $file=shift;

    $file = $self->GetAbsolutePath($file);

    my $permLFN=$self->checkPermissions( "w", $file)
      or return;

    if (! $self->isFile($file, $permLFN) ) {
      $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
      return;
    }
    $self->debug(2, "Let's put the expiration time of $file");
    $self->{DATABASE}->setExpire($file, $seconds) or return;

    $self->info("The file $file will expire in $seconds seconds");
  }
    return 1;

}

sub f_ln {
  my $self = shift;
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
  my $filehash = $self->checkPermissions("r",$source,0, 1);
  $filehash 
    or $self->{LOGGER}->error("File", "ERROR: checkPermission failed for $source") 
    and return;
  $filehash = $self->checkPermissions("w",$target,0, 1);
  $filehash 
    or $self->{LOGGER}->error("File", "ERROR: checkPermission failed for $target")
    and return;
  return $self->{DATABASE}->{LFN_DB}->softLink($source,$target);
}

# This subroutine is used to find all the metadata of a file that is going
# to be copied. The result is giving back in the variable $todoMetadata
#
sub getCPMetadata{
  my $self=shift;
  my $source=shift;
  my $targetDir=shift;
  my $targetName=shift;
  my $todoMetadata=shift;

  $targetDir=~ s{/?$}{/};
  $self->info("We are supposed to copy also the metadata of $source to $targetDir");
  my $sourceDir=$source;
  $sourceDir=~ s{/[^/]*$}{/};
  my $tags=$self->f_showTags("allr", $sourceDir);
  my $entries={};
  foreach my $tag (@$tags){
    #making sure that the destination has all the tags#
    $self->debug(1, "We should add the tag $tag->{tagName}");
    $self->info("Adding tag $tag->{tagName}....");
    $self->f_addTag($targetDir, $tag->{tagName}, $tag->{tagName}, $tag->{path}) 
      or $self->info("Error defining the metadata $tag->{tagName}") 
      and return;
    my $tableName= $self->{DATABASE}->getTagTableName($targetDir, $tag->{tagName});
    $tableName  or $self->info("Error getting the name of the table") 
      and next;
    #let's put the entries 
    my @list=();
    $entries->{$tag->{tagName}} and push @list, @{$entries->{$tag->{tagName}}};
    $self->info( "Getting the metadata for $sourceDir and $tag->{tagName}");
    my ($columns, $info)= $self->f_showTagValue("",$sourceDir, $tag->{tagName});
    $self->info( "Getting the extra metadata for $sourceDir and $tag->{tagName}");
    my ($columns2, $info2)= $self->f_showTagValue("r",$sourceDir, $tag->{tagName});

    $self->info("Processing metadata values for $tag");
    foreach my $entry (@$info, @$info2){
      my $toInsert={file=>"$targetName"};
      if (! $targetName){
        $toInsert->{file}=$entry->{file};
        if ($toInsert->{file} =~ s/^$source//) {
          $toInsert->{file}="$targetDir$toInsert->{file}";
        } else {
          $self->info("The file doesn't start with $source");
          $toInsert->{file}=$targetDir;
        }
        $self->info( "Since we are copying a directory, the info is from $toInsert->{file} (from $entry->{file}, $source and $targetDir");
        my $tempDir=$toInsert->{file};
        $tempDir =~ s/[^\/]*$//;
        $self->f_addTag($tempDir, $tag->{tagName}, $tag->{tagName}, $tag->{path}) 
          or $self->info("Error creating the tag $tag->{tagName} in $tempDir") 
          and return;
      }
      foreach my $key (keys %$entry){
        $key =~ /^(entryId)|(file)$/ and next;
        $toInsert->{$key}=$entry->{$key};
      }
      push @list, $toInsert;
    }
    @list or $self->info("For the tag $tag->{tagName}, there wasn't any metadata. Ignoring it") and next;
    $entries->{$tableName}=\@list;
  }

  $self->info("Adding metadata values for $targetName....");
  foreach my $key (keys %$entries){
    my @list=();
    $todoMetadata->{$key} and push @list, @{$todoMetadata->{$key}};
    push @list, @{$entries->{$key}};
    $todoMetadata->{$key}=\@list;
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
    $self->f_addTagValue($targetName,$tagName,@data) 
      or $self->{LOGGER}->error("Catalogue::File","Could not add tag value @data for tag $tagName on tag $targetName");
  }

  return $entries;
}

#
#Move files
#
sub f_mv {
  my $self = shift;
  my $options = shift;
  my $source = shift;
  my $target = shift;
  $source or $target 
    or $self->{LOGGER}->error("File", "ERROR: Source and/or target not specified") 
    and return;

  my $fullSource = $self->GetAbsolutePath($source);
  my $fullTarget = $self->GetAbsolutePath($target);
  $self->info("$fullSource ($source) --> $fullTarget ($target)");
  $self->isDirectory($fullSource)
    and $self->{LOGGER}->error("File", "ERROR: <$source> is a directory")
    and return;
  #Check quotas
  my $filehash = $self->checkPermissions("w",$fullTarget,0, 1);
  $filehash 
    or $self->{LOGGER}->error("File", "ERROR: checkPermission failed for $fullTarget")
    and return;
  $filehash = $self->checkPermissions("w",$fullSource,0, 1);
  $filehash 
    or $self->{LOGGER}->error("File", "ERROR: checkPermission failed for $fullSource")
    and return;
  #Do move
  my @returnVal = $self->{DATABASE}->{LFN_DB}->moveFile($fullSource,$fullTarget);
  #Manage metadata if option specified
  if($options=~/m/)
  {
    my $todoMetadata = {};
    my $targetDir = "$fullTarget";
    $targetDir=~ s{/[^/]*$}{/};
    $self->getCPMetadata($fullSource,$targetDir,$fullTarget,$todoMetadata);
  }
  return @returnVal
}

#
#Delete file from catalogue
#
sub f_removeFile {
  my $self = shift;
  my $options = shift;
  my $file = shift;
  my $silent = ($options =~ /s/);
  if(!$file)
  {
    ( $options =~ /s/ )
      or $self->{LOGGER}->error("File","Error in remove: not enough arguments\nUsage remove [-s] <path>\n
                                Options: -s : silent. Do not print error messages\n")
      and return;
  }
  #Check if file specified is a directory
  my $fullPath = $self->GetAbsolutePath($file);
  $self->isDirectory($fullPath) 
    and $self->{LOGGER}->error("File", "ERROR: $fullPath is a directory") 
    and return;
  #Check permissions
  my $filehash = $self->checkPermissions("w",$fullPath,0, 1);
  if (!$filehash) {
    $self->{LOGGER}->error("File", "Check permission on $fullPath failed");
    return;
  }
  return $self->{DATABASE}->{LFN_DB}->removeFile($fullPath,$filehash);
}

#
#Delete directory and all associated files from catalogue
#
sub f_rmdir {
  my $self = shift;
  my ( $options, $path ) = @_;
  my $deleteall = ( ( $options =~ /r/ ) ? 1 : 0 );
  my $message = "";
  ($path) or $message = "no directory specified";
  ( $path and $path eq "." )  and $message = "Cannot remove current directory";
  ( $path and $path eq ".." ) and $message = "Cannot remove parent directory.";
  $message and $self->{LOGGER}->error( "File", "Catalogue", "Error $message\nUsage: rmdir [-r] <directory>" )
    and return;
  #Check if path specifed is a file
  $path = $self->GetAbsolutePath( $path, 1 );
  unless($self->isDirectory($path)) {
    $self->{LOGGER}->error("File", "ERROR: $path is not a directory");
    return;
  }
  #Check permissions
  my $parentdir = $self->GetParentDir($path);
  my $filehash = $self->checkPermissions("w",$parentdir,0, 1);
  $filehash 
    or $self->{LOGGER}->error("File", "ERROR: checkPermissions failed on $parentdir")
    and return;
  $filehash = $self->checkPermissions("w",$path,0, 1);
  $filehash 
    or $self->{LOGGER}->error("File", "ERROR: checkPermsissions failed on $path")
    and return;
  return $self->{DATABASE}->{LFN_DB}->removeDirectory($path,$parentdir);
}



#
#returns the flags and the files of the input line
# (
sub Getopts {
  my $self = shift;
  my ( $word, @files, $flags );

  $flags = "";
  @files = ();

  foreach $word (@_) {
    if ( $word =~ /^-.*/ ) {
      $flags = substr( $word, 1 ) . $flags;
    }
    else {
      @files = ( @files, $word );
    }
  }
  return ( $flags, @files );
}

#
#touch file in catalogue
#
sub f_touch {
  my $self=shift;
  my $options=shift;
  my $lfn=shift 
    or $self->{LOGGER}->error("Catalogue::File","Error missing the name of the file to touch") 
    and return;
  $lfn = $self->GetAbsolutePath($lfn);
  my ($ok, $message) = $self->checkFileQuota( $self->{CONFIG}->{ROLE}, 0 );
  if($ok eq -1) {
    $self->{LOGGER}->error($message) 
      or return; 
  }
  #Insert file in catalogue
  $self->info("Inserting file $lfn");
  $self->f_registerFile($options, $lfn,0) 
    or $self->{LOGGER}->error("Catalogue::File","Could not touch file")
    and return;
  $self->info("$lfn successfully created") 
    and return 1;
}

sub f_du_HELP{
  return "Gives the disk space usge of a directory
Usage:
\tdu [-hf] <dir>

Options:
\t\t-h: Give the output in human readable format
\t\t-f: Count only files (ignore the size of collections)
";
}
sub f_du {
  my $self=shift;
  my $options=shift;
  my $path=$self->GetAbsolutePath(shift);
  my $entry=$self->{DATABASE}->existsEntry( $path);
  $entry or $self->info( "du: `$path': No such file or directory", 11,1) and return;
  $self->info( "Checking the disk space usage of $path");
  my $space=$self->{DATABASE}->getDiskUsage($entry, $options);
  my $unit="";
  if ($options=~ /h/){
    my @possible=("K", "M", "G", "T","P", "H");
    while (@possible  and $space>1024){
      $space=sprintf("%.2f",$space/1024);
      $unit=shift @possible;
    }
    
  }
  $self->info( "$path uses $space ${unit}bytes");

  return $space;
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


sub f_whereis_HELP {
  return "whereis: gives the PFN of a LFN or GUID.
Usage:
\twhereis [-lg] lfn

Options:
\t-l: Get only the list of SE (not the pfn)
\t-g: Use the lfn as guid
\t-r: Resolve links (do not give back pointers to zip archives)
\t-s: Silent
"
}

sub f_whereis{
  my $self=shift;
  my $options=shift;
  my $lfn=shift;
  my @failurereturn;
  my $failure;
  my $returnval;
  $failure->{"__result__"} = 0;

  push @failurereturn,$failure;
  my $silent=$self->{SILENT};
  $options=~ /s/ and $silent =1;

  if (!$lfn) {
    $self->info( "Error not enough arguments in whereis. ".$self->f_whereis_HELP());
    if ($options=~/z/) {return @failurereturn;} 
    else {return};
  }

  my $guidInfo;
  my $info;
  if ($options =~ /g/){
    $DEBUG and $self->debug(2, "Let's get the info from the guid");
    $guidInfo=$self->{DATABASE}->getAllInfoFromGUID({pfn=>1},$lfn)
      or $self->info("Error getting the info of the guid '$lfn'") and return;
    $info=$guidInfo;
  }else {
    $lfn = $self->GetAbsolutePath($lfn);
    my $permFile=$self->checkPermissions( 'r', $lfn  )  or  return;
    $info=$self->{DATABASE}->getAllExtendedInfoFromLFN($lfn)
      or $self->info("Error getting the info from '$lfn'") and return;
    $info->{guidInfo} or 
    $self->info("That lfn is not associated with a guid") and return;
    $guidInfo=$info->{guidInfo};
  }

  ($guidInfo and $guidInfo->{pfn}) 
    or $self->info("Error getting the data from $lfn") and return; 
  my @SElist=@{$guidInfo->{pfn}};
  $silent or $self->info("The file $lfn is in");


  if ($options =~ /r/){
    $DEBUG and $self->debug(2, "We are supposed to resolve links");

    my @realSE=();
    my @pfns;
    my @allReal;
    foreach my $entry (@SElist){
      $self->debug(1, "What do we do with $entry  ($entry->{pfn} and $entry->{seName} }??");
      if ($entry->{pfn} =~ m{^guid://[^/]*/([^\?]*)(\?.*)?$} ){
        my $anchor=$2 || "";

        $DEBUG and $self->debug(2,"We should check the link $1!!");
        my @done=$self->f_whereis("grs", $1)
          or $self->info("Error doing the where is of guid '$1'") and return;
        while (@done) {
          my ($se, $pfn)=(shift @done, shift @done);
          grep (/^$se$/, @realSE) or  push @realSE, $se;
          $pfn =~ /^auto$/ or push @pfns, "$pfn$anchor";
          push @allReal, {seName=>$se, pfn=>"$pfn$anchor"};
        }
      }else {
        grep (/^$entry->{seName}$/, @realSE) or push @realSE, $entry->{seName};
        push @allReal, $entry;
      }
    }
    $info->{REAL_SE}=\@realSE;
    $info->{REAL_PFN}=\@pfns;
    @SElist=@allReal;
    $silent or  
    $self->info("The file is really in these SE: @{$info->{REAL_SE}}");

  }

  if ($options =~ /t/){
    $DEBUG and $self->debug(2,"Let's take a look at the transfer methods");
    my @newlist;
    foreach my $entry (@SElist){
      if ($entry->{seName} eq "no_se") {
        # zip files have 'no_se' set, so we need to add this 'virtual' SE anyway
        push @newlist, $entry;
      } else {
        # non-zip files have to be checked for the required protocols
        push @newlist, $self->checkIOmethods($entry, @_);
      }
    }
    @SElist=@newlist;
  }

  my @return=();
  foreach my $entry (@SElist){
    my ($se, $pfn)=($entry->{seName}, $entry->{pfn} || "auto");
    $silent or $self->info("\t\t SE => $se  pfn =>$pfn\n", undef,0);
    if ($options !~ /l/){
      if ($options=~ /z/){
        push @return, {se=>$se, guid=>$guidInfo->{guid}, pfn=>$pfn};
      } else{
        push @return, $se, $pfn;
      }
    } else {
      if ($options=~ /z/){
        push @return, {se=>$se};
      } else{
        push @return, $se;
      }
    }
  }

  $options =~ /i/ and return $info;
  return @return;
}

sub getIOProtocols{
  my $self=shift;
  my $seName=shift;

  my $cache=AliEn::Util::returnCacheValue($self, "io-$seName");
  if ($cache) {
    $DEBUG and $self->debug(2, "$$ Returning the value from the cache (@$cache)");
    return $cache;
  }
  my $protocols=$self->{DATABASE}->{LFN_DB}->queryValue("select seiodaemons from SE where seName=?", undef, {bind_values=>[$seName]});
  my @protocols=split(/,/, $protocols);
  AliEn::Util::setCacheValue($self, "io-$seName", [@protocols]);
  $DEBUG and $self->debug(2, "Giving back the protocols supported by $seName (@protocols)");
  return \@protocols
}

sub getStoragePath{
  my $self=shift;
  my $seName=shift;

  my $cache=AliEn::Util::returnCacheValue($self, "prefix-$seName");
  if ($cache) {
    $DEBUG and $self->debug(2, "$$ Returning the value from the cache ($cache)");
    return $cache;
  }
  my $storagepath=$self->{DATABASE}->{LFN_DB}->queryValue("select seStoragePath from SE where seName=?", undef, {bind_values=>[$seName]});
  if ( (! defined $storagepath ) || ($storagepath eq "") ) {
      $storagepath="/";
  }
  AliEn::Util::setCacheValue($self, "prefix-$seName", $storagepath);
  $DEBUG and $self->debug(2, "Returning the storagepath supported by $seName ($storagepath)");
  return $storagepath
}

sub createFileName {
  my $self=shift;
  my $seName=shift or return;
  my $guid=(shift or 0);
  my $prefix=shift || $self->getStoragePath($seName);
  my $filename;
  if (!$guid) {
      $guid = $self->{GUID}->CreateGuid();
      if (!$guid) {
	  $self->{LOGGER}->error("File","cannot create new guid");
	  return;
      }
  }
  $filename = sprintf "%s/%02.2d/%05.5d/%s",$prefix,$self->{GUID}->GetCHash($guid),$self->{GUID}->GetHash($guid),$guid;
  $filename =~ s{/+}{/}g;
  return ($filename,$guid);
}


sub createTURLforSE{
  my $self = shift;
  my $se   = shift;
  my $guid = (shift or 0);

  my $protocols = $self->getIOProtocols($se)
      or $self->info("Error getting the IO protocols of $se") and return;

  my ($newpath,$newguid) = $self->createFileName($se,$guid)
      or return;
  return ("$$protocols[0]/$newpath",$newpath);
}


sub createDefaultUrl {
  my $self=shift;
  my $se=shift;
  my $guid=shift;
  my $size=shift;
  my $prefix=$self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryValue('select concat(method,"/",mountpoint) from SE_VOLUMES where freespace>? and sename=?', undef, {bind_values=>[$size, $se]});
  if (!$prefix){
    $self->info("There is no space in '$se' to put that file (size $size)!!",1);
    return;
  }
  $self->info("So far so good: $prefix (and $guid)");
  my ($filename, $nguid)=$self->createFileName($se,$guid, "/");
  return ("$prefix$filename", $nguid);
    
}

sub createFileUrl {
  my $self = shift;
  my $se   = shift;
  my $clientprot = shift;
  my $guid = (shift or 0);

  my $protocols = $self->getIOProtocols($se)
      or $self->info("Error getting the IO protocols of $se") and return;
  my $selectedprotocol=0;

  foreach (@$protocols) {
    if ( $_ =~ /^$clientprot/) { $selectedprotocol =$_; last;}
  }

  $selectedprotocol or $self->info("The client protocol '$clientprot' could not be found in the list of supported protocols of se $se") and return;
  
  my ($newpath,$newguid) = $self->createFileName($se,$guid)
      or return;
  return ("$selectedprotocol/$newpath",$newguid,$se);
}

sub checkIOmethods {
  my $self=shift;
  my $entry=shift;
  my @methods=@_;

  my $protocols=$self->getIOProtocols($entry->{seName})
    or $self->info("Error getting the IO protocols of $entry->{seName}") and return;

  if (@methods){
      $DEBUG and $self->debug(2, "The client supports @methods. Let's remove from @$protocols the ones that are not supported");
    my @newProtocols;
    foreach my $method (@methods){
      push @newProtocols, grep (/^$method:/i, @$protocols);
    }
      $DEBUG and $self->debug(2,"Now we have @newProtocols");
    $protocols=\@newProtocols;
  }
  my @list;
  foreach my $method (@$protocols){
    $DEBUG and $self->debug(2,"Putting $method");
    my $item={};
    foreach (keys %$entry){
      $item->{$_}=$entry->{$_};
    }
    if (! ($item->{pfn} =~ /guid:\/\//)) {
	if ( $method !~ /\/$/) {
	    $item->{pfn}=~ s{^[^:]*://[^/]*}{$method/}i;
	} else {
	    $item->{pfn}=~ s{^[^:]*://[^/]*}{$method}i;
	}
    }

    push @list, $item;
  }
  return @list;
}

return 1;
