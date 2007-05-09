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
    $entry->{se} and $insert->{se}=$entry->{se};
    $entry->{md5} and $insert->{md5}=$entry->{md5};
    $entry->{selist} and $insert->{se}=$entry->{selist};
    $entry->{seStringlist} and $insert->{seStringList}=$entry->{seStringlist};
    $entry->{user} and $insert->{owner}=$insert->{gowner}=$entry->{user};
    $entry->{pfn} and $insert->{pfn}=$entry->{pfn};
    $entry->{pfns} and $insert->{pfns}=$entry->{pfns};
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

  my $permFile=$self->checkPermissions( 'r', $lfn, undef, {RETURN_HASH=>1} )  or  return;
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

  $self->{SILENT} or print "File $file updated in the catalog\n";
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
  my $info=$self->checkPermissions( 'r', $file,undef, {RETURN_HASH=>1} )
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
  my $permLFN=$self->checkPermissions( 'r', $file, undef, {RETURN_HASH=>1} )
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


sub f_getByGuid {
  my $self = shift;
  $DEBUG and $self->debug(2, "In FileInterface getByGuid @_");
  my $options = shift || "";
  my $guid = shift;
  my @result;
  ($guid)
    or print STDERR "Error: not enough arguments in f_getByGuid!\n"
      and return;
  return $self->{DATABASE}->getLFNfromGUID($guid);
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

sub f_addMirror {
  my $self = shift;

  ( $self->{SILENT} ) or print "Adding a mirror @_\n";

  my $file = shift;
  my $se   = shift || $self->{CONFIG}->{SE_FULLNAME};
  my $pfn  =shift || "";
  my $md5 =shift;
  $file or $self->info( "Error not enough arguments in addMirror\nUsage:\n\t addMirror <lfn> <se>\n",1) and return;
  $file = $self->f_complete_path($file);

  my $permLFN=$self->checkPermissions( 'w', $file )  or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }
  if (!$md5){
    $md5=AliEn::MD5->new($pfn);
    $md5 or $self->info("Error getting the md5sum of '$pfn'") and return;
  }
  $self->{DATABASE}->insertMirrorFromFile($file, $se, $pfn, $md5) or return;
  $self->{SILENT}
    or print "File '$file' has a mirror in '${se}'\n";
  return 1;
}



sub f_deleteMirror {
  my $self = shift;

  ( $self->{SILENT} ) or print "Deleting a mirror @_\n";

  my $file = shift;
  my $se   = shift;

  $file or $self->info( "Error not enough arguments in addMirror\nUsage:\n\t addMirror <lfn> <se>\n",1) and return;

  $file = $self->f_complete_path($file);

  my $permLFN= $self->checkPermissions( 'w', $file ) or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }

  $self->{DATABASE}->deleteMirrorFromLFN( $file, $se,@_) or 
    $self->info( "Error removing the mirror of $file in $se") and return;
  $self->{SILENT}
    or print "Mirror from ${se} removed\n";
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
sub f_removeFile {
  my $self = shift;
  my $options = shift;
  my $file = shift;
  my $silent = ($options =~ /s/);
  
  if ( !$file ) {
    ( $options =~ /s/ )
      or print STDERR
	"Error in remove: not enough arguments\nUsage remove [-s] <path>\nOptions: -s : silent. Do not print error messages\n";
    return;
  }
  $file = $self->GetAbsolutePath($file);
#  my $parentdir = $self->GetParentDir($file);
  my $permLFN=$self->checkPermissions( "w", $file, $silent )
    or return;

  if (! $self->isFile($file, $permLFN) ) {
      if ($options =~ /f/) {
          return $file;
      }
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }
  # First, we check that we have permission in that directory

  return $self->{DATABASE}->deleteFile($file);
}

sub f_cp_HELP {
  return "cp - copy files and directories
Syntax:
       cp [OPTION]... SOURCE DEST
       cp [OPTION]... SOURCE... DIRECTORY

Possible options:
   -k: do not copy the source directory, but the content of the directory
   -u <user>: copy with a different user name (only for admin)
   -m: copy also the metadata
";
}
sub f_cp {
  my $self   = shift;
  my $opt= {};
  @ARGV=@_;
  Getopt::Long::GetOptions($opt,  "k", "user=s", "m") or 
      $self->info("Error parsing the ") and return;;
  @_=@ARGV;
  my $source = shift;
  my $target = pop;

  $opt->{user} and $self->{DATABASE}->{ROLE} !~ /^admin(ssl)?$/
    and $self->info("Only the admin can copy on behave of other users") and 
      return;

  my @moreFiles=@_;
  ($target)
    or print STDERR
      "Error: not enough arguments in cp!!\nUsage: cp <source> <target>\n"
	and return;

  $target = $self->GetAbsolutePath($target, 1);

  my $targetPerm=$self->checkPermissions( "w", $target, undef, {ROLE=>$opt->{user}} ) or return;
  my $targetIsDir=$self->isDirectory($target, $targetPerm);
  my $targetDir=$target;
  ( $targetIsDir) or $targetDir=~ s{/[^/]*$}{/}; 

  if (@moreFiles && not $targetIsDir) {
    $self->info("cp: copying multiple files, but last argument '$target' is not a directory", 0,0);
    return;
  } 
  if ($self->isFile($target, $targetPerm)){
    $self->info("cp: file $target already exists", 0,0);
    return;
  }
  my @done;
  my @todo;
  my $todoMetadata={};
  foreach my $file ($source,@moreFiles) {
    $source = $self->GetAbsolutePath($file, 1);

    my $sourceHash=$self->checkPermissions( "r", $source,undef, {RETURN_HASH=>1} )
    or return;

    $self->existsEntry($source, $sourceHash->{lfn})
      or $self->info("$source: no such file or directory",1,0) 
	and return;
    # fast directory -> directory copy mechanism ...
    if ($sourceHash->{lfn}=~ m{/$} ) {
      $DEBUG and $self->debug(1, "Copying a whole directory");
      push @done, $self->{DATABASE}->copyDirectory($opt, $source, $target);
      if ($self->{DATABASE}->{ROLE} =~  /^admin(ssl)?$/) {
	  if ($self->{ROLE} !~  /^admin(ssl)?$/) {
	      $self->f_chown("s",$self->{ROLE},$target);
	  }
      }
      next;
    }
    my $targetName=$self->f_basename($target);
    if ($targetIsDir) {
      $targetName = $self->f_basename($source);
    } else {
      $target=~ s{/[^/]*$}{};
    }
#    $sourceHash->{guid}=$self->{GUID}->getGUIDfromIntegers($sourceHash->{guid1},$sourceHash->{guid2},$sourceHash->{guid3},$sourceHash->{guid4});
    $DEBUG and $self->debug(2, "Let's copy $source into $targetName");
    $sourceHash->{lfn}=$targetName;
    $self->{SILENT} or print "Copying $source to $targetName...\n";
    $opt->{user} and $sourceHash->{user}= $opt->{user};
    delete $sourceHash->{Groupname};
    delete $sourceHash->{Username};
    delete $sourceHash->{gowner};
    delete $sourceHash->{owner};
    delete $sourceHash->{PrimaryGroup};

    push @todo, $sourceHash;
    if ($opt->{'m'}){
      my $info=$self->getCPMetadata($source, $targetDir, $targetName)
	or return;
      foreach my $key (keys %$info){
	my @list=();
	$todoMetadata->{$key} and push @list, @{$todoMetadata->{$key}};
	push @list, @{$info->{$key}};
	$todoMetadata->{$key}=\@list;
      }
    }
  }
  if (@todo) {
    push @done, $self->f_bulkRegisterFile("k", $target, \@todo);
  }
  foreach my $key (keys %$todoMetadata){
    $self->info("Inserting also the metadata information");
    $self->{DATABASE}->{LFN_DB}->multiinsert($key, $todoMetadata->{$key}) 
      or return;
  }
    

  return @done;
}

sub getCPMetadata{
  my $self=shift;
  my $source=shift;
  my $targetDir=shift;
  my $targetName=shift;

  $targetDir=~ s{/?$}{/};
  $self->info("We are supposed to copy also the metadata");
  my $sourceDir=$source;
  $sourceDir=~ s{/[^/]*$}{};
  my $tags=$self->f_showTags("all", $sourceDir);
  my $entries={};
  foreach my $tag (@$tags){
    #making sure that the destination has all the tags#
    print "We should add the tag $tag\n";
    $self->f_addTag($targetDir, $tag->{tagName}, $tag->{tagName}, $tag->{path}) or $self->info("Error defining the metadata $tag->{tagName}") and return;
    my $tableName= $self->{DATABASE}->getTagTableName($targetDir, $tag->{tagName}) or $self->info("Error getting the name of the table") and return;
    #let's put the entries 
    my @list=();
    $entries->{$tag->{tagName}} and push @list, @{$entries->{$tag->{tagName}}};
    print "Getting the metadata\n";
    my ($columns, $info)= $self->f_showTagValue($sourceDir, $tag->{tagName}) or return;
    foreach my $entry (@$info){
      my $toInsert={file=>"$targetDir$targetName"};
      foreach my $key (keys %$entry){
	$key =~ /^(entryId)|(file)$/ and next;
	$toInsert->{$key}=$entry->{$key};
      }
      push @list, $toInsert;
    }
    $entries->{$tableName}=\@list;
  }
  return $entries;
}

sub f_mv {
  my $self = shift;
  my $options = shift;
  my $source = shift;
  my $target = shift;
  my $oldsilent = $self->{SILENT};
  $self->{SILENT} = 1;
  my $docopy = $self->f_cp($source,$target);
  $self->{SILENT} = $oldsilent;

  if ( $docopy ) {
    $source = $self->GetAbsolutePath($source, 1);
    my $sourceIsFile=$self->isFile($source);
    if (!$sourceIsFile) {
      return $self->f_rmdir("-r",$source);
    } else {
      $self->info("In removeFile");
      
      return $self->f_removeFile("", $source);
    }
  }


  return;

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
sub f_touch {
  my $self=shift;
  my $options=shift;
  my $lfn=shift or $self->info("Error missing the name of the file to touch",undef,2) and return;
  
  $self->info( "Touching $lfn");
  
  return $self->f_registerFile($options, $lfn,0);
}
sub f_du {
  my $self=shift;
  my $options=shift;
  my $path=$self->GetAbsolutePath(shift);
  my $entry=$self->{DATABASE}->existsEntry( $path);
  $entry or $self->info( "du: `$path': No such file or directory", 11,1) and return;
  $self->info( "Checking the disk space usage of $path");
  my $space=$self->{DATABASE}->getDiskUsage($entry);
  $self->info( "$path uses $space bytes");

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


  if (!$lfn) {
    $self->info( "Error not enough arguments in whereis. Usage:\n\t whereis [-l] lfn
Options:
\t-l: Get only the list of SE (not the pfn)");
    if ($options=~/z/) {return @failurereturn;} else {return};
  }
  my $guidInfo;
  my $info;
  if ($options =~ /g/){
    $self->info("Let's get the info from the guid");
    $guidInfo=$self->{DATABASE}->getAllInfoFromGUID({pfn=>1},$lfn)
      or $self->info("Error getting the info of the guid '$lfn'") and return;
    $info=$guidInfo;
  }else {
    $lfn = $self->GetAbsolutePath($lfn);
    my $permFile=$self->checkPermissions( 'r', $lfn,  )  or  return;
    $info=$self->{DATABASE}->getAllExtendedInfoFromLFN($lfn)
      or $self->info("Error getting the info from '$lfn'") and return;
    $info->{guidInfo} or 
      $self->info("That lfn is not associated with a guid") and return;
    $guidInfo=$info->{guidInfo};
  }

  ($guidInfo and $guidInfo->{pfn}) 
    or $self->info("Error getting the data from $lfn") and return; 
  my @SElist=@{$guidInfo->{pfn}};

  $self->info( "The file $lfn is in");
  my @return=();
  if ($options =~ /r/){
    $self->info("We are supposed to resolve links");
    my @newlist=();
    foreach my $entry (@SElist){
      if ($entry->{pfn} =~ m{^guid://[^/]*/(.*)(\?.*)$} ){
	$self->info("We should check the link $1!!");
	my @done=$self->f_whereis("g$options", $1)
	  or $self->info("Error doing the where is of guid '$1'") and return;
	push @return, @done;
      }else {
	push @newlist, $entry;
      }
    }
    @SElist=@newlist;
  }
  if ($options =~ /t/){
    $self->info("Let's take a look at the transfer methods");
    my @newlist;
    foreach my $entry (@SElist){
	my $found=0;
	foreach my $checkentry (@newlist) {
	    if ($checkentry->{seName} eq $entry->{seName}) {
		$found=1;
	    }
	}
	! $found && push @newlist, $self->checkIOmethods($entry, @_);
    }
    @SElist=@newlist;
  }
  foreach my $entry (@SElist){
    my ($se, $pfn)=($entry->{seName}, $entry->{pfn} || "auto");
    $self->info("\t\t SE => $se  pfn =>$pfn ",undef, 0);
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
    $self->info( "$$ Returning the value from the cache (@$cache)");
    return $cache;
  }
  my $protocols=$self->{DATABASE}->{LFN_DB}->queryValue("select seiodaemons from SE where seName=?", undef, {bind_values=>[$seName]});
  my @protocols=split(/,/, $protocols);
  AliEn::Util::setCacheValue($self, "io-$seName", [@protocols]);
  $self->info("Giving back the protocols supported by $seName (@protocols)");
  return \@protocols
}

sub getStoragePath{
  my $self=shift;
  my $seName=shift;

  my $cache=AliEn::Util::returnCacheValue($self, "prefix-$seName");
  if ($cache) {
    $self->info( "$$ Returning the value from the cache (@$cache)");
    return $cache;
  }
  my $storagepath=$self->{DATABASE}->{LFN_DB}->queryValue("select seStoragePath from SE where seName=?", undef, {bind_values=>[$seName]});
  if ( (! defined $storagepath ) || ($storagepath eq "") ) {
      $storagepath="/";
  }
  AliEn::Util::setCacheValue($self, "prefix-$seName", [$storagepath]);
  $self->info("Returning the storagepath supported by $seName ($storagepath)");
  return $storagepath
}

sub createFileName {
  my $self=shift;
  my $seName=shift or return;
  my $guid=(shift or 0);
  my $prefix=$self->getStoragePath($seName);
  my $filename;
  if (!$guid) {
      $guid = $self->{GUID}->CreateGuid();
      if (!$guid) {
	  $self->{LOGGER}->error("File","cannot create new guid");
	  return;
      }
  }
  $filename = sprintf "%s/%02.2d/%05.5d/%s",$prefix,$self->{GUID}->GetCHash($guid),$self->{GUID}->GetHash($guid),$guid;
  while ($filename =~ /\/\//) {$filename =~ s/\/\//\//g;}
  return ($filename,$guid);
}

sub createFileUrl {
  my $self = shift;
  my $se   = shift;
  my $clientprot = shift;
  my $guid = (shift or 0);

  my $protocols = $self->getIOProtocols($se)
      or $self->info("Error getting the IO protocols of $se") and return;
  my $selectedprotocol=0;

  foreach (@$protocols) {if ( $_ =~ /^$clientprot/) { $selectedprotocol =$_; last;} }

  $selectedprotocol or $self->info("The client protocol $clientprot could not be found in the list of supported protocols of se $se") and return;
  
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
    $self->info("The client supports @methods. Let's remove from @$protocols the ones that are not supported");
    my @newProtocols;
    foreach my $method (@methods){
      push @newProtocols, grep (/^$method:/i, @$protocols);
    }
    $self->info("Now we have @newProtocols");
    $protocols=\@newProtocols;
  }
  my @list;
  foreach my $method (@$protocols){
    $self->info("Putting $method");
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
