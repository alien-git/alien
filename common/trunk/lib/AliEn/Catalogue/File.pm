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
  $selist and $insert->{seStringList}=$selist;
  $self->{DATABASE}->createFile($opt, $insert)
    or print STDERR "Error inserting entry into directory\n"
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


  # First, we check that we have permission in that directory
#  my $tempname = $self->f_dirname($file);

  my $permLFN=$self->checkPermissions( 'w', $directory ) or  return;
  $self->isDirectory($directory, $permLFN) or
    $self->{LOGGER}->error("File", "$directory is not a directory!!",1) 
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
      $self->info("The entry $entry->{lfn} cannot be inserted") and return;
    $entry->{lfn}="$directory/$entry->{lfn}";
    # Now, insert it into D0, and in the table
    my $insert={lfn=>$entry->{lfn},  perm=>$self->{UMASK}, owner=>$self->{ROLE},
		gowner=>$self->{MAINGROUP}, size =>$entry->{size},
		guid=>$entry->{guid},  };
    $entry->{se} and $insert->{se}=$entry->{se};
    $entry->{md5} and $insert->{md5}=$entry->{md5};
    $entry->{selist} and $insert->{se}=$entry->{selist};
    $entry->{seStringlist} and $insert->{seStringList}=$entry->{seStringlist};
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

  my $permFile=$self->checkPermissions( 'r', $lfn, undef, {RETURN_HASH=>1} )  or  return;
  if (! $self->isFile($lfn, $permFile->{lfn}) ) {
    $self->{LOGGER}->error("File", "file $lfn doesn't exist!!",1);
    return;
  }
  if ($options =~ /g/){
    return $self->{DATABASE}->getSEListFromFile($lfn, $permFile->{seStringlist}), $permFile;
  }else {
    return $self->{DATABASE}->getSEListFromFile($lfn, $permFile->{seStringlist});
  }
}
#
#
#
sub f_updateFile {
  my $self = shift;
  $self->debug(2, "In File Interface, f_updateFile @_");
  my $file = shift;
  my $args=join(" ", @_);

  my $size="";
  my $se="";
  my $update = {};

  $args =~ s/-?-s(ize)?[\s=]+(\S+)// and $update->{size}=$2;
  $args =~ s/-?-s(e)?[\s=]+(\S+)// and $update->{se}="$2";
  $args =~ s/-?-g(uid)?[\s=]+(\S+)// and $update->{guid}="string2binary(\"$2\")";
  $args =~ s/-?-m(d5)?[\s=]+(\S+)// and $update->{md5}="\"$2\"";

  my $message="";

  (keys %$update) or $message="You should update at least one of the fields";
  $args =~ /^\s*$/ or $message="Argument $args not known";

  $message
    and print STDERR "Error: $message\nUsage:\n\t update <lfn> [-size <size>] [-guid <guid>] [-md5 <md5>]\n"
      and return;

  $file = $self->f_complete_path($file);

  my $permLFN= $self->checkPermissions( 'w', $file )
    or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }

  $DEBUG and $self->debug(2, "Ready to do the update");

  $self->{DATABASE}->updateFile($file, $update, {noquotes=>1})
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
  ( $self->checkPermissions( 'r', $file ) ) or return;

  return $self->{DATABASE}->getAllInfoFromDTable({retrieve=>"guid",
						  method=>"queryValue"},
						 $file,);
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
  my $permLFN=$self->checkPermissions( 'r', $file ) or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }

  my $hash={retrieve=>"md5", 
	    method=>"queryValue"};
  #options g -> let's return as well the guid
  $options=~ /g/ and $hash={method=>"queryRow"};
  my $data = $self->{DATABASE}->getAllInfoFromDTable($hash, $file,)
    or return;

  if ($options=~ /g/ ){
    $data->{md5} or $data->{md5}="";
    $options =~ /s/ or 
      $self->info("$data->{md5}\t$file (guid $data->{guid})", undef,0);
    return ($data->{md5},$data->{guid});
  }
  $options =~ /s/ or $self->info("$data\t$file", undef,0);
  return $data;
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
  my $se   = (shift or $self->{CONFIG}->{SE_FULLNAME});

  $file or $self->info( "Error not enough arguments in addMirror\nUsage:\n\t addMirror <lfn> <se>\n",1) and return;
  $file = $self->f_complete_path($file);

  my $permLFN=$self->checkPermissions( 'w', $file )  or return;
  if (! $self->isFile($file, $permLFN) ) {
    $self->{LOGGER}->error("File", "file $file doesn't exist!!",1);
    return;
  }

  $self->{DATABASE}->insertMirrorFromFile($file, $se) or return;
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

  $self->{DATABASE}->deleteMirrorFromFile( $file, $se) or 
    $self->info( "Error removing the mirror of $file in $se") and return;
  $self->{SILENT}
    or print "Mirror from ${se} removed\n";
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
";
}
sub f_cp {
  my $self   = shift;
  my $opt= {};
  @ARGV=@_;
  Getopt::Long::GetOptions($opt,  "k", "user=s") or 
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
    push @todo, $sourceHash;

  }
  if (@todo) {
    push @done, $self->f_bulkRegisterFile("k", $target, \@todo);
  }

  return @done;
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
  my $lfn=shift;
  
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
return 1;
