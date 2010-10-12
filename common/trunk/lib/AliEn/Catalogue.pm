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
package AliEn::Catalogue;

=head1 NAME

AliEn::Catalogue

=head1 SYNOPSIS

=over 4

=item new

=item f_pwd

=item f_ls

=item f_cd

=item f_mkdir

=item f_quit

=item f_disconnect

=item f_user

=item f_find

=item f_tree

=item f_zoom


=back

=head1 DESCRIPTION

For how tho use this, please see the USAGE section

This is the main package for the alien Catalog. It defines functions to browse the catalog, create directories and remove them. It inherits the rest of the functions from other packages (see also FileInterface, AdminInterface, GroupInterface, TagInterface and ENVInterface).

The alien Catalog will contact to a database belonging to the system. The name of the first database will be obtained through the Config package. Once it is connected to a database, it will reconnect to the other databases of the system as the user changes directories.

The structure of the catalog is like a normal UNIX file system. Each directory and file has privileges for the user, the group and the rest of the universe. Each user has a home directory, where (s)he can put the files that (s)he wants. 

Each entry in the catalog is a Logical File Name (LFN). Each LFN points to a Physical File Name (PFN), that is a real file in a computer. To be able to access that file, we use Transport File Names (TFN), or a way to access files from other machines.




=head1 USAGE

To access the Catalog, type the command "alien". First, you have to authenticate (see Authen package for more details). After that, you can browse the directories like in a normal file system.


=head1 METHODS

=over

=cut
use DBI;
use File::Basename;
use AliEn::Catalogue::File;
require AliEn::Catalogue::Admin;
require AliEn::Catalogue::Authorize;
use AliEn::Catalogue::Group;
use AliEn::Catalogue::Tag;
use AliEn::Catalogue::GUID;
use AliEn::Catalogue::Trigger;
use AliEn::Catalogue::Env;
use AliEn::Catalogue::Basic;
use AliEn::Catalogue::Collection;
use AliEn::Dataset;
use AliEn::Logger::LogObject;
use AliEn::Util;
use AliEn::GUID;
use Data::Dumper;
use strict;
use vars qw($DEBUG @ISA);
$DEBUG = 0;
@ISA = (
         'AliEn::Catalogue::File',       'AliEn::Catalogue::Admin',
         'AliEn::Catalogue::Group',      'AliEn::Catalogue::Tag',
         'AliEn::Catalogue::Env',        'AliEn::Catalogue::Basic',
         'AliEn::Catalogue::Trigger',    'AliEn::Catalogue::GUID',
         'AliEn::Catalogue::Collection', 'AliEn::Logger::LogObject',
         'AliEn::Catalogue::Authorize',
         @ISA
);
use AliEn::Database::Catalogue;
use AliEn::Database::TaskPriority;
use AliEn::Database::TaskQueue;

#use AliEn::Utilities;
require AliEn::Config;
require AliEn::SOAP;
use Getopt::Std;

# OBJECTS VARIABLES:
# $curpath, $curdir, $remotepath, $localpath, $disppath
# $curDB, $curHostID, $debug, $firstHost
#BASIC USER FUNCIONS FOR BROWSING A DATABASE
#
# help
# ls
# cd
# rmdir
# mkdir
# quit$localdir
sub getDispPath {
  my $self = shift;
  return $self->{DISPPATH};
}

sub f_getTabCompletion {
  my $self=shift;
  my $word=shift;
  my $path = $self->f_complete_path($word);
  $path or return;

  my ($dirname) = $self->f_dirname($path);

  $self->selectDatabase($dirname) or return;
  my @result=$self->{DATABASE}->tabCompletion ($dirname);
  @result = grep (s/^$path/$word/, @result);
  return @result;
}

sub getHost{
  my $self=shift;
  return $self->f_Database_getVar("HOST");
}

sub new {
  my $proto   = shift;
  my $class   = ref($proto) || $proto;
  my $self    = {};
  my $options = shift;
  $options->{DEBUG}  = $self->{DEBUG}  = ( $options->{debug}  or 0 );
  $options->{SILENT} = $self->{SILENT} = ( $options->{silent} or 0 );
  $self->{GLOB}      = 1;
  $self->{CONFIG} = new AliEn::Config($options);
  ( $self->{CONFIG} )
    or print STDERR "Error: Initial configuration not found!!\n" and return;
  my $user = ( $options->{user} or $self->{CONFIG}->{LOCAL_USER} );
  $self->{ROLE} = ( $options->{role} or $user );
  my $token    = ( $options->{token}    or "" );
  my $password = ( $options->{password} or "" );
  $self->{LOGGER} = new AliEn::Logger;
  $self->{DEBUG} and $self->{LOGGER}->debugOn( $self->{DEBUG} );
  bless( $self, $class );
  $self->SUPER::new();
  $DEBUG and $self->debug(
    1, "\tLocaluser: $user
\t Role     : $self->{ROLE}
In UserInterface:new with $user ($self->{ROLE}) $self->{DEBUG} $self->{SILENT}
Site name:$self->{CONFIG}->{SITE}"
  );
  $self->{SOAP} = new AliEn::SOAP
    or print "Error creating AliEn::SOAP $! $?" and return;
  $DEBUG
    and $self->{CONFIG}->{SITE_HOST}
    and $self->debug( 1, "\tHost name:$self->{CONFIG}->{SITE_HOST}" );
  $self->{CURDIR}     = 1000;
  $self->{LOCALDIR}   = 1000;
  $self->{REMOTEPATH} = "/";
  $self->{LOCALHOST}  = $ENV{'ALIEN_HOSTNAME'} . "." . $ENV{'ALIEN_DOMAIN'};
  chomp( $self->{LOCALHOST} );
  $self->{UMASK}       = 755;
  $self->{FIRSTORG}    = $self->{CONFIG}->{ORG_NAME};
  $self->{FIRSTHOST}   = $self->{CONFIG}->{CATALOG_HOST};
  $self->{FIRSTDRIVER} = $self->{CONFIG}->{'CATALOG_DRIVER'};
  $self->{FIRSTDB}     = $self->{CONFIG}->{'CATALOG_DATABASE'};
  $DEBUG and $self->debug( 1, "Creating the database" );

  #    $self->{SQLAPI} = AliEn::Database::SQLInterface->new(
  my $DBoptions = {
                    "DB"                 => $self->{FIRSTDB},
                    "HOST"               => $self->{FIRSTHOST},
                    "DRIVER"             => $self->{FIRSTDRIVER},
                    "DEBUG"              => $self->{DEBUG},
                    "USER"               => $user,
                    "SILENT"             => $self->{SILENT},
                    "TOKEN"              => $token,
                    "LOGGER"             => $self->{LOGGER},
                    "ROLE"               => $self->{ROLE},
                    "FORCED_AUTH_METHOD" => $options->{FORCED_AUTH_METHOD},
  };
  defined $options->{USE_PROXY}
    and $DBoptions->{USE_PROXY} = $options->{USE_PROXY};
  defined $options->{passwd} and $DBoptions->{PASSWD} = $options->{passwd};
  $self->{FORCED_AUTH_METHOD} = $options->{FORCED_AUTH_METHOD};
  $self->{DATABASE}           = AliEn::Database::Catalogue->new($DBoptions)
    or return;

#  my ($host, $driver, $db) = split("/", $self->{CONFIG}->{"JOB_DATABASE"});
#  $self->{TASK_DB} = AliEn::Database::TaskQueue->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin', SKIP_CHECK_TABLES=> 1}) or return;
#  $self->{PRIORITY_DB} = AliEn::Database::TaskPriority->new({DB=>$db,HOST=>$host,DRIVER=>$driver,ROLE=>'admin',SKIP_CHECK_TABLES=> 1}) or return;
  $self->{ROLE} = $self->{DATABASE}->{LFN_DB}->{ROLE};

  # check if an entry exists in PRIORITY table
  #$self->{PRIORITY_DB}->checkPriorityValue($self->{ROLE});
  $self->_setUserGroups( $self->{ROLE} );
  ( $self->{CURHOSTID} ) =
    $self->{DATABASE}->getHostIndex( $self->{FIRSTHOST}, $self->{FIRSTDB} );
  $self->{"DATABASE_$self->{CONFIG}->{ORG_NAME}_$self->{CURHOSTID}"} =
    $self->{DATABASE};
  $self->{"DATABASE_FIRST"} = $self->{DATABASE};
  $self->{MOUNT}            = "";
  $self->{GUID}             = new AliEn::GUID();
  if ( !$self->{GUID} ) {
    $self->f_disconnect();
    return;
  }
  $self->loadEnvironment();
  my $oldSilent = $self->{SILENT};
  $self->{SILENT} = 1;
  $self->f_pwd();
  $self->{SILENT}   = $oldSilent;
  $self->{LIMIT_SE} = "";
  $self->initEnvelopeEngine();
  return $self;
}



sub setSElimit {
  my $self = shift;
  my $se   = shift;
  if ($se) {
    my $number = $self->{DATABASE}->getSENumber($se);
    if ( !$number ) {
      $self->info("Error getting the se number of '$se'");
      return;
    }
    $self->{LIMIT_SE} = $number;
    $self->info("Displaying only the files in the se '$se'");
  } else {
    $self->info("Displaying all the files");
    $self->{LIMIT_SE} = "";
  }
  return 1;
}

# sub validateDatabase {
#     my $self = shift;
#     return $self->{DATABASE}->validate();
# }
sub findEx {
  my $self      = shift;
  my $silent    = 0;
  my $oldSilent = $self->{SILENT};
  $self->{SILENT} = 1;
  my @files = $self->f_find(@_);
  $oldSilent and $self->{SILENT} = $oldSilent
    or delete $self->{SILENT};
  my @result;
  for (@files) {
    my %info;
    $info{lfn} = $_;
    my @pfns;
    my @pfnsRaw = $self->f_getFile( "s", $_ );
    while ( $#pfnsRaw > -1 ) {
      my %pfn;
      $pfn{se}  = shift @pfnsRaw;
      $pfn{pfn} = shift @pfnsRaw;
      push @pfns, \%pfn;
    }
    $info{pfns} = \@pfns;
    push @result, \%info;
  }
  unless ($silent) {
    for (@result) {
      print STDOUT "LFN:    $_->{lfn}\n";
      my $first = 1;
      for ( @{ $_->{pfns} } ) {
        if ($first) {
          print STDOUT "PFN:    ";
          undef $first;
        } else {
          print STDOUT "MIRROR: ";
        }
        print "$_->{pfn} $_->{se}\n";
      }
      print "\n";
    }
  }
  return \@result;
}

sub f_pwd {
  my $self            = shift;
  my $returnarrayhash = grep ( /-z/, @_ );
  my $silent          = grep ( /-s/, @_ );
  my $short           = grep ( /-1/, @_ );
  $DEBUG and $self->debug( 1, "\n\t\t UserInterface pwd:@_" );
  #$self->checkPermissions( 'x', $self->{DISPPATH}, 1 ) or return;
  $self->{DISPPATH} = $self->{DISPPATH};

  if ( ( !$self->{SILENT} ) and ( !$silent ) ) {
    if ($short) {
      print STDOUT "$self->{DISPPATH}\n";
    } else {
      $self->info( "Current path is: $self->{DISPPATH}", undef, 0 );
    }
  }
  $DEBUG and $self->debug( 4, "Done UserInterface pwd:" );
  if ($returnarrayhash) {
    my @retarray = ();
    my $newhash  = {};
    $newhash->{'cwd'} = $self->{DISPPATH};
    push @retarray, $newhash;
    return @retarray;
  } else {
    return $self->{DISPPATH};
  }
}

sub f_getLinkPath {
  my $self     = shift;
  my $pathIdx  = shift;
  my $filename = shift;
  my $newfilename;
  if ( $filename =~ /(.*)\/$/ ) {
    $newfilename = $1;
  } else {
    $newfilename = $filename;
  }
  my $pfntype = $self->getPfnType( $pathIdx, $newfilename );
  if ( ($pfntype) and defined( $pfntype->{type} ) ) {
    my $type = substr( $pfntype->{type}, 0, 1 );
    if ( $type eq 'l' ) {
      if ( $pfntype->{pfn} =~ /^lfn\:\/\/(.*)/ ) {
        my $resolvedpath = $self->f_complete_path($1);
        return $resolvedpath;
      } else {
        return;
      }
    }
  } else {
    $DEBUG and $self->debug( 1, "Cannot find $filename - does it exist?" );
    return;
  }
}

sub f_lsInternal {
  my $self    = shift;
  my $options = shift;
  my $path    = ( shift or "" );
  $path = $self->GetAbsolutePath($path);
  $DEBUG and $self->debug( 1, "Listing $path with options $options" );
  my $entryInfo =
    $self->checkPermissions( 'r', $path, 0, 1 )
    or return;
  $DEBUG and $self->debug( 1, "Check Permission done $path " );
  my $lfn = $entryInfo->{lfn};
  $self->existsEntry( $path, $lfn )
    or $self->info( "$path no such file or directory", 1 )
    and return;
  my @all;

  if ( ( $lfn =~ m{/$} ) && ( $options !~ /t/ ) ) {
    $DEBUG
      and $self->debug( 1, "Listing a directory $lfn (se $self->{LIMIT_SE})" );
    push @all, $self->{DATABASE}
      ->listDirectory( $entryInfo, $options, $self->{LIMIT_SE} );
  } else {

   #in case we are listing a directory with -t, the path is the parent directory
    $path = $lfn;
    $path =~ s{/[^/]*/$}{/};
    $DEBUG and $self->debug( 1, "Listing an entry" );
    push @all, $entryInfo;

#    push @all, $self->{DATABASE}->getAllInfoFromLFN ({method=>"queryRow"}, $entry);
  }
  my $dir = $lfn;
  $dir =~ s{[^/]*$}{};

  #Finally, if we have -a, we have to look also for the parent of this directory
  if ( ( $path ne "/" ) and ( $options =~ /a/ ) and ( $lfn =~ m{/$} ) ) {
    my $parentpath = $path;
    $parentpath =~ s{[^/]*/?$}{};
    $DEBUG
      and
      $self->debug( 1, "Getting the info of the parent ($parentpath of $path)"
      );
    ( $self->checkPermissions( 'r', $parentpath ) ) or return;
    my $entry =
      $self->{DATABASE}
      ->getAllInfoFromLFN( { method => "queryRow" }, $parentpath );
    if ($entry) {
      $entry->{lfn} = "..";
      @all = ( shift @all, $entry, @all );
    }
  }
  return ( $dir, \@all );
}

sub getPfnType {
  my $self     = shift;
  my $pathIdx  = shift;
  my $filename = shift;
  $DEBUG and $self->debug( 1, "Get PfnType for $pathIdx $filename" );
  my $result = $self->{DATABASE}->getPfnType( $pathIdx, $filename );
  return $result;
}

sub getDirList {
  my $self     = shift;
  my $pathIdx  = shift;
  my $filename = shift;
  my $options  = shift;
  my $result   = $self->{DATABASE}->getDirList( $pathIdx, $filename, $options );
  return $self->prependMountPoint($result);
}

sub prependMountPoint {
  my $self = shift;
  my $list = shift;
  $DEBUG and $self->debug( 1, "Checking the mount point" );
  $self->{MOUNT} or return $list;
  $DEBUG and $self->debug( 1, "Prepending $self->{MOUNT}" );
  my @list = @{$list};
  $DEBUG and $self->debug( 1, "Got @list" );
  foreach my $d (@list) {
    $DEBUG and $self->debug( 1, "Doing $d" );
    $d->{name} =~ s/^\//$self->{MOUNT}\//;
  }
  return $list;
}

sub f_ls_HELP {
  return "Usage: ls [-laFn|b|h] [<directory>]
\t-l : long format
\t-a : show hidden .* files
\t-F : add trailing / to directory names
\t-n: switch off the colour output
\t-b : print in guid format
\t-h : print the help text
\t-e : display also the expire date";
}

sub f_ls {
  my $self    = shift;
  my $options = shift;
  my $path    = ( shift or "" );
  if ( $options =~ /h/ ) {
    $self->info( $self->f_ls_HELP() );
    return;
  }
  my ( $retrievedpath, $rlist ) = $self->f_lsInternal( $options, $path );
  my @result;
  $DEBUG and $self->debug( 1, "The ls found $#$rlist +1 entries" );
  if ( $options =~ /z/ ) {
    if ( ( !defined $retrievedpath ) || ( $retrievedpath eq "" ) ) {
      my $errorresult;
      $errorresult->{"__result__"} = 0;
      push @result, $errorresult;
      return @result;
    }
  }
  for (@$rlist) {
    $DEBUG and $self->debug( 1, "Printing " . Dumper($_) );
    push @result, $self->f_print( $retrievedpath, $options, $_ );
  }
  return @result;
}

sub f_guid2lfn_HELP {
  return "guid2lfn: look for the LFNs pointing to a guid. Usage

guid2lfn [-a] [-s] <guid>

Options: 
   -s: silent
   -a: all tables. Do a deep search in all the possible tables of the catalogue
";
}

sub f_guid2lfn {
  my $self    = shift;
  my $options = shift;
  my $guid    = shift
    or print STDERR "Error: you have to specify a guid to translate!"
    and return;
  my @lfns = $self->{DATABASE}->getLFNfromGUID( $options, $guid );
  if ( $options !~ /s/ ) {

    # be silent
    my $format = "";
    foreach (@lfns) {
      $format = sprintf "$format%-64s %-40s\n", $_, $guid;
    }
    $self->info( $format, 0, 0 );
  }
  return @lfns;
}

sub f_lfn2guid {
  my $self    = shift;
  my $options = shift;
  my $lfn     = shift
    or print STDERR "Error: you have to specify a lfn to translate!" and return;
  my $guid = $self->f_getGuid( $options, $lfn );
  $guid or return;
  $DEBUG and $self->debug( 1, "The guid is $guid" );
  if ( $options !~ /s/ ) {
    my $format = sprintf "%-64s %-40s\n", $lfn, $guid;
    $self->info( $format, 0, 0 );
  }
  return $guid;
}

sub f_glob {
  my $self    = shift;
  my $options = shift;
  my $state   = shift;
  if ( !defined($state) ) {
    print( "Glob state is: " . $self->{GLOB} . "\n" );
    return;
  }
  if ( $state != 0 and $state != 1 ) {
    print STDERR "Wrong arguments to glob\n0 = on, 1 = off\n";
    return;
  }
  $self->{GLOB} = $state;
  return;
}

# Gets all the files that match a certain pattern
# The pattern can contain:  * match any name until
#                           ? match any character
sub ExpandWildcards {
  my $self         = shift;
  my $path         = shift;
  my $preservelast = ( shift or 0 );
  $DEBUG and $self->debug( 1, "ExpandWildcards: S $path" );
  $path = $self->GetAbsolutePath($path);
  $DEBUG and $self->debug( 1, "ExpandWildcards: S $path" );

  #  my @dirs = split "/", $path;
  #delete trailing empty dir
  #  shift @dirs;
  my $lastdir;
  $preservelast = 1 if ( $path =~ m{/$} );
  my $result = $self->{DATABASE}->getLFNlike($path)
    or return;
  my @result = @$result;
  if ( $preservelast == 0 ) {
    map { s{/$}{} } @result;
  }
  return @result;
}

sub f_cd {
  my $self    = shift;
  my $path    = shift;
  my $pathIdx = "";
  ( defined $path ) or ( $path = $self->GetHomeDirectory() );
  $path = $self->GetAbsolutePath( $path, 2 );
  my $targetPerm = $self->checkPermissions( "x", $path )
    or $self->info( "cd $path: Not a directory", 3, 0 )
    and return;
  $self->isDirectory( $path, $targetPerm )
    or $self->info( "cd $path: Not a directory", 3, 0 )
    and return;
  $self->{DISPPATH} = $path;
  $self->f_pwd("-s");
  return 1;
}

sub checkPermissionOnDirectory {
  my $self = shift;
  my $path = shift;
  (defined $path) or return;
  my $targetPerm = $self->checkPermissions( "x", $path )
    or $self->{LOGGER}->error("Check permissions failed for $path")
    and return;
  $self->isDirectory( $path, $targetPerm )
    or $self->{LOGGER}->error("$path is not a directory")
    and return;
  return 1;
}


=item f_mkdir(arguments, lfn)

Creates a new directory. lfn is the directory to create. The possible options are:

=over 

=item p 

create all the parents directories if needed. Does not return error if the directory exists

=item s

silent mode. Does not put anything in the output

=back

=cut

sub f_mkdir_HELP {
  return "Usage: mkdir [-ps] <directory>
Options: 
\t-s silent
\t-p create parents as needed
\t-d return the directory number";
}

sub f_mkdir {
  my $self = shift;
  my ($options,$path) = @_;
  $DEBUG and $self->debug( 1, "In UserInterface f_mkdir @_" );
  my $message;
  ( defined $path ) or $message = "not enough arguments";
  $path =~ s{\\@}{@}g;
  ( $options =~ /^[s|p|d]*$/ ) or $message = "unknown option '$options'";
  $message 
    and $self->{LOGGER}->error( "Catalogue", "Error $message\n " . $self->f_mkdir_HELP() )
    and return;
  my $silent = ( $options =~ /s/ ) ? 1 : undef;
  $path = $self->GetAbsolutePath( $path, 1 );
  if ( $self->existsEntry($path) ) {
    ( $options =~ /d/ )
      and return
      $self->{DATABASE}->getAllInfoFromLFN({options  => 'd',
                                            retrieve => 'entryId',
                                            method   => 'queryValue'},"$path/");
    $options =~ /p/ and return 1;
    $self->info("Directory $path already exists.\n");
    return;
  }

  my $parentdir = "$path";
  $parentdir =~ s {/([^/]+/?)$}{/};
  $DEBUG and $self->debug( 1, "Checking the parent: $parentdir" );
  if ( $options =~ /p/ and $parentdir ne '/' ) {
    if ( !$self->existsEntry($parentdir) ) {
      $self->f_mkdir( $options . "s", $parentdir ) or 
        $self->{LOGGER}->error("Catalogue", "Error building $parentdir") and 
        return;
    }
  }
  
  $DEBUG and $self->debug( 1, "Creating directory in $path" );

  #Check permissions
  $self->checkPermissions("w",$path,0, 1) 
    or return;
  
  my @returnVal = $self->{DATABASE}->createDirectory( "$path", $self->{UMASK} );

  #Get directory number
  if ( $options =~ /d/ and $self->existsEntry($path) ) {
      return ($self->{DATABASE}->getAllInfoFromLFN({
                                              options  => 'd',
                                              retrieve => 'entryId',
                                              method   => 'queryValue'},"$path/"));
  }
  return @returnVal;
}

sub f_quit {
  my $self = shift;
  $self->saveEnvironment();
  $self->f_disconnect();
  print("bye now!\n");
  exit;
}

sub f_disconnect {
  my $d = shift;
  $d and $d->{DATABASE} and $d->{DATABASE}->destroy();

  #  shift->_executeInAllDatabases("destroy",@_);
}

sub f_mkremdir {
  my $self = shift;
  $DEBUG and $self->debug( 1, "In UserInterdace mkremdir @_" );
  my $host   = shift;
  my $driver = shift;
  my $DB     = shift;
  my $lfn    = shift;
  if ( !$lfn ) {
    $self->info(
"ERROR: wrong arguments in mkremdir.\n Usage: mkremdir <host> <driver> <database> <lfn>"
    );
    return;
  }
  $lfn =~ s/\/$//;
  $lfn = $self->f_complete_path($lfn);
  my $permLFN = $self->checkPermissions( "w", $lfn ) or return;
  $self->existsEntry( $lfn, $permLFN )
    and $self->info( "That file or directory already exists", 1 )
    and return;

  #pratik
  my ($hostIndex) = $self->{DATABASE}->getHostIndex( $host, $DB, $driver );
  if ( !$hostIndex ) {
    print STDERR
"Error: $DB in $host (driver $driver) is not in the current list of remote hosts. Add it first with 'addHost'\n";
    return;
  }
  return $self->{DATABASE}->createRemoteDirectory( $hostIndex, $host, $DB, $driver, $lfn );
}

#sub f_rmlink {
#    my $self = shift;
#    my $link = shift;
#
#    if ( !$link ) {
#        print STDERR (
#            "ERROR: wrong arguments in rmlink.\n Usage: rmlink <link>\n");
#        return;
#    }
#
#    $link =~ s/\/$//;
#    my $newpath = $self->f_complete_path( $link . "/" );
#    if ( !$self->f_dir($newpath) ) {
#        print STDERR "Error: directory $newpath does not exist!\n";
#        return;
#    }
#    my $parent   = $self->f_parent_dir($link);
#    my $basename = $self->f_basename($link);
#    my ($out) = $self->{DATABASE}->getFieldsFromDir($parent, $basename,"type,owner");
#    defined $out
#    	or return;
#	my $type = $out->{"type"}; my $owner = $out->{"owner"};
#    if ( $type ne "l" ) {
#        print STDERR "Error: $link is not a link!!\n";
#        return;
#    }
#    if ( $owner ne $self->{ROLE} ) {
#        print STDERR "Error: You do not have permission to delete the link!!\n";
#        return;
#    }
#    #now, delete the entry from the father and D0
#	#$self->{DATABASE}->deleteDirFromParent($parent, $newpath);
#    $self->{DATABASE}->deleteLink($parent, $basename, $newpath);
#}
sub f_Database_existsEntry {
  my $self = shift;
  return $self->{DATABASE}->existsEntry(@_);
}

sub f_Database_do {
  my $self = shift;
  return $self->{DATABASE}->do(@_);
}

sub f_Database_getVar {
  my $self = shift;
  my $var = ( shift or return );
  return $self->{DATABASE}->{$var};
}

sub isDirectory {
  my $self   = shift;
  my $file   = shift;
  my $exists = $self->existsEntry( $file, @_ ) or return;
  ( $exists =~ m{/$} ) and return $exists;
  return;
}

=item isFile($lfn, [$permLFN])

This subroutine checks if the entry $lfn exists in the catalogue, and 
is a file. If it doesn't receive $permLFN, it will check the database
If it does receive $permLFN (which is supposed to be the string return
by the function checkPermission), it will do a pattern matching
between lfn and permLFN

=cut

sub isFile {
  my $self   = shift;
  my $file   = shift;
  my $exists = $self->existsEntry( $file, @_ ) or return;
  $exists =~ /\/$/ and return;
  return $exists;
}

sub existsEntry {
  my $self     = shift;
  my $lfn      = shift;
  my $permFile = shift;
  $DEBUG and $self->debug( 1, "Checking if $lfn exists in the catalogue" );
  if ( !$permFile ) {
    $self->selectDatabase($lfn) or return;
    return $self->{DATABASE}->existsLFN($lfn);
  }
  $lfn =~ s/\*/\\\*/g;
  $lfn =~ s/\?/\\\?/g;
  $lfn =~ s{\+}{\\+}g;
  $lfn =~ s{\$}{\\\$}g;
  $DEBUG and $self->debug( 1, "Comparing '$lfn' and '$permFile'" );
  
  $lfn =~ s{/$}{};
  ( $permFile =~ /^$lfn\/?$/ ) or return;
  $DEBUG and $self->debug( 1, "The entry exists ($permFile)" );
  return $permFile;
}

#
#Returns the name of the file of a path
#
sub f_basename {
  my $self = shift;
  my $arg  = shift;
  $arg =~ s{^.*/([^/]*)$}{$1};
  return $arg;
}

#
#Returns the table of the father
#
#sub f_parent_dir {
#  my ( $self, $path ) = @_;
#
#  $path = $self->f_complete_path($path);
#  $path = $self->f_dirname($path);
#  if ( $path eq "" ) {
#    return ( $self->{CURDIR} );
#  }
#  else {
#    return $self->{DATABASE}->getFieldFromD0($path,"dir");;
#  }
#}
#
#Returns the directory of a path
#
sub f_dirname {
  my $self = shift;
  my ($arg) = @_;
  $arg or return;
  $arg =~ s{[^/]*$}{};
  return $arg;
}

sub GetParentDir {
  return shift->f_dirname(@_);
}

sub f_print {
  my ( $self, $path, $opt, $rentry ) = @_;
  my $t = "";
  my ( $type, $perm, $name, $user, $date, $group, $size, $md5, $expire ) = (
      $rentry->{type},  $rentry->{perm},
      $rentry->{lfn},   $rentry->{owner} || "unknown",
      $rentry->{ctime}, $rentry->{gowner} || "unknown",
      $rentry->{size} || 0, $rentry->{md5},
      $rentry->{expiretime} || ""
  );
  $opt =~ /e/ or $expire = "";
  $name =~ s{^$path}{};
  defined $name or $name = ".";
  my $permstring = $rentry->{type};
  my $colorterm  = 0;

  if ( $ENV{ALIEN_COLORTERMINAL} and $opt !~ /n/ ) {
    $colorterm = 1;
  }
  my $textcolour  = "";
  my $textneutral = "";
  if ($colorterm) {
    $textneutral = AliEn::Util::textneutral();
    if ( $permstring =~ /^d/ ) {
      $textcolour = AliEn::Util::textgreen();
    } else {
      $textcolour = AliEn::Util::textblue();
    }
    if ( $name =~ /^\./ ) {
      $textcolour = AliEn::Util::textred();
    }
    if ( $name =~ /jdl$/ ) {
      $textcolour = AliEn::Util::textred();
    }
  }
  if ( $opt =~ /l/ ) {
    $permstring =~ /f/ and $permstring = "-";
    $permstring eq "d" and $t = "/";
    for ( my $i = 0 ; $i < 3 ; $i++ ) {
      my $oneperm = substr( $perm, $i, 1 );
    SWITCH: for ($oneperm) {
        /0/ && do { $permstring .= "---"; last; };
        /1/ && do { $permstring .= "--x"; last; };
        /2/ && do { $permstring .= "-w-"; last; };
        /3/ && do { $permstring .= "-wx"; last; };
        /4/ && do { $permstring .= "r--"; last; };
        /5/ && do { $permstring .= "r-x"; last; };
        /6/ && do { $permstring .= "rw-"; last; };
        /7/ && do { $permstring .= "rwx"; last; };
      }
    }
    $self->{SILENT}
      or ( $opt =~ /s/ )
      or $self->info(sprintf ("%s   %-8s %-8s %12s %s%12s%s      %-10s %-20s\n", $permstring,
      $user, $group, $size, $date, $textcolour, $name, $textneutral, $expire), undef, 0);
    if ( $opt =~ /z/ ) {
      my $rethash = {};
      $rethash->{permissions} = $permstring;
      $rethash->{user}        = $user;
      $rethash->{group}       = $group;
      $rethash->{size}        = $size;
      $rethash->{date}        = $date;
      $rethash->{name}        = $name;
      $rethash->{path}        = $path;
      $rethash->{md5}         = $md5;
      return $rethash;
    }
    return "$permstring###$user###$group###$size###$date###$name";
  }
  if ( $opt =~ /m/ ) {
    if ( ( !defined $md5 ) || ( $md5 eq "" ) ) {
      $md5 = "00000000000000000000000000000000";
    }
    if (! $self->{SILENT} and $opt !~ /s/ ) {
      $self->info(sprintf( "%s   %s\n", $md5, $path . $name), undef, 0);
    }
    if ( $opt =~ /z/ ) {
      my $rethash = {};
      $rethash->{path} = $path . $name;
      $rethash->{md5}  = $md5;
      return $rethash;
    }
    return "$md5###$path";
  }
  if ( $opt =~ /b/ ) {

    # retrieve the GUID from D0
    if ( $permstring eq "d" ) {
      return;
    }
    $path .= $name;
    my $guid  = $rentry->{guid};
    my $pguid = "";
    my $rguid = "";
    if ($guid) {
      $pguid = $guid;
      if ( $guid eq "" ) {
        $pguid = "           -- undef --             ";
      }
      $rguid = $pguid;
      $self->{SILENT} or $self->info(sprintf( "%36s   %s\n", $pguid, $path), undef, 0);
    } else {
      $pguid = "------------------------------------";
      $rguid = "";
      $self->{SILENT} or $self->info(sprintf( "%36s   %s\n", $pguid, $path),undef, 0);
    }
    if ( $opt =~ /z/ ) {
      my $rethash = {};
      $rethash->{guid} = $rguid;
      $rethash->{path} = $path;
      return $rethash;
    }
    return "$rguid###$path";
  }
  $self->{SILENT} or ( $opt =~ /s/ ) or $self->info(sprintf( "%s%s\n", $name, $t),undef, 0);
  if ( $opt =~ /z/ ) {
    my $rethash = {};
    $rethash->{path} = $path;
    $rethash->{name} = $name;
    return $rethash;
  }
  return $name;
}

sub f_whoami {
  my $self = shift;
  $self->{SILENT} or $self->info( " $self->{ROLE}", undef, 0 );
  return $self->{ROLE};
}

sub f_user {
  my $self = shift;
  my $user = shift;
  if ( !$user ) {
    print "Enter user name:";
    chomp( $user = <> );
  }
  my $changeUser = 1;
  if ( $user ne "-" ) {
    if ( !$self->_executeInAllDatabases( "changeRole", $user ) ) {
      print STDERR "Password incorrect or user does not exist\n";
      return;
    }
  } else {
    $changeUser = 0;
    $user       = shift;
    $self->info(
"Executing super user code [change $self->{DATABASE}->{ROLE}/$self->{ROLE} to $user]",
      undef, 0
    );
    if ( !( $self->{DATABASE}->{ROLE} =~ /^admin(ssl)?$/ ) ) {
      print STDERR "You have to be admin to use the super user functionality";
      return;
    }
    if ( ( !defined $user ) || ( $user eq "" ) ) {
      print STDERR "You have to specify the user identity you want to become";
      return;
    }
  }
  $self->{ROLE} = $user;
  $self->_setUserGroups( $user, $changeUser );

  # Check if a changeUser exists
  #  $self->{PRIORITY_DB}->checkPriorityValue($user);
}

sub _executeInAllDatabases {
  my $self   = shift;
  my $call   = shift;
  my $result = 1;
  my $name   = "DATABASE_";
  $self->{CONFIG}
    and $self->{CONFIG}->{ORG_NAME}
    and $name .= "$self->{CONFIG}->{ORG_NAME}_";
  my @allDatabases = grep ( /^$name\d+/, keys %{$self} );
  if ( !@allDatabases ) {
    $self->{LOGGER}
      and $self->{LOGGER}->error( "Catalogue", "No databases found" );
    return;
  }
  foreach (@allDatabases) {
    if ( $self->{$_} ) {
      $self->{LOGGER} and $DEBUG
        and $self->debug( 1,
          "Executing $call(" . ( join( " ", @_ ) or "" ) . ") in database $_" );
      $self->{$_} and $self->{$_}->$call(@_)
        or undef $result;
    }
  }
  $result;
}

#sub f_mvdir
#{
#    my ($this, $dbh, $source, $target, @rest, $oldParent, $newParent, $sth);
#    my (@list,  $oldName);
#
#    ($source, $target, @rest)=split(/ /, @_[0]);
#
#    if (($source eq "") || ($target eq ""))
#    {
#	print ("Usage: mv <source> <target>\n");
#	return;
#    }
#    $source= f_complete_path($this, $source);
#    $target= f_complete_path($this, $target);
#    print ("MOVING $source to $target\n");
#    $oldParent= f_parent_dir($source);
#    $newParent= f_parent_dir($target);
#    print ("OLD PARENT $oldParent New $newParent\n");
#    #if any of the directories do not exist, return error
#    if (($newParent eq "") || ($oldParent eq ""))
#    {
#	print ("Error: that directory does not exist\n");
#	return -1;
#    }
#    #if the father is not the same, delete the entry in the old father,
#    #and put it in the new
#    $oldName= baseName($source);
#    $sth = $dbh->prepare("SELECT * from T$oldParent where name = '$oldName'");
#    $sth->execute or print "$DBI::errstr\n";
#    @list=$sth->fetchrow();
#    "INSERT INTO T$newParent"
#	"DELETE FROM T$oldParent"
#}
sub f_passwd {
  my $self = shift;
  my ( $oldpasswd, $passwd, $passwd2 );
  system("stty -echo");
  print STDERR "Enter old password:";
  chomp( $oldpasswd = <STDIN> );
  print STDERR "\nEnter new password:";
  chomp( $passwd = <STDIN> );
  print STDERR "\nReenter new password:";
  chomp( $passwd2 = <STDIN> );
  system("stty echo");

  if ( $passwd ne $passwd2 ) {
    print STDERR "\nError: passwords do not match!! Password not changed.\n";
    return;
  }
  my $done =
    SOAP::Lite->uri('AliEn/Service/Authen')
    ->proxy(
           "http://$self->{CONFIG}->{PROXY_HOST}:$self->{CONFIG}->{PROXY_PORT}")
    ->passwd( $self->{DATABASE}->{HOST},
              $self->{DATABASE}->{DB},
              $self->{ROLE}, $oldpasswd, $passwd )->result;
  if ( !$done ) {
    print STDERR "\nError: password not changed!!\n";
  } else {
    print "\nPassword changed!!\n";
  }
}

sub f_verifyToken {
  my $self  = shift;
  my @arg   = grep ( !/-z/, @_ );
  my $jobId = shift @arg
    or print STDERR "You have to provide a job identifier" and return;
  my $token = shift @arg
    or print STDERR "You have to provide a job token" and return;
  my @results;
  $#results = -1;
  my $rethash =
    $self->{DATABASE}->{TOKEN_MANAGER}->validateJobToken( $jobId, $token );
  if ( ( defined $rethash ) && ( $rethash->{'user'} ) ) {
    push @results, "$rethash->{'user'}";
  }
  return @results;
}

sub f_verifySubjectRole {
  my $self = shift;
  my @arg  = grep ( !/-z/, @_ );
  my $role = shift @arg
    or print STDERR "You have to specify a role or <default> !\n" and return;
  my $subject;
  my @results;
  $#results = -1;
  my $rethash = ();
  $subject = join " ", @arg;
  $subject or print STDERR "You have to specify a subject!\n" and return;
  print "Verifying subject $subject\n";
  my $done =
    $self->{SOAP}
    ->CallSOAP( "Authen", "verifyRoleFromSubject", $subject, $role )
    or return;
  $done = $done->result;
  $DEBUG
    and $self->debug(
     1,
     "The Subject $subject requested as role $role will be mapped to role $done"
    );

  if ($done) {
    $rethash->{subject}     = $subject;
    $rethash->{desiredrole} = $role;
    $rethash->{role}        = $done;
    push @results, $rethash;
  }
  return @results;
}

sub f_find_HELP {
  return
"Usage: find [-<flags>] <path> <fileName> [-name <fileName>]* [[<tagname>:<condition>] [ [and|or] [<tagname>:<condition>]]*]\nPossible flags are:
   z => return array of hash
   v => switch on verbose mode (write files found etc.)
   p => set the printout format
   l => limit number of returned files per database host
   o => offset for the limit per database host
   x => write xml - 2nd arg is collection name
   r => resolve all file information (should be used together with -x -z)
   g => file group query (has to be used together with -x -z)
   s => no sorting
   d => return also the directories
   c => put the output in a collection - 2nd arg is the collection name
   m => metadata on file level 
   y => (FOR THE OCDB) return only the biggest version of each file
";
}

# Internal subroutine. Called from find, to get all the constraint
# Input: Constraints as received from the command line (<tagName>:<tagCond> [and|or [<tagName>:<tagCond]]+
#
# Ouput status (1 or undef if error)
#       queries reference to a list of queries
#       paths   reference to a list of paths with the tags
#       unions  reference to a list of unions between the queries
sub getFindConstraints {
  my $self = shift;
  my ( @unions, @tagNames, @tagQueries ) = ( (), (), () );
  my @constraints = ();
  @_ and @constraints = ( "and", @_ );
  while (@constraints) {
    my $union = shift @constraints;
    my $tempName = ( shift @constraints or ":" );
    my ( $name, $query ) = split ":", $tempName, 2;
    $query or $query = "";
    $DEBUG
      and $self->debug( 1, "There is a constraint  $union, $name, $query" );
    my @total = $query =~ /[\'\"]/g;
    my $error = "";
    while ( ( $#total + 1 ) % 2 ) {
      $DEBUG
        and $self->debug( 1, "So far There are an odd number of brackets" );
      if ( !@constraints ) {
        $error = "unbalanced number of parentheses";
        last;
      }
      $query .= " " . shift @constraints;
      $DEBUG and $self->debug( 1, "Appending to the query $query" );
      @total = $query =~ /[\'\"]/g;
    }
    ( $union      eq "and" )
      or ( $union eq "or" )
      or $error = "I don't understnad union '$union'";
    $name  or $error = "Missing the name of the Tag";
    $query or $error = "Missing the condition";
    $error
      and print STDERR "Error: not enough arguments in find\n(\t\t$error ) \n"
      . $self->f_find_HELP()
      and return;
    $self->info("Filtering according to '$union' $name $query");
    $query =~ s/===/ like / and $self->info("This is a like query");
    push @unions,     $union;
    push @tagNames,   $name;
    push @tagQueries, $query;
  }
  shift @unions;
  return ( 1, \@tagQueries, \@tagNames, \@unions );
}

sub f_linkfind {
  my $self          = shift;
  my @arg           = grep ( !/-\w+/, @_ );
  my $path          = ( $self->f_complete_path( $arg[0] ) or "" );
  my @searchdirs    = ();
  my $checkonlylast = grep ( /-1/, @_ );
  my $recursive     = grep ( /-r/, @_ );
  my $replace       = grep ( /-e/, @_ );

  #   print "Path is $path\n";
  my @rpath = split '\/', $path;
  my $newpath = "/";
  if ($checkonlylast) {

    # list the links in this directory
    my $oldsilent = $self->{SILENT};
    $self->{SILENT} = 1;
    my @links = $self->f_ls( "-s", $path );
    foreach my $link (@links) {
      $newpath = "$path" . '/' . $link . '/';
      $newpath =~ s/\/\//\//g;
      my $newnewpath;
      if ($replace) {
        $newnewpath = $self->GetAbsolutePath( $newpath, 1 );
      } else {
        $newnewpath = $path;
      }

      #	   print "Found link $newpath -> $newnewpath\n";
      push @searchdirs, $newnewpath;
    }
    $self->{SILENT} = $oldsilent;
  } else {
    foreach (@rpath) {
      if ( $_ eq "" ) {
        next;
      }
      $newpath = $newpath . $_;
      if ( $self->isDirectory($newpath) ) {
        $newpath = $newpath . '/';
      }
      $self->info("linkfind: Checking $newpath");

      # list the links in this directory
      my $oldsilent = $self->{SILENT};
      $self->{SILENT} = 1;
      my @links = $self->f_ls( "-s", $newpath );
      foreach my $link (@links) {
        $DEBUG and $self->debug( 1, "linkfind: Found link $link" );
        ### resolve the link
        $newpath = $newpath . $link . '/';
        my $newnewpath = $self->GetAbsolutePath($newpath);

        #	       print "Found $link => $newnewpath\n";
        push @searchdirs, $newnewpath;
      }
      $self->{SILENT} = $oldsilent;
    }
  }
  if ($recursive) {
    foreach (@searchdirs) {
      my @newsearchdirs = $self->f_linkfind( $_, "-1" );
      push @searchdirs, @newsearchdirs;
    }
  }
  return @searchdirs;
}

sub f_lsguid {
  my $self    = shift;
  my $path    = shift;
  my @results = ();
  if ( !( $path =~ /\/$/ ) ) {
    $path .= "/";
  }
  ( $self->checkPermissions( "r", $path ) ) or return;
  my $pathIdx = $self->GetDirIdx($path);
  $DEBUG
    and $self->debug( 1,
                  "f_lsguid: Listing guids in dirIdx $pathIdx and path $path" );
  if ($pathIdx) {
    return $self->{DATABASE}
      ->getFieldsFromD0Ex( "path, guid ", "WHERE dir=$pathIdx" );
  }
  return;
}

sub f_outputformat {
  my $self = shift;
  my ( $a, $v ) = @_;
  my $r = $a->get_value($v);
  $r = "" unless defined $r;
  $r =~ s/\s*$//o;
  return $r;
}

sub f_stat {
  my $self = shift;
  my $lfn  = shift
    or $self->info("Error: missing path in stat")
    and return;
  $lfn = $self->GetAbsolutePath($lfn);
  $DEBUG and $self->debug( 1, "Getting the stat of $lfn" );
  my $info = $self->checkPermissions( "r", $lfn, 0, 1 );
  $info or return;
  $self->existsEntry( $lfn, $info->{lfn} )
    or $self->info("The entry '$lfn' doesn't exist")
    and return;
  $self->info(
"File $info->{lfn} Type: $info->{type}  Perm: $info->{perm} Size: $info->{size}",
    undef, 0
  );
  return $info;
}

sub f_showcertificates {
  my $self                = shift;
  my $returnarrayhash     = grep ( /-z/, @_ );
  my $silent              = grep ( /-s/, @_ );
  my $allcertificatehash  = {};
  my @allcertificatearray = ();
  local $, = "\n", $\ = "\n";
  my $ldap = $self->{CONFIG}->GetLDAPDN();
  my $msg = $ldap->search( base   => "ou=People,$self->{CONFIG}{LDAPDN}",
                           filter => "(objectClass=AliEnUser)" );
  my $num = $msg->count;

  for ( my $i = 0 ; $i < $num ; ++$i ) {
    my $a       = $msg->entry($i);
    my $newhash = {};
    $newhash->{"subject"} = $self->f_outputformat( $a, 'subject' );
    $newhash->{"uid"}     = $self->f_outputformat( $a, 'uid' );
    $allcertificatehash->{ $newhash->{"uid"} } = $newhash;
    push @allcertificatearray, $newhash;
    if ( !$silent ) {
      printf "%-32s \t Certificate: %-24s\n", $newhash->{"uid"},
        $newhash->{"subject"};
    }
  }
  if ( !$returnarrayhash ) {
    return $allcertificatehash;
  } else {
    return @allcertificatearray;
  }
}

sub f_partitions {
  my $self            = shift;
  my $returnarrayhash = grep ( /-z/, @_ );
  my $silent          = grep ( /-s/, @_ );
  if ($returnarrayhash) { shift; }
  if ($silent)          { shift; }
  my $ldap   = $self->{CONFIG}->GetLDAPDN();
  my $config = {};
  my $mesg;
  my $total   = 0;
  my $verbose = 0;
  $mesg = $ldap->search( base   => "ou=Partitions,$self->{CONFIG}{LDAPDN}",
                         filter => "(&(objectClass=top))" );
  $total = $mesg->count;
  my @result;

  for ( my $i = 0 ; $i < $total ; $i++ ) {
    my $entry     = $mesg->entry($i);
    my $partition = $entry->get_value('name');
    if ( !defined $partition or $partition eq "" ) {
      next;
    }
    $silent or print "Partition:=>  $partition\n";
    if ($returnarrayhash) {
      my $newhash;
      $newhash->{partition} = $partition;
      push @result, $newhash;
    } else {
      push @result, $partition;
    }
  }
  return @result;
}

sub f_getsite {
  my $self = shift;
  my $returnarrayhash = grep ( /-z/, @_ );
  if ($returnarrayhash) { shift; }
  my $host = shift
    or print STDERR "ERROR: you have to give a hostname as argument!\n"
    and return;
  my $domain = $1 if $host =~ /[^\.]+\.(.*)$/;
  my $ldap   = $self->{CONFIG}->GetLDAPDN();
  my $config = {};
  my $mesg;
  my $total;
  my $verbose = 0;
  my $se;

  if ($domain) {
    $mesg = $ldap->search(base   => "ou=Sites,$self->{CONFIG}{LDAPDN}",
                          filter => "(&(domain=$domain)(objectClass=AliEnSite))"
    );
    $total = $mesg->count;
  }
  if ( !$total ) {
    $verbose
      and print STDERR
"ERROR: There is no site in $self->{CONFIG}->{ORGANISATION} for your domain ($domain)\n";
    if ($returnarrayhash) {
      my @result;
      my $newhash;
      $newhash->{site} = "none";
      $newhash->{se}   = "none";
      push @result, $newhash;
      return @result;
    }
    return "";
  }
  my $entry = $mesg->entry(0);
  my $site  = $entry->get_value('ou');
  print "You are om site $site\n";
  my $fullLDAPdn = "ou=$site,ou=Sites,$self->{CONFIG}{LDAPDN}";
  my $service    = "SE";
  $mesg = $ldap->search( base   => "ou=$service,ou=services,$fullLDAPdn",
                         filter => "(objectClass=AliEn$service)" );
  $total = $mesg->count;

  if ( !$total ) {
    $se = "none";
    print STDERR
      "ERROR: Service $service is not configured for your site ($site)\n";
  } else {
    $entry = $mesg->entry(0);
    $se    = $entry->get_value('name');
    print "You have $self->{CONFIG}->{ORG_NAME}::${site}::${se} as site SE\n";
  }
  if ($returnarrayhash) {
    my @result;
    my $newhash;
    $newhash->{site} = $site;
    $newhash->{se}   = "$self->{CONFIG}->{ORG_NAME}::${site}::${se}";
    push @result, $newhash;
    return @result;
  }
  return "$site/$se";
}

sub f_mlconfig {
  my $self                 = shift;
  my $host                 = shift;
  my $DEFAULT_APMON_CONFIG = "aliendb5.cern.ch";
  my $domain               = $1 if $host =~ /[^\.]+\.(.*)$/;
  my $ldap                 = $self->{CONFIG}->GetLDAPDN();
  my $config               = {};
  my $mesg;
  my $total;
  my $verbose = 0;

  if ($domain) {
    $mesg = $ldap->search(base   => "ou=Sites,$self->{CONFIG}{LDAPDN}",
                          filter => "(&(domain=$domain)(objectClass=AliEnSite))"
    );
    $total = $mesg->count;
  }
  if ( !$total ) {
    $verbose
      and print STDERR
"ERROR: There is no site in $self->{CONFIG}->{ORGANISATION} for your domain ($domain)\n";
    $config = $DEFAULT_APMON_CONFIG;
    printf "APMON_CONFIG=$DEFAULT_APMON_CONFIG\n";
    return "$DEFAULT_APMON_CONFIG";
  }
  my $entry      = $mesg->entry(0);
  my $site       = $entry->get_value('ou');
  my $fullLDAPdn = "ou=$site,ou=Sites,$self->{CONFIG}{LDAPDN}";
  my $service    = "MonaLisa";
  $mesg = $ldap->search( base   => "ou=$service,ou=services,$fullLDAPdn",
                         filter => "(objectClass=AliEn$service)" );
  $total = $mesg->count;
  if ( !$total ) {
    $verbose
      and print STDERR
      "ERROR: Service $service is not configured for your site ($site)\n";
  } else {
    $entry = $mesg->entry(0);
    for my $attr ( $entry->attributes ) {
      $config->{ uc("$service\_$attr") } = $entry->get_value($attr)
        if $attr ne "objectClass";
    }
    for my $key ( keys %$config ) {
      print "$key=\"$config->{$key}\"\n";
    }
  }
  my $apmonConfig = $DEFAULT_APMON_CONFIG;
  if ( $config->{MONALISA_APMONCONFIG} ) {
    my $cfg = eval( $config->{MONALISA_APMONCONFIG} );
    if ($cfg) {
      if ( ref($cfg) eq "HASH" ) {
        my @k = keys(%$cfg);
        $cfg = $k[0];
      } elsif ( ref($cfg) eq "ARRAY" ) {
        $cfg = $$cfg[0];
      }
      $apmonConfig = $cfg;
    }
  } elsif ( $config->{MONALISA_HOST} ) {
    $apmonConfig = $config->{MONALISA_HOST};
  }
  print "APMON_CONFIG=$apmonConfig\n";
  return "$apmonConfig";
}

sub f_locatesites {
  my $self            = shift;
  my $seIndex         = $self->{DATABASE}->query("SELECT * from SE");
  my $returnarrayhash = grep ( /-z/, @_ );
  my $silent          = grep ( /-s/, @_ );
  my $allsitehash     = {};
  my @allsitearray    = ();
  local $, = "\n", $\ = "\n";
  my $ldap = $self->{CONFIG}->GetLDAPDN();
  my $msg = $ldap->search( base   => "ou=Sites,$self->{CONFIG}{LDAPDN}",
                           filter => "(objectClass=AliEnSite)" );
  my $num = $msg->count;

  for ( my $i = 0 ; $i < $num ; ++$i ) {
    my $a       = $msg->entry($i);
    my $newhash = {};
    $newhash->{"site"}      = $self->f_outputformat( $a, 'ou' );
    $newhash->{"location"}  = $self->f_outputformat( $a, 'location' );
    $newhash->{"domain"}    = $self->f_outputformat( $a, 'domain' );
    $newhash->{"latitude"}  = $self->f_outputformat( $a, 'latitude' );
    $newhash->{"longitude"} = $self->f_outputformat( $a, 'longitude' );
    $newhash->{"seIndex"}   = ",";

    # look for the se indices
    foreach (@$seIndex) {
      my ( $vo, $d1, $site, $d2, $unit ) = split ':', $_->{seName};
      if ( ( uc $site ) eq ( uc $newhash->{"site"} ) ) {
        $newhash->{seIndex} .= $_->{seNumber} . ",";
      }
    }
    $allsitehash->{ $newhash->{"site"} } = $newhash;
    if ( $newhash->{"latitude"}  eq "" ) { $newhash->{"latitude"}  = 0; }
    if ( $newhash->{"longitude"} eq "" ) { $newhash->{"longitude"} = 0; }
    push @allsitearray, $newhash;
    if ( !$silent ) {
      printf
"%-32s \t Location: %-20s Domain: %-24s Lat: %04.2f Lon: %04.2f SeIndx: %s\n",
        $newhash->{"site"},     $newhash->{"location"},  $newhash->{"domain"},
        $newhash->{"latitude"}, $newhash->{"longitude"}, $newhash->{"seIndex"};
    }
  }
  if ( !$returnarrayhash ) {
    return $allsitehash;
  } else {
    return @allsitearray;
  }
}

sub f_find {
  my $self = shift;
  my $cmdline = "find " . join( ' ', @_ );
  #### standard to retrieve options with and without parameters
  my %options = ();
  @ARGV = @_;
  getopts( "mvzrpO:o:l:x:g:sO:q:dc:y", \%options );
  @_ = @ARGV;

  # option v => verbose
  # option z => return array of hash
  # option p => set the printout format
  # option l => limit in query per host
  # option x => write xml - 2nd arg is collection name
  # option g => file group query
  # option r => resolve all
  # option s => no sorting
  # option O => add opaque information to the results
  # option q => quiet mode
  # option d => return directories
  # option m => metadata on file level
  my $quiet   = $options{'q'};
  my $verbose = $options{v};
  #### -p option
  my @printfields = ("lfn");
  if ( defined $options{p} ) {
    @printfields = ();
    map { push @printfields, $_; } ( split( ",", $options{p} ) );
  }
  $DEBUG and $self->debug( 1, "printfields are: @printfields" );
  #### -g option
  my @filegroup = ();
  if ( defined $options{g} ) {
    map { push @filegroup, $_; } ( split( ",", $options{g} ) );
    $DEBUG
      and $self->debug( 1, "Setting file group queries for files @filegroup" );
  }
  my $path = ( $self->f_complete_path(shift) or "" );
  my $file = ( shift or "" );
  $path =~ s/\*/%/g;
  $file =~ s/\*/%/g;
  ($file)
    or print STDERR"Error: not enough arguments in find\n"
    . $self->f_find_HELP()
    and return;
  #### -g option
  if ( defined $options{g} ) {
    if ( $file =~ /%/ ) {
      print STDERR
"To query filegroups, you need to specify an exact reference file to find a file group - no wildcards are allowd!\n"
        and return;
    }
  }
  ( $self->checkPermissions( "r", $path ) ) or return;
  if ( !defined $options{x} ) {
    $quiet
      or $verbose
      and $self->info(
                 "Doing a find in directory $path for files with name '$file'");
  }
  $file =~ s{'}{\\'};
  #### -r option
  if ( defined $options{r} ) {
    $DEBUG and $self->debug( 1, "Setting resolve all tag to $options{r}" );

    #      $sitelocationhash = $self->f_locatesites("-s");
    if ( !defined $self->{sitelocationarray}
         or ( ( time - $self->{sitelocationtime} ) > 600 ) )
    {
      my @allsites = $self->f_locatesites("-z");
      $self->{sitelocationarray} = \@allsites;
      $self->{sitelocationtime}  = time;
    }
  }
  $file = [$file];
  while ( $_[0] and $_[0] =~ /^-name$/i ) {
    $self->info("The option is -name!!");
    shift;
    push @$file, shift;
  }
  my ( $status, $refQueries, $refNames, $refUnions ) =
    $self->getFindConstraints(@_);
  $status or return;
  my $pattern = join( "* or $path*", @$file );
  $DEBUG and $self->debug( 1, "Searching for files like $path*$pattern* ..." );
  $options{selimit} = $self->{LIMIT_SE};
  my $entriesRef =
    $self->{DATABASE}
    ->findLFN( $path, $file, $refNames, $refQueries, $refUnions, %options )
    or return;
  my @result = @$entriesRef;
  my $total  = @result;

  if ( defined $options{r} ) {

    # add the additional information like longitude, latitude, MSD
    foreach (@result) {
      $_->{msd}       = ",";
      $_->{longitude} = ",";
      $_->{latitude}  = ",";
      $_->{location}  = ",";
      $_->{domain}    = ",";
      my @indices = split ',', $_->{seStringlist};
      foreach my $index (@indices) {
        if ( $index eq "" ) {
          next;
        }

        # lookup this index in the site location hash
        foreach my $site ( @{ $self->{sitelocationarray} } ) {
          if ( $site->{seIndex} =~ /,$index,/ ) {
            $_->{msd}       .= $site->{site} . ",";
            $_->{location}  .= $site->{location} . ",";
            $_->{longitude} .= $site->{longitude} . ",";
            $_->{latitude}  .= $site->{latitude} . ",";
            $_->{domain} = $site->{domain};
            last;
          }
        }
        if ( $_->{msd} eq "," ) {
          $_->{msd}       = ",none,";
          $_->{longitude} = ",0,";
          $_->{latitude}  = ",0,";
          $_->{domain}    = ",no-domain,";
          $_->{location}  = ",unknown,";
        }
      }
    }
  }
  if ( defined $options{c} ) {
    $self->createFindCollection( $options{c}, \@result );
  }
  if ( defined $options{x} ) {
    @result =
      $self->createFindXML( $file, $cmdline, \%options, \@result, \@filegroup );
  } else {
    if ( !$self->{SILENT} ) {
      $quiet or (@result) or $verbose and print "No files found!!\n";
    }
    if ( $options{O} ) {
      map { $_->{turl} = "alien://" . $_->{lfn} . "?$options{O}"; } @result;
    } else {
      map { $_->{turl} = "alien://" . $_->{lfn}; } @result;
    }
    if ( !$self->{SILENT} and !$quiet ) {
      map {
        foreach my $field (@printfields) { print STDOUT "$_->{$field}   "; }
        print STDOUT "\n";
      } @result;
      ($total) and $verbose and print "$total files found\n";
    }
    if ( !$options{z} ) {
      my @plainresult;
      map { push @plainresult, $_->{lfn}; } @result;
      return @plainresult;
    }
    ($total) and print "$total files found\n";
  }
  return @result;
}

sub createFindXML {
  my $self      = shift;
  my $file      = shift;
  my $cmdline   = shift;
  my $ref       = shift;
  my $ref2      = shift;
  my $ref3      = shift;
  my %options   = %$ref;
  my @result    = @$ref2;
  my @filegroup = @$ref3;
  $DEBUG
    and $self->debug( 1, "Setting xml dump collection name to $options{x}" );
  my $dumpxml = $options{x};

  if ( $options{O} ) {
    map { $_->{turl} = "alien://" . $_->{lfn} . "?$options{O}"; } @result;
  } else {
    map { $_->{turl} = "alien://" . $_->{lfn}; } @result;
  }
  map {
    foreach my $lkey ( keys %{$_} ) {
      if ( !defined $_->{$lkey} ) { $_->{$lkey} = ""; }
    }
  } @result;
  my @newresult;
  map {
    my $bname   = $self->f_basename( $_->{lfn} );
    my $dname   = $self->f_dirname( $_->{lfn} );
    my $newhash = {};
    $newhash->{$bname} = $_;
    if ( $options{g} ) {
      if ( grep ( /$bname/, @$file ) ) {
        push @newresult, $newhash;
      }
    } else {
      push @newresult, $newhash;
    }
  } @result;
  foreach (@newresult) {
    my $filename;
    for my $lkeys ( keys %{$_} ) {
      $filename = $lkeys;
    }
    my $bname = $self->f_basename( $_->{$filename}->{lfn} );
    my $dname = $self->f_dirname( $_->{$filename}->{lfn} );
    if ( $options{g} ) {
      for my $lfile (@filegroup) {
        if ( !defined $_->{$lfile} ) {
          $_->{$lfile}->{lfn}  = $dname . "/" . $lfile;
          $_->{$lfile}->{turl} = "alien://" . $dname . "/" . $lfile;
        }
      }
    }
  }
  $dumpxml =~ s/\"//g;
  my $dataset = new AliEn::Dataset;
  $dataset->setarray( \@newresult, "$dumpxml", "[$self->{DISPPATH}]: $cmdline",
                      "", "", "$self->{CONFIG}->{ROLE}" );
  $self->{DEBUG} and $dataset->print();
  my $xml = $dataset->writexml();
  $self->{SILENT} or print $xml;
  $result[0]->{xml} = $xml;
  return 1;
}

sub createFindCollection {
  my $self     = shift;
  my $collec   = shift;
  my $filesRef = shift;
  $self->f_createCollection($collec) or return;
  foreach my $file (@$filesRef) {
    $file->{type} =~ /f/
      or $self->info("Skipping $file->{lfn} (not a file)")
      and next;
    $self->info("And now we have to add $file to the collection");
    $self->f_addFileToCollection( $file->{lfn}, $collec, "-n" );
  }
  $self->updateCollection( "", $collec );
  return 1;
}

sub f_revalidateToken {
  my $self  = shift;
  my $hours = shift;
  if ($hours) {
    if ( $self->{ROLE} ne "admin" ) {
      print STDERR
        "Only the administrator can specify length for token update.\n";
      $hours = 24;
    }
  } else {
    $hours = 24;
  }
  my $done =
    SOAP::Lite->uri('AliEn/Service/Authen')
    ->proxy(
           "http://$self->{CONFIG}->{PROXY_HOST}:$self->{CONFIG}->{PROXY_PORT}")
    ->addTimeToToken( $self->{ROLE}, $hours )->result;
  if ($done) {
    print STDERR "Your token has been revalidated for $hours hours\n";
    return 1;
  } else {
    print STDERR "Error while trying to request token update\n";
    return;
  }
  return 1;
}

sub createRemoteTable {
  my $self = shift;
  ( $self->{DEBUG} > 3 )
    and print "DEBUG LEVEL 3\tIn UserInterface createRemoteTable @_\n";
  my $host   = shift;
  my $db     = shift;
  my $driver = shift;
  my $user   = shift;
  my $table  = shift;
  my $SQL    = shift;
  ($table)
    or print STDERR "Error: in CreateRemoteTable. table not specified\n"
    and return;
  my $done =
    SOAP::Lite->uri('AliEn/Service/Authen')
    ->proxy("http://$self->{CONFIG}->{AUTH_HOST}:$self->{CONFIG}->{AUTH_PORT}")
    ->createTable( $host, $db, $driver, $user, $table, $SQL );
  $self->{SOAP}->checkSOAPreturn($done) or return;
  $done = $done->result;
  $DEBUG and $self->debug( 1, "Making the remote table worked, got $done" );   #
  return $done;
}

sub printTreeLevel {
  my $self  = shift;
  my $first = shift;
  $DEBUG and $self->debug( 1, "UserInterface::printreeLevel $first" );
  my @files = grep( /^[^\/]*\/?$/i, @_ );
  $DEBUG and $self->debug( 1, "There are $#files in @_" );
  my $file;
  my $sec = 0;
  if ( (@_) and ( !@files ) ) { push @files, "/"; }

  foreach $file (@files) {
    if ( $file =~ /\/$/ ) {
      ($sec) and print STDOUT "$first\n";
      $sec = 1;
      print STDOUT "$first--$file\n";
      $file =~ s/\+/\\\+/g;
      my @dir = grep( s/^$file(.)/$1/i, @_ );
      $self->printTreeLevel( "$first  |", @dir );
    } else {
      print STDOUT "$first--$file\n";
    }
  }
  return 1;
}

sub f_tree {
  my $self = shift;
  my $dir = ( shift or $self->{DISPPATH} );
  $dir = $self->GetAbsolutePath($dir);
  $DEBUG and $self->debug( 1, "In UserInterface::f_tree $dir" );
  $dir =~ s{/?$}{/};
  my $ref = $self->{DATABASE}->findLFN( $dir, [], [], [], [], 'd', 1 )
    or return;
  my @entries    = @$ref;
  my @entriesLFN = ();

  foreach my $entry (@entries) {
    push @entriesLFN, $entry->{lfn};
  }
  $DEBUG and $self->debug( 1, "There are " . ( $#entries + 1 ) . " entries" );
  map { $_ =~ s/$dir/.\//i } @entriesLFN;
  $self->printTreeLevel( "|", @entriesLFN );
  print STDOUT "\n";
  return 1;
}

sub f_zoom {
  my $self       = shift;
  my $likestring = shift;
  if(defined $likestring) {
    $likestring = $self->GetAbsolutePath($likestring) . "%";
  }
  else {
    $likestring = $self->{DISPPATH} . "%";
  }
  my $rdirs      = $self->{DATABASE}->getFieldFromD0Ex( "path",
                      "where path like '$likestring' order by path limit 100" );
  defined $rdirs
    or $self->{LOGGER}
    ->error( "Catalogue", "Error in database while fetching path" )
    and return;
  my @files = grep ( !/\/$/, @$rdirs );
  ( $files[0] )
    or print STDERR "No files under the current directory!!\n" and return;
  $files[0] =~ /^(.*\/)[^\/]*$/;
  $self->{DISPPATH} = "$1";
  $self->f_pwd();
  return 1;
}

#sub filterFiles {
#  my $self = shift;
#  my $rfiles = shift;
#
#  ( $self->{DEBUG} > 2 )
#    and print "DEBUG LEVEL 2\tIn UserInterface: filterFiles @$rfiles\n";
#
#  my @visibles;
#  for my $rfile (@$rfiles) {
#    my $push = 1;
#    ($rfile->{comment} =~ /AlienOnlyGroup/) and $push = 0;
#    ($rfile->{comment} =~ /AlienOnlyGroup="(.*)"/)
#      and ((" $self->{MAINGROUP} $self->{GROUPS} ") =~ / $1 /)
#	and $push = 1;
#
#    $push and push @visibles, $rfile;
#  }
#
#  return \@visibles;
#}
sub f_echo {
  my $self  = shift;
  my $var   = ( shift or "" );
  my $value = ( $self->{CONFIG}->{$var} or "" );
  if ($var) {
    $var eq '$?' and return $self->displayLastError();
    my $print = "'$value'";
    if ( $var =~ /^((LOGGER)|(DATABASE)|(G_CONTAINER))/ ) {

      #we just skip the logger
    } elsif ( UNIVERSAL::isa( $value, "ARRAY" ) ) {

      #	  print "CHANGING $value\n";
      map { s/^(.*)$/'$1'/ } @{$value};
      $print = join( ", ", @{$value} );
    } elsif ( UNIVERSAL::isa( $value, "HASH" ) ) {
      $print = Dumper($value);
      $print =~ s/^\$VAR1 =//;
    }
    $self->{SILENT} or print "Configuration: $var = $print\n";
  } else {
    my @total = sort keys %{ $self->{CONFIG} };
    foreach (@total) {
      ($_) and $self->f_echo($_);
    }
  }
  return $value;
}

sub DESTROY {
  my $self = shift;
  ($self) and ( $self->{DATABASE} ) and $self->f_disconnect;
}

sub _setUserGroups {
  my $self       = shift;
  my $user       = shift;
  my $changeUser = shift;
  my $result     = $self->{DATABASE}->getUserGroups($user);
  $result
    or $self->{LOGGER}
    ->error( "Catalogue", "Error during database query execution" )
    and return;
  ( $self->{MAINGROUP} ) = $result->[0];
  $result = $self->{DATABASE}->getUserGroups( $user, 0 );
  $self->{DATABASE}->setUserGroup( $user, $self->{MAINGROUP}, $changeUser );
  $result
    or $self->{LOGGER}
    ->error( "Catalogue", "Error during database query execution" )
    and return;
  ( $self->{GROUPS} ) = join " ", @$result;
}

sub displayLastError {
  my $self     = shift;
  my $str      = $self->{LOGGER}->error_msg();
  my $error_no = $self->{LOGGER}->error_no();
  $self->info("Last error message: '$str' (error code $error_no)");
  return $str, $error_no;
}

sub f_type_HELP {
  return
"type: returns the type of entry an lfn is. Possibilities are: file, directory, collection
 Usage: type [-z] lfn

 Options:
   -z return an array of hash
";
}

sub f_df {
  my $self = shift;
  return $self->{DATABASE}->getDF(@_);
}

sub f_type {
  my $self = shift;
  my $hash = grep ( /^-z$/, @_ );
  @_ = grep( !/^-z$/, @_ );
  my $lfn = shift;
  $lfn = $self->f_complete_path($lfn);
  my $permFile =
    $self->checkPermissions( 'r', $lfn, 0, 1 )
    or return;
  my $type;
  if ( $self->isCollection( $lfn, $permFile ) ) {
    $type = 'collection';
  } elsif ( $self->isFile( $lfn, $permFile->{lfn} ) ) {
    $type = 'file';
  } elsif ( $self->isDirectory( $lfn, $permFile->{lfn} ) ) {
    $type = 'directory';
  } else {
    $self->info("I don't know the type of the file $lfn");
    return;
  }
  $self->info("File '$lfn' is a '$type'");
  $hash and return ( { type => $type } );
  return $type;
}


sub checkFileQuota {
#######
## return (0,message) for normal error
## return (-1,message) for error that should throw access exception. Consequence is all 
##                     remaining write accesses will be dropped, as they will fail anyway.
##
  my $self= shift;
  my $user = shift
    or $self->{LOGGER}->error("In checkFileQuota user is not specified.\n")
    and return (-1, "user is not specified.");
  my $size = shift;
        (defined $size) and ($size ge 0)
            or $self->{LOGGER}->error("In checkFileQuota invalid file size (undefined or negative).\n")
            and return (-1, "size is not specified.");

  $self->info("In checkFileQuota for user: $user, request file size:$size");

  my $array = $self->{DATABASE}->{LFN_DB}->queryRow("SELECT nbFiles, totalSize, maxNbFiles, maxTotalSize, tmpIncreasedNbFiles, tmpIncreasedTotalSize FROM processes.PRIORITY WHERE user='$user'")
    or $self->{LOGGER}->error("Failed to get data from the PRIORITY quota table.")
    and return (0, "Failed to get data from the PRIORITY quota table. ");
  $array or $self->{LOGGER}->error("There's no entry for user $user in the PRIORITY quota table.")
    and return (-1, "There's no entry for user $user in the PRIORITY quota table.");

  my $nbFiles = $array->{'nbFiles'};
  my $maxNbFiles = $array->{'maxNbFiles'};
  my $tmpIncreasedNbFiles = $array->{'tmpIncreasedNbFiles'};
  my $totalSize = $array->{'totalSize'};
  my $maxTotalSize = $array->{'maxTotalSize'};
  my $tmpIncreasedTotalSize = $array->{'tmpIncreasedTotalSize'};
 
  $DEBUG and $self->debug(1, "size: $size");
  $DEBUG and $self->debug(1, "nbFile: $nbFiles/$tmpIncreasedNbFiles/$maxNbFiles");
  $DEBUG and $self->debug(1, "totalSize: $totalSize/$tmpIncreasedTotalSize/$maxTotalSize");
  $self->info("nbFile: $nbFiles/$tmpIncreasedNbFiles/$maxNbFiles");
  $self->info("totalSize: $totalSize/$tmpIncreasedTotalSize/$maxTotalSize");

  #Unlimited number of files
  if($maxNbFiles==-1){
    $self->info("Unlimited number of files allowed for user ($user)");
  }
  else{
    if ($nbFiles + $tmpIncreasedNbFiles + 1 > $maxNbFiles) {
      $self->info("Uploading file for user ($user) is denied - number of files quota exceeded.");
      return (-1, "Uploading file for user ($user) is denied - number of files quota exceeded." );
    }
  }
  #Unlimited size for files
  if($maxTotalSize==-1){
    $self->info("Unlimited file size allowed for user ($user)");
  }
  else{
    if ($size + $totalSize + $tmpIncreasedTotalSize > $maxTotalSize) {
      $self->info("Uploading file for user ($user) is denied, file size ($size) - total file size quota exceeded." );
      return (-1, "Uploading file for user ($user) is denied, file size ($size) - total file size quota exceeded." );
    }
  }
  
  #$self->{PRIORITY_DB}->do("update PRIORITY set tmpIncreasedNbFiles=tmpIncreasedNbFiles+1, tmpIncreasedTotalSize=tmpIncreasedTotalSize+$size where user LIKE  '$user'") or $self->info("failed to increase tmpIncreasedNbFile and tmpIncreasedTotalSize");

  $self->info("In checkFileQuota $user: Allowed");
  return (1,undef);
}



return 1;
__END__




