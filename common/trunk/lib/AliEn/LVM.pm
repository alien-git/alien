package AliEn::LVM;
use strict;

use AliEn::Config;
use AliEn::MSS;
use vars qw(@ISA $DEBUG);
use AliEn::MD5;


push @ISA, 'AliEn::Logger::LogObject';
$DEBUG=0;

my $volumeFields = {
      'volume' => 'CHAR(255)',
      'mountpoint' => 'CHAR(255)',
      'size'       => 'INTEGER',
      'freespace'  => 'INTEGER',
      'usedspace'  => 'INTEGER',
      };

my $fileFields = {
      'file'          => 'CHAR(255)',
      'volume'        => 'CHAR(255)',
      'expires'       => 'INTEGER',
      'size'          => 'INTEGER',
      'ttl'           => 'INTEGER',
      };


sub new {
  my ($this) = shift;
  my $class = ref($this) || $this;

  my $self = shift || {} ;

  bless $self, $class;

  $self->{CONFIG}=new AliEn::Config;
  $self->{LOGGER}=new AliEn::Logger;

  $self->GetDefaultOptions() or return;

  return $self;
}

sub GetDefaultOptions{
  my $self=shift;

    # set default DB path to the first SE disk
  my $dbconfig = ($self->{CONFIG}->{'SE_LVMDATABASE'} or "");
  my $defaultttl = -1;

  my ($seoptions) = $self->{CONFIG}->{'SE_OPTIONS_LIST'};
  my @singleoption=();
  $seoptions and  @singleoption=@{$seoptions};

  foreach (@singleoption) {
    my ($identifier,$value) = split ("=", $_);
    my $cidentifier = $identifier;
    if ($cidentifier=~/^lvmttl/) {
      defined $value and $defaultttl   = $value;
    }
    if ($cidentifier=~/^lvmdbpath/) {
       defined $value and $dbconfig = $value;
    }
  }

  if ($defaultttl == -1) {
    $self->{LOGGER}->info( "LVM", "LVM TTL    infinit");
  } else {
    $self->{LOGGER}->info( "LVM", "LVM TTL    $defaultttl");
  }

  if (!$self->{DB}) {
    if ($dbconfig) {
      my ($driver,$host,$db) = split '\/', $dbconfig;
      $self->{DB} = AliEn::Database::Lvm->new(
					      {
					       "USE_PROXY" => 0,  
					       "DB"     => $db,
					       "HOST"   => $host,
					       "DRIVER" => $driver,
					       "ROLE"   => $ENV{'ALIEN_LVM_USER'},
					       "PASSWD"=> $ENV{'ALIEN_LVM_PASSWORD'},
					       "SKIP_CHECK_TABLES"=> 0
					      }
					     );
    } else {
      $self->{LOGGER}->info("LVM", "Using the SQLite database");
      $self->{DB} = AliEn::Database::SQLite::LVM->new( {"TABLES" =>
							{VOLUME =>$volumeFields,
							 FILE=> $fileFields,
							} });
    }
  }
  $self->{DB}
    or print "Not able to get database with SSH\n" and return;

  $self->{LOGGER}->info( "LVM", "LVM DBPATH $dbconfig");
  my $lvmname = $self->{CONFIG}->{SE_FULLNAME};
  $lvmname =~ s/\:\:/\_/g;

  $self->{'DEFAULTTTL'}=$defaultttl;
  $self->{INITIALISED} = 0;
  return 1;
}


=head1 METHODS

=head2 subroutine initialiseDatabase

 - Takes no arguments
 - Creates the file and volume tables if they do not already exist
 - The columns of the tables are the hash entries provided earlier

=cut

sub initialised{
  my $self = shift;
  return   $self->{INITIALISED};
}

sub initialiseDatabase{

  my $self= shift;
  my $rebuild = 0;


  if ((defined $ENV{'ALIEN_LVM_RESYNC'}) and ($ENV{'ALIEN_LVM_RESYNC'} eq "1" )) {
     $self->{DB}->_do("delete from FILES where 1");
     $self->{DB}->_do("delete from VOLUMES where 1");      		
     $rebuild=1;				
  }

  if ($ENV{'ALIEN_LVM_USER'} && $ENV{'ALIEN_LVM_PASSWORD'}) {
    $self->{INITIALISED} = 1;
  }

  return $rebuild;
}

=head2 subroutine syncDatabase

 - Automatically checks for expired files
 - Checks that all the files in the file table exist
   if not it removes them from the table
 - Checks that volume freespace entry is correct.

=cut

sub syncDatabase{

   my $self = shift;
   $self->cleanUpExpired();
   # retrieve all the files
#   my $filelistref = $self->retrieveFileList();
   # This checks that all the files in the file table actually exist

# we skip this, cause on castor we have a problem, if we sync with every restart
#   foreach my $file (@{$filelistref})
#   {
#      my $filedetails = $self->{DB}->retrieveFileDetails({'file' => $file});
#      my $voldetails = $self->{DB}->retrieveVolumeDetails({'volume' => $filedetails->{'volume'}});
#      # Get list of files on that volume
#      my $path = $filedetails->{'file'};
#      $path =~ s/\/\//\//g;
#      if (! ( -f $path )) {
#	  print("Remove $path $filedetails->{'file'}\n");
#	  $self->removeFile($filedetails);
#      }
#  }
   # Now check that each volume has the right amount of free space in its table
   # This should be fine - but lets check anyway!!
   my $vollistref = $self->{DB}->retrieveVolumeDetailsList();   
   foreach my $voldetails (@{$vollistref})
   {
      my $volumeId=$voldetails->{volumeId};
      my $usedspace =  $voldetails->{usedspace};
      my $freespace = $voldetails->{'size'};
      $freespace-=$usedspace;

      if ($voldetails->{'size'} == -1 ) {
	$freespace = 2000000000;
      }

      if ($freespace ne $voldetails->{freespace}) {
	$self->info("Updating the disk space of $volumeId to $freespace");
	$voldetails->{freespace}=$freespace;
	$self->{DB}->updateVolumeDetails($voldetails);
      }
   }

}

=head2 subroutine cleanUpExpired

 - Checks for expired files and removes them

=cut

sub cleanUpExpired{

    my $self = shift;
    my $time = time;
    my $string = "SELECT * FROM FILES WHERE "." expires < $time and expires != -1";
#    my $dbh = DBI->connect("DBI:CSV:f_dir=$self->{'DATABASELOCATION'}");
#    my $sth = $dbh->prepare($string);
#    $sth->execute() or die "Cannot execute: " . $sth->errstr();
    my @info;
#    while ( my  $item = $sth->fetchrow_hashref()  ) {
#      push @info, $item;
#    }
#    $sth->finish();
#    $dbh->disconnect();
#    foreach my $item (@info)
#    {
#      print "Removing $item->{'file'}\n";
#       $self->internalremoveFile($item);
#    }

}

=head2 subroutine addVolume
 - This is the subroutine for adding another volume
 - it should be passed the following hash 
   {
      'volume'     => 'CHAR(30)',
      'mountpoint' => 'CHAR(100)',
      'size'       => 'INTEGER',
      'freespace'  => 'INTEGER',
   }

=cut

sub addVolume{
   my $self = shift;
   my $hashref = shift;

   my $mss=shift;
   my $rebuild=shift;
   my $method=shift;

   my $details = $self->{DB}->retrieveVolumeDetails({'volume' => $hashref->{'volume'}});
   ($details) and return 0;

   $hashref->{'freespace'}=$hashref->{'size'};
   if ($hashref->{'size'} == -1 ) {
     $hashref->{'freespace'} = 2000000000;
   }
   $hashref->{'usedspace'}= 0;
   $hashref->{method}=$method;
   $self->{DB}->insertVolume($hashref);

   #First, let's create the directory:
   $mss->mkdir($hashref->{mountpoint});
   #now, let's add all the files;
   if ($rebuild) {
     my $filelist = $self->{MSS}->lslist($hashref->{mountpoint});
     $self->{LOGGER}->info( "LVM", "LVM Inserting Volume name=$hashref->{'volume'}} ...");

     if ($filelist ) {
       $self->cleanUpExpired();
       # Loop over all files of that volume and insert them ...
       print "===============================================\n";
       my $voldetails = $self->{DB}->retrieveVolumeDetails({volume =>  $hashref->{'volume'}});
       foreach my $insertfile (@{$filelist}) {
	 my $file = {
		     'file'          => $insertfile->{'name'},
		     'ttl'           => $self->{'DEFAULTTTL'},
		     'size'          => $insertfile->{'size'},
		     'volume'        => $hashref->{'volume'},
		    };
	 ($file->{size} == 0) and ($file->{size} = 1);
	 
	 print "=> LVM Inserting $file->{size} \t : $file->{file} \n";
	 $self->addFile($file, $voldetails) or 
	   print "Error adding $file->{file}\n";;
       }
       print "===============================================\n";
     }

    }

   return 1;
}

=head2 subroutine addFile

 - This is the subroutine for adding a file
 - Will allocate space on one of the volumes and returns fullpath to file created if this was done correctly
 - Needs to be passed the following hash
   {
      'file'          => 'CHAR(30)',
      'size'          => 'INTEGER',
      'ttl'           => 'INTEGER',
   }
 - returns 0 if failed

=cut

sub addFile{
   my $self = shift;
   my $hashref = shift;
   my $voldetails=shift;

   # First cleanup old files
   #   $self->cleanUpExpired();

   #### Check to see if it exists 
#  At some point we should check if the md5 matches if we try to add a replica
   $self->info("Adding the file to the LVM");

#   if ( defined $hashref->{'guid'} and $hashref->{'guid'}) {
#     my $details = $self->{DB}->retrieveFileDetails({guid => $hashref->{guid},
#						    }, {silent=>1,noquotes=>1 });
#     if ($details  and $details->{guid} ) {
#       $self->info("Trying to add a guid that already exists... let's check if the md5sum matches");
#       #       my $newMD5=AliEn::MD5->new($hashref->{pfn});
#       my $oldMD5=$details->{md5sum};
#       if (! $oldMD5) {
#	 $self->info("Generating the md5 of the previous entry");
#	 $oldMD5=AliEn::MD5->new($details->{pfn})
#	   or $self->info("Error generating the md5sum of $details->{pfn}")
#	     and return 0;
#	 $self->{DB}->update("FILES",{md5=>'$oldMD5'}, "guid=string2binary('$details->{guid}')");
#       }
#       if ($hashref->{md5} ne $oldMD5) {
#	 $self->info("Trying to add a mirror of $hashref->{guid}, but the signature is different!!!",111);
#	 return 0;
#       }
#     }
#   } else {
#     $details = $self->{DB}->retrieveFileDetails({'lfn' => $hashref->{'file'}});
#     return 0 if (($details->{'file'} eq $hashref->{'file'})&&($details->{'mountpoint'} eq $hashref->{'mountpoint'}));
#   }

   if (!$voldetails ||  ! $voldetails->{volumeId}) {
     $self->info("Choosing a volume");
     $voldetails= $self->{DB}->chooseVolume($hashref->{'size'});
     $voldetails or 
       $self->info("There are no extra volumes that can hold that file") 
	 and return 0;
   }
   $hashref->{'volumeId'}    = $voldetails->{volumeId};
   $self->info("Adding the file to volume '$hashref->{volumeId}'");

   $voldetails->{'freespace'} -= $hashref->{'size'};
   $voldetails->{'usedspace'} += $hashref->{'size'};
   defined $voldetails->{'numfiles'} and $voldetails->{'numfiles'} ++;
   if ($voldetails->{'size'} == -1) {
     $voldetails->{'freespace'} = 2000000000;
   }
   $voldetails->{volumeId} and $self->{DB}->updateVolumeDetails($voldetails);
   if ($hashref->{'ttl'} != -1) {
     $hashref->{'expires'}       = time + $hashref->{'ttl'}; 
   } else {
     $hashref->{'expires'}       = -1;
   }

   my $subfilename = $hashref->{'file'};
    $hashref->{pfn} or 
      $hashref->{pfn}="$voldetails->{method}$voldetails->{mountpoint}/$subfilename";
   $hashref->{'volumeId'}=$voldetails->{'volumeId'};

   my $fullpath = "$voldetails->{mountpoint}/$hashref->{'file'}";

   delete $hashref->{file};


#   $self->{DB}->insertFile($hashref) or return 0;

   $self->info("File '$fullpath' inserted in the database");
   return $fullpath;
}

=head2 subroutine getVolumeSpace

 - Requires the volumename
 - Returns the freespace on that volume

=cut

#sub getFreeBlocks{
#  my $self = shift;
#  my $freeSpace=$self->{DB}->queryValue("SELECT sum(freespace) FROM VOLUMES");
#  $freeSpace or $freeSpace=0;
#  return $freeSpace;
#}#
#
#sub getUsedBlocks{
#  my $self = shift;
#  my $usedSpace=$self->{DB}->queryValue("SELECT sum(usedspace) FROM VOLUMES");
#  $usedSpace or $usedSpace=0;
#  return $usedSpace;
#}
#sub getTotalBlocks{
#  my $self = shift;
#  my $totalSpace=$self->{DB}->queryValue("SELECT sum(size) FROM VOLUMES");
#  $totalSpace or $totalSpace=0;
#  return $totalSpace;
#}

sub getVolumeSpace{
   my $self = shift;
   my $volumename = shift;
   return $self->{DB}->queryValue("SELECT freespace from VOLUME where volume = ?", undef, {bind_values=>[$volumename]});
}

sub getUsedSpace{
  my $self = shift;
  my $volumename = shift;
  return $self->{DB}->queryValue("SELECT usedspace from VOLUME where volume = ?", undef, {bind_values=>[$volumename]});
}


=head2 subroutine retrieveVolumeSize

 - Requires the volumename
 - Returns the space on that volume

=cut


sub retrieveVolumeSize{

  my $self = shift;
  my $volumename = shift;
  my $hashref = $self->{DB}->retrieveVolumeDetails({'volume'=>"$volumename"});
  return $hashref->{'size'};

}

=head2 subroutine retrieveFileSize

 - Requires the filename
 - Returns the size of that file as registered in the file table.

=cut

sub retrieveFileSize{

  my $self = shift;
  my $filename = shift;
  my $hashref = $self->{DB}->retrieveFileDetails({'file'=>"$filename"});
  return $hashref->{'size'};

}

=head2 subroutine retrieveFileList

 - Returns a reference to an array containing a full file listing from the file table.

=cut


sub retrieveFileList{

  my $self = shift;
  return $self->{DB}->queryRow("SELECT file from FILES");
}

=head2 subroutine retrieveVolumeList

 - Returns a reference to an array containing a full volume listing from the volume table.

=cut


sub retrieveVolumeList{

  my $self = shift;
  return $self->{DB}->queryRow("SELECT volume from VOLUMES");
}


sub removeVolume{

   my $self = shift;
   my $hashref = shift;
#   my $volumedetails = retrieveVolumeDetails({'volumename' => $hashref->{'volumename'}});
   my $filelist = $self->retrieveTableRow("FILES",{'volume'=> $hashref->{'volume'} });
   unless(defined $filelist->[0]){
      $self->deleteTableRow("VOLUMES",$hashref);
      return 1;
   }
   return 0;
}

=head2 subroutine removeFile

 - Pass it a hash with at least file name
   {
     file => 'filename'
   }
 - If file exists it is deleted, the space it reserved is returned to freespace 
   and the entry is removed from the table.

=cut



sub removeFile{

  my $self = shift;
  my $hashref = shift;
  return 0 unless(defined $hashref->{'file'});
  my $fullfiledetails= $self->{DB}->retrieveFileDetails($hashref);
  my $voldetails = $self->{DB}->retrieveVolumeDetails({volume =>  $fullfiledetails->{'volume'}}) ;
  my $fullpath = $fullfiledetails->{'file'};


  $self->info("And now lets call the removeFile from the DB");
  #$self->{DB}->removeFile($hashref);
  $voldetails->{'freespace'} = $voldetails->{'freespace'} + $hashref->{'size'};
  $voldetails->{'usedspace'} -= $hashref->{'size'};
  if ($voldetails->{'size'} == -1 ) {
    $voldetails->{'freespace'} = 2000000000;
  }
  $self->{DB}->updateVolumeDetails($voldetails);
  # and just before we go 
  # lets clean up 
  $self->cleanUpExpired();
  return 1;
}



sub removeFileFromTable{

   my $self = shift;
   my $hashref = shift;
   if ((defined $hashref->{'file'})&&(defined $hashref->{'mountpoint'})){
      $self->deleteTableRow("FILES",$hashref);
   }
}

sub retrieveTableRow{
    my $self = shift;
    my $tablename = shift;
    my $hashref   = shift;

    my $string = "SELECT * FROM $tablename";
    my @bind = ();
    if (defined $hashref){
      $string .= " WHERE ".( join (" AND ", map {push(@bind, $hashref->{$_}); "$_ =  ?"} keys(%{$hashref})));
    }
    $self->{LOGGER}->debug("LVM", "Doing the query $string");
    my $info = $self->{DB}->query($string, undef, {bind_values=>[@bind]});
    $info or die "Cannot execute: " . $string;
    return $info;
}

sub deleteTableRow{
   my $self = shift;
   my $tablename = shift;
   my $hashref   = shift;
   my @bind = ();
   my $string ="DELETE FROM $tablename WHERE ".( join (" AND ", map {push(@bind, $hashref->{$_}); "$_ =  ?"} keys(%{$hashref})));
#   print "$string\n";
   my $sth = $self->{DB}->_do($string, {bind_values=>[@bind]});
   return $sth;
}

#sub updateFileDetails{
#
#   my $self = shift;
#   my $hashref = shift;
#   if (defined $hashref->{'ttl'})
#   {
#      $hashref->{'expires'} = time + $hashref->{'ttl'}; 
#   }
#   $self->updateTableRow("FILES","file",$hashref);
#
#}


sub updateTableRow{

   my $self = shift;
   my $tablename = shift;
   my $entry = shift;
   my $hashref   = shift;
#   my $set =  join (" , ", map {"$_ =  \'$hashref->{$_}\'"} removeElement($entry,keys(%{$hashref}))   );
#   my $string ="UPDATE $tablename SET ". $set ."  WHERE $entry =  \'$hashref->{$entry}\'";
#   print "$string\n";
   my $info = $self->{DB}->update($tablename, $hashref, "WHERE $entry = ?", {bind_values=>[$hashref->{$entry}]});
#   my $info = $self->{DB}->_do($string);
   return $info;
}

#sub removeElement{
#
#  my $element = shift;
#  my @array = @_;
#  my @temparray;
#  my $arb;
#  foreach $arb (@array)
#  {
#    push @temparray, $arb unless ( $element eq $arb  );
#  }
#  return @temparray;
#}

# return's the local filesize in 1024 bytes fragments
sub sizeofLocalFile {
#    my $self = shift;
    my $file = shift;

    my (
        $dev,  $ino,   $mode,  $nlink, $uid,     $gid,  $rdev,
        $size, $atime, $mtime, $ctime, $blksize, $blocks
        );

    $size = 0;

    if ( -f $file ) {
        (
            $dev,  $ino,   $mode,  $nlink, $uid,     $gid,  $rdev,
            $size, $atime, $mtime, $ctime, $blksize, $blocks
          )
            = stat($file);
        return ( int($size/1024 + 0.5) );
    }
    else {
        return (0);
    }
}

return 1;




__END__



