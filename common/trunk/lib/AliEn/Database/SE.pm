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
package AliEn::Database::SE;

use AliEn::Database;
use strict;

use vars qw(@ISA);

@ISA=("AliEn::Database");

sub preConnect {
  my $self=shift;
  $self->{ROLE}=$self->{CONFIG}->{CLUSTER_MONITOR_USER};
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;

  my $info=$self->{CONFIG}->{SE_LVMDATABASE};
  my $seName=$self->{CONFIG}->{SE_FULLNAME};
  if ($self->{VIRTUAL}) {
    $self->info("Creating the DB for the virtual object $self->{VIRTUAL}");
    $info=$self->{CONFIG}->{"SE_$self->{VIRTUAL}"}->{LVMDATABASE};
    $seName=$self->{CONFIG}->{"SE_$self->{VIRTUAL}"}->{FULLNAME};
  }

  if (! $info) {
    $self->{LOGGER}->info("SE", "Warning! There is no database specified for the SE. Using the one of the catalogue");
    $info=$self->{CONFIG}->{CATALOGUE_DATABASE};
    $info =~ s{/[^/]*$}{/\Lse_$seName\E};
    $info =~ s{::}{_}g;
  }
  $self->{LOGGER}->info("Catalogue", "Using  $info");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB})
    =split ( m{/}, $info);
  $ENV{ALIEN_LVM_PASSWD} and $self->{PASSWD}= $ENV{ALIEN_LVM_PASSWD} and $self->{USE_PROXY}=0;

  if ($self->{DRIVER}=~  /^lfc$/i) {
    $self->info("We are trying to connect to the lfc");
    eval {require AliEn::Database::SE::LFC;};
    if ($@) {
      $self->info("Error requiring the LFC module: $@");
      return;
    }
    $self=bless ($self, "AliEn::Database::SE::LFC") or return;
  }

  return 1;
}
sub initialize{
  my $self=shift;
  my %columns=(entryId=>"int(11) NOT NULL auto_increment PRIMARY KEY",
	       guid=>"binary(16)",
	       pfn=> "varchar(255) NOT NULL",
	       size=>"int(20)", 
	       ttl=>"int(11)",
	       expires=>"int",
	       volumeId=>"int(11)",
	       md5=>"char(32)"
	      );
  $self->checkTable("FILES",  "entryId", \%columns, 'entryId', ['INDEX(guid)']) or return;

  $self->checkTable("TODELETE",  "guid", {guid=>"binary(16)",
					   pfn=>"varchar(255)"}
		   ) or return;

  $self->checkTable("BROKENLINKS",  "guid", {guid=>"binary(16)",
					     })
    or return;
  
  #This table is used to check the files that are no longer in the SE
  $self->checkTable("FILES2",  "guid", {guid=>"binary(16)",
				       })
    or return;


  $self->checkTable("FTPSERVERS", "port", {port=>"int(11) NOT NULL PRIMARY KEY",
					  pid=>"int(11)",
					  pfn=>"char(255)",
					  time=>"int(11)",
					  user=>"varchar(255)"}, "port") or return;
  $self->checkTable("LOCALFILES", "pfn",{pfn=>"char(255) NOT NULL",
					 localCopy=>"char(255)",
					 size=>"int",
					 transferid=>"int"},"pfn") or return;
  
  return $self->checkTable("VOLUMES", "volume", {volumeId=>"int(11) NOT NULL auto_increment PRIMARY KEY",
						 volume=>"char(255) NOT NULL",
						 mountpoint=>"char(255)",
						 usedspace=>"int(11)",
						 freespace=>"int(11)",
						 size=>"int(11)",
						 method=>"char(255)",}, 
			   "volumeId", ['UNIQUE INDEX (volume)']);
}

##############################################################################
##############################################################################

=head2 subroutine retrieveVolumeDetails

 - Pass a hash with at least the volumename
   {
     volume => 'volumename'
   }
 - Returns a hash of the volume details as stored in the volume table.

=cut



sub retrieveVolumeDetails{
   my $self = shift;
   my $hashref = shift;

   my $string="SELECT * FROM VOLUMES";
   if (defined $hashref){
     $string .= " WHERE ".( join (" AND ", map {"$_ =  \'$hashref->{$_}\'"} keys(%{$hashref})));
   }  
   my $info=$self->queryRow($string);
   $info->{volume} or return;
   return $info;
}


sub insertVolume {
  my $self=shift;
  return $self->insert("VOLUMES", @_);
}

sub retrieveVolumeDetailsList{
  my $self=shift;
  return $self->query("SELECT * from VOLUMES");
}



=head2 subroutine retrieveFileDetails

 - Pass a hash with at least the filename 
   {
     file => 'filename'
   }
 - Returns a hash of the files details as stored in the file table.

=cut


sub retrieveFileDetails{
  my $self = shift;
  my $hashref = shift;
  my $string="SELECT * FROM FILES";
  my $options=shift || {};
  my $quotes="'";
  $options->{noquotes} and $quotes="";
  if (defined $hashref){
    $string .= " WHERE ".( join (" AND ", map {"$_ =  $quotes$hashref->{$_}$quotes"} keys(%{$hashref})));
  }  

  return $self->queryRow($string);
}



=head2 subroutine updateVolumeDetails

 - pass reference to a hash containing the volume name and any details that have changed.
 - call resyncDatabase for any further free space calculations
 - if volume size is decreased and volume is to small to contain files its free space is negative and no further file will be added until it has sufficient free space again.

=cut

sub updateVolumeDetails{
   my $self = shift;
   my $hashref = shift;
   my $where="WHERE volume='$hashref->{volume}'";
   undef $hashref->{volume};
   my $set =  join (" , ", map {"$_ =  \'$hashref->{$_}\'"} keys(%{$hashref})   );
   my $string ="UPDATE VOLUMES SET ". $set ."  WHERE volume=  \'$hashref->{volume}\'";
#   print "$string\n";
   return  $self->_do($string);

}


sub chooseVolume {
  my $self = shift;
  my $size = shift;
  my $guid=shift;
  
  my $query="SELECT * from VOLUMES where freespace>$size";
  if ($guid){
    my $volumes=$self->queryColumn("SELECT concat('volumeId !=', volumeId) from FILES where guid=string2binary($guid)");
    $volumes and $query = join (" and ", $query, @$volumes);
   }
  my $listref = $self->query($query);
  $listref or return;
  return shift @$listref;

}


sub retrieveAllVolumesUsage {
  my $self=shift;
  my $info=$self->queryRow("SELECT sum(freespace) as freespace, sum(usedspace) as usedspace, sum(size) as size from VOLUMES");
  $info->{freespace} or $info->{freespace}=0;
  $info->{usedspace} or $info->{usedspace}=0;
  $info->{size} or $info->{size}=0;

  return $info

  
}

sub insertFile {
  my $self=shift;
  my $hash=shift;
  delete $hash->{sizeBytes};
  return $self->multiinsert("FILES", [$hash], {noquotes=>1});
}
sub getPFNFromGUID{
  my $self=shift;
  my $guid=shift;
  return $self->queryColumn("SELECT pfn from FILES where guid=string2binary('$guid')")
}

sub getNumberOfFiles{
  my $self = shift;

  my $info = $self->queryValue("select count(*) from FILES");	
  if ($info) {
    return $info;
  }
  return -1;
}
sub deleteLocalCopies {
  my $self=shift;
  my $pfn=shift;
  return $self->delete("LOCALFILES","pfn='$pfn'");
}
sub insertLocalCopy {
  my $self=shift;
  my $insert=shift;
  return $self->insert("LOCALFILES",$insert);
}
sub checkLocalCopies {
  my $self=shift;
  my $pfn=shift;
  my $query="SELECT localCopy,size FROM LOCALFILES where pfn='$pfn' and localCopy is not NULL";
  return   $self->queryRow($query);
  
}

sub updateLocalCopy {
  my $self=shift;
  my $pfn=shift;
  my $size=shift;
  my $transferId=shift;

  return $self->do("UPDATE LOCALFILES set localCopy='$pfn', size=$size  where transferid=$transferId");
}
1;
