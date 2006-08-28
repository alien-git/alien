package AliEn::Database::SE::LFC;

use AliEn::Database::SE;
use AliEn::GUID;
use AliEn::Database::TXT::SE;
use LFC;
use vars qw(@ISA);
use strict;
push @ISA, "AliEn::Database::SE";


sub initialize {
  my $self=shift;

  $ENV{LCG_CATALOG_TYPE} = 'lfc';
  $ENV{LFC_HOST} = $self->{HOST};
  $self->{DB}=~ s/_/\//g;
  $ENV{LFC_HOME} = $self->{DB};
  $self->info("Using LFC for the local file catalogue in $ENV{LFC_HOST}");
  $self->{GUID}=AliEn::GUID->new() or return;

  $self->createDirectory("$ENV{LFC_HOME}/VOLUMES") or return;

  #this is to keep the files that have been downloaded
  $self->{TXT}=AliEn::Database::TXT::SE->new() or return;

  return 1;
}


sub createDirectory {
  my $self=shift;
  my $lfn=shift;
  $self->debug(1,"IN LFC, trying to create the directory $lfn");
  if ($self->_LFC_command({silent=>1}, "chdir", $lfn)) {
    return 1;
  }
  $self->debug(1,"Checking if the parent exist");
  my $parent=$lfn;
  $parent=~ s{/[^/]*/?$}{};
  if ($parent eq $lfn) {
    $self->info("We can't create the directory $parent");
    return;
  }
  $self->createDirectory($parent) or return;
  $self->info("Making the directory $lfn");
  my $mode = 0770;
  $self->_LFC_command({}, "mkdir", $lfn, $mode) or return;
  $self->info("Directory $lfn was created!");
  return 1;
}
sub removeFile {
  my $self=shift;
  my $hash=shift;
  $self->info("In the LFC, ready to delete $hash->{guid} ($hash->{pfn})");
  use Data::Dumper;
        print Dumper($hash);
#  $self->_LFC_command({},"delreplica", $hash->{guid}, undef, $hash->{pfn});
#  $self->_LFC_command({},"unlink", $hash->{lfc_path});
   my $vo=lc("$self->{CONFIG}->{ORG_NAME}");
   $self->info("Ready to delete:  lcg-del --vo $vo -a $hash->{guid}");
   system("lcg-del --vo $vo -a guid:$hash->{guid}") and return;
  $self->info("Entry deleted from the LFC");
  return 1;
}

sub removeDirectory {
  my $self=shift;
  my $dirName=shift;

  print "Removing directory $dirName\n";
  my $entries=$self->_listDirectory($dirName) or return;
  foreach my $entry (@$entries) {
    my $error2=LFC::lfc_unlink($entry);
    if ($error2){
      print "Couldn't delete the entry $entry\n";
      $self->removeDirectory($entry) or return;
    }
  }
  return $self->_LFC_command({}, "rmdir", $dirName);
}

sub _LFC_command{
  my $self=shift;
  my $options=shift;
  my $command="LFC::lfc_".shift;
  my $error;
  my @list=@_;

  map { if (defined $_ ){$_ =~ /^\d+$/ or $_="'$_'"}
        else {
          $_="undef";
        }
      }  @list;
  $self->debug(2, "Let's do  $command (".join(',', @list).")");
  eval "\$error=$command (".join(',', @list).");";
  if ($@){
    $self->info("it didn't work: $command (".join(',', @list)."\n $@");
    return;
  }
  if ($error) {
    $options->{silent} or
      $self->info("Error: $LFC::serrno (".POSIX::strerror($LFC::serrno).")");
    return
  }
  return 1;
}
##
sub _listDirectory{
  my $self=shift;
  my $dirName=shift;
  my $dir=LFC::lfc_opendir($dirName);
  if (!$dir) {
    $self->info("Error: $LFC::serrno (".POSIX::strerror($LFC::serrno).")");
    return;
  }
  my @entries;
  while (my $entry=LFC::lfc_readdirg($dir)){
    my $name=LFC::lfc_direnstatg_d_name_get($entry);
    push @entries, "$dirName/$name";
  }
  LFC::lfc_closedir($dir);

  return \@entries;
}
#
sub _connect{
  my $self=shift;
  $self->info("SKIPPING THE CONECCTION TO THE DATABASE");
  return 1;
}
sub existsVolume{
  my $self=shift;
  my $hashref=shift;
  $hashref->{volume} or $self->info("Don't know how to retrieve the volumedetails!!")
     and return;
  my $volume=$hashref->{volume};
  $volume=~ s/\//_/g;
  $volume="$ENV{LFC_HOME}/VOLUMES/$volume";
  $self->debug(1, "Checking if the volume $volume exists");
  $self->_LFC_command({silent=>1}, "chdir", $volume) or return;
  return $volume
}

sub retrieveVolumeDetails{
   my $self = shift;
   my $hashref = shift;
   $hashref->{volume} or $self->info("Don't know how to retrieve the volumedetails!!")
     and return;
   my $volume=$self->existsVolume($hashref)
     or return;

   $self->info("The volume  $volume exists!!");
   my $comment = pack("x".(1000));
   my $error = LFC::lfc_getcomment($volume,$comment);
   my $stat=LFC::new_lfc_filestatg();
   my ($size, $methodName, $numfiles,$freespace, $usedspace );
   my $real;
   #We have to do this to get rid of the pointers created by pack
   $comment and $comment=~ /(\S*),/ and $real=$1;
   if ($real) {
     $self->info("The comment is defined!! '$real'");
     my %hash=split (/[=,]/, $real);
     $size=$hash{SIZE};
     $methodName=$hash{METHOD};
     ($numfiles,$freespace, $usedspace)=($hash{NUMFILES}, $hash{FREESPACE}, $hash{USEDSPACE});
   }else {
     my $stat=LFC::new_lfc_filestatg();
     $error=LFC::lfc_statg( "$volume/SIZE", undef,$stat);
     $error and $self->info("Error: $LFC::serrno (".POSIX::strerror($LFC::serrno)."  Error getting the size") and return;
     $size=LFC::lfc_filestatg_filesize_get($stat);
     my $method=$self->_listDirectory("$volume/method") or return;
     $methodName=shift @$method;
     $methodName=~ s{^$volume/method/}{};
     $methodName=~ s{_}{/}g;
     $error=LFC::lfc_statg( "$volume/USEDSPACE", undef,$stat);
     $error and $self->info("Error: $LFC::serrno (".POSIX::strerror($LFC::serrno)."Error getting the size") and return;
     $usedspace=LFC::lfc_filestatg_filesize_get($stat);
     $error=LFC::lfc_statg( "$volume/FREESPACE", undef,$stat);
     $error and $self->info("Error: $LFC::serrno (".POSIX::strerror($LFC::serrno)."Error getting the size") and return;
     $freespace=LFC::lfc_filestatg_filesize_get($stat);
     $error=LFC::lfc_statg( "$volume/NUMBERFILES", undef,$stat);
     $error and $self->info("Error: $LFC::serrno (".POSIX::strerror($LFC::serrno)."Error getting the size") and return;
     $numfiles=LFC::lfc_filestatg_filesize_get($stat);
   }
   LFC::delete_lfc_filestatg($stat);

   $size eq "18446744073709551615" and $size=-1;

   $self->info("Returning the info of the volume $hashref->{volume}");
   $self->debug(2, "{volume=>$hashref->{volume}, volumeId=> $hashref->{volume}, size=>$size, usedspace=>$usedspace, freespace=>$freespace, method=>$methodName, numfiles=>$numfiles, mountpoint=>$hashref->{volume}}");
   return {volume=>$hashref->{volume}, volumeId=> $hashref->{volume}, size=>$size,
           usedspace=>$usedspace, freespace=>$freespace, method=>$methodName,
           numfiles=>$numfiles, mountpoint=>$hashref->{volume}};
}



sub insertVolume {
  my $self=shift;
  my $volumeHash=shift;

  $self->info("Inserting a new volume");
  my $volumeId=$volumeHash->{volume};
  $volumeId=~ s/\//_/g;
  use Data::Dumper;
  print Dumper($volumeHash);
  my $dir="$ENV{LFC_HOME}/VOLUMES/$volumeId";
  eval{
    $self->info("Creating the directory");
    my $mode=0777;
    $self->_LFC_command({}, "mkdir", $dir, $mode) or die("error creating the directory");
    $self->info("Setting some comment");
    my $comment="SIZE=$volumeHash->{size},USEDSPACE=$volumeHash->{usedspace},FREESPACE=$volumeHash->{freespace},NUMFILES=0,";
    if ($volumeHash->{method}) {
       $comment.="METHOD=$volumeHash->{method},";
    }
    $self->_LFC_command({}, "setcomment", $dir, $comment) or die("error setting the comments");
#    $self->_LFC_command({}, "creatg","$dir/USED",$self->{GUID}->CreateGuid(),$mode) or die("Error creating the entry USED");
#     $self->_LFC_command({}, "setcomment","$dir/USED", "USEDSPACE:$volumeHash->{usedspace},FREESPACE:$volumeHash->{freespace},NUMBERFILES:0") or die("error settting the comments of USED");
#    $self->_LFC_command({}, "setfsize","$dir/SIZE",undef, $volumeHash->{size})
#      or die("Error setting the size");
#    $self->_LFC_command({}, "creatg","$dir/USEDSPACE",$self->{GUID}->CreateGuid(),$mode) or die("Error creating the entry");
#    $self->_LFC_command({}, "setfsize","$dir/USEDSPACE",undef, $volumeHash->{usedspace})
#      or die("Error setting the size");
#    $self->_LFC_command({}, "creatg","$dir/FREESPACE",$self->{GUID}->CreateGuid(),$mode) or die("Error creating the entry");
#    $self->_LFC_command({}, "setfsize","$dir/FREESPACE",undef, $volumeHash->{freespace})
#      or die("Error setting the size");
#    $self->_LFC_command({}, "creatg","$dir/NUMBERFILES",$self->{GUID}->CreateGuid(),$mode) or die("Error creating the entry");
#    $self->_LFC_command({}, "setfsize","$dir/NUMBERFILES",undef, 0)
#      or die("Error setting the size");
#    my $method=$volumeHash->{method};
#    if ($method){
#      $method=~ s{/}{_}g;
#      $method= "$dir/method/$method";
#      $self->info("Let's create the directory $method");
#      $self->createDirectory($method) or die ("error creating $method");
#    }
  };
  if ($@){
    $self->info("Error creating the volume: $@");
    $self->removeDirectory($dir);
    return;
  }
  return 0;
}
sub retrieveVolumeDetailsList{
  my $self=shift;
  $self->info("Giving back all the volumes defined in LFC");

  my $volumes=$self->_listDirectory("$ENV{LFC_HOME}/VOLUMES") or
    $self->info("Error reading the directory VOLUMES!!") and return;
  my @volumesInfo;

  foreach my $vol(@$volumes){
    $self->debug(1,"Checking volume '$vol'");
    my $volName=$vol;
    $volName=~ s{^$ENV{LFC_HOME}/VOLUMES/}{};
    $volName=~ s{_}{/}g;
    $self->debug(1, "Let's get the information of $volName");
    my $info=$self->retrieveVolumeDetails({volume=>$volName})
      or $self->info("Error getting the information of $volName") and next;
    push @volumesInfo, $info
  }
  $self->info("Returning the info of all the volumes");
  return \@volumesInfo;
}

sub chooseVolume {
  my $self = shift;
  my $size = shift;
  my $guid=shift;

  my $listref = $self->retrieveVolumeDetailsList();
  $listref or return;
  foreach my $volume (@$listref){
    $self->debug (1, "Checking volume $volume->{volume}");
    $size <$volume->{freespace} and return $volume;
    $self->info("Not enough disk space in $volume->{volume}");
  }
  $self->info("No volume can contain that file");
  return ;

}


sub retrieveFileDetails{
  my $self=shift;
  my $file=shift;
  my $options=shift;
  $self->info("Trying to retrieve the file $file->{guid}");


  my $stat=LFC::new_lfc_filestatg();
  my $guid="\U$file->{guid}\E";
  my $error = LFC::lfc_statg(undef,$guid,$stat);
  if ($error){
    $self->info("To be on the safe side, let's look in lower case");
    $guid=lc($guid);
    $error= LFC::lfc_statg(undef,$guid,$stat);
    if ($error){
      LFC::delete_lfc_filestatg($stat);
      if (!$options->{silent}){
	print "Error: $LFC::serrno (".POSIX::strerror($LFC::serrno).")\n";
	$self->info("Error doing the stat");
      }
      return;
    }
  }

  my $size=LFC::lfc_filestatg_filesize_get($stat);
  my $checksum=LFC::lfc_filestatg_csumvalue_get($stat);
  print "Size:           $size
Checksum type:  ",LFC::lfc_filestatg_csumtype_get($stat),"
Checksum:       $checksum\n";

#  my $comment = '';
#  $error = LFC::lfc_getcomment("$ENV{LFC_HOME}/$file",\$comment); ##??? Does not work like this...
  print "Error: $LFC::serrno (".POSIX::strerror($LFC::serrno).")\n" if $error;
 # print "Comment:        $comment\n" unless $error;
  my @sfns = ();
  my $replica = LFC::new_lfc_filereplica();
  my $list = LFC::new_lfc_list();
  my $flag = $LFC::CNS_LIST_BEGIN;
  while ($replica = LFC::lfc_listreplica(undef,$guid,$flag,$list)) {
    $flag = $LFC::CNS_LIST_CONTINUE;
    push @sfns,LFC::lfc_filereplica_sfn_get($replica);
    print "SFN:\t\t$sfns[$#sfns]\n";
  }
  LFC::lfc_listreplica(undef,$guid,$LFC::CNS_LIST_END,$list);
  print "Got ",$#sfns+1," replica(s).\n";
  my $pfn=shift @sfns;

  my $volume=$pfn;
  #Let's remove the method from the volume
  $volume=~ s{^[^/]*//[^/]*/}{/};
  #Let's remove also the guid;
  $volume=~   s{/([^/]*)/[^/]*/[^/]*/$file->{guid}\..*$}{}i;
  $volume=~ s{/}{_}g;
  my $stat2={guid=>$file->{guid}, pfn=>$pfn, size=>$size, md5sum=>$checksum, volume=>$volume, pfns=>[$pfn,@sfns]};

  LFC::delete_lfc_filestatg($stat);

  LFC::delete_lfc_filereplica($replica);
  LFC::delete_lfc_list($list);

  use Data::Dumper;
  print Dumper($stat2);

  return $stat2;
}


#sub _getLFNfromGUID {
#  my $self=shift;
#  my $guid=shift;
#  $self->info("Creating the lfn that is supposed to contain $guid");#
#
#  my $lfn="$ENV{LFC_HOME}/".$self->{GUID}->GetCHash($guid)."/$guid";
#  $self->info("Returning $lfn");
#  return $lfn;
#}

sub updateVolumeDetails{
  my $self = shift;
  my $hashref = shift;
  $self->info("Updating the info of the volume");
  my $volume=$self->existsVolume($hashref) or return;

  my $buffer=pack("x".(1000));
  my $error=LFC::lfc_getcomment($volume, $buffer);
  $buffer or $buffer="";
  foreach my $var ("size", "usedspace", "freespace", "numfiles"){
    if ($hashref->{$var}){
      my $upper="\U$var\E";
      $buffer =~ s/$upper=[^,]*,/$upper=$hashref->{$var},/
          or $buffer.="$upper=$hashref->{$var},";
    }
  }
  $buffer or $self->info("Error getting the information to update the LFC") and return;
  $error=LFC::lfc_setcomment( $volume, $buffer) and $self->info("error updating the values of $volume to '$buffer'") and return;
  $self->info("Volume $volume updated ($buffer)");
#if ($hashref->{size}){
#    $self->_LFC_command({}, "setfsize","$volume/SIZE",undef, $hashref->{size})
#      or $self->info("Error setting the size") and return;
#  }
#  if ($hashref->{usedspace}){
#    $self->_LFC_command({}, "setfsize","$volume/USEDSPACE",undef, $hashref->{usedspace})
#      or $self->info("Error setting the size") and return;
#  }
#  if ($hashref->{freespace}){
#    $self->_LFC_command({}, "setfsize","$volume/FREESPACE",undef, $hashref->{freespace})
#      or $self->info("Error setting the size") and return;
#  }
#  if ($hashref->{numfiles}){
#    $self->_LFC_command({}, "setfsize","$volume/NUMBERFILES",undef, $hashref->{numfiles})
#      or $self->info("Error setting the size") and return;
#  }
#  $self->info("Volume updated");
  return 1;
}
sub retrieveAllVolumesUsage {
  my $self=shift;
  $self->info("Returning the usage of all the volumes");
  my $info=$self->retrieveVolumeDetailsList() or return;
  my $size=0;
  my $freespace=0;
  my $usedspace=0;
  foreach my $entry (@$info){
    $size+=$entry->{size};
    $freespace+=$entry->{freespace};
    $usedspace+=$entry->{usedspace};
  }
  $self->info("Returning $size, $freespace and $usedspace");
  return {size=>$size, freespace=>$freespace, usedspace=>$usedspace};

}

sub insertFile {
  my $self=shift;
  my $hashref=shift;
  $self->info("Adding a file to the LFC");
  $hashref->{volume}=$hashref->{volumeId};
  my $volume=$self->existsVolume($hashref) or return;

  my $guid=$hashref->{guid};
  my $lfnDirectory="$volume/FILES/".$self->{GUID}->GetCHash($guid)."/".$self->{GUID}->GetHash($guid);
  my $lfn="$lfnDirectory/$guid";
  my $mode=0777;
  eval {
    $guid=uc($guid);
    $self->createDirectory($lfnDirectory) or die("Error creating the directory");
    if ($self->_LFC_command({}, "creatg",$lfn,$guid,$mode) ){
      my $size=$hashref->{sizeBytes} || 1024*$hashref->{size};
      $self->_LFC_command({}, "setfsizeg",$guid,$size,'MD',$hashref->{md5})
      or die("Error setting the size $size for guid $guid with md5 $hashref->{md5}");
    }
    $self->_LFC_command({}, "addreplica",$guid, undef,"$self->{CONFIG}->{SE_FULLNAME}",$hashref->{pfn},
                        "-","D",undef,undef) or die ("Error adding the replica")

#    $self->_LFC_command({}, "creatg",$lfn,$guid,$mode) or die("Error creating the entry");

  };

  if ($@) {
    $self->info("Adding the entry didn't work: $@",undef, $LFC::serrno);
    $self->_LFC_command({}, "delete", $lfn);
    return;
  }
  $self->info("YUUUHUUUU!\n");
  return 1;
}

sub getPFNFromGUID{
  my $self=shift;
  my $guid=shift;
  $self->info("In LFC, trying to retrieve the info of $guid");
  my $info=$self->retrieveFileDetails({guid=>"\L$guid\E"}) or return;
  return [$info->{pfn}];

}

sub getNumberOfFiles{
  my $self = shift;
  $self->info("********************In LFC, getting the number of files");
  my $info=$self->retrieveVolumeDetailsList() or return;
  my $files=0;
  foreach my $entry (@$info){
    $files+=$entry->{numfiles};
  }
  $self->info("There are $files registered in the LFC");
  return $files;
}
sub deleteLocalCopies {
  my $self=shift;
  my $pfn=shift;
  return $self->{TXT}->delete("LOCALFILES","pfn='$pfn'");
}
sub insertLocalCopy {
  my $self=shift;
  my $insert=shift;
  return $self->{TXT}->insert("LOCALFILES",$insert);  my $pfn=shift;
}
sub checkLocalCopies {
  my $self=shift;
  my $pfn=shift;
  my $query="SELECT localCopy,size FROM LOCALFILES where pfn='$pfn' and localCopy is not NULL";
  $self->info( $query);
  return   $self->{TXT}->queryRow($query);

}

sub updateLocalCopy {
  my $self=shift;
  my $pfn=shift;
  my $size=shift;
  my $transferId=shift;

  return $self->{TXT}->do("UPDATE LOCALFILES set localCopy='$pfn', size=$size  where transferid=$transferId");
}
return 1;
