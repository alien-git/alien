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

package AliEn::Catalogue::Authorize;

=head1 NAME AliEn::UI::Catalogue::LCM

This class inherits the basic functionality from the AliEn::UI::Catalogue. It expands it with the Local Cache Manager (LCM), who manages the transfer of the files.

=head1 SYNOPSYS

=over


use AliEn::UI::Catalogue::LCM;
my $cat=AliEn::UI::Catalogue::LCM->new();

$cat->execute("add", "mylfn", "file://myhost/myfile");
$cat->close();


=back

=head1 DESCRIPTION

This object provides the access to the Local Cache Manager (LCM) to the prompt. Look at the manual of the AliEn::LCM for more details of what that module does. 
]

=head1 METHODS

=over 

=cut

use strict;
use AliEn::LCM;
use List::Util 'shuffle';

require AliEn::UI::Catalogue;
require AliEn::Catalogue::Admin;
require AliEn::Database::Catalogue::LFN;
use AliEn::SOAP;
use Getopt::Long;
use Compress::Zlib;
use AliEn::TMPFile;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use Data::Dumper;
use AliEn::Util;
use POSIX "isdigit";
use MIME::Base64; # not needed after signed envelopes are in place
use vars qw($DEBUG @ISA);
require Crypt::OpenSSL::RSA;
require Crypt::OpenSSL::X509;
$DEBUG = 0;





sub initEnvelopeEngine {
  my $self=shift; 

  $self->{envelopeCipherEngine} =0;
  $self->{noshuffle} = 0;
  defined $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}  or return;
  defined $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} or return;

  $self->info("Checking if we can create envelopes...");
  $self->info("local private key          : $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}");
  $self->info("local public  key          : $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'}");

  open(PRIV, $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}); my @prkey = <PRIV>; close PRIV;
  my $private_key = join("",@prkey);
  my $public_key = Crypt::OpenSSL::X509->new_from_file( $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} )->pubkey();
  $self->{signEngine} = Crypt::OpenSSL::RSA->new_private_key($private_key);
  $self->{verifyEngine} = Crypt::OpenSSL::RSA->new_public_key($public_key);


  # This we can drop as soon as we want to get rid of encrypted envelopes...


  require SealedEnvelope;
#  print "AT THE MOMENT, THE ENVELOPEENGINE IS NOT THERE\n";
 # return 1;
  $self->{envelopeCipherEngine} = SealedEnvelope::TSealedEnvelope->new("$ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}","$ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'}","$ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'}","$ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}","Blowfish","CatService\@ALIEN",0);
      # we want ordered results of se lists, no random
  $self->{noshuffle} = 1;

  if ($self->{MONITOR}) {
    $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","admin_readreq");
  }
  $self->{apmon} = 1;
  if (!$self->{envelopeCipherEngine}->Initialize(2)) {
    $self->info("Warning: the initialization of the envelope engine failed!!");
    $self->{envelopeCipherEngine} = 0;
    return;
  }
 

  return 1;
}



sub resortArrayToPriorityArrayIfExists {
   my $self=shift;
   my $prio=shift;
   my $list=shift;
   my @newlist=();
   my @remainer=();
   my $exists=0;
   my @priors = ();
   (UNIVERSAL::isa($prio, "ARRAY") and @priors=@$prio)
   or push @priors, $prio;



   foreach my $pr (@priors) {
     foreach my $se (@$list) { 
       (lc($pr) eq lc($se)) and push @newlist, $se
        or push @remainer, $se; 
     }
   }
   @newlist = (@newlist,@remainer); 
   return \@newlist;

}


sub resortArrayToPrioElementIfExists {
   my $self=shift;
   my $prio=shift;
   my $list=shift;
   my @newlist=();
   my $exists=0;
   $self->info("gron: prio: $prio, list: @$list");
   foreach (@$list) {
     (lc($prio) eq lc($_)) and $exists=1 
      or push @newlist, $_;
   }
   $exists and  @newlist = ($prio,@newlist);
    $self->info("gron: resorted SE list: @newlist");
   return \@newlist;
}



sub OLDselectClosestRealSEOnRank {
   my $self=shift;
   my $sitename=(shift || 0);
   my $user=(shift || return 0);
   my $readOrDelete=shift;
   my $seList=(shift || return 0 );
   my $sePrio = (shift || 0);
   my $excludeList=(shift || []);
   my $nose=0;
   my @cleanList=();
   my $result={};
   my $exclusiveUserCheck = "";
   ($readOrDelete  =~/^read/) and $exclusiveUserCheck = "seExclusiveRead";
   ($readOrDelete  =~/^delete/) and $exclusiveUserCheck = "seExclusiveWrite";


   foreach (@$seList) { 
      UNIVERSAL::isa($_, "HASH") and $_=$_->{seName};
      ($_ eq "no_se") and $nose=1 and next;
      $self->identifyValidSEName($_) and push @cleanList, $_;
   }
   $seList=\@cleanList;
      
   my $catalogue = $self->{DATABASE}->{LFN_DB}->{FIRST_DB};
   my @queryValues = ();
   my $query = "";
   if($sitename) {
      $self->checkSiteSECacheForAccess($sitename) or return 0;
      push @queryValues, $sitename;
   
      $query="SELECT DISTINCT b.seName FROM SERanks a right JOIN SE b on (a.seNumber=b.seNumber and a.sitename LIKE ?) WHERE ";
      $query .= " (b.$exclusiveUserCheck is NULL or b.$exclusiveUserCheck = '' or b.$exclusiveUserCheck  LIKE concat ('%,' , ? , ',%') ) ";
      push @queryValues, $user;
      if(scalar(@{$seList}) > 0)  { $query .= " and ( "; foreach (@{$seList}){ $query .= " b.seName LIKE ? or"; push @queryValues, $_;  } 
           $query =~ s/or$/)/;}
      foreach (@{$excludeList}) {   $query .= " and b.seName NOT LIKE ? ";   push @queryValues, $_; };
      $query .= " ORDER BY if(a.rank is null, 1000, a.rank) ASC ;";
   } else { # sitename not given, so we just delete the excluded SEs and check for exclusive Users
       $query="SELECT seName FROM SE WHERE ";
       foreach(@$seList){   $query .= " seName LIKE ? or"; push @queryValues, $_;  };
       $query =~ s/or$//;
       foreach(@$excludeList){   $query .= " and seName NOT LIKE ? "; push @queryValues, $_;  }
       $query .= " and ($exclusiveUserCheck is NULL or $exclusiveUserCheck = '' or $exclusiveUserCheck  LIKE concat ('%,' , ? , ',%') ) ;";
       push @queryValues, $user;
   }
   $result = $self->resortArrayToPrioElementIfExists($sePrio,$catalogue->queryColumn($query, undef, {bind_values=>\@queryValues}));
   $nose and @$result = ( "no_se", @$result);
   return $result;
}


sub findCloseSE {
  my $self=shift;
  my $type=shift;
  my $excludeListRef=shift || undef;
  my @excludeList=();
  $excludeListRef and push @excludeList, @$excludeListRef;
  $type =~ /^(custodial)|(replica)$/ or $self->info("Error: type of SE '$type' not understood") and return;
  $self->info("Looking for the closest $type SE");
  
  if ($self->{CONFIG}->{SE_RETENTION_POLICY} and 
      $self->{CONFIG}->{SE_RETENTION_POLICY} =~ /$type/){
    $self->info("We are lucky. The closest is $type");
    return $self->{SE_FULLNAME};
  }
  
  my $se=$self->{SOAP}->CallSOAP("IS", "getCloseSE", $self->{SITE}, $type, $excludeListRef);
  $self->{SOAP}->checkSOAPreturn($se) or return ;
  my $seName=$se->result;
  $self->info("We are going to put the file in $seName");
  return $seName;
}



#############################################################################################################
sub access_eof {
  my $error=(shift || "error creating the envelope");
  my $exception=(shift || 0);
  my $newhash;
  my @newresult=();
  $newhash->{eof} = "1";
  $newhash->{error}=$error;
  $exception and $newhash->{exception} = $exception;
  push @newresult, $newhash;
  return @newresult;
}

sub OLDgetPFNforReadOrDeleteAccess {
  my $self=shift;
  my $user=(shift || return 0);
  my $readOrDelete=(shift || -1);
  my $guid=shift;
  my $se=shift;
  my $excludedAndfailedSEs=shift;
  my $lfn=shift;
  my $sitename=(shift || 0);
  my $options=shift;


  my $sesel = 0;
  # if excludedAndfailedSEs is an int, we have the old <= AliEn v2-17 version of the envelope request, to select the n-th element
  if ($$excludedAndfailedSEs[0] =~ /^[0-9]+$/ ) { 
     $sesel=$excludedAndfailedSEs;
     @{$excludedAndfailedSEs} = ();
  }

  my $pfn;
  my @where=$self->f_whereis("sgztr","$guid");

  if (! @where){
    $self->info("There were no transfer methods....");
    @where=$self->f_whereis("sgzr","$guid");
  }

  my @whereis=();
  foreach (@where) {
    push @whereis, $_->{se};
  }
  my $error="There was no SE for the guid '$guid'";
  $self->{LOGGER}->error_msg() and $error=$self->{LOGGER}->error_msg();
  #Get the file from the LCM
  @whereis or $self->info( "access: $error" )
    and return 0;

  my $closeList = $self->OLDselectClosestRealSEOnRank($sitename, $user, $readOrDelete, \@whereis, $se, $excludedAndfailedSEs);

  (scalar(@$closeList) eq 0) 
           and $self->info("access ERROR within getPFNforReadOrDeleteAccess: SE list was empty after checkup. Either problem with the file's info or you don't have access on relevant SEs.") 
           and return 0;

  # if excludedAndfailedSEs is an int, we have the old <= AliEn v2-17 version of the envelope request, to select the n-th element
  $se = @{$closeList}[$sesel];

  my $origpfn;
  foreach (@where) { ($_->{se} eq $se) and $origpfn = $_->{pfn} }

  $self->debug(1, "We can ask the following SE: $se");
  (!($options =~/s/)) and $self->info( "The guid is $guid");

  my $nonRoot;
  my $se2 = lc $se;
  foreach (@where) {
    (!($options =~/s/)) and $self->info("comparing $_->{se} to $se");
    my $se1 = lc $_->{se};
    ($se1 eq $se2) or next;
    $nonRoot=$_->{pfn};

    if (( $_->{pfn} =~ /^root/ ) || ( $_->{pfn} =~ /^guid/) ) {
      $pfn = $_->{pfn};
    }
  }
	
  if (!$pfn && $nonRoot) {
    $self->info("access: this is not a root pfn: $nonRoot ");
    return ($se, $nonRoot, "", ,$lfn, $origpfn);
  }

  my ($urlprefix,$urlhostport,$urlfile,$urloptions);
  $urlprefix="root://";
  $urloptions="";
  $urlfile="";
  $urlhostport="";
  if ($pfn =~ /([a-zA-Z]*):\/\/([0-9a-zA-Z.\-_:]*)\/(.*)/) {
    (defined $1) and  $urlprefix = "$1://";
    (defined $2) and   $urlhostport = $2;
    (defined $3) and    $urlfile = $3;
  } else {
    $self->info("access ERROR within getPFNforReadOrDeleteAccess: parsing error for $pfn [host+port]");
    return ($se, "", "", $lfn, 1, $origpfn);  
  }

  if ($urlfile =~ s/([^\?]*)\?([^\?]*)/$1/) {
     (defined $2 )  and $urloptions = $2;
  }


  # fix // in the urlfile part
  $urlfile =~ s{/+}{/}g;
  $urlfile=~ /^\// or $urlfile="/$urlfile";
  $pfn = "$urlprefix$urlhostport/$urlfile";

  if ($urloptions ne "") {
    $pfn .= "?$urloptions";
  }
  my $anchor;
  if ($pfn=~ s/\?ZIP=(.*)$//){
    $self->info("The anchor is $1");
    $anchor=$1  
  }

  return ($se, $pfn, $anchor, $lfn, $origpfn);
}


sub OLDcheckPermissionsOnLFN {
  my $self=shift;
  my $lfn=shift;
  my $access=shift;
  my $perm=shift;
  
  my $filehash = {};
  ($access =~ /^write[\-a-z]*/) && ($filehash = $self->checkPermissions($perm,$lfn,undef, 
						    {RETURN_HASH=>1}));
  if (!$filehash) {
    $self->info("access: access denied to $lfn");
    return;
  }

  if  ($access eq "read")  {
    if (!$self->isFile($lfn, $filehash->{lfn})) {
      $self->info("access: access find entry for $lfn");
      return ;
    }
  }elsif ($access eq "delete") {
    if (! $self->existsEntry($lfn, $filehash->{lfn})) {
      $self->info("access: delete of non existant file requested: $lfn");
      return ;
    }
  } else {
    my $parentdir = $self->f_dirname($lfn);
    my $result = $self->checkPermissions($perm,$parentdir);
    if (!$result) {
      $self->info("access: parent dir missing for lfn $lfn");
      return ;
    }
    if (($access =~ /^write[\-a-z]*/) && ($lfn ne "")
      && $self->existsEntry($lfn, $filehash->{lfn})) {
	$self->info( "access: lfn <$lfn> exists - creating backup version ....\n");
	my $filename = $self->f_basename($lfn);
	
	$self->f_mkdir("ps","$parentdir"."/."."$filename/") or
	  $self->info("access: cannot create subversion directory - sorry") and return;
	
	my @entries = $self->f_ls("s","$parentdir"."/."."$filename/");
	my $last;
	foreach (@entries) {
	  $last = $_;
	}
	
	my $version=0;
	if ($last ne "") {
	  $last =~ /^v(\d)\.(\d)$/;
	  $version = (($1*10) + $2) - 10 +1;
	}
	if ($version <0) {
	  $self->info("access: cannot parse the last version number of $lfn");
	  return ;
	}
	my $pversion = sprintf "%.1f", (10.0+($version))/10.0;
	my $backupfile = "$parentdir"."/."."$filename/v$pversion";
	$self->info( "access: backup file is $backupfile \n");
	if (!$self->f_mv("",$lfn, $backupfile)) {
	  $self->info("access: cannot move $lfn to the backup file $backupfile");
	  return ;
	}
      #}
    }
  }
  return $filehash;
}

#################################################################
# Create envelope, only for backward compability on < v2.19
# replaced by authorize/consultAuthenService below
################################################################
sub access {
    # access <access> <lfn> 
    # -p create public url in case of read access 
  my $self = shift;
#  #
#  # Start of the Client side code
#  if (!  $self->{envelopeCipherEngine}) {
#    my $user=$self->{CONFIG}->{ROLE};
#    $self and $self->{ROLE} and $user=$self->{ROLE};
#
#    if($_[0] =~ /^-user=([\w]+)$/)  {
#      $user = shift;
#      $user =~ s/^-user=([\w]+)$/$1/;
#    }
#
#    $self->info("Connecting to Authen...");
#    my $info=0;
#    for (my $tries = 0; $tries < 5; $tries++) { # try five times 
#      $info=$self->{SOAP}->CallSOAP("Authen", "createEnvelope", $user, @_) and last;
#      sleep(5);
#    }
#    $info or $self->info("Connecting to the [Authen] service failed!") 
#       and return ({error=>"Connecting to the [Authen] service failed!"}); 
#    my @newhash=$self->{SOAP}->GetOutput($info);
#    if (!$newhash[0]->{envelope}){
#      my $error=$newhash[0]->{error} || "";
#      $self->info($self->{LOGGER}->error_msg());
#      $self->info("Access [envelope] creation failed: $error", 1);
#      ($newhash[0]->{exception}) and 
#        return ({error=>$error, exception=>$newhash[0]->{exception}});
#      return (0,$error) ;
#     }
#    $ENV{ALIEN_XRDCP_ENVELOPE}=$newhash[0]->{envelope};
#    $ENV{ALIEN_XRDCP_URL}=$newhash[0]->{url};
#    return (@newhash);
#  }
#
  #
  # Start of the Server/Authen side code
  $self->info("STARTING envelope creation: @_ ");
  my $options = shift;
  my $maybeoption = ( shift or 0 );
  my $access;
  if ( $maybeoption =~ /^-/ ) {
    $options .= $maybeoption;
    $access = (shift or 0),
  } else {
    $access = ( $maybeoption or 0);
  }
  my $lfns    = (shift or 0);
  my $se      = (shift or "");
  my $size    = (shift or "0");
  my $sesel   = (shift or 0);
  my @accessOptions = @_;
  my $extguid = (shift or 0);
  my $user=$self->{CONFIG}->{ROLE};
  $self->{ROLE} and $user=$self->{ROLE};

  my @ses = ();
  my @tempSE= split(/;/, $se);
  foreach (@tempSE) { AliEn::Util::isValidSEName($_) and push @ses, $_; }
  my $seList= \@ses;

  my @exxSEs = ();
  @tempSE= split(/;/, $sesel);
  foreach (@tempSE) { AliEn::Util::isValidSEName($_) and push @exxSEs, $_; }
  my $excludedAndfailedSEs = \@exxSEs;
  ($sesel =~ /^[0-9]+$/) or $sesel = 0;

  my $sitename= (shift || 0);
  ($sitename eq "") and $sitename=0;
  my $writeQos = (shift || 0);
  ($writeQos eq "") and $writeQos=0;
  my $writeQosCount = (shift || 0);

  if ($access =~ /^write[\-a-z]*/) {
    # if nothing is or wrong specified SE info, get default from Config, if there is a sitename
    if ( (scalar(@ses) eq 0) and ($sitename ne 0) and ( ($writeQos eq 0) or ($writeQosCount eq 0) ) and $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT} ) {
      my ($repltag, $copies)=split (/\=/, $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT},2);
      $writeQos = $repltag;
      $writeQosCount = $copies;
    }

    my $copyMultiplyer = 1;
    ($writeQosCount and $copyMultiplyer = $writeQosCount)
     or (scalar(@ses) gt 1 and $copyMultiplyer = scalar(@ses));

    my ($ok, $message) = $self->checkFileQuota( $user, $size * $copyMultiplyer);
    if($ok eq -1) {
       $self->info("We gonna throw an access exception: "."[quotaexception]") and return  access_eof($message,"[quotaexception]");
    }elsif($ok eq 0) {
       return  access_eof($message);
    }

    (scalar(@ses) eq 0) or $seList = $self->checkExclWriteUserOnSEsForAccess($user,$size,$seList) and @ses = @$seList;
    if(($sitename ne 0) and ($writeQos ne 0) and ($writeQosCount gt 0)) {
       my $dynamicSElist = $self->getSEListFromSiteSECacheForWriteAccess($user,$size,$writeQos,$writeQosCount,$sitename,$excludedAndfailedSEs);
       push @ses,@$dynamicSElist;
    }
  }

  my $nosize  = 0;
  ($size eq "0") and $size = 1024*1024*1024 and $nosize =1 ;
  my $perm;
  if ($access eq "read") {
    $perm = "r";
  } elsif ($access =~ /^((write[\-a-z]*)|(delete))$/ ) {
    $perm = "w";
  } else {
    $self->info("access: illegal access type <$access> requested");
    return access_eof("access: illegal access type <$access> requested");
  }

  my @list=();
  my @lfnlist = split(",",$lfns);
  my @lnewresult;
  my $newresult = \@lnewresult;
  my $globalticket="";

  foreach my $lfn (@lfnlist) {
    my $ticket = "";
    my $guid="";
    my $pfn ="";
    my $seurl =""; 
    my $nses = 0;
    my $filehash = {};

    if(AliEn::Util::isValidGUID($lfn)) {
      $self->info("Getting the permissions from the guid");
      $guid = $lfn;
      $self->debug(1, "We have to translate the guid $1");
      $lfn = "";
      $filehash=$self->{DATABASE}->{GUID_DB}->checkPermission($perm, $guid, {retrieve=>"size,md5"});
      $filehash 
	or $self->info("access: access denied to guid '$guid'")
	and return access_eof("access: access denied to guid '$guid'");
      delete $filehash->{db};
    } else {
      $lfn = $self->f_complete_path($lfn);
    }

    if ($lfn eq "/NOLFN") {
       $lfn = "";
       #$guid = $extguid;
       if(AliEn::Util::isValidGUID($extguid)) {
          my $guidCheck = $self->getInfoFromGUID($extguid);
          $guidCheck and $guidCheck->{guid} and (lc $extguid eq lc $guidCheck->{guid})
            and return access_eof("The requested guid ($extguid) as already in use.");
          $guid = $extguid;
       }
    }
    my $whereis;
    while(1) {
      ($lfn eq "/NOLFN") and $lfn = "";
      if ( $lfn ne "") {
	$filehash=$self->OLDcheckPermissionsOnLFN($lfn,$access, $perm)
	  or return access_eof("OLDcheckPermissionsOnLFN failed for $lfn");
      }
      $DEBUG and $self->debug(1, "We have permission on the lfn");
      if ($access =~ /^write[\-a-z]*/) {
        $se = shift(@ses);
        AliEn::Util::isValidSEName($se) or $self->info("access: no SE asked to write on") and 
		return access_eof("List of SE is empty after checkups, no SE to create write envelope on."); 
	($seurl,my $guid2,my $se2) = $self->createFileUrl($se, "root", $guid);
	$guid2 and $guid=$guid2;
	if (!$se2){
	  $self->info("Ok, let's create a default pfn (for $guid)");
	  ($seurl, $guid)=$self->createDefaultUrl($se, $guid,$size);
	  $seurl or return access_eof("Not an xrootd se, and there is no place in $se for $size");
	  $self->info("Now, $seurl and $guid");
	}
	$pfn = $seurl;
	$pfn=~ s/\/$//;
	$seurl=~ s/\/$//;
	
	$filehash->{storageurl} = $seurl;
	if ($nosize) {
	  $filehash->{size} = 0;
	} else {
	  $filehash->{size} = $size;
	}

      }
      
      my $anchor="";
      if (($access =~ /^read/) || ($access =~/^delete/) ) {
	my $cnt=0;
	if (!$guid  ){
	  $guid=$self->f_lfn2guid("s",$lfn)
	    or $self->info( "access: Error getting the guid of $lfn",11) and return;
	}
        $filehash->{filetype}=$self->f_type($lfn);
        $self->info("Calling getPFNforReadOrDeleteAccess with sitename: $sitename, $user, $access.");
	($se, $pfn, $anchor, $lfn, $nses, $whereis)=$self->OLDgetPFNforReadOrDeleteAccess($user, $access, $guid, $se, $excludedAndfailedSEs, $lfn, $sitename, $options);
        $self->info("Back from getPFNforReadOrDeleteAccess.");

        $se or return access_eof("Not possible to get file info for file $lfn [getPFNforReadOrDeleteAccess error]. File info is not correct or you don't have access on certain SEs.");
	if (UNIVERSAL::isa($se, "HASH")){
	  $self->info("Here we have to return eof");
	  return access_eof("Not possible to get file info for file $lfn [getPFNforReadOrDeleteAccess error]. File info is not correct or you don't have access on certain SEs.");
	}
	$DEBUG and $self->debug(1, "access: We can take it from the following SE: $se with PFN: $pfn");
      }

      $ticket = "<authz>\n  <file>\n";
      ($globalticket eq "") and $globalticket .= $ticket;
      $pfn =~ m{^((root)|(file))://([^/]*)/(.*)};
      my $pfix = $4;
      my $ppfn = $5;
      $filehash->{pfn} = "$ppfn";
      #($pfn =~ /^soap:/) and $filehash->{pfn} = "$pfn" or $filehash->{pfn} = "$ppfn";
      #$filehash->{pfn} = "$pfn";
      (($lfn eq "")  && ($access =~ /^write[\-a-z]*/)) and $lfn = "/NOLFN";
      $filehash->{turl} = $pfn;

      # patch for dCache
      $filehash->{turl} =~ s/\/\/pnfs/\/pnfs/;
      $filehash->{se}   = $se;
      $filehash->{nses} = $nses;

      $filehash->{lfn}  = $lfn || $filehash->{pfn};
      $filehash->{guid} = $guid;
      if ((!defined $filehash->{md5}) || ($filehash->{md5} eq "")) {
	$filehash->{md5} = "00000000000000000000000000000000";
      }
      $ticket .= "    <lfn>$filehash->{'lfn'}</lfn>\n";
      $globalticket .= "    <lfn>$filehash->{'lfn'}</lfn>\n";
      $ticket .= "    <access>$access</access>\n";
      $globalticket .= "    <access>$access</access>\n";
      foreach ( keys %{$filehash}) {
	if ($_ eq "lfn") {
	  next;
	}
	if (defined $filehash->{$_}) {
	  $ticket .= "    <${_}>$filehash->{$_}</${_}>\n";
	  $globalticket .= "    <${_}>$filehash->{$_}</${_}>\n";
	}
      }
      $ticket .= "  </file>\n</authz>\n";
      $self->info("The ticket is $ticket");
      $self->{envelopeCipherEngine}->Reset();
      #    $self->{envelopeCipherEngine}->Verbose();
      my $coded = $self->{envelopeCipherEngine}->encodeEnvelopePerl("$ticket","0","none");
      my $newhash;
      $newhash->{guid} = $filehash->{guid};
      $newhash->{md5}  ="$filehash->{md5}";
      $newhash->{nSEs} = $nses;
      $newhash->{lfn}=$filehash->{lfn};
      $newhash->{size}=$filehash->{size};
      $filehash->{type} and $newhash->{type}=$filehash->{type};
      foreach my $t (@$whereis){
        $self->info("HELLO $t");
        $t->{pfn} and $t->{pfn} =~ s{//+}{//}g;
      }
      $newhash->{origpfn}=$whereis;
    
      # the -p (public) option creates public access url's without envelopes
      $newhash->{se}="$se";
      
      if ( ($options =~ /p/) && ($access =~ /^read/) ) {
	$newhash->{envelope} = "alien";
	# we actually need this code, but then 'isonline' does not work anymore ...
	#	      if ($anchor ne "") {
	#		  $newhash->{url}="$pfn#$anchor";
	#		  $newhash->{lfn}="$lfn#$anchor";
	#	      } else {
	$newhash->{url}="$pfn";
	#	      }
      } else {
	$newhash->{envelope} = $self->{envelopeCipherEngine}->GetEncodedEnvelope();
	#$newhash->{pfn}=$filehash->{pfn};
	$newhash->{pfn}="$ppfn";
        $newhash->{url}=$filehash->{turl} ;#"root://$pfix/$ppfn";
        ($se =~ /dcache/i)  and $newhash->{url}="root://$pfix/$filehash->{lfn}";
        ($se =~ /alice::((RAL)|(CNAF))::castor/i) and $newhash->{url}="root://$pfix/$filehash->{lfn}";

	($anchor) and $newhash->{url}.="#$anchor";
      }
      $ENV{ALIEN_XRDCP_ENVELOPE}=$newhash->{envelope};
      $ENV{ALIEN_XRDCP_URL}=$newhash->{url};

      if ($self->{MONITOR}) {
	my @params= ("$se", $filehash->{size});
	my $method;
	($access =~ /^((read)|(write[\-a-z]*))/)  and $method="${1}req";
	$access =~ /^delete/ and $method="delete";
	$method and
	  $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","$self->{ROLE}_$method", @params); 		      
      }
      push @lnewresult,$newhash; 
      if (!$coded) {
	$self->info("access: error during envelope encryption");
	return access_eof("access: error during envelope encryption");
      } else {
	(!($options=~ /s/)) and $self->info("access: prepared your access envelope");
      }
      
      ($options=~ /v/) or
	print STDERR "========================================================================
$ticket
========================================================================
",$$newresult[0]->{envelope},"
========================================================================\n", $ticket,"\n";
      #last;
      ($access =~ /^write[\-a-z]*/) and (scalar(@ses) gt 0)
	and ($self->info("gonna recall next iteration") ) 
      or last;
    }
  }

  if ($options =~ /g/) {
    $globalticket .= "  </file>\n</authz>\n";
    $self->{envelopeCipherEngine}->Reset();
    my $coded = $self->{envelopeCipherEngine}->encodeEnvelopePerl("$globalticket","0","none");
    $lnewresult[0]->{genvelope} = $self->{envelopeCipherEngine}->GetEncodedEnvelope();
  }

  return @$newresult; 
}













sub authorize{
  my $self = shift;

  #
  # Start of the Server/Authen side code
   $self->info("STARTING envelope creation: @_ ");
   return $self->AuthenConsultation(@_); 

}


sub authorize_return {
  my $self=shift;
  my $rc=(shift || 0);
  my $message=(shift || 0);
  my $envelopes=(shift || [] );
  my $hash={};


#  ($rc eq 1 ) and $hash->{ok}=1;
  $hash->{ok}=$rc;
  ($rc eq -1 ) and $hash->{stop}=1 and $hash->{ok}=0;
  if(!$hash->{message}) {
    $hash->{ok} and $message = "OK";
    !$hash->{ok} and $message = "Authorize: unspecified error!";
  } else {
    $message = $hash->{message};
  }
  $self->info("JUMP: authen_return: $message");

  $hash->{envelopecount}=scalar(@$envelopes);
  $hash->{envelopes} = [];

  foreach (@$envelopes) {
    push @{$hash->{envelopes}}, $_;
  }
  #$hash->{envelopes} = {};

  #for (my $c = 0; $c < $hash->{envelopecount}; $c++) {
  #  $hash->{envelopes}->{($c+1)} = $$envelopes[$c];
  #}
  return $hash;
}



sub getValFromEnvelope {
  my $env=(shift || return 0);
  my $rKey=(shift || return 0);

  foreach ( split(/&/, $env)) {
     my ($key, $val) = split(/=/,$_);
     ($rKey eq $key) and return $val;
  }
  return 0;
}






sub deleteFileFromCatalogue {
  my $self=shift;
  my $lfns=(shift || return 0);
  my $user=(shift || return 0);


  #Check permissions
  my $filehash=$self->checkPermissionsOnLFN($lfns,"delete","w")
          or return $self->authorize_return(0,"ERROR: checkPermissionsOnLFN failed for $lfns","[checkPermission]");
  
  #Insert into LFN_BOOKED
  my $parent = "$lfns";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $db = $self->selectDatabase($parent) or return $self->authorize_return(0,"Error selecting the database of $parent","[connectToDatabse]");
  my $tableName = "$db->{INDEX_TABLENAME}->{name}";
  my $tablelfn = "$db->{INDEX_TABLENAME}->{lfn}";
  my $lfnOnTable = "$lfns";
  $lfnOnTable =~ s/$tablelfn//;
  my $size = $db->queryValue("SELECT l.size FROM $tableName l WHERE l.lfn LIKE '$lfnOnTable'") || 0;
  my $guid = $db->queryValue("SELECT binary2string(l.guid) as guid FROM $tableName l WHERE l.lfn LIKE '$lfnOnTable'") || 0;
  $filehash = $self->{DATABASE}->{GUID_DB}->checkPermission("w",$guid); # Check permissions on the GUID
  if($filehash) {
      $db->do("INSERT INTO LFN_BOOKED(lfn, owner, expiretime, size, guid, gowner)
           SELECT '$lfns', l.owner, -1, l.size, l.guid, l.gowner FROM $tableName l WHERE l.lfn LIKE '$lfnOnTable'")
           or return $self->authorize_return(0,"ERROR: Could not add entry $lfns to LFN_BOOKED","[insertIntoDatabase]");
  }
  else {
      $self->info("$user does not have permissions for deleting $guid. Only deleting $lfns");
  }
  
  #Delete LFN and update quotas
  $db->do("DELETE FROM $tableName WHERE lfn LIKE '$lfnOnTable'");
  $self->{PRIORITY_DB} or $self->{PRIORITY_DB}=AliEn::Database::TaskPriority->new({ROLE=>'admin',SKIP_CHECK_TABLES=> 1});
  $self->{PRIORITY_DB}or return $self->authorize_return(0,"Could not get access to PRIORITY DB","[updateQuotas]");
  $self->{PRIORITY_DB}->lock("PRIORITY");
  $self->{PRIORITY_DB}->do("UPDATE PRIORITY SET nbFiles=nbFiles+tmpIncreasedNbFiles-1, totalSize=totalSize+tmpIncreasedTotalSize-$size, tmpIncreasedNbFiles=0, tmpIncreasedTotalSize=0 WHERE user LIKE '$user'") or return $self->authorize_return(0, "ERROR: Could not write to PRIORITY DB","[updateQuotas]");
  $self->{PRIORITY_DB}->unlock();
#  return $self->authorize_return(0,"Success - File<$lfns> scheduled for deletion");
  $self->info("Success - File<$lfns> scheduled for deletion");
  return 1;
}



sub deleteFolderFromCatalogue {
  my $self=shift;
  my $lfns=(shift || return 0);
  my $user=(shift || return 0);


  #Check permissions
  my $parentdir = $self->GetParentDir($lfns);
  my $filehash=$self->checkPermissionsOnLFN($parentdir,"delete","w")
          or return $self->authorize_return(0,"ERROR: checkPermissionsOnLFN failed for $parentdir","[checkPermission]");
  $filehash=$self->checkPermissionsOnLFN("$lfns/","delete","w")
          or return $self->authorize_return(0,"ERROR: checkPermissionsOnLFN failed for $lfns","[checkPermission]");
  
  #Insert into LFN_BOOKED and delete lfns
#          $self->{LFN_DB} or $self->{LFN_DB}=AliEn::Database::Catalogue::LFN->new();
  my $entries=$self->{DATABASE}->{LFN_DB}->getHostsForEntry($lfns) or return $self->authorize_return(0,"ERROR: Could not get hosts for $lfns","[getHosts]");
  my @index=();
  my $size = 0;
  my $count = 0;          
  foreach my $db (@$entries) {
          $self->info(1, "Deleting all the entries from $db->{hostIndex} (table $db->{tableName} and lfn=$db->{lfn})");
          my ($db2, $lfns2)=$self->{DATABASE}->{LFN_DB}->reconnectToIndex($db->{hostIndex}, $lfns);
          $db2 or return $self->authorize_return(0,"ERROR: Could not reconnect to host","[getHosts]");
          my $tmpPath="$lfns/";
          $tmpPath=~ s{^$db->{lfn}}{};
          $count += ($db2->queryValue("SELECT count(*) FROM L$db->{tableName}L l WHERE l.lfn LIKE '$tmpPath%' AND l.type<>'d'")||0);
          $size += ($db2->queryValue("SELECT SUM(l.size) FROM L$db->{tableName}L l WHERE l.lfn LIKE '$tmpPath%'")||0);
          $db2->do("INSERT INTO LFN_BOOKED(lfn, owner, expiretime, size, guid, gowner)
                    SELECT l.lfn, l.owner, -1, l.size, l.guid, l.gowner FROM L$db->{tableName}L l WHERE l.lfn LIKE '$tmpPath%'")
                    or return $self->authorize_return(0,"ERROR: Could not add entries $tmpPath to LFN_BOOKED","[insertIntoDatabase]");
          $db2->delete("L$db->{tableName}L", "lfn like '$tmpPath%'");
          $db->{lfn} =~ /^$lfns/ and push @index, "$db->{lfn}\%";
  }
  #Clean up index
  if ($#index>-1) {
          $self->deleteFromIndex(@index);
          if (grep( m{^$lfns/?\%$}, @index)){
                  my $entries=$self->{DATABASE}->{LFN_DB}->getHostsForEntry($parentdir) or 
                      return $self->authorize_return( "Error getting the hosts for '$lfns'","[getHosts]");
                  my $db=${$entries}[0];
                  my ($newdb, $lfns2)=$self->{DATABASE}->{LFN_DB}->reconnectToIndex($db->{hostIndex}, $parentdir);
                  $newdb or return $self->authorize_return(0,"Error reconecting to index","[getHosts]");
                  my $tmpPath="$lfns/";
                  $tmpPath=~ s{^$db->{lfn}}{};
                  $newdb->delete("L$db->{tableName}L", "lfn='$tmpPath'");
          }
  }

  #Update quotas
  $self->{PRIORITY_DB} or $self->{PRIORITY_DB}=AliEn::Database::TaskPriority->new({ROLE=>'admin',SKIP_CHECK_TABLES=> 1});
  $self->{PRIORITY_DB}or return $self->authorize_return(0,"ERROR: Could not get access to PRIORITY DB","[updateQuotas]");
  $self->{PRIORITY_DB}->lock("PRIORITY");
  $self->{PRIORITY_DB}->do("UPDATE PRIORITY SET nbFiles=nbFiles+tmpIncreasedNbFiles-$count, totalSize=totalSize+tmpIncreasedTotalSize-$size, tmpIncreasedNbFiles=0, tmpIncreasedTotalSize=0 WHERE user LIKE '$user'") or $self->authorize_return(0,"ERROR: Could not write to PRIORITY DB","[updateQuotas]");
  $self->{PRIORITY_DB}->unlock();

#  return $self->authorize_return(0,"Success - Folder<$lfns> scheduled for deletion [$count files ; $size size]");
  $self->info("Success - Folder<$lfns> scheduled for deletion [$count files ; $size size]");
  return 1;
}


sub moveFileInCatalogue{
  my $self=shift;
  my $source=(shift || return 0);
  my $target=(shift || return 0);


  my $filehash1=$self->checkPermissionsOnLFN($source,"delete","w")
                or return $self->authorize_return(0,"ERROR: checkPermissionsOnLFN failed for $source","[checkPermission]");
  my $filehash2=$self->checkPermissionsOnLFN($target,"write","w")
                or return $self->authorize_return(0,"ERROR: checkPermissionsOnLFN failed for $target","[checkPermission]");
  
  my $parent = "$source";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $dbSource = $self->selectDatabase($parent)
                 or return $self->authorize_return(0,"Error selecting the database of $parent","[connectToDatabse]");
  my $tableName_source = "$dbSource->{INDEX_TABLENAME}->{name}";
  my $tablelfn_source = "$dbSource->{INDEX_TABLENAME}->{lfn}";
  $parent = "$target";
  my $dbTarget = $self->selectDatabase($parent)
                  or return $self->authorize_return(0,"Error selecting the database of $parent","[connectToDatabse]");
  my $tableName_target = "$dbTarget->{INDEX_TABLENAME}->{name}";
  my $tablelfn_target = "$dbTarget->{INDEX_TABLENAME}->{lfn}";
  
  my $lfnOnTable_source = "$source";
  $lfnOnTable_source =~ s/$tablelfn_source//;
  my $lfnOnTable_target = "$target";
  $lfnOnTable_target =~ s/$tablelfn_target//;
  
  if($tablelfn_source eq $tablelfn_target) {
     #If source and target are in same L#L table then just edit the names
     $dbSource->do("UPDATE $tableName_source SET lfn='$lfnOnTable_target' WHERE lfn LIKE '$lfnOnTable_source'")
                  or return $self->authorize_return(0,"Error updating database","[updateDatabse]");
  }
  else {
     #If the source and target are in different L#L tables then add in new table and delete from old table
     my $schema = $dbSource->queryRow("SELECT h.db FROM HOSTS h, INDEXTABLE i WHERE i.hostIndex=h.hostIndex AND i.lfn LIKE '$tablelfn_source'")
                                       or return $self->authorize_return(0,"Error updating database","[updateDatabse]");
     my $db = $schema->{db};
     $dbTarget->do("INSERT INTO $tableName_target(owner, replicated, ctime, guidtime, aclId, lfn, broken, expiretime, size, dir, gowner, type, guid, md5, perm) 
                    SELECT owner, replicated, ctime, guidtime, aclId, '$lfnOnTable_target', broken, expiretime, size, dir, gowner, type, guid, md5, perm FROM $db.$tableName_source WHERE lfn LIKE '$lfnOnTable_source'")
                   or return $self->authorize_return(0,"Error updating database","[updateDatabse]");
     my $parentdir = "$lfnOnTable_target";
     $parentdir =~ s/([.]*)\/([^\/]+)$/$1\//;
     my $entryId = $dbTarget->queryValue("SELECT entryId FROM $tableName_target WHERE lfn LIKE '$parentdir'");
     $dbTarget->do("UPDATE $tableName_target SET dir=$entryId WHERE lfn LIKE '$lfnOnTable_target'")
                   or return $self->authorize_return(0,"Error updating database","[updateDatabse]");
     $dbSource->do("DELETE FROM $tableName_source WHERE lfn LIKE '$lfnOnTable_source'")
                   or return $self->authorize_return(0,"Error updating database","[updateDatabse]");
  }
  
  #return $self->authorize_return(0,"Success - $source moved to $target");
  $self->info("Success - $source moved to $target");
  return 1;
}




sub selectPFNOnClosestRootSEOnRank{
   my $self=shift;
   my $sitename=(shift || 0);
   my $user=(shift || return 0);
   my $guid=(shift || return 0);
   my $sePrio = (shift || 0);
   my $excludeList=(shift || []);
   my $nose=0;
   my $result={};
   my $seList={};
   my $nonRoot={};

   my @where=$self->f_whereis("sgztr","$guid");
   @where 
     or $self->debug(1,"There were no transfer methods....")
     and @where=$self->f_whereis("sgzr","$guid");

   foreach my $tSE (@where) {
     $self->info("gron: se $tSE->{se}, pfn $tSE->{pfn}");
     AliEn::Util::isValidSEName($tSE->{se}) || next;
     $self->info("gron: se $tSE->{se} valid!");
     (grep (/^$tSE->{se}$/i,@$excludeList)) && next;
     if($tSE->{pfn} =~ /^root/) { 
        $seList->{$tSE->{se}} = $tSE->{pfn}; 
        $self->info("gron: se $tSE->{se} recognized!");
    # } elsif ($tSE->{pfn} =~ /^guid/) {
       # $nose=$tSE->{pfn};
     } else {
        $nose=$tSE->{pfn};
       #$nonRoot->{se} = $tSE->{se};
       #$nonRoot->{pfn} = $tSE->{pfn};
     }
   } 

   if(scalar(keys %{$seList}) eq 0) {
     # we don't have any root SE to get it from
     $nose and return ("no_se",$nose);      
     $self->info("access: no root file or archive to read from.");
     $nonRoot->{pfn} and return ($nonRoot->{se},$nonRoot->{pfn});
     return;
   }

   my $catalogue = $self->{DATABASE}->{LFN_DB}->{FIRST_DB};
   my @queryValues = ();
   my $query = "";
   if($sitename) {
      $self->checkSiteSECacheForAccess($sitename) || return;
      push @queryValues, $sitename;
   
      $query="SELECT DISTINCT b.seName FROM SERanks a right JOIN SE b on (a.seNumber=b.seNumber and a.sitename LIKE ?) WHERE ";
      $query .= " (b.seExclusiveRead is NULL or b.seExclusiveRead = '' or b.seExclusiveRead  LIKE concat ('%,' , ? , ',%') ) and ";
      push @queryValues, $user;
      foreach (keys %{$seList}){ $query .= " b.seName LIKE ? or"; push @queryValues, $_;  } 
      $query =~ s/or$//;
      $query .= " ORDER BY if(a.rank is null, 1000, a.rank) ASC ;";
   } else { # sitename not given, so we just delete the excluded SEs and check for exclusive Users
       $query="SELECT seName FROM SE WHERE ";
       foreach(keys %{$seList}){   $query .= " seName LIKE ? or"; push @queryValues, $_;  }
       $query =~ s/or$//;
       $query .= " and (seExclusiveRead is NULL or seExclusiveRead = '' or seExclusiveRead  LIKE concat ('%,' , ? , ',%') ) ;";
       push @queryValues, $user;
   }
   $self->info("gron: query: $query, values: @queryValues");
   my $sePriority = $self->resortArrayToPrioElementIfExists($sePrio,$catalogue->queryColumn($query, undef, {bind_values=>\@queryValues}));
   $self->info("gron: choosen se: ".$$sePriority[0].", pfn: ".$seList->{$$sePriority[0]}." .");
   return ($$sePriority[0], $seList->{$$sePriority[0]});
}



sub getBaseEnvelopeForReadAccess {
  my $self=shift;
  my $user=(shift || return 0);
  my $lfn=(shift || return 0);
  my $seList=shift;
  my $excludedAndfailedSEs=shift;
  my $sitename=(shift || 0);


  
  $self->info("gron: getBaseEnvelopeForReadAccess...");

  $self->info("gron: user $user");
  $self->info("gron: lfn $lfn");
  $self->info("gron: seList @$seList");
  $self->info("gron: sitename $sitename");
  $self->info("gron: excludedAndfailedSEs @$excludedAndfailedSEs");


  my $filehash = {};
  if(AliEn::Util::isValidGUID($lfn)) {
    $self->info("gron: recognized lfn/guid as a GUID");
    $filehash=$self->{DATABASE}->{GUID_DB}->checkPermission("r", $lfn, {retrieve=>"guid,type,size,md5"})
      or return $self->authorize_return(0,"access: access denied for $lfn");
    $filehash->{guid} = $lfn;
    $filehash->{lfn} = $lfn;
  } else {
    $self->info("gron: recognized lfn/guid as a LFN");
    $filehash=$self->checkPermission("r",$lfn,undef, {RETURN_HASH=>1})
     or return $self->authorize_return(0,"access: access denied for $lfn");
    ($filehash->{type} eq "f") or return $self->authorize_return(0,"access: $lfn is not a file, so read not possible");
  }
  # gron: what about the parent folder and tree checking ?!

  foreach ( keys %{$filehash}) { $self->info("gron: after checkPermissions for read, filehash: $_: $filehash->{$_}"); }


  ($filehash->{size} eq "0") and $filehash->{size} = 1024*1024*1024 ; #gron: does this make any sense ?

  my $packedEnvelope = $self->reduceFileHashAndInitializeEnvelope("read",$filehash,"lfn","guid","size","md5");

  ($packedEnvelope->{se}, $packedEnvelope->{pfn})
    = $self->selectPFNOnClosestRootSEOnRank($sitename, $user, $filehash->{guid}, ($$seList[0] || 0), $excludedAndfailedSEs)
       or $self->info("access ERROR within selectPFNOnClosestRootSEOnRank: SE list was empty after checkup. Either problem with the file's info or you don't have access on relevant SEs.")
       and return;

  if ($packedEnvelope->{se} eq "no_se") {
     ($packedEnvelope->{pfn} =~ /^([a-zA-Z]*):\/\//);
     if(($1 eq "guid") and ($packedEnvelope->{pfn} =~ s/\?ZIP=(.*)$//)) {
       my $archiveFile = $1;
       $self->info("Getting file out of archive with GUID, $filehash->{guid}...");
       $packedEnvelope=$self->getBaseEnvelopeForReadAccess($user, $filehash->{guid}, $seList, $excludedAndfailedSEs, $sitename);
       $packedEnvelope->{pfn} = "?ZIP=".$archiveFile;
       return $packedEnvelope;
     }
     $packedEnvelope->{turl} = $packedEnvelope->{pfn};
  } else {
    $self->info("gron: envelope... se: $packedEnvelope->{se}, pfn: $packedEnvelope->{pfn}");
    ($packedEnvelope->{turl},$packedEnvelope->{pfn}) = $self->parseAndCheckStorageElementPFN2TURL($packedEnvelope->{se}, $packedEnvelope->{pfn});
  }
  my @seList = ("$packedEnvelope->{se}");
  return ($packedEnvelope, \@seList);
}


sub parseAndCheckStorageElementPFN2TURL {
  my $self=shift;
  my $se=(shift || return);
  my $pfn=(shift || return);
  my $turl="";
  my $urloptions="";

  my $parsedPFN = $self->parsePFN($pfn);
  $parsedPFN or return (0,$pfn);

  my @queryValues = ("$se");
  my $seiostring = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow("SELECT seioDaemons FROM SE where seName like ? ;", 
              undef, {bind_values=>\@queryValues});
  ($seiostring->{seioDaemons} =~ /[a-zA-Z]*:\/\/[0-9a-zA-Z.\-_:]*/) and $turl = $seiostring->{seioDaemons} or return (0,$pfn);

  $self->info("gron: seiostring is : $seiostring->{seioDaemons} ");
  $turl= "$turl/$parsedPFN->{path}";
  $parsedPFN->{vars} and $turl= $turl."?$parsedPFN->{vars}";
  $self->info("gron: turl: $turl");
  return ($turl,$parsedPFN->{path});
}

sub getSEforPFN{
  my $self=shift;
  my $pfn=(shift || return);

  $pfn = $self->parsePFN($pfn);
  $pfn or return 0;
  my @queryValues = ("$pfn->{proto}://$pfn->{host}");
  $self->info("gron: Asking for seName of $pfn->{proto}:$pfn->{host}");
  my $sestring = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow("SELECT seName FROM SE where seioDaemons LIKE concat ( ? , '%') ;",
              undef, {bind_values=>\@queryValues});
  $sestring->{seName} or return 0;
  $self->info("gron: seiostring is : $sestring->{seName}");
  return $sestring->{seName};
}


sub parsePFN {
  my $self=shift;
  my $pfn=(shift|| return {});
  my $result={};
  $pfn =~ /^([a-zA-Z]*):\/\/([0-9a-zA-Z.\-_:]*)\/(.*)$/;
  $1 and $2 or return 0;
  $result->{proto}  = $1;
  $result->{host}   = $2;
  ($result->{path},$result->{vars}) = split (/\?/,$3);
  $result->{path} =~ s/^(\/*)/\//;
  $self->info("gron: parsing PFN we got: $result->{proto}, $result->{host} , $result->{path}, $result->{vars} ."); 
  return $result;
}


sub  getBaseEnvelopeForWriteAccess {
  my $self=shift;
  my $user=(shift || return 0);
  my $lfn=(shift || return 0);
  my $size=(shift || 0);
  my $md5=(shift || 0);
  my $guidRequest=(shift || 0); 
  my $envelope={};

  $self->info("gron: Doing checkPermissionsOnLFN for $lfn, size: $size, md5: $md5 ");
  $envelope= $self->checkPermissions("w",$lfn,undef, {RETURN_HASH=>1});
  $envelope or return $self->authorize_return(0,"access: access denied to $lfn");

  #Check parent dir permissions:
  my $parent = $self->f_dirname($lfn);
  $self->checkPermissions("w",$parent) 
     or return $self->authorize_return(0,"access: parent dir missing for lfn $lfn");
  # gron: still to be discussed what we do to the tree below ... 

  my $reply = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
      "SELECT lfn FROM LFN_BOOKED WHERE lfn=? ;"
      , undef, {bind_values=>[$lfn]});
  $reply->{lfn} and  $reply = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
      "SELECT lfn FROM LFN_BOOKED WHERE lfn=? and owner<>? and gowner<>?;"
      , undef, {bind_values=>[$lfn,$user,$user]});

  $reply->{lfn} and return $self->authorize_return(0,"access: the LFN is already in use (reserved in [LFN_BOOKED], not in the catalogue)");


  # gron: guidRequest needs to be checked and accounted
  if (!$guidRequest) { 
    $self->{GUID} or $self->{GUID}=AliEn::GUID->new(); 
    $envelope->{guid} = $self->{GUID}->CreateGuid();
  }



#  ($packedEnvelope->{lfn} eq $parent) and $packedEnvelope->{lfn} = $lfn;
  $envelope->{lfn} = $lfn;
  $envelope->{size} = $size;
  $envelope->{md5} = $md5;

  my ($ok, $message) = $self->checkFileQuota($user, $envelope->{size});
  ($ok eq -1) and $self->info("We gonna throw an access exception: "."[quotaexception]") and return  $self->authorize_return(-1, $message."[quotaexception]");
  ($ok eq 0) and return  $self->authorize_return(0,$message);
  
  foreach ( keys %{$envelope}) { $self->info("gron: filehash for write before cleaning checkPermissions: $_: $envelope->{$_}"); }
  return $self->reduceFileHashAndInitializeEnvelope("write-once",$envelope,"lfn","guid","size","md5");
}


sub calculateXrootdTURLForWriteEnvelope{
  my $self=shift;
  my $envelope=(shift || return {});

  ($envelope->{turl}, my $guid, my $se) = $self->createFileUrl($envelope->{se}, "root", $envelope->{guid});
  $se 
    or $self->info("Creating a default pfn (for $envelope->{guid})")
    and ($envelope->{turl}, $guid)=$self->createDefaultUrl($envelope->{se}, $envelope->{guid},$envelope->{size});
  $envelope->{guid} or $envelope->{guid} = $guid;
  $envelope->{turl} =~ s/\/$//;
  $envelope->{turl} =~ m{^((root)|(file))://([^/]*)/(.*)};
  $envelope->{pfn} = "$5";

  return $envelope;
}


sub  getBaseEnvelopeForMirrorAccess {
  my $self=shift;
  my $user=(shift || return 0);
  my $guid=(shift || return 0);
  my $envelope={};

  AliEn::Util::isValidGUID($guid) or return $self->authorize_return(0,"ACCESS ERROR: $guid is not a valid GUID.");
  $envelope=$self->{DATABASE}->{GUID_DB}->checkPermission("w", $guid, {retrieve=>"guid,type,size,md5"})
      or return $self->authorize_return(0,"access: access denied for $guid");
  ($envelope->{gowner} eq $user) or return $self->authorize_return(0,"ACCESS DENIED: You are not the owner of the GUID '$guid'.");
  $envelope->{guid} = $guid;
  $envelope->{lfn} = $guid;

  ( defined($envelope->{size}) && ($envelope->{size} gt 0)) or return $self->authorize_return(0,"ACCESS ERROR: You are trying to mirror a zero sized file '$guid'");

  foreach ( keys %{$envelope}) { $self->info("gron: packedEnvelope for write after checkPermissions: $_: $envelope->{$_}"); }

  $envelope->{lfn} = $guid;
  $envelope->{access} = "write-once";
  return $self->reduceFileHashAndInitializeEnvelope("write-once",$envelope,"lfn","guid","size","md5","access");
}



sub  getSEsAndCheckQuotaForWriteOrMirrorAccess{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my $seList=(shift || []);
  my $sitename=(shift || 0);
  my $writeQos=(shift || {});
  my $writeQosCount=(shift || 0);
  my $excludedAndfailedSEs=(shift || []);

  # if nothing is or wrong specified SE info, get default from Config, if there is a sitename
  ( (scalar(@$seList) eq 0) and ($sitename ne 0) and ( ($writeQos eq 0) or ($writeQosCount eq 0) ) and $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT} ) 
    and ($writeQos, $writeQosCount) =split (/\=/, $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT},2);

  (scalar(@$seList) eq 0) or $seList = $self->checkExclWriteUserOnSEsForAccess($user,$envelope->{size},$seList);
  if(($sitename ne 0) and ($writeQos ne 0) and ($writeQosCount gt 0)) {
     my $dynamicSElist = $self->getSEListFromSiteSECacheForWriteAccess($user,$envelope->{size},$writeQos,$writeQosCount,$sitename,$excludedAndfailedSEs);
     push @$seList,@$dynamicSElist;
  }
  # gron: make entry in te /getSEsAndCheckQuotaForWriteOrMirrorAccess


  return ($envelope, $seList);
}


sub registerPFNInCatalogue{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my $pfn=(shift || return 0);
  my $se=(shift || 0);

  $envelope->{lfn} or return  $self->authorize_return(0,"The access to registering a PFN with LFN $envelope->{lfn} could not be granted.");
  $se or $se=$self->getSEforPFN($pfn);
  $se or return $self->authorize_return(0, "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn could not be registered. The PFN doesn't correspond to any known SE.");
 
  $self->f_registerFile( "-f", $envelope->{lfn}, $envelope->{size},
           $se, $envelope->{guid}, undef,undef, $envelope->{md5},
                $pfn) 
     or return $self->authorize_return(0, "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn could not be registered.");
  return $self->authorize_return(1, "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn was successfully registered.");

}

sub registerFileInCatalogueAccordingToEnvelopes{
  my $self=shift;
  my $user=(shift || return 0);
  my $signedEnvelopes=(shift || []);
  my $returnMessage= "";
  my $success=0;
  $self->info("gron: the envelopes for registration are: @$signedEnvelopes");
  my @successMap = ();

 
  foreach my $envelope (@$signedEnvelopes) {
     my $justRegistered=0;
     push @successMap,"0";
     $envelope = $self->verifyAndDeserializeEnvelope($envelope);
     $envelope 
            or $self->info("An envelope could not be verified.") 
            and $returnMessage .= "An envelope could not be verified.\n" 
            and  next; # gron: we have to track this error with "could not verify an envelope"
     $envelope = $self->ValidateRegistrationEnvelopesWithBookingTable($user,$envelope);
     $envelope 
            or $self->info("An envelope could not be validated based on pretaken booking.") 
            and $returnMessage .=  "An envelope could not be validated based on pretaken booking.\n"
            and next; # gron: we have to track this error with "could not valdite this register with a pretaken booking"

    $self->info("gron: ok, registering the file ...");
    $self->info("gron: $envelope->{lfn}, $envelope->{size},$envelope->{se}, $envelope->{guid}, $envelope->{md5}, ... $envelope->{turl}");
    if(!$envelope->{existing}) {
      $self->info("gron: file is not yet existing");
      $self->f_registerFile( "-f", $envelope->{lfn}, $envelope->{size},
               $envelope->{se}, $envelope->{guid}, undef,undef, $envelope->{md5}, 
                $envelope->{turl}) and $justRegistered=1 
       or $self->info("File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered.")
       and $returnMessage .= "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered."
       and next;
     } else {
        $self->f_addMirror( $envelope->{lfn}, $envelope->{se}, $envelope->{turl}, "-c","-md5=".$envelope->{md5})
          or $self->info("File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered as a replica.")
          and $returnMessage .= "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered as a replica."
        and next;
     }
     $self->info("gron: deleting entry from booking table");
     $self->deleteEntryFromBookingTableAndOptionalExistingFlagTrigger($user, $envelope, $justRegistered) 
            or $self->info("File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered properly as a replica (LFN_BOOKED error).")
            and $returnMessage .= "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered properly as a replica (LFN_BOOKED error)."
            and next;
     pop @successMap; push @successMap, 1; 
     $success++;
  }
  return $self->authorize_return(
              (($success eq scalar(@$signedEnvelopes)) || (($success gt 0) ? -1 : 0))
              ,$returnMessage." $success of the requested ".scalar(@$signedEnvelopes)." PFNs where correctly registered.",\@successMap);
}


sub ValidateRegistrationEnvelopesWithBookingTable{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my @verifiedEnvelopes= ();

  $self->info("gron: validate check BOOKING output: values: $envelope->{guid},$envelope->{turl},$envelope->{se},$envelope->{size},$envelope->{md5},$user,$user");

  my $reply = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
      "SELECT lfn,binary2string(guid) as guid,existing FROM LFN_BOOKED WHERE guid=string2binary(?) and pfn=? and se like ? and size=? and md5sum=? and owner like ? and gowner like ? ;"
      , undef, {bind_values=>[$envelope->{guid},$envelope->{turl},$envelope->{se},$envelope->{size},$envelope->{md5},$user,$user]});
  $self->info("gron: verification output is: $envelope->{guid}");
  $self->info("gron: control verification output is: $reply->{guid}");
  
  lc $envelope->{guid} eq lc $reply->{guid} or return 0;
  $envelope->{lfn} = $reply->{lfn};
  $envelope->{existing} = $reply->{existing};

  $self->info("gron: validate sign of env down... ");

  return $envelope;
}

 
sub deleteEntryFromBookingTableAndOptionalExistingFlagTrigger{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my $trigger=(shift || 0);
  $self->info("gron: delete Entry check BOOKING output: values: $envelope->{lfn},$envelope->{guid},$envelope->{size},$envelope->{md5},$user,$user");


  my $triggerstat=1;
  $trigger 
    and $triggerstat = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
    "UPDATE LFN_BOOKED SET existing=1 WHERE lfn=? and binary2string(guid) LIKE ? and size=? and md5sum=? and owner=? and gowner=? ;",
    {bind_values=>[$envelope->{lfn},$envelope->{guid},$envelope->{size},$envelope->{md5},$user,$user]});


  $self->info("gron: UPDATE LFN_BOOKED done.");

  return ($self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
    "DELETE FROM LFN_BOOKED WHERE lfn=? and binary2string(guid) LIKE ? and pfn=? and se LIKE ? and size=? and md5sum=? and owner=? and gowner=? ;",
    {bind_values=>[$envelope->{lfn},$envelope->{guid},$envelope->{turl},$envelope->{se},$envelope->{size},$envelope->{md5},$user,$user]})
    && $triggerstat);
}


sub addEntryToBookingTableAndOptionalExistingFlagTrigger{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my $trigger=(shift || 0);

  use Time::HiRes qw (time); 
  my $lifetime= time() + 60;

  $self->info("gron: adding Entry check BOOKING output: values: $envelope->{lfn},$user,1 ,$envelope->{md5},200,$envelope->{size},$envelope->{turl},$envelope->{se},$user,$envelope->{guid},$trigger");


  return $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
    "INSERT INTO LFN_BOOKED (lfn, owner, quotaCalculated, md5sum, expiretime, size, pfn, se, gowner, guid, existing) VALUES (?,?,?,?,?,?,?,?,?,string2binary(?),?);"
    ,{bind_values=>[$envelope->{lfn},$user, "1" ,$envelope->{md5},$lifetime,$envelope->{size},$envelope->{turl},$envelope->{se},$user,$envelope->{guid},$trigger]});
}


sub reduceFileHashAndInitializeEnvelope{
  my $self=shift;
  my $access=(shift || return 0);
  my $filehash=(shift || return 0);
  my @tags=@_;
  my $envelope = {};
  
  $envelope->{access} = $access;
  foreach my $tag (keys %{$filehash}) {
     grep(/^$tag$/,@tags) 
         and $envelope->{$tag} = $filehash->{$tag};
  }
  foreach ( keys %{$envelope}) { $self->info("gron: packedEnvelope during reduction : $_: $envelope->{$_}"); }

  return $envelope;
}


sub AuthenConsultation {
  my $self = shift;
  my $access = (shift || return),
  my @registerEnvelopes=@_;
  my $lfn    = (shift || "");
  my $seOrLFNOrEnv= (shift || "");
  my $size    = (shift || "0");
  my $md5 = (shift || 0);
  my $guidRequest = (shift || 0);
  my $sitename= (shift || 0);
  my $writeQos = (shift || 0);
  my $writeQosCount = (shift || 0);
  my $excludedAndfailedSEs = $self->validateArrayOfSEs(split(/;/, shift));

  my $user=$self->{CONFIG}->{ROLE};
  $self and $self->{ROLE} and $user=$self->{ROLE};
  #
  #

  $self->info("gron: user $user");
  $self->info("gron: access $access");
  $self->info("gron: lfn $lfn");
  $self->info("gron: size $size");
  $self->info("gron: guidRequest $guidRequest");
  $self->info("gron: seOrLFNOrEnv $seOrLFNOrEnv");
  $self->info("gron: sitename $sitename");
  $self->info("gron: writeQos $writeQos");
  $self->info("gron: writeQosCount $writeQosCount");
  $self->info("gron: excludedAndfailedSEs @$excludedAndfailedSEs");


  ($access =~ /^write[\-a-z]*/) and $access = "write-once";
  my $writeReq = ( ($access =~ /^write-once$/) || 0 );
  my $mirrorReq = ( ($access =~ /^mirror$/) || 0 );
  my $readReq = ( ($access =~ /^read$/) || 0 );
  my $delFileReq = ( ($access =~/^deletefile$/) || 0 );
  my $delFolderReq = ( ($access =~/^deletefolder$/) || 0 );
  my $moveReq = ( ($access =~/^move$/) || 0 );
  my $versionReq = ( ($access =~/^version$/) || 0 );
  my $registenvsReq = ( ($access =~/^registerenvs$/) || 0 );
  my $registerReq = ( ($access =~/^register$/) || 0 );

  my $exceptions = 0;

#  ($writeReq or $readReq or $delReq or $mirrorReq or $versionReq) or
#    return $self->authorize_return(0,"access: illegal access type <$access> requested");


  # the following three return immediately without envelope creation
  $delFileReq and return $self->deleteFileFromCatalogue($lfn,$user);
  $delFolderReq and return $self->deleteFolderFromCatalogue($lfn,$user);
  $moveReq and return $self->moveFileInCatalogue($lfn,$seOrLFNOrEnv);
  $registenvsReq and return $self->registerFileInCatalogueAccordingToEnvelopes($user,\@registerEnvelopes);


  my $seList = $self->validateArrayOfSEs(split(/;/, $seOrLFNOrEnv));
  $self->info("gron: seList @$seList");


  my @packedEnvelopeList = ();
  my $packedEnvelope = {};

  ($writeReq or $registerReq) and $packedEnvelope = $self->getBaseEnvelopeForWriteAccess($user,$lfn,$size,$md5,$guidRequest);

  $registerReq and return $self->registerPFNInCatalogue($user,$packedEnvelope,$seOrLFNOrEnv);

  $mirrorReq and $packedEnvelope = $self->getBaseEnvelopeForMirrorAccess($user,$lfn,$guidRequest,$size,$md5);

  ($writeReq or $mirrorReq )
       and ($packedEnvelope, $seList) = $self->getSEsAndCheckQuotaForWriteOrMirrorAccess($user,$packedEnvelope,$seList,$sitename,$writeQos,$writeQosCount,$excludedAndfailedSEs);
    

  $readReq and ($packedEnvelope, $seList)=$self->getBaseEnvelopeForReadAccess($user, $lfn, $seList, $excludedAndfailedSEs, $sitename);

  foreach ( keys %{$packedEnvelope}) { $self->info("gron: packedEnvelope after init : $_: $packedEnvelope->{$_}"); }
   

  ($seList && (scalar(@$seList) gt 0)) || return $self->authorize_return(0,"access: After checkups there's no SE left to make an envelope for.");


  # $packedEnvelope = $self->initializeEnvelope($access,$lfn,$packedEnvelope);
  # $packedEnvelope = $self->initializeEnvelope($access,$packedEnvelope);
  (scalar(@$seList) lt 0) and return $self->authorize_return(0,"ACCESS ERROR: There are no SE's after checkups to create an envelope for '$$packedEnvelope->{lfn}/$packedEnvelope->{guid}'");

  while (scalar(@$seList) gt 0) {
 
       $packedEnvelope->{se} = shift(@$seList);
   
       $self->info("gron: Starting the loop...");
       foreach ( keys %{$packedEnvelope}) { $self->info("gron: packedEnvelope before (write) fill up: $_: $packedEnvelope->{$_}"); }
   
       if ($writeReq or $mirrorReq) {
         $packedEnvelope = $self->calculateXrootdTURLForWriteEnvelope($packedEnvelope);
         $self->addEntryToBookingTableAndOptionalExistingFlagTrigger($user,$packedEnvelope,$mirrorReq)
         and $self->info("gron: LFN BOOK ADD OK");
         # or next;
         
         $self->info("gron: LFN_BOOKED DONE");
   
       }
   
  #     $packedEnvelope->{lfn} = $packedEnvelope->{pfn};
       my $encryptedEnvelope = $self->createAndEncryptEnvelopeTicket($access, $packedEnvelope); 
   
  #     $packedEnvelope->{access} = $access;
       $packedEnvelope = $self->signEnvelope($packedEnvelope);
   
       $packedEnvelope->{envelope} = $encryptedEnvelope;
   
       # patch for dCache
   #    ( ($se =~ /dcache/i) or ($se =~ /alice::((RAL)|(CNAF))::castor/i)) 
   #       and  $packedEnvelope->{turl}="root://$pfix/".($packedEnvelope->{lfn} || "/NOLFN");
   
       foreach ( keys %{$packedEnvelope}) { $self->info("gron: final packedEnvelope, $_: $packedEnvelope->{$_}"); }
   
       $self->info("gron: finally se is: $packedEnvelope->{se}");  
         
       push @packedEnvelopeList, $packedEnvelope;
   
       if ($self->{MONITOR}) {
   	#my @params= ("$se", $packedEnvelope->{size});
   	my $method;
   	($access =~ /^((read)|(write))/)  and $method="${1}req";
   	$access =~ /^delete/ and $method="delete";
   	$method and
   	$self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","$self->{ROLE}_$method", ("$packedEnvelope->{se}", $packedEnvelope->{size}) ); 		      
       }
  } 
  return $self->authorize_return(1,$exceptions,\@packedEnvelopeList);
}




sub  initializeEnvelope{
  my $self=shift;
  my $access=(shift || return {});
  my $lfn=(shift || return "");
  my $preEnvelope=(shift || return {});
  my @tags = ("guid","size","md5","pfn","turl","se");

  my $packedEnvelope = {};
  foreach (@tags) { (defined $preEnvelope->{$_}) and ($preEnvelope->{$_} ne "") and $packedEnvelope->{$_} = $preEnvelope->{$_} };
  $preEnvelope->{md5} or $preEnvelope->{md5} = "00000000000000000000000000000000";
  $packedEnvelope->{access}=$access;
  ($preEnvelope->{lfn} ne "") and $packedEnvelope->{lfn}=$lfn;
  return $packedEnvelope;
}


sub createAndEncryptEnvelopeTicket {
  my $self=shift;
  my $access=(shift || return);
  my $env=(shift || return);

    my $ticket = "<authz>\n  <file>\n";
    $ticket .= "    <access>$access</access>\n";
    foreach ( keys %{$env}) { ($_ ne "access" && defined $env->{$_}) and $ticket .= "    <${_}>$env->{$_}</${_}>\n"; }
    $ticket .= "  </file>\n</authz>\n";
    $self->info("The ticket is $ticket");

    $self->{envelopeCipherEngine}->Reset();
    #    $self->{envelopeCipherEngine}->Verbose();
    $self->{envelopeCipherEngine}->encodeEnvelopePerl("$ticket","0","none")
       or return $self->authorize_return(0,"access: error during envelope encryption");

    return $self->{envelopeCipherEngine}->GetEncodedEnvelope();
}
sub decryptEnvelopeTicket {
  my $self=shift;
  my $ticket=(shift || return {});

  $self->{envelopeCipherEngine}->Reset();
#    $self->{envelopeCipherEngine}->Verbose();
  $self->{envelopeCipherEngine}->IsInitialized();
  print STDERR "Decoding Envelope: \n $ticket\n";

  my $decoded = $self->{envelopeCipherEngine}->decodeEnvelopePerl($ticket);
  $decoded or $self->info("error during envelope decryption") and return {};

  $decoded = $self->{envelopeCipherEngine}->GetDecodedEnvelope();
  my $xsimple = XML::Simple->new();
  my $filehash = $xsimple->XMLin($decoded,
                                        KeyAttr => {lfn=> 'name'},
                                        ForceArray => [ 'file' ],
                                        ContentKey => '-content');
  return @{$filehash->{file}}[0];
} 



sub signEnvelope {
  my $self=shift;
  my $env=(shift || return);

  my @keyVals = keys %{$env};
  $env->{hashOrder} = join ("&",@keyVals);
  my $envelopeString= join("&", map { $_ = "$_=$env->{$_}"} @keyVals);

  $env->{signature} = encode_base64($self->{signEngine}->sign($envelopeString));
  $env->{signature} =~  s/\n//g;

  $env->{signedEnvelope}= $envelopeString."&signature=$env->{signature}";
  
  return $env;
}

sub verifyAndDeserializeEnvelope{
  my $self=shift;
  my $env=(shift || return {});

  my $signature=0;
  my $envelopeString="";
  my $envelope = {};

  foreach ( split(/&/, $env)) {
     my ($key, $val) = split(/=/,$_);
     ($key =~ /signature/) and $signature = decode_base64($val) and next;
     $envelope->{$key} = $val; 
     $envelopeString .= $_."&";
  }
  $envelopeString =~ s/&$//;

  $self->{verifyEngine}->verify($envelopeString, $signature)
    and return $envelope;
  return 0;
} 



sub checkExclWriteUserOnSEsForAccess{
   my $self=shift;
   my $user=(shift || return 0);
   my $fileSize=(shift || 0);
   my $seList=(shift || return 0);
   (scalar(@$seList) gt 0) or return [];

   my $catalogue = $self->{DATABASE}->{LFN_DB}->{FIRST_DB};
   my @queryValues = ();
   my $query="SELECT seName FROM SE WHERE (";
   foreach(@$seList){   $query .= " seName LIKE ? or";   push @queryValues, $_; }
   $query =~ s/or$/);/;
   my $seList2 = $catalogue->queryColumn($query, undef, {bind_values=>\@queryValues});
   if(scalar(@$seList) ne scalar(@$seList2)){
      my @dropList = ();
      foreach my $se (@$seList) { 
         my $in = 0; 
         foreach (@$seList2) { ($se eq $1) and $in =1; }
         $in or push @dropList, $se;
      } 
      @$seList = @$seList2;
      $self->info("Attention: The following SE names were dropped since the they are not existing in the system: @dropList");
   }
   (scalar(@$seList) gt 0) or return $seList;

   @queryValues = ();
   $query="SELECT seName FROM SE WHERE (";
   foreach(@$seList){   $query .= " seName LIKE ? or";   push @queryValues, $_; }
   $query =~ s/or$//;
   $query  .= ") and seMinSize <= ? ;";
   push @queryValues, $fileSize;
   $seList2 = $catalogue->queryColumn($query, undef, {bind_values=>\@queryValues});
   if(scalar(@$seList) ne scalar(@$seList2)){
      my @dropList = ();
      foreach my $se (@$seList) { 
         my $in = 0; 
         foreach (@$seList2) { ($se eq $1) and $in =1; }
         $in or push @dropList, $se;
      } 
      @$seList = @$seList2;
      $self->info("Attention: The following SEs were dropped since the file's size is too small concerning the SEs min file size specification: @dropList");
   }
   (scalar(@$seList) gt 0) or return $seList;


   @queryValues = ();
   $query="SELECT seName FROM SE WHERE (";
   foreach(@$seList){   $query .= " seName LIKE ? or";   push @queryValues, $_; }
   $query =~ s/or$//;
   $query  .= ") and ( seExclusiveWrite is NULL or seExclusiveWrite = '' or seExclusiveWrite  LIKE concat ('%,' , ? , ',%') );";
   push @queryValues, $user;
   $seList2 = $catalogue->queryColumn($query, undef, {bind_values=>\@queryValues});
   if(scalar(@$seList) ne scalar(@$seList2)){
      my @dropList = ();
      foreach my $se (@$seList) { 
         my $in = 0; 
         foreach (@$seList2) { ($se eq $1) and $in =1; }
         $in or push @dropList, $se;
      } 
      $self->info("Attention: The following SEs were dropped since you are excluded from write access due to exclusiveWrite: @dropList");
   }

   return $seList2;
}


sub validateArrayOfSEs {
  my $self=shift;
  my @ses = ();
  foreach (@_) { AliEn::Util::isValidSEName($_) && push @ses, $_; }
  return \@ses;
}





sub getSEListFromSiteSECacheForWriteAccess{
   my $self=shift;
   my $user=(shift || return 0);
   my $fileSize=(shift || 0);
   my $type=(shift || return 0);
   my $count=(shift || return 0);
   my $sitename=(shift || return 0);
   my $excludeList=(shift || "");

   my $catalogue = $self->{DATABASE}->{LFN_DB}->{FIRST_DB};

   $self->checkSiteSECacheForAccess($sitename) or return 0;

   my $query="SELECT DISTINCT SE.seName FROM SERanks,SE WHERE "
       ." sitename LIKE ? and SERanks.seNumber = SE.seNumber ";

   my @queryValues = ();
   push @queryValues, $sitename;

   foreach(@$excludeList){   $query .= "and SE.seName NOT LIKE ? "; push @queryValues, $_;  }
   
   $query .=" and SE.seMinSize <= ? and SE.seQoS  LIKE concat('%,' , ? , ',%' ) "
    ." and (SE.seExclusiveWrite is NULL or SE.seExclusiveWrite = '' or SE.seExclusiveWrite  LIKE concat ('%,' , ? , ',%') )"
    ." ORDER BY rank ASC limit ? ;";
 
   push @queryValues, $fileSize;
   push @queryValues, $type;
   push @queryValues, $user;
   push @queryValues, $count;

   return $catalogue->queryColumn($query, undef, {bind_values=>\@queryValues});

}


sub checkSiteSECacheForAccess{
   my $self=shift;
   my $site=shift;
   my $catalogue = $self->{DATABASE}->{LFN_DB}->{FIRST_DB};

   my $reply = $catalogue->query("SELECT sitename FROM SERanks WHERE sitename LIKE ?;", undef, {bind_values=>[$site]});

   (scalar(@$reply) < 1) and $self->info("We need to update the SERank Cache for the not listed site: $site")
            and return $self->execute("refreshSERankCache", $site);

   return 1;
}






return 1;

