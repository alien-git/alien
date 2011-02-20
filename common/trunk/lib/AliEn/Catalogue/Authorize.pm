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
use Data::Dumper;
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
use MIME::Base64; 
use vars qw($DEBUG @ISA);
require Crypt::OpenSSL::RSA;
require Crypt::OpenSSL::X509;
$DEBUG = 0;





sub initEnvelopeEngine {
  my $self=shift; 

  $self->{envelopeCipherEngine} =0;
  $self->{noshuffle} = 0;
  defined $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}  or return 0;
  defined $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} or return 0;
  defined $ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'}  or return 0;
  defined $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'} or return 0;

  $self->info("Authorize: Checking if we can create envelopes...");
  $self->info("Authorize: local private key          : $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}");
  $self->info("Authorize: local public  key          : $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'}");
  $self->info("Authorize: remote private key          : $ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'}");
  $self->info("Authorize: remote public  key          : $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}");

  open(PRIV, $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}); my @prkey = <PRIV>; close PRIV;
  my $privateLocalKey = join("",@prkey);
  my $publicLocalKey = Crypt::OpenSSL::X509->new_from_file( $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} )->pubkey();
  my $publicRemoteKey = Crypt::OpenSSL::X509->new_from_file( $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'} )->pubkey();


  $self->{signEngine} = Crypt::OpenSSL::RSA->new_private_key($privateLocalKey);
  $self->{signEngine}->use_sha384_hash();


  $self->{verifyLocalEngine} = Crypt::OpenSSL::RSA->new_public_key($publicLocalKey);
  $self->{verifyLocalEngine}->use_sha384_hash();
  $self->{verifyRemoteEngine} = Crypt::OpenSSL::RSA->new_public_key($publicRemoteKey);
  $self->{verifyRemoteEngine}->use_sha384_hash();




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
    $self->info("Authorize: Warning! the initialization of the envelope engine failed!!",1);
    $self->{envelopeCipherEngine} = 0;
    return 0;
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
   foreach (@$list) {
     (lc($prio) eq lc($_)) and $exists=1 and $prio=$_
      or push @newlist, $_;
   }
   $exists and  @newlist = ($prio,@newlist);
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
   
      $query="SELECT DISTINCT b.seName FROM SERanks a right JOIN SE b on (a.seNumber=b.seNumber and a.sitename=?) WHERE ";
      $query .= " (b.$exclusiveUserCheck is NULL or b.$exclusiveUserCheck = '' or b.$exclusiveUserCheck  LIKE concat ('%,' , ? , ',%') ) ";
      push @queryValues, $user;
      if(scalar(@{$seList}) > 0)  { $query .= " and ( "; foreach (@{$seList}){ $query .= " b.seName=? or"; push @queryValues, $_;  } 
           $query =~ s/or$/)/;}
      foreach (@{$excludeList}) {   $query .= " and b.seName<>? ";   push @queryValues, $_; };
      $query .= " ORDER BY if(a.rank is null, 1000, a.rank) ASC ;";
   } else { # sitename not given, so we just delete the excluded SEs and check for exclusive Users
       $query="SELECT seName FROM SE WHERE ";
       foreach(@$seList){   $query .= " seName=? or"; push @queryValues, $_;  };
       $query =~ s/or$//;
       foreach(@$excludeList){   $query .= " and seName<>? "; push @queryValues, $_;  }
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
  $type =~ /^(custodial)|(replica)$/ or $self->info("Authorize: Error: type of SE '$type' not understood",1) and return 0;
  $self->info("Authorize: Looking for the closest $type SE");
  
  if ($self->{CONFIG}->{SE_RETENTION_POLICY} and 
      $self->{CONFIG}->{SE_RETENTION_POLICY} =~ /$type/){
    $self->info("Authorize: We are lucky. The closest is $type");
    return $self->{SE_FULLNAME};
  }
  
  my $se=$self->{SOAP}->CallSOAP("IS", "getCloseSE", $self->{SITE}, $type, $excludeListRef);
  $self->{SOAP}->checkSOAPreturn($se) or return ;
  my $seName=$se->result;
  $self->info("Authorize: We are going to put the file in $seName");
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
    $self->info("Authorize: There were no transfer methods....");
    @where=$self->f_whereis("sgzr","$guid");
  }

  my @whereis=();
  foreach (@where) {
    push @whereis, $_->{se};
  }
  my $error="There was no SE for the guid '$guid'";
  $self->{LOGGER}->error_msg() and $error=$self->{LOGGER}->error_msg();
  #Get the file from the LCM
  @whereis or $self->info("Authorize: $error",1 ) and return 0;

  my $closeList = $self->OLDselectClosestRealSEOnRank($sitename, $user, $readOrDelete, \@whereis, $se, $excludedAndfailedSEs);

  (scalar(@$closeList) eq 0) 
           and $self->info("Authorize: ERROR within getPFNforReadOrDeleteAccess: SE list was empty after checkup. Either problem with the file's info or you don't have access on relevant SEs.",1) 
           and return 0;

  # if excludedAndfailedSEs is an int, we have the old <= AliEn v2-17 version of the envelope request, to select the n-th element
  $se = @{$closeList}[$sesel];

  my $origpfn;
  foreach (@where) { ($_->{se} eq $se) and $origpfn = $_->{pfn} }

  $self->debug(1, "We can ask the following SE: $se");
  (!($options =~/s/)) and $self->info("Authorize: The guid is $guid");

  my $nonRoot;
  my $se2 = lc $se;
  foreach (@where) {
    (!($options =~/s/)) and $self->info("Authorize: comparing $_->{se} to $se");
    my $se1 = lc $_->{se};
    ($se1 eq $se2) or next;
    $nonRoot=$_->{pfn};

    if (( $_->{pfn} =~ /^root/ ) || ( $_->{pfn} =~ /^guid/) ) {
      $pfn = $_->{pfn};
    }
  }
	
  if (!$pfn && $nonRoot) {
    $self->info("Authorize: this is not a root pfn: $nonRoot ");
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
    $self->info("Authorize: ERROR within getPFNforReadOrDeleteAccess: parsing error for $pfn [host+port]",1);
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
    $self->info("Authorize: The anchor is $1");
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
  ($access =~ /^write[\-a-z]*/) && ($filehash = $self->checkPermissions($perm,$lfn, 0, 1));
  if (!$filehash) {
    $self->info("Authorize: access: access denied to $lfn",1);
    return 0;
  }

  if  ($access eq "read")  {
    if (!$self->isFile($lfn, $filehash->{lfn})) {
      $self->info("Authorize: access: access find entry for $lfn",1);
      return ;
    }
  }elsif ($access eq "delete") {
    if (! $self->existsEntry($lfn, $filehash->{lfn})) {
      $self->info("Authorize: access: delete of non existant file requested: $lfn",1);
      return ;
    }
  } else {
    my $parentdir = $self->f_dirname($lfn);
    my $result = $self->checkPermissions($perm,$parentdir);
    if (!$result) {
      $self->info("Authorize: access: parent dir missing for lfn $lfn",1);
      return ;
    }
    if (($access =~ /^write[\-a-z]*/) && ($lfn ne "")
      && $self->existsEntry($lfn, $filehash->{lfn})) {
	$self->info("Authorize: lfn <$lfn> exists - creating backup version ....\n");
	my $filename = $self->f_basename($lfn);
	
	$self->f_mkdir("ps","$parentdir"."/."."$filename/") or
	  $self->info("Authorize: access: cannot create subversion directory - sorry",1) and return 0;
	
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
	  $self->info("Authorize: access: cannot parse the last version number of $lfn",1);
	  return ;
	}
	my $pversion = sprintf "%.1f", (10.0+($version))/10.0;
	my $backupfile = "$parentdir"."/."."$filename/v$pversion";
	$self->info( "access: backup file is $backupfile \n");
	if (!$self->f_mv("",$lfn, $backupfile)) {
	  $self->info("Authorize: access: cannot move $lfn to the backup file $backupfile",1);
	  return ;
	}
      #}
    }
  }
  return $filehash;
}

sub access {
   my $self=shift;
   #
   # Start of the Server/Authen side code
   $self->info("Authorize: STARTING envelope creation: @_ ");
   my $options = shift;
   my $access = (shift || 0);
   my $lfn    = (shift || 0);
   my $se      = (shift || "");
   my $size    = (shift || 0 );
   my $sesel   = (shift || 0);
   my @accessOptions = @_;
   my $extguid = (shift || 0);
 
   my @ses = ();
   my @tempSE= split(/;/, $se);
   foreach (@tempSE) { AliEn::Util::isValidSEName($_) and push @ses, $_; }
 
   my @exxSEs = ();
   @tempSE= split(/;/, $sesel);
   foreach (@tempSE) { AliEn::Util::isValidSEName($_) and push @exxSEs, $_; }
   ($sesel =~ /^[0-9]+$/) or $sesel = 0;
   my $sitename= (shift || 0);
   (($sitename eq 0) or ($sitename eq "")) and $sitename=$self->{CONFIG}->{SITE};
   my $writeQos = (shift || 0);
   ($writeQos eq "") and $writeQos=0;
   my $writeQosCount = (shift || 0);

   my @envelopes = ();
   my $nSEs = 0; 

  if($access eq "read" ) {
 
    if(scalar(@ses) eq 0) {
      my $guidorNot = "";
      if(AliEn::Util::isValidGUID($lfn)) { $guidorNot = "g"; }
     
      my @where=$self->f_whereis("s".$guidorNot."ztr","$lfn");
     
      if (! @where){
         $self->info("Authorize: There were no transfer methods....");
         @where=$self->f_whereis("s".$guidorNot."zr","$lfn");
      } 
      my @whereSEs = map { $_->{se} } @where;

      my @queryValues = ();
      my $query = "";
      $self->checkSiteSECacheForAccess($sitename) || return 0;
      push @queryValues, $sitename;

      $query="SELECT DISTINCT b.seName FROM SERanks a right JOIN SE b on (a.seNumber=b.seNumber and a.sitename=?) WHERE ";
      $query .= " (b.seExclusiveRead is NULL or b.seExclusiveRead = '' or b.seExclusiveRead  LIKE concat ('%,' , ? , ',%') ) and ";
      push @queryValues, ($self->{ROLE} || $self->{CONFIG}->{ROLE});
      foreach (@whereSEs){ $query .= " b.seName=? or"; push @queryValues, $_;  }
      $query =~ s/or$//;
      $query .= " ORDER BY if(a.rank is null, 1000, a.rank) ASC ;";

      my $sorted =  $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryColumn($query, undef, {bind_values=>\@queryValues});

      $sorted and defined(@$sorted) and $nSEs = scalar(@$sorted);
      ($sesel > $nSEs-1) and $sesel = $nSEs -1;
      @ses = ($$sorted[$sesel]);
     } 
  
     @envelopes = AliEn::Util::deserializeSignedEnvelopes($self->authorize("read",{lfn=>$lfn,
          wishedSE=>join(";",@ses),excludeSE=>join(";",@exxSEs) ,site=>$sitename}));
    
    
 
  } elsif ($access =~ /^write/ ) {
 
    if ( (scalar(@ses) eq 0) and ($sitename ne 0) and ( ($writeQos eq 0) or ($writeQosCount eq 0) ) and $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT} ) {
      my ($repltag, $copies)=split (/\=/, $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT},2);
      $writeQos = $repltag;
      $writeQosCount = $copies;
    }
    ($size > 0) or $size = 1024*1024;


    if(scalar(@ses) gt 0 ) {

      @envelopes = AliEn::Util::deserializeSignedEnvelopes($self->authorize("write", {lfn=>$lfn, 
     wishedSE=>join(";", @ses), size=>$size, md5=>0, guidRequest=>$extguid}));

    } else {

      @envelopes= AliEn::Util::deserializeSignedEnvelopes($self->authorize("write", {lfn=>$lfn, 
        size=> $size, md5=>0,  guidRequest=>$extguid, site=>$sitename, 
           writeQos=>$writeQos, writeQosCount=>$writeQosCount, excludeSE=>join(";", @exxSEs)}));
    }

    $nSEs = scalar(@envelopes);
 
  }
    #  return $self->{CATALOG}->authorize("register", {lfn=>$targetLFN, 
    #           pfn=>$sourcePFN, size=>$size, md5=>$md5sum, guid=>$guid });
    
    #   @successEnvelopes = $self->{CATALOG}->authorize("registerenvs", @{$result->{usedEnvelopes}});
   
  my @returnEnvelopes = ();

  foreach my $env (@envelopes) { 
    my $renv = {};
    my $xtrdcpenvelope = 0;
    foreach my $key (keys(%$env)) { 
      ($key eq "turl") and  $renv->{url}=$env->{turl} and next;
      ($key eq "oldEnvelope") and  $xtrdcpenvelope=$env->{oldEnvelope} and next;
      $xtrdcpenvelope ||  (($key eq "signedEnvelope") and  $xtrdcpenvelope=$env->{signedEnvelope} and next);
      $renv->{$key}=$env->{$key};
    }
    $renv->{envelope} = $xtrdcpenvelope;
    my @pfn = split(/\/\//, $renv->{url},3);
    $renv->{pfn} = "/".$pfn[2];
    $renv->{nSEs} = $nSEs;
    push @returnEnvelopes, $renv;  
  }

  return @returnEnvelopes;

}






#################################################################
# Create envelope, only for backward compability on < v2.19
# replaced by authorize/consultAuthenService below
################################################################
sub OLDaccess {
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
#    $self->info("Authorize: Connecting to Authen...");
#    my $info=0;
#    for (my $tries = 0; $tries < 5; $tries++) { # try five times 
#      $info=$self->{SOAP}->CallSOAP("Authen", "createEnvelope", $user, @_) and last;
#      sleep(5);
#    }
#    $info or $self->info("Authorize: Connecting to the [Authen] service failed!") 
#       and return ({error=>"Connecting to the [Authen] service failed!"}); 
#    my @newhash=$self->{SOAP}->GetOutput($info);
#    if (!$newhash[0]->{envelope}){
#      my $error=$newhash[0]->{error} || "";
#      $self->info($self->{LOGGER}->error_msg());
#      $self->info("Authorize: Access [envelope] creation failed: $error", 1);
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
  $self->info("Authorize: STARTING envelope creation: @_ ");
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
       $self->info("Authorize: We gonna throw an access exception: "."[quotaexception]") and return  access_eof($message,"[quotaexception]");
    }elsif($ok eq 0) {
       return  access_eof($message);
    }

    (scalar(@ses) eq 0) or $seList = $self->checkExclWriteUserOnSEsForAccess($user,$size,$seList) and @ses = @$seList;
    # following is a patch for making the SE discovery on the API services possible. If the static user specs are not leading to a result, we enable SEdiscovery (if not enabled), 
    # but onlu if it's not a write-version request on an existing file
    if (($access ne "write-version") and (scalar(@{$seList}) eq 0) and (($writeQos eq 0) or($writeQosCount eq 0))) {
       ($sitename ne 0) or $sitename="CERN";
       ($writeQos ne 0) or $writeQos = "disk";
       ($writeQosCount gt 0) or $writeQosCount = 1;
    }

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
    $self->info("Authorize: access: illegal access type <$access> requested");
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
      $self->info("Authorize: Getting the permissions from the guid");
      $guid = $lfn;
      $self->debug(1, "We have to translate the guid $1");
      $lfn = "";
      $filehash=$self->{DATABASE}->{GUID_DB}->checkPermission($perm, $guid, "size,md5");
      $filehash 
	or $self->info("Authorize: access: access denied to guid '$guid'")
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
        AliEn::Util::isValidSEName($se) or $self->info("Authorize: access: no SE asked to write on") and 
		return access_eof("List of SE is empty after checkups, no SE to create write envelope on."); 
	($seurl,my $guid2,my $se2) = $self->createFileUrl($se, "root", $guid);
	$guid2 and $guid=$guid2;
	if (!$se2){
	  $self->info("Authorize: Ok, let's create a default pfn (for $guid)");
	  ($seurl, $guid)=$self->createDefaultUrl($se, $guid,$size);
	  $seurl or return access_eof("Not an xrootd se, and there is no place in $se for $size");
	  $self->info("Authorize: Now, $seurl and $guid");
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
	    or $self->info( "access: Error getting the guid of $lfn",11) and return 0;
	}
        $filehash->{filetype}=$self->f_type($lfn);
        $self->info("Authorize: Calling getPFNforReadOrDeleteAccess with sitename: $sitename, $user, $access.");
	($se, $pfn, $anchor, $lfn, $nses, $whereis)=$self->OLDgetPFNforReadOrDeleteAccess($user, $access, $guid, $se, $excludedAndfailedSEs, $lfn, $sitename, $options);
        $self->info("Authorize: Back from getPFNforReadOrDeleteAccess.");

        $se or return access_eof("Not possible to get file info for file $lfn [getPFNforReadOrDeleteAccess error]. File info is not correct or you don't have access on certain SEs.");
	if (UNIVERSAL::isa($se, "HASH")){
	  $self->info("Authorize: Here we have to return eof");
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
      $self->info("Authorize: The ticket is $ticket");
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
        $self->info("Authorize: HELLO $t");
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
	$self->info("Authorize: access: error during envelope encryption");
	return access_eof("access: error during envelope encryption");
      } else {
	(!($options=~ /s/)) and $self->info("Authorize: access: prepared your access envelope");
      }
      
      ($options=~ /v/) or
	print STDERR "========================================================================
$ticket
========================================================================
",$$newresult[0]->{envelope},"
========================================================================\n", $ticket,"\n";
      #last;
      ($access =~ /^write[\-a-z]*/) and (scalar(@ses) gt 0)
	and ($self->info("Authorize: gonna recall next iteration") ) 
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













sub getValFromEnvelope {
  my $env=(shift || return 0);
  my $rKey=(shift || return 0);

  foreach ( split(/&/, $env)) {
     my ($key, $val) = split(/=/,$_);
     ($rKey eq $key) and return $val;
  }
  return 0;
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

   my @where=$self->f_whereis("sgzt","$guid");
   @where 
     or $self->debug(1,"There were no transfer methods....")
     and @where=$self->f_whereis("sgz","$guid");

   foreach my $tSE (@where) {
     #AliEn::Util::isValidSEName($tSE->{se}) || next;
     (grep (/^$tSE->{se}$/i,@$excludeList)) && next;
     #if($tSE->{pfn} =~ /^root/) { 
     $seList->{lc($tSE->{se})} = $tSE->{pfn}; 
    # } elsif ($tSE->{pfn} =~ /^guid/) {
       # $nose=$tSE->{pfn};
     #} else {
     #   $nose=$tSE->{pfn};
       #$nonRoot->{se} = $tSE->{se};
       #$nonRoot->{pfn} = $tSE->{pfn};
     #}
   } 

   if(scalar(keys %{$seList}) eq 0) {
     # we don't have any root SE to get it from
    # $nose and return ("no_se",$nose);      
    $self->info("There are no more SE holding that file",1);
    # $nonRoot->{pfn} and return ($nonRoot->{se},$nonRoot->{pfn});
     return 0;
   }

   my $catalogue = $self->{DATABASE}->{LFN_DB}->{FIRST_DB};
   my @queryValues = ();
   my $query = "";
   if($sitename) {
      $self->checkSiteSECacheForAccess($sitename) || return 0;
      push @queryValues, $sitename;

      $query="SELECT DISTINCT b.seName FROM SERanks a right JOIN SE b on (a.seNumber=b.seNumber and a.sitename=?) WHERE ";
      $query .= " (b.seExclusiveRead is NULL or b.seExclusiveRead = '' or b.seExclusiveRead  LIKE concat ('%,' , ? , ',%') ) and ";
      push @queryValues, $user;
      foreach (keys %{$seList}){ $query .= " b.seName=? or"; push @queryValues, $_;  } 
      $query =~ s/or$//;
      $query .= " ORDER BY if(a.rank is null, 1000, a.rank) ASC ;";
   } else { # sitename not given, so we just delete the excluded SEs and check for exclusive Users
       $query="SELECT seName FROM SE WHERE ";
       foreach(keys %{$seList}){   $query .= " seName=? or"; push @queryValues, $_;  }
       $query =~ s/or$//;
       $query .= " and (seExclusiveRead is NULL or seExclusiveRead = '' or seExclusiveRead  LIKE concat ('%,' , ? , ',%') ) ;";
       push @queryValues, $user;
   }
   my $sePriority = $self->resortArrayToPrioElementIfExists($sePrio,$catalogue->queryColumn($query, undef, {bind_values=>\@queryValues}));
   return ($$sePriority[0], $seList->{lc($$sePriority[0])});
}



sub getBaseEnvelopeForReadAccess {
  my $self=shift;
  my $user=(shift || return 0);
  my $lfn=(shift || return 0);
  my $seList=shift;
  my $excludedAndfailedSEs=shift;
  my $sitename=(shift || 0);



  my $filehash = {};
  if(AliEn::Util::isValidGUID($lfn)) {
    $filehash=$self->{DATABASE}->{GUID_DB}->checkPermission("r", $lfn, "guid,type,size,md5")
      or $self->info("Authorize: access denied for $lfn",1) and return 0;
    $filehash->{guid} = $lfn;
    $filehash->{lfn} = $lfn;
  } else {
    $filehash=$self->checkPermissions("r",$lfn,0, 1)
     or $self->info("Authorize: access denied for $lfn",1) and return 0;
     
    if ($filehash->{type} ne "f") {
      my $error="entry doesn't exist";
      $filehash->{type} eq "c" and $error="it is a collection"; 
      $self->info("Authorize: access: $lfn is not a file ($error), so read not possible",1);
      return 0;
    }
  }

  ($filehash->{size} eq "0") and $filehash->{size} = 1024*1024*1024 ;

  my $prepareEnvelope = $self->reduceFileHashAndInitializeEnvelope("read",$filehash);
  
  ($prepareEnvelope->{se}, $prepareEnvelope->{pfn})
    = $self->selectPFNOnClosestRootSEOnRank($sitename, $user, $filehash->{guid}, ($$seList[0] || 0), $excludedAndfailedSEs);
  $prepareEnvelope->{se} 
       or $self->info("Authorize: access ERROR within selectPFNOnClosestRootSEOnRank: SE list was empty after checkup. Either no more replicas, a problem with the file's info,\nor you don't have access on relevant SEs.",1)
       and return 0;

 
  if ($prepareEnvelope->{se} eq "no_se") {
     if(($prepareEnvelope->{pfn} =~ s/^guid:\/\/\///i) and ($prepareEnvelope->{pfn} =~ s/\?ZIP=(.*)$//)) {
       my $archiveFile = $1;
       $self->info("Authorize: Getting file out of archive with GUID, $filehash->{guid}...");
       my $prepareArchiveEnvelope = $self->getBaseEnvelopeForReadAccess($user, $prepareEnvelope->{pfn}, [], $excludedAndfailedSEs, $sitename) or return 0;
       $prepareEnvelope->{turl} = $prepareArchiveEnvelope->{turl}."#".$archiveFile;
       $prepareEnvelope->{pfn} = $prepareArchiveEnvelope->{pfn};
       $prepareEnvelope->{se} = $prepareArchiveEnvelope->{se};
       $prepareEnvelope->{zguid} = $prepareArchiveEnvelope->{guid};
       return $prepareEnvelope;
     }
     $prepareEnvelope->{turl} = $prepareEnvelope->{pfn};
  } else {
    ($prepareEnvelope->{turl},$prepareEnvelope->{pfn}) = $self->parseAndCheckStorageElementPFN2TURL($prepareEnvelope->{se}, $prepareEnvelope->{pfn});
  }
  return $prepareEnvelope;
}


sub parseAndCheckStorageElementPFN2TURL {
  my $self=shift;
  my $se=(shift || return);
  my $pfn=(shift || return);
  my $turl="";
  my $urloptions="";

  my $parsedPFN = $self->parsePFN($pfn);
  $parsedPFN or return ($pfn,$pfn);

  my @queryValues = ("$se");
  my $seiostring = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow("SELECT seioDaemons FROM SE where seName = ? ;", 
              undef, {bind_values=>\@queryValues});
  ($seiostring->{seioDaemons} =~ /[a-zA-Z]*:\/\/[0-9a-zA-Z.\-_:]*/) and $turl = $seiostring->{seioDaemons} or return ($pfn,$pfn);

  $turl= "$turl/$parsedPFN->{path}";
  $parsedPFN->{vars} and $turl= $turl."?$parsedPFN->{vars}";
  return ($turl,$parsedPFN->{path});
}

sub getSEforPFN{
  my $self=shift;
  my $pfn=(shift || return);

  #$pfn =~ /^((guid)|(soap)):/ and return "no_se";
  $pfn = $self->parsePFN($pfn);
  $pfn or $self->info("Error parsing the pfn '$pfn'") and return 0;
  my @queryValues = ("$pfn->{proto}://$pfn->{host}","$pfn->{path}");
  my $sestring = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryValue("SELECT seName FROM SE WHERE seioDaemons LIKE concat ( ? , '%') AND ? LIKE CONCAT(seStoragePath, '%')  ORDER BY length(seStoragePath) DESC LIMIT 1 ;",
              undef, {bind_values=>\@queryValues});
  $sestring or return "no_se";
  return $sestring;
}


sub parsePFN {
  my $self=shift;
  my $pfn=(shift|| return {});
  my $result={};
  ($pfn=~/^\//) and $pfn="file://".$self->{CONFIG}->{HOST}.$pfn;
  $pfn =~ /^([a-zA-Z]*):\/\/([0-9a-zA-Z.\-_:]*)\/(.*)$/;
  $1 or $2 or return 0;
  $result->{proto}  = $1;
  $result->{host}   = $2;
  ($result->{path},$result->{vars}) = split (/\?/,$3);
  $result->{path} =~ s/^(\/*)/\//;
  return $result;
}


sub getBaseEnvelopeForDeleteAccess { 
 my $self=shift;
  my $user=(shift || return 0);
  ($user eq "admin") or return 0;
  my $lfnORGUIDORpfn=(shift || return 0);

  my $query = "SELECT lfn,binary2string(guid) as guid, pfn as turl, se, size, md5sum as md5 FROM LFN_BOOKED WHERE ";

  if(AliEn::Util::isValidGUID($lfnORGUIDORpfn)) {
     $query .= " guid=string2binary(?) ;";
  } elsif (AliEn::Util::isValidPFN($lfnORGUIDORpfn)) {
     $query .= " pfn=? ;";
  } else {
     $query .= " lfn=? ;";
  }  

  my $envelope = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow($query, undef, {bind_values=>[$lfnORGUIDORpfn]});

  return ($self->reduceFileHashAndInitializeEnvelope("delete",$envelope),[$envelope->{se}]);
}


sub  getBaseEnvelopeForWriteAccess {
  my $self=shift;
  my $user=(shift || return 0);
  my $lfn=(shift || return 0);
  my $size=(shift || 0);
  my $md5=(shift || 0);
  my $guidRequest=(shift || 0); 
  my $envelope={};

  $size and ( $size gt 0 ) or $self->info("Authorize: File has zero size and will not be allowed to registered") and return 0;

  ####

  $envelope= $self->checkPermissions("w",$lfn,0, 1);
  $envelope or $self->info("Authorize: access denied to $lfn",1) and return 0;

  #Check parent dir permissions:
  my $parent = $self->f_dirname($lfn);
  $self->checkPermissions("w",$parent)
     or $self->info("Authorize: access: parent dir missing for lfn $lfn",1) and return 0;

  $envelope->{lfn} = $self->GetAbsolutePath($lfn);

  $self->existsEntry($envelope->{lfn}) and 
     $self->info("Authorize: The file is already existing in the catalogue, you need to delete it first manually.",1) and return 0;
  
    if($user ne "admin") {
      my $reply = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
          "SELECT lfn FROM LFN_BOOKED WHERE lfn=? and (owner<>? or gowner<>? ) and expiretime > 0;"
          , undef, {bind_values=>[$envelope->{lfn},$user,$user]});
      $reply->{lfn} and $self->info("Authorize: access: the LFN is already in use (reserved in [LFN_BOOKED], not in the catalogue)",1) and return 0;
    }

  $envelope->{guid} = $guidRequest; 
  if (!$envelope->{guid} or !AliEn::Util::isValidGUID($envelope->{guid})) { 
    $self->{GUID} or $self->{GUID}=AliEn::GUID->new(); 
    $envelope->{guid} = $self->{GUID}->CreateGuid();
  } else {

      my $collision = $self->{DATABASE}->getAllInfoFromGUID({retrieve=>""}, $envelope->{guid});
      $collision->{guid} and $self->info("Authorize: access: the requested GUID is already in use",1) and return 0;
      if($user ne "admin") {
         my $collision = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
           "SELECT guid FROM LFN_BOOKED WHERE guid=string2binary(?) and (lfn<>? or gowner<> ?) and expiretime > 0 ;"
           , undef, {bind_values=>[$envelope->{guid}],$envelope->{lfn},$user});
         $collision->{guid} 
         and $self->info("Authorize: access: the requested GUID is already in use (reserved in [LFN_BOOKED], not in the catalogue)",1) and return 0;
      }
  }

  $envelope->{size} = $size;
  $envelope->{md5} = $md5;

  my ($ok, $message) = $self->checkFileQuota($user, $envelope->{size});
  ($ok eq 0) and $self->info($message,1) and return 0;
  return $self->reduceFileHashAndInitializeEnvelope("write",$envelope);
}


sub prepookArchiveLinksInBookingTable{
  my $self=shift;
  my $user=(shift || return 0);
  my $jobID=(shift || 0);
  my $line=(shift || return 0);
  my $archGUID=(shift || return 0);

  $self->debug(3,"Preparing booking of archive entries: user: $user, jobid: $jobID, registrationString: $line, archive GUID: $archGUID .");

  my @links=split (/;;/, $line);
  foreach my $link (@links){
      my ($l, $s, $m, $g)=split (/###/, $link);
      my $env = $self->getBaseEnvelopeForWriteAccess($user,$l,$s,$m,$g) or $self->info("Authrorize: The requested link $l could not been booked or had permission denied") and return 0;
      $env->{turl} = "guid:///$archGUID?ZIP=".$self->f_basename($env->{lfn});
      $env->{se} = "no_se";
      $self->addEntryToBookingTableAndOptionalExistingFlagTrigger($user,$env,$jobID,0);
  }
  return 1;
}


sub calculateXrootdTURLForWriteEnvelope{
  my $self=shift;
  my $envelope=(shift || return {});

  ($envelope->{turl}, my $guid, my $se) = $self->createFileUrl($envelope->{se}, "root", $envelope->{guid});
  $se 
    or $self->info("Authorize: Creating a default pfn (for $envelope->{guid})")
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
  my $lfn=(shift || "/NOLFN");
  my $envelope={};

  AliEn::Util::isValidGUID($guid) or $self->info("Authorize: ERROR! $guid is not a valid GUID.",1) and return 0;
  $envelope=$self->{DATABASE}->{GUID_DB}->checkPermission("w", $guid, "guid,type,size,md5")
      or $self->info("Authorize: access denied for $guid",1) and return 0;
  $envelope->{guid}
      or $self->info("Authorize: ACCESS DENIED: You are not allowed to write on GUID '$guid'.",1) and return 0;

  ( defined($envelope->{size}) && ($envelope->{size} gt 0)) or $self->info("Authorize: ACCESS ERROR: You are trying to mirror a zero sized file '$guid'",1) and return 0;

  $envelope->{lfn} = $lfn;
  $envelope->{access} = "write";
  return $self->reduceFileHashAndInitializeEnvelope("write",$envelope);
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

  $envelope->{size} and ( $envelope->{size} gt 0 ) or $self->info("Authorize: ERROR: File has zero size, we don't allow that.") and return 0;
 
  # if nothing is or wrong specified SE info, get default from Config, if there is a sitename
  ( (scalar(@$seList) eq 0) and ($sitename ne 0) and ( ($writeQos eq 0) or ($writeQosCount eq 0) ) and $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT} ) 
    and ($writeQos, $writeQosCount) =split (/\=/, $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT},2);

  (scalar(@$seList) eq 0) or $seList = $self->checkExclWriteUserOnSEsForAccess($user,$envelope->{size},$seList);
  if(($sitename ne 0) and ($writeQos ne 0) and ($writeQosCount gt 0)) {
     my $dynamicSElist = $self->getSEListFromSiteSECacheForWriteAccess($user,$envelope->{size},$writeQos,$writeQosCount,$sitename,$excludedAndfailedSEs);
     push @$seList,@$dynamicSElist;
  }


  return ($envelope, $seList);
}


sub registerPFNInCatalogue{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my $pfn=(shift || return 0);
  my $se=(shift || 0);

  $envelope->{lfn} or $self->info("Authorize: The access to registering a PFN with LFN $envelope->{lfn} could not be granted.",1) and return 0;
  $envelope->{size} and ( $envelope->{size} gt 0 ) or $self->info("Authorize: File has zero size and will not be allowed to registered",1) and return 0;
#  (!($pfn =~ /^file:\/\//) and !($pfn =~ /^root:\/\//)) and $se = "no_se";
#  if($pfn =~ /^guid:\/\/\//){
#    $se = "no_se";
#    my $guid = "$pfn";
#    $guid =~ s{^guid:///([^/]+)(\?[.]*)*}{$1};
#    $self->{DATABASE}->{GUID_DB}->checkPermission("r",$envelope->{guid}) or $self->info("Authorize: Could not get read permissions on GUID $envelope->{guid} .",1) and return 0;
#  }
  $se or $se=$self->getSEforPFN($pfn);
  $se or $self->info("Authorize: File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn could not be registered. The PFN doesn't correspond to any known SE.",1) and return 0;
 
  $self->f_registerFile( "-f", $envelope->{lfn}, $envelope->{size},
    $se, $envelope->{guid}, undef ,undef, $envelope->{md5},
    $pfn) 
    or $self->info("Authorize: File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn could not be registered.",1) and return 0;
  $self->info( "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn was successfully registered.") and return 1;

}

sub validateSignedEnvAndRegisterAccordingly{
  my $self=shift;
  my $user=(shift || return 0);
  my $signedEnvelopes=(shift || []);
  my @successEnvelopes = ();

 
  foreach my $signedEnvelope (@$signedEnvelopes) {
    my $justRegistered=0;
    $signedEnvelope or next;
    my $envelope = $self->verifyAndDeserializeEnvelope($signedEnvelope);
     
    $envelope 
      or $self->info("Authorize: An envelope could not be verified.") 
        and  next; 
 
     $envelope = $self->ValidateRegistrationEnvelopesWithBookingTable($user,$envelope);
     $envelope 
       or $self->info("Authorize: An envelope could not be validated based on pretaken booking.") 
         and next; 
     $self->registerFileInCatalogueAccordingToEnvelope($user, $envelope)
       and push @successEnvelopes,$signedEnvelope;
  }
  
  (scalar(@successEnvelopes) eq scalar(@$signedEnvelopes)) and $self->notice("Authorize: EXCELLENT! All of the ".scalar(@$signedEnvelopes)." PFNs where correctly registered.") 
    and return @successEnvelopes;
  (scalar(@successEnvelopes) gt 0) and $self->notice("Authorize: WARNING! Only ".scalar(@successEnvelopes)." PFNs could be registered correctly registered.") 
    and return @successEnvelopes;
   $self->error("Authorize: ERROR! We could not register any of the requested PFNS.",1)
    and return  @successEnvelopes;
}

sub registerFileInCatalogueAccordingToEnvelope{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my $links=(shift || 0);
  my $justRegistered=0;
  my @successEnvelopes = ();

  if(!$envelope->{existing}) {
      $self->f_registerFile( "-f", $envelope->{lfn}, $envelope->{size},
               $envelope->{se}, $envelope->{guid}, undef,undef, $envelope->{md5}, 
                $envelope->{turl}, $envelope->{jobid}) and $justRegistered=1 
       or $self->info("Authorize: File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered.")
       and return 0;
  } else {
        $self->f_addMirror( $envelope->{lfn}, $envelope->{se}, $envelope->{turl}, "-c","-md5=".$envelope->{md5})
          or $self->info("Authorize: File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered as a replica.")
        and return 0;
  }
  $self->deleteEntryFromBookingTableAndOptionalExistingFlagTrigger($user, $envelope, $justRegistered) 
       or $self->info("Authorize: File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered properly as a replica (LFN_BOOKED error).")
       and return 0;
  return 1;
}


sub registerOutputForJobPFNS{
  my $self=shift;
  my $user=(shift || return 0);
  my $jobid=(shift || return 0);
  my @successEnvelopes=();
  my $outputdir=0;
  my $regok=1;

  foreach my $pfn (@_) {
     my $reply = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
      "SELECT lfn,binary2string(guid) as guid,existing,pfn as turl, se, size, md5sum as md5 FROM LFN_BOOKED WHERE jobid=? and pfn=? and owner=? and gowner=? ;"
      , undef, {bind_values=>[$jobid,$pfn,$user,$user]});
     $reply->{lfn} or $self->info("Error getting entries from LFN_BOOKED for PFN: $pfn",2) and $regok=0 and next;
     $reply->{jobid} = $jobid;
     if(!$outputdir) {
        $outputdir = $self->f_dirname($reply->{lfn});
        $self->info("Creating the output directory: $outputdir");
        $self->f_mkdir("ps", "$outputdir");
     }
     $self->registerFileInCatalogueAccordingToEnvelope($user, $reply)
         or $self->info("Error: The entry $pfn could not been registered.",2) and $regok=0 and next;
     my $links = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->query(
      "SELECT lfn,binary2string(guid) as guid,existing,pfn as turl, se, size, md5sum as md5 FROM LFN_BOOKED WHERE jobid=? and upper(pfn) LIKE concat ('GUID:///' , ? , '?ZIP=%' ) and owner=? and gowner=? ;"
      , undef, {bind_values=>[$jobid,uc($reply->{guid}),$user,$user]});
     foreach my $link (@$links) {
        $link->{lfn} or $self->info("Error getting link entries from LFN_BOOKED for PFN: $pfn",2) and $regok=0 and next;
        $link->{jobid} = $jobid;
        $self->registerFileInCatalogueAccordingToEnvelope($user, $link) 
             or $self->info("Error: The link entry $link->{lfn} for $pfn could not been registered.",2) and $regok=0;
     }
  }
  $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do("UPDATE LFN_BOOKED set expiretime=-1 where jobid=? and owner=? and gowner=? ;",{bind_values=>[$jobid,$user,$user]});
  return ($regok,($outputdir || 0));
}


sub commit{
  my $self = shift;
  my $user=$self->{CONFIG}->{ROLE};
  $self->{ROLE} and $user=$self->{ROLE};
  my $RAWenvelope     = (shift or "0");
  my $size         = (shift or "0");
  my $lfn          = (shift or 0);
  my $perm         = shift;
  my $expire       = shift;
  my $pfn          = (shift or 0);
  my $se           = (shift or 0);
  my $guid         = (shift or 0);
  my $md5          = (shift or 0);

  my $newresult = [];
  my $newhash = {};
  $newhash->{lfn}  = $lfn;
  push @$newresult,$newhash;


  if(grep(/signature=/, $RAWenvelope)) {

     my @successEnvelopes = $self->{CATALOG}->authorize("registerenvs", ("$RAWenvelope") );
     $$newresult[0]->{$lfn} = 0;
     if(scalar(@successEnvelopes == 1)) {
        $$newresult[0]->{$lfn} = 1;
     }
     return $newresult;

  } else {

     $$newresult[0]->{$lfn} = 0;
           
         $self->debug(2,"Decrypting envelope for commit ...\n$RAWenvelope");
      
         my $envelope  = $self->decryptEnvelopeTicket($RAWenvelope);
         
         defined($envelope) and defined($envelope->{turl})  or return $newresult;

             $lfn and (($lfn eq "$envelope->{lfn}") or return $newresult);
             ($envelope->{access} =~ /^write[\-a-z]*/) or return $newresult;
             $size and $envelope->{size} = $size ;
             $pfn and $envelope->{turl} = $pfn ;
             $md5 and $envelope->{md5} = $md5 ;
             $se and $envelope->{se} = $se ;
             $guid and $envelope->{guid} = $guid ;
             $size and $envelope->{size} = $size ;
             my $justRegistered=0;
             $envelope = $self->ValidateRegistrationEnvelopesWithBookingTable($user,$envelope);
             $envelope 
                    or $self->info("Authorize: An envelope could not be validated based on pretaken booking.",1) 
                    and return $newresult;
            
            if(!$envelope->{existing}) {
              $self->f_registerFile( "-f", $envelope->{lfn}, $envelope->{size},
                       $envelope->{se}, $envelope->{guid}, undef,undef, $envelope->{md5}, 
                        $envelope->{turl}) and $justRegistered=1 
               or $self->info("Authorize: File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered.",1)
               and return $newresult;
             } else {
                $self->f_addMirror( $envelope->{lfn}, $envelope->{se}, $envelope->{turl}, "-c","-md5=".$envelope->{md5})
                  or $self->info("Authorize: File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered as a replica.",1)
                and return $newresult;
             }
             $self->deleteEntryFromBookingTableAndOptionalExistingFlagTrigger($user, $envelope, $justRegistered) 
                    or $self->info("Authorize: File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered properly as a replica (LFN_BOOKED error).",1)
                    and return $newresult;

             $$newresult[0]->{$lfn} = 1;
           
             # send write-commit info
             if ($self->{MONITOR}) {
               my @params;
               push @params,"$se";
               push @params,"$size";
               $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","$self->{CATALOG}->{ROLE}_written", @params);
             }
     return $newresult;
   }
}





sub ValidateRegistrationEnvelopesWithBookingTable{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my @verifiedEnvelopes= ();

  my $reply = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
      "SELECT lfn,binary2string(guid) as guid,existing FROM LFN_BOOKED WHERE guid=string2binary(?) and pfn=? and se=? and owner=? and gowner=? ;"
      , undef, {bind_values=>[$envelope->{guid},$envelope->{turl},$envelope->{se},$user,$user]});

  $envelope->{guid} or return 0;
  lc $envelope->{guid} eq lc $reply->{guid} or return 0;
  $envelope->{lfn} = $reply->{lfn};
  $envelope->{existing} = $reply->{existing};

  return $envelope;
}

 
sub deleteEntryFromBookingTableAndOptionalExistingFlagTrigger{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my $trigger=(shift || 0);


  my $triggerstat=1;

  if($user ne "admin") {
    $trigger 
      and $triggerstat = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
      "UPDATE LFN_BOOKED SET existing=1 WHERE lfn=? and guid=string2binary(?) and size=? and md5sum=? and owner=? and gowner=? ;",
      {bind_values=>[$envelope->{lfn},$envelope->{guid},$envelope->{size},$envelope->{md5},$user,$user]});

      return ($self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
        "DELETE FROM LFN_BOOKED WHERE lfn=? and guid=string2binary(?) and pfn=? and se=? and owner=? and gowner=? ;",
        {bind_values=>[$envelope->{lfn},$envelope->{guid},$envelope->{turl},$envelope->{se},$user,$user]})
        && $triggerstat);
  } else {
    $trigger 
      and $triggerstat = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
      "UPDATE LFN_BOOKED SET existing=1 WHERE lfn=? and guid=string2binary(?) and size=? and md5sum=? ;",
      {bind_values=>[$envelope->{lfn},$envelope->{guid},$envelope->{size},$envelope->{md5}]});

    return ($self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
      "DELETE FROM LFN_BOOKED WHERE lfn=? and guid=string2binary(?) and pfn=? and se=? ;",
      {bind_values=>[$envelope->{lfn},$envelope->{guid},$envelope->{turl},$envelope->{se}]})
      && $triggerstat);
  }
}


sub addEntryToBookingTableAndOptionalExistingFlagTrigger{
  my $self=shift;
  my $user=(shift || return 0);
  my $envelope=(shift || return 0);
  my $jobid=(shift || 0);
  my $trigger=(shift || 0);

  use Time::HiRes qw (time); 
  my $lifetime= time() + 604800; # one week to prevent data loss
  $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
    "REPLACE INTO LFN_BOOKED (lfn, owner, quotaCalculated, md5sum, expiretime, size, pfn, se, gowner, guid, existing, jobid) VALUES (?,?,?,?,?,?,?,?,?,string2binary(?),?,?);"
    ,{bind_values=>[$envelope->{lfn},$user, "1" ,$envelope->{md5},$lifetime,$envelope->{size},$envelope->{turl},$envelope->{se},$user,$envelope->{guid},$trigger,$jobid]})
     or return 0;
 
  my $negexpire= -$lifetime ;
  $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
     "UPDATE LFN_BOOKED SET expiretime=? WHERE lfn=? and guid<>string2binary(?) ; ",{bind_values=>[$negexpire,$envelope->{lfn},$envelope->{guid}]}) or return 0;
  return 1;
}


sub reduceFileHashAndInitializeEnvelope{
  my $self=shift;
  my $access=(shift || return 0);
  my $filehash=(shift || return 0);
  my @tags=("lfn", "guid", "size", "md5", @_);
  my $envelope = {};
  $envelope->{zguid} and push @tags, "zguid";
  
  $envelope->{access} = $access;
  foreach my $tag (@tags){
     defined($filehash->{$tag}) or $self->info("Warning! there was supposed to be a $tag, but it is not there".Dumper($filehash)) and next;
     $envelope->{$tag} = $filehash->{$tag};
  }

  return $envelope;
}


sub authorize{
  my $self = shift;
  my $access = (shift || return),
  my @registerEnvelopes=@_;
  my $jid = pop(@registerEnvelopes); # remove the added jobID from Service/Authen
  ($jid =~ m/^\d+$/) or push @registerEnvelopes, $jid;
  my $options=shift;

  my $user=$self->{CONFIG}->{ROLE};
  $self->{ROLE} and $user=$self->{ROLE};
  #

  ($access =~ /^write[\-a-z]*/) and $access = "write";
  my $writeReq = ( ($access =~ /^write$/) || 0 );
  my $mirrorReq = ( ($access =~ /^mirror$/) || 0 );
  my $readReq = ( ($access =~ /^read$/) || 0 );
  my $registerReq = ( ($access =~/^register$/) || 0 );
  my $deleteReq = ( ($access =~/^delete$/) || 0 );

  my $exceptions = 0;

  ($access =~/^registerenvs$/) and 
    return $self->validateSignedEnvAndRegisterAccordingly($user,\@registerEnvelopes);
  
  
  my $lfn    = ($options->{lfn} || "");
  my $wishedSE = ($options->{wishedSE} || "");
  my $size    = (($options->{size} and int($options->{size})) || 0);
  my $md5 = ($options->{md5} || 0);
  my $guidRequest = ($options->{guidRequest} || 0);
  $options->{guid} and $guidRequest=$options->{guid};
  my $sitename= ($options->{site} || 0);
  my $writeQos = ($options->{writeQos} || 0);
  my $writeQosCount = (($options->{writeQosCount} and int($options->{writeQosCount})) || 0);
  my $excludedAndfailedSEs = $self->validateArrayOfSEs(split(/;/, ($options->{excludeSE} || "" )));
  my $pfn = ($options->{pfn} || "");
  my $links = ($options->{links} || 0);
  my $linksToBeBooked=1;
  my $jobID = (shift || 0);


  my $seList = $self->validateArrayOfSEs(split(/;/, $wishedSE));


  my @returnEnvelopes = ();
  my $prepareEnvelope = {};

  if ($writeReq or $registerReq) {
    $prepareEnvelope = $self->getBaseEnvelopeForWriteAccess($user,$lfn,$size,$md5,$guidRequest);
    $prepareEnvelope or $self->info("Authorize: Error preparing the envelope for $user and $lfn",1) and return 0;
    if($registerReq) {
      $prepareEnvelope or $self->info("Authorize: Permission denied. Could not register $lfn.",1) and return 0;
      return $self->registerPFNInCatalogue($user,$prepareEnvelope,$pfn,$wishedSE);
    }
  } 
  $deleteReq and 
     ($prepareEnvelope,$seList) = $self->getBaseEnvelopeForDeleteAccess($user,$lfn);

  $mirrorReq and $prepareEnvelope = $self->getBaseEnvelopeForMirrorAccess($user,$guidRequest,$lfn);

  ($writeReq or $mirrorReq )
       and ($prepareEnvelope, $seList) = $self->getSEsAndCheckQuotaForWriteOrMirrorAccess($user,$prepareEnvelope,$seList,$sitename,$writeQos,$writeQosCount,$excludedAndfailedSEs);

  $readReq and $prepareEnvelope=$self->getBaseEnvelopeForReadAccess($user, $lfn, $seList, $excludedAndfailedSEs, $sitename) and @$seList = ($prepareEnvelope->{se});
  $prepareEnvelope or $self->info("Authorize: We couldn't create any envelope.",1) and return 0;
   

  ($seList && (scalar(@$seList) gt 0)) or $self->info("Authorize: access: After checkups there's no SE left to make an envelope for.",1) and return 0;

  (scalar(@$seList) lt 0) and $self->info("Authorize: Authorize: ERROR! There are no SE's after checkups to create an envelope for '$$prepareEnvelope->{lfn}/$prepareEnvelope->{guid}'",1) and return 0;

   while (scalar(@$seList) gt 0) {
       $prepareEnvelope->{se} = shift(@$seList);
   
       if ($writeReq or $mirrorReq) {
         $prepareEnvelope = $self->calculateXrootdTURLForWriteEnvelope($prepareEnvelope);
         $self->addEntryToBookingTableAndOptionalExistingFlagTrigger($user,$prepareEnvelope,$jobID,$mirrorReq) or next;
         if($links and $linksToBeBooked) {
           $self->prepookArchiveLinksInBookingTable($user,$jobID,$links,$prepareEnvelope->{guid})
               or $self->info("Authorize: The requested links of the archive could not been booked",1) and return 0;;
           $linksToBeBooked=0;
         }
         # or next;
       }

       $prepareEnvelope->{xurl} = 0;

       if ( ($prepareEnvelope->{se} =~ /dcache/i) 
          or ($prepareEnvelope->{se} =~ /alice::((RAL)|(CNAF))::castor/i) and !($prepareEnvelope->{se}=~ /alice::RAL::castor2_test/i)  ) {
              $prepareEnvelope->{turl}=~ m{^((root)|(file))://([^/]*)/(.*)};
              $prepareEnvelope->{xurl} ="root://$4/$prepareEnvelope->{lfn}";
       }

   
       my $signedEnvelope  = $self->signEnvelope($prepareEnvelope,$user);

       $self->isOldEnvelopeStorageElement($prepareEnvelope->{se}) and 
          $signedEnvelope .= '\&oldEnvelope='.$self->createAndEncryptEnvelopeTicket($access, $prepareEnvelope);
   
       push @returnEnvelopes, $signedEnvelope;
   
       if ($self->{MONITOR}) {
   	#my @params= ("$se", $prepareEnvelope->{size});
   	my $method;
   	($access =~ /^((read)|(write))/)  and $method="${1}req";
   	$access =~ /^delete/ and $method="delete";
   	$method and
   	$self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","$self->{ROLE}_$method", ("$signedEnvelope->{se}", $signedEnvelope->{size}) ); 		      
       }
  }  

  $self->debug(2,"End of authorize, giving back ENVELOPES: ".Dumper(@returnEnvelopes));

  return @returnEnvelopes;
}




sub  initializeEnvelope{
  my $self=shift;
  my $access=(shift || return {});
  my $lfn=(shift || return "");
  my $preEnvelope=(shift || return {});
  my @tags = ("guid","size","md5","turl","se");

  my $prepareEnvelope = {};
  foreach (@tags) { (defined $preEnvelope->{$_}) and ($preEnvelope->{$_} ne "") and $prepareEnvelope->{$_} = $preEnvelope->{$_} };
  $preEnvelope->{md5} or $preEnvelope->{md5} = "00000000000000000000000000000000";
  $prepareEnvelope->{access}=$access;
  ($preEnvelope->{lfn} ne "") and $prepareEnvelope->{lfn}=$lfn;
  return $prepareEnvelope;
}


sub isOldEnvelopeStorageElement{
  my $self=shift;
  my $se=(shift || return 1);

  ($se eq "no_se") and return 0;
  
  my @queryValues = ("$se");
  my $seVersion = $self->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryValue("SELECT seVersion FROM SE WHERE seName=? ;", undef, {bind_values=>\@queryValues});

  eval {
    (defined($seVersion)) and ($seVersion =~ m/^\d+$/) and (int($seVersion) > 218) and return 0;
  };

  return 1;
}


sub createAndEncryptEnvelopeTicket {
  my $self=shift;
  my $access=(shift || return);
  my $env=(shift || return);
  $access =~ /^(mirror|write)$/ and $access = "write-once";


  my @envelopeElements= ("lfn","guid","se","turl","md5","size");
  my $ticket = "<authz>\n  <file>\n";
  $ticket .= "    <access>$access</access>\n";
  foreach my $key ( keys %{$env}) { 
     if (grep (/^$key$/i,@envelopeElements) and defined($env->{$key})) {
          $ticket .= "    <$key>$env->{$key}</$key>\n"; 
     }
  }
  my @pfns = split (/\/\//, $env->{"turl"});
  $pfns[2] =~ s/#(.*)$//;
  $ticket .= "    <pfn>/".$pfns[2]."</pfn>\n"; 
  $ticket .= "  </file>\n</authz>\n";

  $self->{envelopeCipherEngine}->Reset();
  #    $self->{envelopeCipherEngine}->Verbose();
  $self->{envelopeCipherEngine}->encodeEnvelopePerl("$ticket","0","none")
     or $self->info("Authorize: access: error during envelope encryption",1) and return 0;

  return $self->{envelopeCipherEngine}->GetEncodedEnvelope();
}

sub decryptEnvelopeTicket {
  my $self=shift;
  my $ticket=(shift || return {});

  $self->{envelopeCipherEngine}->Reset();
#    $self->{envelopeCipherEngine}->Verbose();
  $self->{envelopeCipherEngine}->IsInitialized();

  my $decoded = $self->{envelopeCipherEngine}->decodeEnvelopePerl($ticket);
  $decoded or $self->info("Authorize: error during envelope decryption",1) and return 0;

  $decoded = $self->{envelopeCipherEngine}->GetDecodedEnvelope();
  
  $self->debug(2,"Decoded envelope: $decoded");  

  my $xsimple = XML::Simple->new();
  my $filehash = $xsimple->XMLin($decoded,
                                        KeyAttr => {lfn=> 'name'},
                                        ForceArray => [ 'file' ],
                                        ContentKey => '-content');
  return shift @{$filehash->{file}};
} 



sub signEnvelope {
  my $self=shift;
  my $env=(shift || return);
  my $user=(shift || return);


  $env->{issued} = int(time);
  $env->{expires} = int($env->{issued}) + 86400; # 24h
  $env->{issuer} = "Authen.".$self->{CONFIG}->{VERSION} ;
  $env->{user} = $user;
   
  my $envelopeString="";
  # turl is the transaction URL that becomes later the PFN in the catalogue, xurl below is the connection URL (overwrite for dcache etc. above)
  my @keyVals = ("turl","access","lfn","size","se","guid","md5","user","issuer","issued","expires","hashord");
  $env->{zguid} and push @keyVals, "zguid";
  $env->{hashord} = join ("-",@keyVals);
  foreach(@keyVals) {
    ($_ eq "lfn")
        and $envelopeString=$envelopeString."$_=".AliEn::Util::escapeSEnvDelimiter($env->{$_})."&" and next;
    ($_ eq "user")
        and $envelopeString=$envelopeString."user=$user&" and next;
    $envelopeString=$envelopeString."$_=".($env->{$_} || 0)."&";
    
  }
  $envelopeString=~ s/&$//;

  my $signature = encode_base64($self->{signEngine}->sign($envelopeString));
  $signature =~  s/\n//g;
  $envelopeString =~  s/&/\\&/g;
  
  $env->{"xurl"} and $signature .= '\&xurl='.$env->{"xurl"};

  return $envelopeString.'\&signature='.$signature;
}

sub verifyAndDeserializeEnvelope{
  my $self=shift;
  my $env=(shift || return 0);
  my $signature=0;
  my $envelopeString="";
  my $envelope = {};
  
  foreach ( split(/\\&/, $env)) {
     my ($key, $val) = split(/=/,$_,2);
     $envelope->{$key} = $val; 
  }
  my @keys = split(/-/, $envelope->{hashord});

  foreach (@keys) {
    $envelopeString .= $_."=".$envelope->{$_}.'&';
  } 
  $envelopeString =~ s/&$//;
  $signature = decode_base64($envelope->{signature});
  $envelope->{lfn} = AliEn::Util::descapeSEnvDelimiter($envelope->{lfn});

  # if we signed the presented returnEnvelope
  $self->{verifyLocalEngine}->verify($envelopeString, $signature)
    and return $envelope;
  # if an SE signed the presented returnEnvelope
  $self->{verifyRemoteEngine}->verify($envelopeString, $signature)
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
   foreach(@$seList){   $query .= " seName=? or";   push @queryValues, $_; }
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
      $self->info("Authorize: Attention: The following SE names were dropped since the they are not existing in the system: @dropList");
   }
   (scalar(@$seList) gt 0) or return $seList;

   @queryValues = ();
   $query="SELECT seName FROM SE WHERE (";
   foreach(@$seList){   $query .= " seName=? or";   push @queryValues, $_; }
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
      $self->info("Authorize: Attention: The following SEs were dropped since the file's size is too small concerning the SEs min file size specification: @dropList");
   }
   (scalar(@$seList) gt 0) or return $seList;


   @queryValues = ();
   $query="SELECT seName FROM SE WHERE (";
   foreach(@$seList){   $query .= " seName=? or";   push @queryValues, $_; }
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
      $self->info("Authorize: Attention: The following SEs were dropped since you are excluded from write access due to exclusiveWrite: @dropList");
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
       ." sitename=? and SERanks.seNumber = SE.seNumber ";

   my @queryValues = ();
   push @queryValues, $sitename;

   foreach(@$excludeList){   $query .= "and SE.seName<>? "; push @queryValues, $_;  }
   
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

   my $reply = $catalogue->query("SELECT sitename FROM SERanks WHERE sitename=?;", undef, {bind_values=>[$site]});

   (scalar(@$reply) < 1) and $self->info("Authorize: We need to update the SERank Cache for the not listed site: $site")
            and return $self->refreshSERankCache( $site);

   return 1;
}






return 1;

