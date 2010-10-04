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

package AliEn::UI::Catalogue::LCM;

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
use AliEn::PackMan;
use POSIX "isdigit";
use MIME::Base64; # not needed after signed envelopes are in place





use vars qw(@ISA $DEBUG);
@ISA = qw( AliEn::UI::Catalogue );
$DEBUG=0;
my %LCM_commands;

%LCM_commands = (
    #File Interface
		 'fquota' => ['$self->fquota', 0],
		 'add' => ['$self->addFile', 0],
		 'get'      => ['$self->get', 0],
		 'authorize'   => ['$self->authorize', 2+4+8],
		 'commit'   => ['$self->commit', 0],
		 'relocate' => ['$self->relocate', 2+4+8],
		 'mirror'   => ['$self->mirror', 0],
		 'cat'      => ['$self->cat', 0],
		 'less'      => ['$self->less', 0],
		 'head'      => ['$self->head', 0],
		 'addTag'   => ['$self->addTag', 0],
		 'access'   => ['$self->access', 0],
		 'authorize'   => ['$self->authorize', 0],
		 'resolve'  => ['$self->resolve', 0],
		 'mssurl'   => ['$self->mssurl', 0],
		 'df'       => ['$self->df', 0],
		 'services' => ['$self->services', 0],
		 'preFetch'   => '$self->preFetch',
		 'vi'       => ['$self->vi', 0],
		 'whereis'  => ['$self->{CATALOG}->f_whereis', 66],
		 'purge'    => ['$self->purge', 0 ],
		 'erase'    => ['$self->erase', 0 ],
		 'listTransfer'=> ['$self->{STORAGE}->listTransfer', 0],
		 'killTransfer'=> ['$self->{STORAGE}->killTransfer', 0],
		 'stage'=> ['$self->stage', 2],
		 'isStage'=> ['$self->isStaged', 2],
		 'find'=> ['$self->find',0],
		 'zip'=> ['$self->zip',16+64],
		 'unzip'=> ['$self->unzip',0],
		 'getLog' =>['$self->getLog', 0],
		 'checkSEVolumes' =>['$self->{CATALOG}->checkSEVolumes', 0],
		 'createCollection' => ['$self->createCollection', 0],
		 'updateCollection' => ['$self->updateCollection', 2],
		 'resubmitTransfer'=> ['$self->{STORAGE}->resubmitTransfer', 0],
		 'getTransferHistory'=> ['$self->{STORAGE}->getTransferHistory', 0],
		 'packman'  => ['$self->{PACKMAN}->f_packman',0],
		 'masterSE' => ['$self->masterSE',0],
);

my %LCM_help = (
    'fquota'   => "\tDisplay catalogue quota information",
    'get'      => "\tGet a copy of the file",
    'mirror'   => "Add mirror method for a file",
    'cat'      => "\tDisplay a file on the standard output",
    'addTag'   => "Creates a new tag and asociates it with a directory",
    'add' => "\tCopies a pfn to a SE, and then registers it in the catalogue",
    'authorize'   => "Submit an file request to Authen",
    'resolve'  => "Resolves the <host:port> address for services given in the <A>::<B>::<C> syntax",
    'mssurl'   => "Resolve the <protocol>://<host>:<port><path> format for the file under <path> in mss <se>",
    'df'       => "\tRetrieve Information about the usage of storage elements",
    'services' => "Retrieve Status of all services",
    'preFetch' => "Gets a list of LFN, orders them according to accessibility, and starts transfering all the files that are in remote SE",
    'vi'       => "\tGets and opens the file with vi. In case of changes the file is uploaded into the catalogue.",
    'whereis'      => "Displays the SE and pfns of a given lfn",
    'getLog' =>"\tGets the log file of a service",
    'resubmitTransfer' =>"\tResubmits a Transfer",
    'showTransferHistory'=>"\tShows the history of a transfer",
    'packman'  => "\tTalks to the Package Manager (PackMan). Use 'packman --help' for more info",

);

sub initialize {
  my $self    = shift;
  my $options = shift;

  $self->SUPER::initialize($options) or return;

  $self->{STORAGE} = AliEn::LCM->new($options);

  ( $self->{STORAGE} ) or return;

  $self->{SOAP}=new AliEn::SOAP;

  $self->AddCommands(%LCM_commands);
  $self->AddHelp(%LCM_help);

  $self->{MONITOR} = 0;
  AliEn::Util::setupApMon($self);


  $self->{envelopeCipherEngine} =0;
  $self->{noshuffle} = 0;


  if (defined $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'} && defined $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} && defined $ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'} and defined $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}) {
    $self->info("local private key          : $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}");
    $self->info("local public  key          : $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'}");
    $self->info("remote private key         : $ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'}");
    $self->info("remote public key          : $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}");
    require Crypt::OpenSSL::RSA;
    require Crypt::OpenSSL::X509;
    
    open(PRIV, $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}); my @prkey = <PRIV>; close PRIV;
    my $private_key = join("",@prkey);
    my $public_key = Crypt::OpenSSL::X509->new_from_file( $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} )->pubkey();
    $self->{signEngine} = Crypt::OpenSSL::RSA->new_private_key($private_key);
    $self->{verifyEngine} = Crypt::OpenSSL::RSA->new_public_key($public_key);

 
    require SealedEnvelope;
    
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
    }
  }

  my $packOptions={PACKMAN_METHOD=> $options->{packman_method}|| "",
                   CATALOGUE=>$self};

  $self->{PACKMAN}= AliEn::PackMan->new($packOptions) or return;
  $self->{tracelogenabled}=0;
  $self->{tracelog}=();

  return 1;
}

sub resolve {
  my $self  = shift;
  my $name    = shift or print STDERR
"Error: not enough arguments in resolve\nUsage: resolve <name> <servicename>\n" and return ;
  my $service = shift or print STDERR
"Error: not enough arguments in resolve\nUsage: resolve <name> <servicename>\n" and return ;

  my $response;
  my $method;

  ( $service =~ /SE/) and $method="getSE";
  ( $service =~ /FTD/) and $method="getFTD";

  if (!$method ) {
    print STDERR "The service $service can not be queried!\n";
    return ;
  }

  $response = $self->{SOAP}->CallSOAP("IS", $method, $name) or return;

  $response = $response->result;

  if ( $response == -1) {
    return;
  }

  my $serviceHost  = $response->{HOST};
  my $servicePort  = $response->{PORT};

  my $hostport = "$serviceHost:$servicePort";
  $self->info("Service $service of $name is accessible under: $hostport",0,0);
  return $hostport;

}

sub mssurl {
  my $self  = shift;
  my $se   = shift or print STDERR
"Error: not enough arguments in mssurl\nUsage: mssurl <SE> <PATH>\n" and return ;
  my $file = shift or print STDERR
"Error: not enough arguments in mssurl\nUsage: mssurl <SE> <PATH>\n" and return ;

  my $hostport = resolve($self,$se,"SE");
  print STDERR "H", $hostport;
  if ( $hostport eq "") {
    return;
  }

  my $response =
    SOAP::Lite->uri("AliEn/Service/SE")
        ->proxy("http://$hostport")
	  ->getURL($file);

  if (! ($response) ) {
    return;
  }

  $response = $response->result;
  
  $self->info("URL: $response",0,0);
  return $response;
}

=item C<get($lfn, $localfile)>

Gets a file from the catalogue

=over

=item INPUT

=over

=item $lfn

name of the file to retrieve

=item $localfile

Optional argument. If it is passed, the catalogue will retrieve the file from the catalogue and copy it in $localfile. If $localfile is undefined, the system will use the cache directory

=back

=back

=cut
sub get_HELP{
  return "get: copies a file from teh file catalogue  to the local computer:
Usage  : get [-<options>] <file> [<localfile>]

Options:
  -n:
  -o:
  -g: The file is a guid instead of an lfn
  -s [<se>][,!<se>]* : Retrieve the file from the se <se> and/or don't use the list of !<se>

Get can also be used to retrieve collections. In that case, there are some extra options:
  -c: Retrieve all the files with their original lfn name
  -b <name>: Requires -c. Remove <name> from the beginning of the lfn

";
}
sub get {
   my $self = shift;
   my %options=();
   @ARGV=@_;
   getopts("gonbt:clfs:", \%options) or $self->info("Error parsing the arguments of [get]\n". $self->get_HELP(),1)
              and return 0;
   @_=@ARGV;
   $options{t} and $self->{tracelogenabled}=1;
   $self->traceLogFlush();
   my $file      = shift;
   my $localFile = shift;
   ($file) or print STDERR "Error: not enough arguments in [get]\n". $self->get_HELP() and return {ok=>0,tracelog=>\@{$self->{tracelog}}};
   my @excludedAndfailedSEs = ();
   my $wishedSE = 0;
   if($options{s}) {
     foreach (split(/,/, $options{s})) {
        ($_ =~ /!/) 
          and $_ =~ s/!//
          and push @excludedAndfailedSEs, $_
          and next;
        $wishedSE = $_;
     }
   }

   my $filename = $file;
   my $class = "";
   $self->{CATALOG} and $class=ref $self->{CATALOG};
   my $filehash = {};

   if(AliEn::Util::isValidGUID($filename)) {
     $filehash=$self->{CATALOG}->{DATABASE}->{GUID_DB}->checkPermission("r", $filename, {retrieve=>"guid,type,size,md5"});
   } else {
     ($class =~ /^AliEn/ ) and $filename = $self->{CATALOG}->f_complete_path($filename);
     $filehash=$self->{CATALOG}->checkPermissions("r",$filename,undef, {RETURN_HASH=>1});
   }
   $filehash->{guid} or $self->tracePlusLogError("error","Could not retrieve file info for: $file") and return {ok=>0,tracelog=>\@{$self->{tracelog}}};
   ($filehash->{type} eq "c") and  $self->tracePlusLogError("info","This is in fact a collection!! Let's get all the files")
     and return $self->getCollection($filehash->{guid}, $localFile, \%options);
   my $result=$self->{STORAGE}->getLocalCopy($filehash->{guid}, $localFile);
   $self->{STORAGE}->checkDiskSpace($filehash->{size}, $localFile) or return {ok=>0,tracelog=>\@{$self->{tracelog}}};


   while (!$result) {
     my  $authenReply = $self->authorize("read",$filename,$wishedSE,0,0,(join(";",@excludedAndfailedSEs) || 0),$self->{CONFIG}->{SITE});
     my $envelope = shift @{$authenReply->{envelopes}};
     ($authenReply->{interrupt} or (!$envelope) or (!defined $envelope->{envelope})) 
      and $self->tracePlusLogError("error","Getting an envelope was not successfull for file $file.") and return {ok=>0,tracelog=>\@{$self->{tracelog}}};
 
 foreach (keys %{$envelope}) { defined($envelope->{$_}) and $self->info("gron: envelopee info, $_: $envelope->{$_}"); }
 
     $ENV{ALIEN_XRDCP_ENVELOPE}=$envelope->{envelope};
     $ENV{ALIEN_XRDCP_URL}=$envelope->{turl};
     my $start=time;
     $result = $self->{STORAGE}->getFile( $envelope->{turl}, $envelope->{se}, $localFile, join("",keys %options), $file, $envelope->{guid},$envelope->{md5} );
     my $time=time-$start; if ($self->{MONITOR}){ $self->sendMonitor('read', $envelope->{se}, $time, $envelope->{size}, $result); }
     $result and last or $self->tracePlusLogError("error","getFile failed with: ".$self->{LOGGER}->error_msg());
     push @excludedAndfailedSEs, $envelope->{se};
     $wishedSE = 0;
   } 
   $result or $self->info("Error: not possible to get the file $file. Message: ".$self->{LOGGER}->error_msg(), 1) and return;
   $self->info("And the file is $result",0,0);
   return $result;
}


sub getCollection{
  my $self=shift;
  my $guid=shift;
  my $localFile=shift || "";
  my $options=shift || {};

  $self->debug(1, "We have to get all the files from $guid");
  my ($files)=$self->execute("listFilesFromCollection", "-silent", "-g", $guid)
    or $self->info("Error getting the list of files from the collection") and return;
  my @return;
  if ($localFile){
    $self->info("We have to rename it to $localFile");
    AliEn::Util::mkdir($localFile) or 
	$self->info("error making the directory $localFile",1) and return;
  }
  my $names={}; 
  my $counter=0;
  foreach my $file (@$files){
    $self->info("Getting the file $file->{guid} from the collection");
    my $localName= $file->{localName} || "";
    if($options->{c}){
      $localFile or $localFile="$ENV{PWD}";
      $self->info("We have to set the local filename according to the lfn");
      my $lfn=$file->{origLFN} || "no_lfn.$counter";
      $counter++;
      $options->{b} and $lfn=~ s/^$options->{b}//;
      $localName="$localFile/$lfn";
    } elsif ($localFile) {
      my $name=$file->{guid};
      if ($file->{origLFN}){
	$name=$file->{origLFN};
	$name =~ s{^.*/([^/]*)$}{$1};
      }
      my $counter=1;
      if ($names->{$name}){
	$counter=$names->{$name}+1;
	$names->{$name}=$counter;
	$name.=".$names->{$name}";
      }
      $names->{$name}=$counter;
      $localName="$localFile/$name";
    }
    $self->debug(2,"In the collection, let's get $file->{guid}  $localName");
    my ($fileName)=$self->execute("get", "-g", $file->{guid}, $localName);
    push @return, $fileName;
  }
  $self->info("Got ". join (",",(map {$_ ? $_  : "<error>"} @return ))." and $#return");
  return \@return;
}
  

sub cachefilename {
    my $self = shift;
    my $filename = shift;
    my $basename = $self->{CATALOG}->f_basename($filename);
    my $nowtime = time;
    my $random  = rand 65535;
    return "$self->{CONFIG}->{CACHE_DIR}/$basename.$random.$nowtime";
}


=item C<df(@args)>

Gives the space usage of a storage element. If the name of the storage element is not provided, it will use the closest SE. 

The arguments can contain the followin flags:

=over

=item -a

give information of all the SE

=item -c

get the cache information

=back


=cut

sub df_HELP {
  return "df: returns the disk space usage of the SE
Usage:
df [-af]

Options:
  -a:  all. Show all the SE
  -f:  force Refresh the information
"
}
sub df {
  my $self = shift;
  my $opt;
    ( $opt, @_ ) = $self->Getopts(@_);
  my $se   = (shift or $self->{CONFIG}->{SE_FULLNAME});
  my $oldsilent = $self->{CATALOG}->{SILENT};
#  my @hostportsName;
  my @results = ();
  if ($opt =~/a/) {
      $se = "";
  }

  my $service="SE";
  my $function="getDF";
  if ($opt=~ /c/ ){
    $service="CLC";$function="getCacheDF";
    $self->info("Cachename               1k-blocks         Used(KB)  Available Use\%    Range \  #Files",0,0);
  } else {
    $self->info("Storagename             1k-blocks         Used(KB)  Available Use\%    \#Files Type   min_size",0,0);
  }



  my $response=$self->{CATALOG}->f_df($se, $opt);
  $self->debug(1, "Got $response");
  foreach my $line (@$response){
    my $details = {};
    ($details->{name}, $details->{size}, $details->{used}, $details->{available}, $details->{usage}, $details->{files}, $details->{type}, $details->{min_size}) 
      =($line->{seName}, $line->{size},$line->{usedspace},$line->{freespace},$line->{used},$line->{seNumFiles},$line->{seType}, $line->{seMinSize});
    push(@results, $details);
    ( $line eq "-1") and next;

    my $buffer  = sprintf " %-19s %+12s %+12s %+12s %+3s%% %+9s %-10s %s",$line->{seName}, $line->{size},$line->{usedspace},$line->{freespace},$line->{used},$line->{seNumFiles},$line->{seType}, $line->{seMinSize};
    $self->info($buffer,0,0);
  }
#  }
  return @results;
}


=item C<services(@args)>

Checks the status of different services

=cut


sub services {
 my $self = shift;

 my @hostports;
 my @results;

 my @checkservices;
 my $replystatus = 0;
 my $dontcall = 0;
 my $returnhash =0;
 my $domain="";
 my $opt={};
 my @returnarray;
 $#returnarray=-1;
 @ARGV=@_;
 Getopt::Long::GetOptions($opt,  "verbose","z", "n", "core", "se", "ce", "domain=s", "clc", "ftd", "packman") or 
   $self->info("Error parsing the options") and return;;
 @_=@ARGV;
 $opt->{z} and $returnhash=1;
 $opt->{n} and $dontcall=1;
 $opt->{verbose} and $replystatus=1;
 $domain=$opt->{domain};
 foreach my $item (@_) {
   ($item =~ /^-?co(re)?/i ) and $opt->{core}=1 and next;
   ($item =~ /^-?s(e)?/i) and  $opt->{se}=1 and next;
   ($item =~ /^-?ce/i)  and $opt->{ce}=1 and next;
   ($item =~ /^-?cl(c)?/i) and $opt->{clc}=1 and next;
   ($item =~ /^-?f(td)?/i) and $opt->{ftd}=1 and next;
   ($item =~ /^-?p(ackman)?/i)  and $opt->{packman}=1 and next;

   if ($item !~ s/-?-h(elp)?//i) {
     print STDERR "Error: Don't know service flag \"$item\"\n";
   }
   print STDERR "Usage: services [-verbose] [-][core] [-][clc] [-][ftd] [-][se] [-][ce] [-domain <domain>]\n";
   print STDERR "  or   services -verbose -co -cl -f -s -ce \n";
   return;
   
 }

 $opt->{core} and push @checkservices, "Services";
 $opt->{se} and push @checkservices, "SE";
 $opt->{ce} and push @checkservices,"ClusterMonitor";
 $opt->{clc} and  push @checkservices,"CLC", "CLCAIO";
 $opt->{ftd} and push @checkservices, "FTD";
 $opt->{packman} and push @checkservices, "PackMan";
 
 @checkservices or   
   push @checkservices, "SE","CLC","CLCAIO","ClusterMonitor","FTD","TcpRouter","Services";

 printf STDERR "==   Service   == Servicename ============================================= Hostname ==   Status    ==";
 if ($replystatus) {
   printf STDERR "  Vers. =  R  S  D  T  Z =\n";
 } else {
   printf STDERR "\n";
 }
 printf STDERR "-----------------------------------------------------------------------------------------------------------------------------\n";
 foreach (@checkservices) {
   @hostports="";
   my $service = $_;
   my $doservice=$service;

   ($service eq "CLCAIO" ) and $doservice = "CLC";

   my $response =$self->{SOAP}->CallSOAP("IS", "getAllServices", $doservice)
     or next;

   $response = $response->result;

   if ( (defined $response) && ( $response eq "-1")) {
     my $printservice = "$service";
     if ($service eq "ClusterMonitor") {
       $printservice = "CluMon";
     }

     printf STDERR "- [ %-9s ]   not running \n",$printservice;

     print STDERR "-----------------------------------------------------------------------------------------------------------------------------\n";
     next;
   }

   my $cnt = 0;
   
   my @hosts;
   my @ports;
   my @names;

   @hosts = split ":",$response->{HOSTS};
   @ports = split ":",$response->{PORTS};
   @names = split "###",$response->{NAMES};
   
   for (@hosts) {
     if ($domain and $hosts[$cnt] !~ /$domain$/){
       $self->info("Skipping host $hosts[$cnt]");
       $cnt++;
       next;
     } 
     push @hostports, "$hosts[$cnt]:$ports[$cnt]";
     $cnt++;
   }
 

#   print "@hostports | @names\n";
   my $soapservice;
   $cnt=0;
   for (@hostports) {
     ($_ eq "") and  next;

     if ($names[$cnt] =~/.*SUBSYS/) {
       $cnt++;
       next;
     }
     if ($service eq "Services") {
       if ($names[$cnt] eq "JobOptimizer") {
	 $names[$cnt] = "Optimizer/Job";
       }
       if ($names[$cnt] eq "CatalogueOptimizer") {
	 $names[$cnt] = "Optimizer/Catalogue";
       }
       if ($names[$cnt] eq "TransferOptimizer") {
	 $names[$cnt] = "Optimizer/Transfer";
       }
       if ($names[$cnt] eq "JobManager") {
	 $names[$cnt] = "Manager/Job";
       }
       if ($names[$cnt] eq "JobBroker") {
	 $names[$cnt] = "Broker/Job";
       }
       if ($names[$cnt] eq "TransferManager") {
	 $names[$cnt] = "Manager/Transfer";
       }
       if ($names[$cnt] eq "TransferBroker") {
	 $names[$cnt] = "Broker/Transfer";
       }

       $soapservice = $names[$cnt];
     } else {
       $soapservice = $service;
     }

     my $function="reply";
     ($service eq "CLCAIO") and $function .="AIO";
     ($service eq "CLCAIO") and $soapservice="CLC";
     ($replystatus eq "1") and $function .="status";
     if (!$dontcall) {
       eval {
	 $response = SOAP::Lite->uri("AliEn/Service/$soapservice")
	 ->proxy("http://$_",timeout => 3)
	   ->$function();
       };
     }

     my $printservice="";
     my $printname = $names[$cnt];
     $printservice=$service;

     my $hashresult=();

     if ($service eq "ClusterMonitor") {
       $printservice="CluMon";
       if ((defined $response) && ($response eq "1") && (defined $response->result) && (defined $response->{'OK'}) && ($response->{OK} == 1)) {
	 my $soapresponse = $response->result;
	 if (defined $soapresponse->{'Name'}) {
	   $printname = $soapresponse->{'Name'};
	 } else {
	   $printname = $names[$cnt];
	 } 
       } else {
	 $printname = $names[$cnt];
       }
     }

     if ($service eq "Services") {
       $printservice="Core";
     }
     $hashresult->{servicetype} = $printservice;
     $hashresult->{servicename} = $printname;
     printf STDERR "- [ %-9s ]   %-25s %40s ",$printservice, $printname,$_;
     if ((! defined $response) || (($response eq "-1") || ($response eq "") || ($response ne "1"))) {
       printf STDERR "-- no response --\n";
       $hashresult->{servicestatus} = "noresponse";
     } else {
       $response = $response->result;
       if ( (defined $response->{'OK'}) && ($response->{'OK'} ==1) ) {
	 $hashresult->{servicestatus} = "ok";
	 if ((defined $response->{'VERSION'}) && ($replystatus)) {
	   print STDERR "--      OK     --  $response->{'VERSION'}";
	 } else {
	   print STDERR "--      OK     --";
	 }
       } else {
	 print STDERR "--     down    --";
	 $hashresult->{servicestatus} = "down";
       }
       
       if ((defined $response->{'Sleep'}) && (defined $response->{'Run'}) && (defined $response->{'Trace'}) && (defined $response->{'Disk'}) && (defined $response->{'Zombie'})) {
	 printf STDERR " %2d %2d %2d %2d %2d\n",$response->{'Run'},$response->{'Sleep'},$response->{'Disk'},$response->{'Trace'}, $response->{'Zombie'};
       } else {
	 print STDERR " \n";
       }
     }
     push @returnarray, $hashresult;
     $cnt++;
   }
   printf STDERR "-----------------------------------------------------------------------------------------------------------------------------\n";
 }
 $returnhash and  return @returnarray;

 return 1;
}
# This subroutine receives a list of SE, and returns the same list, 
# but ordered according to the se
#
#
sub selectClosestSE {
  my $self = shift;
  my $options=shift;
  my ($se, @close, @site, @rest);
  my $prefered;
  if ($options->{se}){
    $self->info("We want to get the file from $options->{se}");
  }
  while (@_) {
    my $newse  = shift;
    my $seName=$newse;
    UNIVERSAL::isa($newse, "HASH") and $seName=$newse->{seName};
    $self->debug(1,"Checking $seName vs " . ( $self->{CONFIG}->{SE_FULLNAME} || 'undef') ." and $self->{CONFIG}->{ORG_NAME}/$self->{CONFIG}->{SITE}" );
    if ($options->{se} and $seName=~ /^$options->{se}$/i){
      $prefered=$newse
    }elsif ($self->{CONFIG}->{SE_FULLNAME} and  $seName =~ /^$self->{CONFIG}->{SE_FULLNAME}$/i ){
      $se=$newse;
    }elsif( grep ( /^$newse$/i, @{ $self->{CONFIG}->{SEs_FULLNAME} } )){
      push @close, $newse;
    }elsif( $seName =~ /$self->{CONFIG}->{ORG_NAME}::$self->{CONFIG}->{SITE}::/i ){
      push @site, $newse;
    }else{
      push @rest, $newse;
    }

  }
  my @return;
  if ($options->{se} and not $prefered){
    $self->info("The se '$options->{se}' doesn't have that file...");
  }
  $prefered and push @return, $prefered;
  $se and push @return, $se;

  if ($self->{noshuffle}) {
      @close and push @return, (@close);
      @site and push @return, (@site);
      @rest and push @return, (@rest);
  } else {
      @close and push @return, shuffle(@close);
      @site and push @return, shuffle(@site);
      @rest and push @return, shuffle(@rest);
  }
  
  $self->debug(1, "After sorting we have ". Dumper(@return));
  return @return;
}




## This subroutine receives a list of SE, and returns the same list, 
## but ordered according to the se
##
##
#sub selectClosestSEOnRank {
#  my $self = shift;
#  my $sitename = (shift || 0);
#  my $seList = (shift || return 0);
#  my $sePrio = (shift || 0);
#  my $excludedAndfailedSEs= (shift || {});
#
#  my $return = [];
#
#  my $res = $self->sortSEListBasedOnSiteSECache($sitename, $seList,$excludedAndfailedSEs);
#
#  #$res and $return = $self->resortArrayToPrioElementIfExists($sePrio,$res)
#  #       or $return = $self->resortArrayToPrioElementIfExists($sePrio,$seList)  
#  #         and $self->info("Error: The sortSEListBasedOnSiteSECache didn't work, replying original SE list.");
#
#  
#  $self->debug(1, "After sorting we have ". Dumper(@$return));
#
#  return $return;
#}

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
      
   my $catalogue = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};
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





sub cat {
  my $self=shift;
  my @done=$self->f_system_call("cat", @_);
  if ($done[0] and $done[0] =~ /1/){
    $self->info("", undef,0);
  }
  return @done;
}
sub less {
  my $self=shift;
  return $self->f_system_call("less", @_);
}
sub head {
  my $self=shift;
  return $self->f_system_call("head", @_);
}

sub f_system_call{
  my $self=shift;
  my $function=shift;
  my $lfn=shift;
  $self->partSilent();
  my ($file) = $self->get($lfn);
  $self->restoreSilent();

  ($file) or
    $self->info( "Error getting the file: ". $self->{LOGGER}->error_msg()) and
      return;
  $self->debug(1, "Ready to execute $function on $file (extra arguments @_)");
  my @oldStat = stat $file;
  system( $function, $file,@_ ) and return;
  return (1, $file, @oldStat);
}


sub vi {
  my $self=shift;
  my $lfn=shift;
  my $reallfn=$self->{CATALOG}->checkPermissions("w", $lfn) or return;

  my ($status, $file, @oldStat)=$self->f_system_call("vi", $lfn, @_) or return;
  $self->debug(1, "After doing vi, we have $status, $file and @oldStat)");
  my @newStat = stat $file;

  $self->debug(1, "Old time: $oldStat[10], new time: $newStat[10]
Old size: $oldStat[7], new size: $newStat[7]");

  ($oldStat[10] == $newStat[10]) and
    ($oldStat[7] == $newStat[7]) and return 1;

  $self->info("File changed, uploading...");
  my $pfn="file://$self->{CONFIG}->{HOST}$file";
  $pfn =~ s/\n//gs;
  my $md5=AliEn::MD5->new($file)
    or $self->info("Error calculating the md5") and return;

  return $self->addFile("-md5=$md5", $reallfn, $pfn);
}


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
  return ( $flags, @files ); }

sub addFile_HELP {
  return "'add' copies a file into the SE, and register an entry in the catalogue that points to the file in the SE\n\t
Usage: add [-g <guid>] [-s <size>] [-md5 <md5>] <lfn> <pfn> [<se>,!<se>,select=N,<qosflag>=N]\n
Possible pfns:\tsrm://<host>/<path>, castor://<host>/<path>, 
\t\tfile://<host>/<path>
If the method and host are not specified, the system will try with 'file://<localhost>' \n";
}

#
# Check if the user can create the file
sub _canCreateFile{
  my $self=shift;
  my $lfn=shift;
  $self->{CATALOG}->checkPermissions( 'w', $lfn )  or  return;
  if ($self->{CATALOG}->f_Database_existsEntry( $lfn)) {
    $self->info( "file $lfn already exists!!",1);
    return;
  }
  return 1;
}

sub addFile {
  my $self  = shift;
  my $options={};
  my $lineOptions=join(" ", @_);
  @ARGV=@_;
  Getopt::Long::GetOptions($options, 
        "silent", "versioning", "tracelog", "user=s", "guid=s", "link", "registerroot") 
      or $self->info("Error checking the options of add") and return;
  @_=@ARGV;
  my $targetLFN   = (shift || ($self->info("ERROR, missing paramter: lfn") and return));
  my $sourcePFN   = (shift || ($self->info("ERROR, missing paramter: pfn") and return));
  my @seSpecs=split(/ /, (shift|| ""));

  $sourcePFN or $self->info("Error: not enough parameters in add\n".
                    $self->addFile_HELP(),2)  and return;
  $targetLFN = $self->{CATALOG}->f_complete_path($targetLFN);
  $options->{tracelog} and $self->{tracelogenabled}=1;


  if($options->{register}) {
    $self->info("gron: Registering pfn ...");
    return $self->registerPFN($targetLFN, $sourcePFN, $options->{guid},$options->{silent},$options->{user});
  }

  $self->info("gron: Adding a file");
  return $self->addFileToSEs($targetLFN, $sourcePFN, \@seSpecs, $options->{guid},$options->{silent},$options->{user});

}

sub registerPFN{
  my $self  = shift;
  my $targetLFN   = (shift || return {ok=>0,tracelog=>\@{$self->{tracelog}}});
  my $sourcePFN   = (shift || return {ok=>0,tracelog=>\@{$self->{tracelog}}});
  my $guid=(shift || 0); # gron: guid is to be handeled
  my $silent=(shift || 0);
  my $user=(shift || $self->{CATALOG}->{ROLE});
  my $result = {};
 
  $self->traceLogFlush();

  my $authenReply = $self->authorize("-user=$user", "register", $targetLFN, $sourcePFN, 0, 0, $guid );

  $authenReply->{ok} and return {ok=>1,tracelog=>\@{$self->{tracelog}}};
  return {ok=>0,tracelog=>\@{$self->{tracelog}}};
}


#  This subroutine mirrors an lfn in another SE. It received the name of the lfn, and the 
#  target SE
#  It may also receive several options:
#      f : force to keep the same relative path in the new SE
#      s : change any transfer of local files to do them through soap 
#      b : (batch) do not wait for the transfer to finish
#      t : (transfer) do not attempt to get the file, but issue a transfer (this implies
#           also batch. 
#      g :

sub mirror_HELP{
  my $self=shift;

  return "mirror Copies a file into another SE
Usage:
\tmirror [-fgui] [-m <number>] [-c <collection>] [-try <number>]<lfn>
Options:\t-f\t keep the same relative path
\t\t-g:\t Use the lfn as a guid
\t\t-m <id>\t Put the transfer under the masterTransfer of <id>
\t\t-u\t Don't issue the transfer if the file is already in that SE
\t\t-r\t If the file is in a zip archive, transfer the whole archive
\t\t-c\t Once the transfer finishes, register the lfn in the collection <collection>
\t\t-try <NumOfAttempts>\t Specifies the number of attempts to try and mirror the file
\t\t-w Wait until the transfer finishes
";
}

sub mirror {
  my $self = shift;
  my $origP= join(" ", @_);

  my $opt={};
  @ARGV=@_;
  Getopt::Long::GetOptions($opt,  "f", "g", "m=i", "b","t", "u", "r", "c=s","try=i", "w") or 
      $self->info("Error parsing the arguments to mirror". $self->mirror_HELP()) and return;;
  @_=@ARGV;
  my $options=join("", keys(%$opt));

  $self->debug(1, "UI/LCM Mirror with @_");
  $opt->{t} and $opt->{b}=1;
  my $lfn  = shift;
  my $se   = ( shift or "$self->{CONFIG}->{SE_FULLNAME}" );
  ($lfn) or $self->info("Error: not enough arguments:\n ". $self->mirror_HELP())     and return;
  $origP=~  s/\s*$lfn\s*/ /;

  my $guid;
  my $realLfn;
  my $pfn="";
  my $seRef;
	my $size;
  if ($opt->{g}){
    $guid=$lfn;
    my $info=$self->{CATALOG}->{DATABASE}->{GUID_DB}->checkPermission( 'w', $guid )  or
      $self->info("You don't have permission to do that") and return;
    $realLfn="";
		$size=$info->{size};
  }else{
    $lfn = $self->{CATALOG}->f_complete_path($lfn);

    my $info=$self->{CATALOG}->checkPermissions( 'w', $lfn, undef, {RETURN_HASH=>1} )  or
      $self->info("You don't have permission to do that") and return;
    $realLfn=$info->{lfn};
    $guid=$info->{guid};
    $self->{CATALOG}->isFile($lfn, $realLfn) or 
      $self->info("The entry $lfn is not a file!") and return;

    if ($info->{type} eq "c"){
      $self->info("Ready to mirror a collection");
      my ($files)=$self->execute("listFilesFromCollection", $lfn);

      foreach my $f (@$files){
	$self->info("Ready to mirror $f");
	$self->execute("mirror","$origP",$f->{origLFN});
      }
      return 1;
    } elsif ($info->{type} ne "f"){
      $self->info("We don't know how to mirror a $info->{type}!!\n",1);
      return;
    }
		$size=$info->{size};
  }

  if ($opt->{u}){
    $self->info("Making sure that the file is not in that SE");
    my $nopt="";
    $opt->{r} and $nopt.="r";
    my @info=$self->{CATALOG}->f_whereis($nopt, $realLfn)
      or $self->info("Error getting the info from $realLfn") and return;
    if (grep (/^$se$/i, @info)){
      $self->info("The file is already in $se!") and return;
    }
  }

  $self->info( "Mirroring file $realLfn at $se");

  my $transfer={"target", "",                      "TYPE", "mirror",
		"USER" => $self->{CATALOG}->{ROLE}, "LFN" =>$realLfn,
		"DESTINATION" =>$se,	             "OPTIONS" => $options,
		guid=>$guid};
#  $opt->{g} and $transfer->{transferGroup}=$opt->{g};
  $opt->{'m'} and $transfer->{transferGroup}=$opt->{'m'};
  $opt->{'r'} and $transfer->{RESOLVE}=$opt->{r};
  $opt->{'c'} and $transfer->{collection}=$opt->{'c'};
  $opt->{'try'} and $transfer->{persevere}=$opt->{'try'}; 

  if ($opt->{f})    {
    $self->info( "Keeping the same relative path");

    my $service=$self->{CONFIG}->CheckService("SE", $se);
    $service or $self->info("SE $se does not exist!") and return;
    my $url=AliEn::SE::Methods->new($pfn) or return;
    $transfer->{target}=$url->path;
    $transfer->{target}=~ s/^$service->{SAVEDIR}//;
  }

  my $result=$self->{SOAP}->CallSOAP("Manager/Transfer","enterTransfer",$transfer);
  $result  or return;

  my $id=$result->result;
  $self->info("The transfer has been scheduled!! ($id)");
  $opt->{'w'} or return (-2, $id);
  $self->info('Waiting until the transfer finishes');
  while (1) {
    sleep 40;
    $self->info("Checking if the transfer finished");
    my ($done)=$self->execute("listTransfer", "-id", $id);
    $done or return;
    my $t=shift  @$done;
    $t or return;
    $self->info("The transfer is $t->{status}");
    $t->{status} =~ /DONE/ and return 1;
    $t->{status} =~ /FAILED/ and return ;

  }
  return ;
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

sub addTag_HELP{
  my $self=shift;
  return "addTag: creates a metadata table so that the entries in the catalogue can have more information
  
Usage:
   addTag [-d] <dir> <meta>
   
 If you add a tag to a directory, everything under it can have that meta information. 
 <meta> should be the name of an entry either in /tags, /$self->{CONFIG}->{ORG_NAME}/tags, or in the \$HOME/tags
 
 Options:
 
   -d: create a new table for the metadata (even if the a parent directory has already that metadata) 
 ";
  
}
  


sub addTag {
    my $self      = shift;
    ( my $opt, @_ ) = $self->Getopts(@_);
    my $directory = shift;
    my $tag       = shift;

    ($tag)
      or print STDERR
"Error: not enough arguments in addTag\n". $self->addTag_HELP()
      and return;

    my @tagdirs  = ("\L/$self->{CONFIG}->{ORG_NAME}/tags\E",
		   $self->{CATALOG}->GetHomeDirectory()."/tags");
    my $file="";
    foreach (@tagdirs){
      $self->{CATALOG}->isFile("$_/$tag") or next;
      $file="$_/$tag";
      last;
    }
    $file or 
      $self->info( "Error: the metadata $tag is not defined in any of these directories:\n@tagdirs") and return;
    my ($tagFile) = $self->execute("get", "-silent", $file);
    $tagFile
      or print STDERR "Error getting the file $file from the catalog\n"
      and return;

    open FILE, "$tagFile"
      or print STDERR "Error openning the file $tagFile\n"
      and return;
    my @FILE = <FILE>;
    close FILE;

    my $description = join "", @FILE;

    $description or return;
    $self->{CATALOG}->f_addTag( $directory, $tag, $description, undef, $opt) or return;

    return 1;
}

sub preFetch {
  my $self=shift;
  my @files=@_;
  @files or $self->info( "Error in preFetch. No files specified") and return; 
  $self->info("Doing preFetch of ".($#files +1)." \n@files");

  my @local=();
  my @remote=();
  foreach my $file (@files) {
    my @se=$self->{CATALOG}->isFile($file) 
      or $self->info("Error file $file does not exist (ignoring this file)")
	and next;
    @se= grep (/::.*::/, @se);
    if (grep (/^$self->{CONFIG}->{SE_FULLNAME}$/, @se)) {
      push @local, $file;
    }else {
      push @remote, $file;
    }
  }

#  fork() and return (@local, @remote);
#  sleep(5);
  $self->info( ($#remote+1). " files have to be transfered");
  foreach my $file (@remote) {
    $self->debug(1, "Starting to get the file $file");
    $self->execute("get",  "-b","-silent", $file);
  }
  return (@local, @remote);

#  exit;
}

# Given an SE and a guid, it returns all the pfns that the SE 
# has of that guid
#sub getPFNfromGUID {
#  my $self=shift;
#  my $se=shift;
#  my $guid=shift;
#  my $options=shift || {};
#  $self->debug(1,"Getting the pfn from $se");

#  my ($seName, $seCert)=$self->{SOAP}->resolveSEName($se) or return;

##  $self->info( "Asking the SE at $seName");
#  my $result=$self->{SOAP}->CallSOAP($seName, "getPFNFromGUID",$seName, $guid, $self->{CONFIG}->{IOMETHODS_LIST}, $options) 
#    or $self->info( "Error asking the SE: $!", 1) and return;
#  my @pfns=$self->{SOAP}->GetOutput($result);
#  $self->debug(1, "Returning the list @pfns");
#  return @pfns;
#}


#############################################################################################################
sub relocate {

  my $self = shift;
  my $options = shift;
  my $lfn = (shift or 0);
  $lfn = $self->{CATALOG}->f_complete_path($lfn);
  my $perm = "r";
  my $guid = "";

  if ( $lfn =~ /(\w\w\w\w\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\w\w\w\w\w\w\w\w).*/ ) {
      $guid = $1;
      $self->debug(1, "We have to translate the guid $1");
      $lfn = "";
      my @alllfns = $self->{CATALOG}->f_guid2lfn("s",$guid);
      foreach (@alllfns) {
	  my $perms = $self->{CATALOG}->checkPermissions($perm,$_,undef, 
							 {RETURN_HASH=>1});
	  if ($perms) {
	      $lfn = $_;
	      last;
	  }
      }
      
      if ($lfn eq "") {
	  $self->info("access: access denied to guid $guid");
	  return;
      }
  }

  
  my $filehash = $self->{CATALOG}->checkPermissions($perm,$lfn,undef, 
						   {RETURN_HASH=>1});
  if (!$filehash) {
      $self->info("access: access denied to $lfn");
      return;
  }
  
  my $dirn = $self->{CATALOG}->f_dirname($lfn);
  my $basen = $self->{CATALOG}->f_basename($lfn);

  return $self->{CATALOG}->f_find("-z","-q","-r", $lfn,"\\");
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
  my @where=$self->{CATALOG}->f_whereis("sgztr","$guid");

  if (! @where){
    $self->info("There were no transfer methods....");
    @where=$self->{CATALOG}->f_whereis("sgzr","$guid");
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
  ($access =~ /^write[\-a-z]*/) && ($filehash = $self->{CATALOG}->checkPermissions($perm,$lfn,undef, 
						    {RETURN_HASH=>1}));
  if (!$filehash) {
    $self->info("access: access denied to $lfn");
    return;
  }

  if  ($access eq "read")  {
    if (!$self->{CATALOG}->isFile($lfn, $filehash->{lfn})) {
      $self->info("access: access find entry for $lfn");
      return ;
    }
  }elsif ($access eq "delete") {
    if (! $self->{CATALOG}->existsEntry($lfn, $filehash->{lfn})) {
      $self->info("access: delete of non existant file requested: $lfn");
      return ;
    }
  } else {
    my $parentdir = $self->{CATALOG}->f_dirname($lfn);
    my $result = $self->{CATALOG}->checkPermissions($perm,$parentdir);
    if (!$result) {
      $self->info("access: parent dir missing for lfn $lfn");
      return ;
    }
    if (($access =~ /^write[\-a-z]*/) && ($lfn ne "")
      && $self->{CATALOG}->existsEntry($lfn, $filehash->{lfn})) {
	$self->info( "access: lfn <$lfn> exists - creating backup version ....\n");
	my $filename = $self->{CATALOG}->f_basename($lfn);
	
	$self->{CATALOG}->f_mkdir("ps","$parentdir"."/."."$filename/") or
	  $self->info("access: cannot create subversion directory - sorry") and return;
	
	my @entries = $self->{CATALOG}->f_ls("s","$parentdir"."/."."$filename/");
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
	if (!$self->{CATALOG}->f_mv("",$lfn, $backupfile)) {
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
#
#  #
#  # Start of the Client side code
#  if (!  $self->{envelopeCipherEngine}) {
#    my $user=$self->{CONFIG}->{ROLE};
#    $self->{CATALOG} and $self->{CATALOG}->{ROLE} and $user=$self->{CATALOG}->{ROLE};
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
  my $extguid = (shift or 0);
  my $user=$self->{CONFIG}->{ROLE};
  $self->{CATALOG} and $self->{CATALOG}->{ROLE} and $user=$self->{CATALOG}->{ROLE};

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
      $filehash=$self->{CATALOG}->{DATABASE}->{GUID_DB}->checkPermission($perm, $guid, {retrieve=>"size,md5"});
      $filehash 
	or $self->info("access: access denied to guid '$guid'")
	and return access_eof("access: access denied to guid '$guid'");
      delete $filehash->{db};
    } else {
      $lfn = $self->{CATALOG}->f_complete_path($lfn);
    }

    if ($lfn eq "/NOLFN") {
       $lfn = "";
       #$guid = $extguid;
       if(AliEn::Util::isValidGUID($extguid)) {
          my $guidCheck = $self->{CATALOG}->getInfoFromGUID($extguid);
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
	($seurl,my $guid2,my $se2) = $self->{CATALOG}->createFileUrl($se, "root", $guid);
	$guid2 and $guid=$guid2;
	if (!$se2){
	  $self->info("Ok, let's create a default pfn (for $guid)");
	  ($seurl, $guid)=$self->{CATALOG}->createDefaultUrl($se, $guid,$size);
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
	  $guid=$self->{CATALOG}->f_lfn2guid("s",$lfn)
	    or $self->info( "access: Error getting the guid of $lfn",11) and return;
	}
        $filehash->{filetype}=$self->{CATALOG}->f_type($lfn);
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
      (($lfn eq "") && ($access =~ /^write[\-a-z]*/)) and $lfn = "/NOLFN";
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
	  $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","$self->{CATALOG}->{ROLE}_$method", @params); 		      
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
    # access <access> <lfn> 
    # -p create public url in case of read access 
  my $self = shift;

  #
  # Start of the Server/Authen side code
  $self->{envelopeCipherEngine}
   and $self->info("STARTING envelope creation: @_ ")
   and return $self->AuthenConsultation(@_); 

  #
  # Start of the Client side code
  my $user=$self->{CONFIG}->{ROLE};
  $self->{CATALOG} and $self->{CATALOG}->{ROLE} and $user=$self->{CATALOG}->{ROLE};

  if($_[0] =~ /^-user=([\w]+)$/)  {
    $user = shift;
    $user =~ s/^-user=([\w]+)$/$1/;
  }
  #gron: isn't the following working and better:
  #($_[0] =~ /^-user=([\w]+)$/) and $user=$1 and shift;


  $self->info("Connecting to Authen...");
  my $info=0;
  for (my $tries = 0; $tries < 5; $tries++) { # try five times 
    $info=$self->{SOAP}->CallSOAP("Authen", "consultAuthenService", $user, @_) and last;
    $self->info("Connecting to the [Authen] service was not seccussful, trying ".(4 - $tries)." more times till giving up.");
    sleep(5);
  }
  $info or $self->info("Connecting to the [Authen] service failed!") 
     and return ({error=>"Connecting to the [Authen] service failed!"}); 
  #my @authorize=$self->{SOAP}->GetOutput($info);
  #my $hash = shift @authorize; 

#  my @hasha =$self->{SOAP}->GetOutput($info);
#  my $hash=$hasha[0];
  my ($hash) =$self->{SOAP}->GetOutput($info);
  $self->info("gron: rc is $hash->{ok} and we got $hash->{envelopecount} envelopes...");
#  my $hash = shift @authorize; 
 
  my $messtype="info";
  my $message="Authorize replied ";
  if($hash->{ok}) {
    $message .= "[OK]. ";
  } else {
    $messtype="error";
    $message .= "[ERROR]. ";
  }
  $hash->{interrupt} and $message .= "Thrown [INTERRUPT]! " ;
  $hash->{message} and  $message .= "Message: $hash->{message} ";
  $self->tracePlusLogError($messtype,$message);
  
 return $hash;
 # my @envelopes = ();
 # for (my $c = 0; $c < $hash->{envelopecount}; $c++) {
 #    push @envelopes, $hash->{envelopes}->{($c+1)};
 # }
 #  return ($hash, @envelopes);
#  (scalar(@envelopes) gt 0) and return @envelopes;
#  return ($hash->{ok} || $hash->{exception} );
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
  my $db = $self->{CATALOG}->selectDatabase($parent) or return $self->authorize_return(0,"Error selecting the database of $parent","[connectToDatabse]");
  my $tableName = "$db->{INDEX_TABLENAME}->{name}";
  my $tablelfn = "$db->{INDEX_TABLENAME}->{lfn}";
  my $lfnOnTable = "$lfns";
  $lfnOnTable =~ s/$tablelfn//;
  my $size = $db->queryValue("SELECT l.size FROM $tableName l WHERE l.lfn LIKE '$lfnOnTable'") || 0;
  my $guid = $db->queryValue("SELECT binary2string(l.guid) as guid FROM $tableName l WHERE l.lfn LIKE '$lfnOnTable'") || 0;
  $filehash = $self->{CATALOG}->{DATABASE}->{GUID_DB}->checkPermission("w",$guid); # Check permissions on the GUID
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
  my $parentdir = $self->{CATALOG}->GetParentDir($lfns);
  my $filehash=$self->checkPermissionsOnLFN($parentdir,"delete","w")
          or return $self->authorize_return(0,"ERROR: checkPermissionsOnLFN failed for $parentdir","[checkPermission]");
  $filehash=$self->checkPermissionsOnLFN("$lfns/","delete","w")
          or return $self->authorize_return(0,"ERROR: checkPermissionsOnLFN failed for $lfns","[checkPermission]");
  
  #Insert into LFN_BOOKED and delete lfns
#          $self->{LFN_DB} or $self->{LFN_DB}=AliEn::Database::Catalogue::LFN->new();
  my $entries=$self->{CATALOG}->{DATABASE}->{LFN_DB}->getHostsForEntry($lfns) or return $self->authorize_return(0,"ERROR: Could not get hosts for $lfns","[getHosts]");
  my @index=();
  my $size = 0;
  my $count = 0;          
  foreach my $db (@$entries) {
          $self->info(1, "Deleting all the entries from $db->{hostIndex} (table $db->{tableName} and lfn=$db->{lfn})");
          my ($db2, $lfns2)=$self->{CATALOG}->{DATABASE}->{LFN_DB}->reconnectToIndex($db->{hostIndex}, $lfns);
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
                  my $entries=$self->{CATALOG}->{DATABASE}->{LFN_DB}->getHostsForEntry($parentdir) or 
                      return $self->authorize_return( "Error getting the hosts for '$lfns'","[getHosts]");
                  my $db=${$entries}[0];
                  my ($newdb, $lfns2)=$self->{CATALOG}->{DATABASE}->{LFN_DB}->reconnectToIndex($db->{hostIndex}, $parentdir);
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
  my $dbSource = $self->{CATALOG}->selectDatabase($parent)
                 or return $self->authorize_return(0,"Error selecting the database of $parent","[connectToDatabse]");
  my $tableName_source = "$dbSource->{INDEX_TABLENAME}->{name}";
  my $tablelfn_source = "$dbSource->{INDEX_TABLENAME}->{lfn}";
  $parent = "$target";
  my $dbTarget = $self->{CATALOG}->selectDatabase($parent)
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

   my @where=$self->{CATALOG}->f_whereis("sgztr","$guid");
   @where 
     or $self->debug(1,"There were no transfer methods....")
     and @where=$self->{CATALOG}->f_whereis("sgzr","$guid");

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

   my $catalogue = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};
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
    $filehash=$self->{CATALOG}->{DATABASE}->{GUID_DB}->checkPermission("r", $lfn, {retrieve=>"guid,type,size,md5"})
      or return $self->authorize_return(0,"access: access denied for $lfn");
    $filehash->{guid} = $lfn;
    $filehash->{lfn} = $lfn;
  } else {
    $self->info("gron: recognized lfn/guid as a LFN");
    $filehash=$self->{CATALOG}->checkPermissions("r",$lfn,undef, {RETURN_HASH=>1})
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
  my $seiostring = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow("SELECT seioDaemons FROM SE where seName like ? ;", 
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
  my $sestring = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow("SELECT seName FROM SE where seioDaemons LIKE concat ( ? , '%') ;",
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



sub versionLFN {
  my $self=shift;
  my $lfn=shift;
  my $perm=shift;
  
  my $filehash = $self->{CATALOG}->checkPermissions($perm,$lfn,undef, {RETURN_HASH=>1});
  if (!$filehash) {
    $self->info("access: access denied to $lfn");
    return;
  }

  my $parentdir = $self->{CATALOG}->f_dirname($lfn);
  my $result = $self->{CATALOG}->checkPermissions($perm,$parentdir);
  if (!$result) {
    $self->info("access: parent dir missing for lfn $lfn");
    return ;
  }

  if (($lfn ne "") && $self->{CATALOG}->existsEntry($lfn, $filehash->{lfn})) {
	$self->info( "access: lfn <$lfn> exists - creating backup version ....\n");
	my $filename = $self->{CATALOG}->f_basename($lfn);
	
	$self->{CATALOG}->f_mkdir("ps","$parentdir"."/."."$filename/") or
	  $self->info("access: cannot create subversion directory - sorry") and return;
	
	my @entries = $self->{CATALOG}->f_ls("s","$parentdir"."/."."$filename/");
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
	if (!$self->{CATALOG}->f_mv("",$lfn, $backupfile)) {
	  $self->info("access: cannot move $lfn to the backup file $backupfile");
	  return ;
	}
  }
  return $filehash;
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
  $envelope= $self->{CATALOG}->checkPermissions("w",$lfn,undef, {RETURN_HASH=>1});
  $envelope or return $self->authorize_return(0,"access: access denied to $lfn");

  #Check parent dir permissions:
  my $parent = $self->{CATALOG}->f_dirname($lfn);
  $self->{CATALOG}->checkPermissions("w",$parent) 
     or return $self->authorize_return(0,"access: parent dir missing for lfn $lfn");
  # gron: still to be discussed what we do to the tree below ... 

  my $reply = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
      "SELECT lfn FROM LFN_BOOKED WHERE lfn=? ;"
      , undef, {bind_values=>[$lfn]});
  $reply->{lfn} and  $reply = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
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

  ($envelope->{turl}, my $guid, my $se) = $self->{CATALOG}->createFileUrl($envelope->{se}, "root", $envelope->{guid});
  $se 
    or $self->info("Creating a default pfn (for $envelope->{guid})")
    and ($envelope->{turl}, $guid)=$self->{CATALOG}->createDefaultUrl($envelope->{se}, $envelope->{guid},$envelope->{size});
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
  $envelope=$self->{CATALOG}->{DATABASE}->{GUID_DB}->checkPermission("w", $guid, {retrieve=>"guid,type,size,md5"})
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
  my $se=(shift || return 0);

  $envelope->{lfn} or return  $self->authorize_return(0,"The access to registering a PFN with LFN $envelope->{lfn} could not be granted.");
  my $size=AliEn::SE::Methods->new($pfn)->getSize();
  $size or return $self->authorize_return(0, "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn could not be registered. We won't insert a zero sized file.");
  my $md5 = 0; #gron
  #my $md5=AliEn::MD5->new($pfn); #gron
  #$md5 or return $self->authorize_return(0, "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn could not be registered. We couldn't get a md5 for the file.");
  $se or $se=$self->getSEforPFN($pfn);
  $se or return $self->authorize_return(0, "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $pfn could not be registered. The PFN doesn't correspond to any known SE.");
 
  $self->{CATALOG}->f_registerFile( "-f", $envelope->{lfn}, $size,
           $se, $envelope->{guid}, undef,undef, $md5,
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
      $self->{CATALOG}->f_registerFile( "-f", $envelope->{lfn}, $envelope->{size},
               $envelope->{se}, $envelope->{guid}, undef,undef, $envelope->{md5}, 
                $envelope->{turl}) and $justRegistered=1 
       or $self->info("File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered.")
       and $returnMessage .= "File LFN: $envelope->{lfn}, GUID: $envelope->{guid}, PFN: $envelope->{turl} could not be registered."
       and next;
     } else {
        $self->{CATALOG}->f_addMirror( $envelope->{lfn}, $envelope->{se}, $envelope->{turl}, "-c","-md5=".$envelope->{md5})
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

  my $reply = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryRow(
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
    and $triggerstat = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
    "UPDATE LFN_BOOKED SET existing=1 WHERE lfn=? and binary2string(guid) LIKE ? and size=? and md5sum=? and owner=? and gowner=? ;",
    {bind_values=>[$envelope->{lfn},$envelope->{guid},$envelope->{size},$envelope->{md5},$user,$user]});


  $self->info("gron: UPDATE LFN_BOOKED done.");

  return ($self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
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


  return $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->do(
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
  $self->{CATALOG} and $self->{CATALOG}->{ROLE} and $user=$self->{CATALOG}->{ROLE};
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
   	$self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","$self->{CATALOG}->{ROLE}_$method", ("$packedEnvelope->{se}", $packedEnvelope->{size}) ); 		      
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

   my $catalogue = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};
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

   my $catalogue = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};

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
   my $catalogue = $self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};

   my $reply = $catalogue->query("SELECT sitename FROM SERanks WHERE sitename LIKE ?;", undef, {bind_values=>[$site]});

   (scalar(@$reply) < 1) and $self->info("We need to update the SERank Cache for the not listed site: $site")
            and return $self->execute("refreshSERankCache", $site);

   return 1;
}


sub commit {
  my $self = shift;
  my @lnewresult;
  my $newresult = \@lnewresult;
  my $envelope;
  $envelope = (shift or 0);
  my $size         = (shift or "0");
  my $lfn          = (shift or 0);
  my $perm         = (shift or $self->{UMASK});
  my $expire       = (shift or 0);
  my $CatRegPFN   = (shift or 0);
  my $se           = (shift or 0);
  my $guid         = (shift or 0);
  my $md5          = (shift or 0);
  my $newhash      = {};
  $newhash->{lfn}  = $lfn;

  push @$newresult,$newhash;

  $self->{envelopeCipherEngine}->Reset();
#    $self->{envelopeCipherEngine}->Verbose();
  $self->{envelopeCipherEngine}->IsInitialized();
  print STDERR "Decoding Envelope: \n $envelope \n";

    my $coded = $self->{envelopeCipherEngine}->decodeEnvelopePerl($envelope);
  if (!$coded) {
      $self->info("commit: error during envelope decryption");
      return;
  } else {
      $$newresult[0]->{authz} = $self->{envelopeCipherEngine}->GetDecodedEnvelope();
      my $xsimple = XML::Simple->new();
      $self->{XMLauthz} = $xsimple->XMLin($$newresult[0]->{authz},
					  KeyAttr => {lfn=> 'name'},
					  ForceArray => [ 'file' ],
					  ContentKey => '-content');
      foreach (@{$self->{XMLauthz}->{file}}) {
	  if ( ($lfn eq "$_->{lfn}") or (!$lfn)) {
	      if ($size eq "0") {
		  $size = $_->{size};
	      } 
	      if (!$lfn) {
		  $lfn = $_->{lfn};
	      }
	      if (!$CatRegPFN) {
	        #	  $CatRegPFN = $_->{CatRegPFN};
                $CatRegPFN = ($_->{turl} or "");
	      }
             
	      if (!$se) {
		  $se = $_->{se};
	      }
	      if (!$guid) {
		  $guid = $_->{guid};
	      }
	      if ($_->{access} =~ /^write[\-a-z]*/) {
		  $$newresult[0]->{$lfn} = 0;
		  $self->debug("commit: Registering file lfn=$lfn CatRegPFN=$CatRegPFN size=$size se=$se guid=$guid md5=$md5");
		  my $result = $self->f_registerFile("-md5=$md5",$lfn,$CatRegPFN, $size , $se, $guid, $perm);
		  if (!$result) {
		      $self->info("commit: Cannot register file lfn=$lfn CatRegPFN=$CatRegPFN size=$size se=$se guid=$guid md5=$md5");
		  }
		  $$newresult[0]->{$lfn} = 1;
		  # send write-commit info
		  if ($self->{MONITOR}) {
		      my @params;
		      push @params,"$se";
		      push @params,"$size";
		      $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","$self->{CATALOG}->{ROLE}_written", @params);
		  }
	      }
	  }
      }
  }

  #remove the authz information, don't send this to the client
  delete $$newresult[0]->{authz};
  return $newresult;
}

sub purge {
  my $self = shift;
  my $lfn = (shift or "");

  if ( ($lfn eq "") || ($lfn =~/^[\-]*\-h/)) {
    print STDERR "Usage: purge <lfn/ldn> \n\t - removes all previous versions of a file\n\t - if a directory is specified it purges all the files in that directory!";
    return;
  }

  my $success=1;

  my @allfiles=();

  $lfn=$self->{CATALOG}->f_complete_path($lfn);

    if ($self->{CATALOG}->isDirectory($lfn)) {
	push @allfiles, $self->{CATALOG}->f_ls("-s",$lfn);
    } else {
	my @exists = $self->{CATALOG}->f_ls("-s",$lfn);
	if ((scalar @exists) <=0) {
	    return; 
	}
	push @allfiles, $lfn;
    }
    
    foreach my $llfn(@allfiles) {
	$self->info("purge: =============================> purging file $llfn\n");
	my $parentdir = $self->{CATALOG}->f_dirname($llfn);
	my $filename  = $self->{CATALOG}->f_basename($llfn);
	my @entries = $self->{CATALOG}->f_ls("-s","$parentdir"."/."."$filename/");
	foreach (@entries) {
	    $self->info("purge: cleaning $_ for $llfn");
	    my $erasefilename = "$parentdir"."/.".$filename."/$_";
	    if (!$self->erase($erasefilename)) {
		$self->info("purge: could not purge $erasefilename");
		$success=0;
	    }
	}
    }
    if ((!$success)) {
	return;
    } else {
	return 1;
    }
}

sub erase {
    my $self = shift;
    my $lfn = (shift or "");

    if ( ($lfn eq "") || ($lfn =~/^[\-]*\-h/)) {
	print STDERR "Usage: erase <lfn> \n\t - removes physically all replicas in storage elements and the catalogue entry!\n";
	return;
    }

    $lfn=$self->{CATALOG}->f_complete_path($lfn);
    my $parentdir = $self->{CATALOG}->f_dirname($lfn);

    my $seRef = $self->{CATALOG}->f_whereisFile("s", $lfn);

    #Get the file from the LCM
    $seRef or $self->info($self->{LOGGER}->error_msg())
      and return;

    my $guid=$self->{CATALOG}->f_lfn2guid("s",$lfn)
	or $self->info("Error getting the guid of $lfn",11) and return;
        
    my (@seList ) = $self->selectClosestSE({}, @$seRef);

    my $failure=0;
    while (my $se=shift @seList) {
#	my $pfn=$self->getPFNfromGUID($se, $guid);
#	$pfn or next;

	my (@envelope) = $self->authorize("-s","delete","$lfn",$se);

	if ((!defined $envelope[0]) || (!defined $envelope[0]->{envelope})) {
	    $self->info("Cannot get access to $lfn for deletion @envelope") and return;
	}
	$ENV{'IO_AUTHZ'} = $envelope[0]->{envelope};

	if (!$self->{STORAGE}->eraseFile($envelope[0]->{turl})) {
	    $self->info("Cannot remove $envelope[0]->{turl} from the storage element $se");
	    $failure=1;
	    next;
	}	
    }

    if (!$failure) {
	if (!$self->{CATALOG}->f_removeFile("-s",$lfn)) {
	    $self->info("Cannot remove lfn $lfn from the catalogue, but I deleted the file in the storage element");
	    return;
	}
    } else {
	$self->info("Could not remove all replicas of lfn $lfn - keeping catalogue entry" );
	return ;
    }
    return 1;
}

sub tracePlusLogError{
  my $self=shift;
  my $message=(shift || return 0); 
  my $flag=(shift || return 0);
  ($flag eq "error") && $self->info("ERROR $message");
  if ($flag eq "info")  {
    ($self->info("$message") and $flag="trace");
  } else {
    ($flag eq "info") || $self->debug(2,$message);
  }
  $self->{tracelogenabled} and push @{$self->{tracelog}}, {flag=>$flag, text=>$message};
  return 1;
}

sub traceLogFlush{
  my $self=shift;
  $self->{tracelogenabled} and @{$self->{tracelog}} = ();
  return 1; 
} 

sub addFileToSEs {
  my $self=shift;
  my $targetLFN   = (shift || return {ok=>0,tracelog=>\@{$self->{tracelog}}});
  my $sourcePFN   = (shift || return {ok=>0,tracelog=>\@{$self->{tracelog}}});
  my $SErequirements=(shift || []);
  my $guid=(shift || 0); # gron: guid is to be handeled
  my $silent=(shift || 0);
  my $user=(shift || $self->{CATALOG}->{ROLE});

  my $result = {};
  my @ses = ();
  my @excludedSes = ();
  my @qosList;

  $self->traceLogFlush();

  foreach my $d ((split(",", join(",",@$SErequirements)))) {
    if (AliEn::Util::isValidSEName($d)) {
        $d=uc($d);
        grep (/^$d$/,@ses) or push @ses, $d;
        next;
    }
    elsif (($d =~ s/!//) and (AliEn::Util::isValidSEName($d))) {
        $d=uc($d);
        grep (/^$d$/,@excludedSes) or push @excludedSes, $d;
        next;
    }
    elsif ($d =~ /\=/){
      grep (/^$d$/, @qosList) or push @qosList, $d;
      next;
    } else {
       $self->tracePlusLogError("info","WARNING: Found the following unrecognizeable option:".$d);
    }
    $d =~ /^\s*$/ and next;
    $self->tracePlusLogError("info","WARNING: Found the following unrecognizeable option:".$d);
  }
  my $maximumCopyCount = 9;
  my $selOutOf=0;
  $self->debug(4,"Saving in @ses, ignoring @excludedSes, and using @qosList");
  push @excludedSes, @ses;
  my $totalCount = 0;
  my $qosTags;
  foreach (@qosList) {
    my ($repltag, $copies)=split (/\=/, $_,2);
    $copies and (isdigit $copies) or next;
    ($totalCount+$copies) < $maximumCopyCount
      or $copies = $maximumCopyCount - $totalCount;
    
    if($repltag eq "select") {
      ($copies < 1 or $copies > scalar(@ses))
	and $copies = scalar(@ses);
      $selOutOf = $copies;
    } else {
      $qosTags->{$repltag} = $copies
    }
    $totalCount += $copies;
    
  }
  # if select Out of the Selist is not correct
  $selOutOf eq 0 and $selOutOf = scalar(@ses) and $totalCount += $selOutOf;
  #if nothing is specified, we get the default case, priority on LDAP entry
  if($totalCount le 0 and $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT}) {
    my ($repltag, $copies)=split (/\=/, $self->{CONFIG}->{SEDEFAULT_QOSAND_COUNT},2);
    $qosTags->{$repltag} = $copies;
    $totalCount += $copies;
  }
  ##### here initialized 
  $result->{usedEnvelopes} = [];
  my $success = 0;
  my $counter=0;
  $sourcePFN=$self->checkLocalPFN($sourcePFN);
  my $size=AliEn::SE::Methods->new($sourcePFN)->getSize();
  my $md5=AliEn::MD5->new($sourcePFN);
  ((!$size) or ($size eq 0)) and $self->tracePlusLogError("error", "The file $sourcePFN has size 0. We won't upload an empty file.") and return {ok=>0,tracelog=>\@{$self->{tracelog}}};
  
  foreach my $qos(keys %$qosTags){
    ($success eq -1) and last;
    $self->tracePlusLogError("info","Uploading file based on Storage Discovery, requesting QoS=$qos, count=$qosTags->{$qos}");

    ($result, $success) = $self->putOnDynamicDiscoveredSEListByQoSV2($result,$user,$sourcePFN,$targetLFN,$size,$md5,"write",$qosTags->{$qos},$qos,$self->{CONFIG}->{SITE},\@excludedSes);
  }
  
  if (($success ne -1) and (scalar(@{$result->{usedEnvelopes}}) le 0) and (scalar(@ses) eq 0) and ($selOutOf le 0)){ 
    # if dynamic was either not specified or not successfull (not even one time, thats the @{$result->{usedEnvelopes}}
    $selOutOf= 1;
    push @ses, $self->{CONFIG}->{SE_FULLNAME};   # and there were not SEs specified in a static list, THEN push in at least the local static LDAP entry not to loose data
    $self->tracePlusLogError("info","SE Discovery is not available, no static SE specification, using CONFIG->SE_FULLNAME as a fallback to try not to lose the file.");
    $totalCount=1;  # if there was simpy no specification, totalCount wasn't set before, so we set it here, if SEdyn fails, it will have already its value.
  }
  my $staticmessage = "Uploading file to @ses (based on static SE specification).";
  ($selOutOf ne scalar(@ses)) and $staticmessage = "Uploading file to @ses, with select $selOutOf out of ".scalar(@ses)." (based on static SE specification).";
  (scalar(@ses) gt 0) and $self->tracePlusLogError("info",$staticmessage);

  (($success ne -1) && (scalar(@ses) gt 0)) and ($result, $success) = $self->putOnStaticSESelectionListV2($result,$user,$sourcePFN,$targetLFN,$size,$md5,"write",$selOutOf,\@ses);

  (scalar(@{$result->{usedEnvelopes}}) gt 0) or $self->tracePlusLogError("error","We couldn't upload any copy of the file.") and return {ok=>0,tracelog=>\@{$self->{tracelog}}};
  
  my $authenReply = $self->authorize("-user=$user", "registerenvs", @{$result->{usedEnvelopes}});
  my @regSuccessMap = @{$authenReply->{envelopes}};

  (scalar(@regSuccessMap) gt 0) or return {ok=>0,tracelog=>\@{$self->{tracelog}}};

  for (0 .. (scalar(@{$result->{usedEnvelopes}})-1)) {
    ($regSuccessMap[$_]) and $counter++  and
         $self->tracePlusLogError("error",getValFromEnvelope($result->{usedEnvelopes}[$_],"pfn")." could not be registered correctly");
  }
  
  if ($totalCount eq $counter){
      $success=1;
      $self->tracePlusLogError("info","OK. The file $targetLFN  was added to $totalCount SEs as specified. Superb!");
  } elsif($counter > 0) {
      $self->tracePlusLogError("info","WARNING: The file $targetLFN was added to ".scalar(@{$result->{usedEnvelopes}})." SEs, yet specified were to add it on $totalCount!");
  } else {
      $self->tracePlusLogError("error","ERROR: Adding the file $targetLFN failed completely!");
  }
  # -1 means a access exception, e.g. exceeded quota limit
  # This will trigger the JobAgent to stop trying further write attempts.
  return  {ok=>$success,tracelog=>\@{$self->{tracelog}}};

}

sub putOnStaticSESelectionListV2{
   my $self=shift;
   my $result=(shift || {});
   my $user=(shift || 0);
   my $sourcePFN=(shift || "");
   my $targetLFN=(shift || "");
   my $size=(shift || 0);
   my $md5=(shift || 0);
   my $envreq=(shift || "");
   my $selOutOf=(shift || 0);
   my $ses=(shift || "");
   my $success=0;

   $selOutOf eq 0 and  $selOutOf = scalar(@$ses);
   while ((scalar(@$ses) gt 0 and $selOutOf gt 0)) {
     (scalar(@$ses) gt 0) and my @staticSes= splice(@$ses, 0, $selOutOf);
     ($selOutOf = scalar(@$ses)) or
     $self->tracePlusLogError("info","We select out of a supplied static list the SEs to save on: @staticSes, count:".scalar(@staticSes));

     my $authenReply = $self->authorize("-user=$user", $envreq, $targetLFN, join(";", @staticSes), $size, $md5, ($result->{guid} || 0));
     
     ((!@{$authenReply->{envelopes}}) || (scalar(@{$authenReply->{envelopes}}) eq 0)) and
           $self->tracePlusLogError("error","We couldn't get envelopes for any of the SEs, @staticSes .") and return($result, $success);
     (scalar(@{$authenReply->{envelopes}}) eq scalar(@staticSes)) or
        $self->tracePlusLogError("info","We couldn't get all envelopes for the SEs, @staticSes .");

     foreach my $envelope (@{$authenReply->{envelopes}}){
       (my $res, $result) = $self->uploadFileAccordingToEnvelope($result, $sourcePFN, $envelope);
       $res && push @{$result->{usedEnvelopes}}, $envelope->{signedEnvelope}; 
       $selOutOf = $selOutOf - $success;
     }

   }
   return ($result, $success);
}  



sub putOnDynamicDiscoveredSEListByQoSV2{
   my $self=shift;
   my $result=(shift || {});
   my $user=(shift || 0);
   my $sourcePFN=(shift || "");
   my $targetLFN=(shift || "");
   my $size=(shift || 0);
   my $md5=(shift || 0);
   my $envreq=(shift || "");
   my $count=(shift || 0);
   my $qos=(shift || "");
   my $sitename=(shift || "");
   my $excludedSes=(shift || []);
   my $success=0;
   my @successfulUploads = ();

   while($count gt 0) {
     my $authenReply = $self->authorize("-user=$user", $envreq, $targetLFN, 0, $size, $md5, ($result->{guid} || 0), $sitename, $qos, $count, (join(";", @$excludedSes) || 0));

     ((!@{$authenReply->{envelopes}}) || (scalar(@{$authenReply->{envelopes}}) eq 0)) and
         $self->tracePlusLogError("error","We couldn't get any envelopes (requested were '$count')  with qos flag '$qos'.") and return($result, $success);
     (scalar(@{$authenReply->{envelopes}}) eq $count) or
         $self->tracePlusLogError("trace","We could get only scalar(@{$authenReply->{envelopes}}) envelopes (requested were '$count') with qos flag '$qos'.");

     foreach my $envelope (@{$authenReply->{envelopes}}){
       (my $res, $result) = $self->uploadFileAccordingToEnvelope($result, $sourcePFN, $envelope);
       push @$excludedSes, $envelope->{se};
       $res or next;
       push @{$result->{usedEnvelopes}}, $envelope->{signedEnvelope}; 
       $count--;
     }
  }
  return ($result, $success);   
}

sub uploadFileAccordingToEnvelope{
  my $self  = shift;
  my $result = (shift || {});
  my $sourcePFN   = (shift || 0);
  my $envelope= ( shift || return (0,$result) );

  $result->{guid} and $self->info("File has guid: $result->{guid}") or $result->{guid} = "";
  ($sourcePFN) or $self->{LOGGER}->warning( "LCM", "Error no pfn specified" )
    and $self->tracePlusLogError("error","Error: No PFN specified [uploadFileAccordingToEnvelope]")
    and return ($result,0,[]) ;
  
     $ENV{ALIEN_XRDCP_ENVELOPE}=$envelope->{envelope};
     $ENV{ALIEN_XRDCP_URL}=$envelope->{turl};

     my $start=time;
     $self->debug(2, "We will upload the file $sourcePFN to $envelope->{se}" );
     $self->tracePlusLogError("info", "We will upload the file $sourcePFN to $envelope->{se}");

     my $res;
     my $z = 0;
     while ($z < 5 ) {   # try five times in case of error
          $res= $self->{STORAGE}->RegisterInRemoteSE($sourcePFN, $envelope);
          $res and $z = 6 or $z++;
     }

     my $time=time-$start;
     $self->sendMonitor("write", $envelope->{se}, $time, $envelope->{size}, $res);

     $res and $res->{pfn} and return (1,$res);

     $res or 
       $self->tracePlusLogError("error", "ERROR storing $sourcePFN in $envelope->{se}, Message: ".$self->{LOGGER}->error_msg );

     $res or print STDERR "ERROR storing $sourcePFN in $envelope->{se}\n";

  return (0,$result);
}

sub sendMonitor {
  my $self=shift;
  my $access=shift;
  my $se=shift|| "";
  my $time=shift;
  my $size=shift || 0;
  my $ok=shift;
  $self->{MONITOR} or return 1;

  my @params=('time', $time); 

  if ($ok){
    $time or $time=1;
    push @params, 'size', $size, status=>1, speed=>$size/$time;
  } else {
    push @params, 'status', 0;
  }
  $self->debug(1,"Sending to monalisa @params (and $se)");


  my $node="PROMPT_$self->{CONFIG}->{HOST}";
  $ENV{ALIEN_PROC_ID} and push @params, 'queueid', $ENV{ALIEN_PROC_ID} 
    and $node="JOB_$ENV{ALIEN_PROC_ID}";

  $self->{MONITOR}->sendParameters(uc("SE_${access}_${se}"), $node, @params);
  return 1;
}
  





#sub arrayEliminateDuplicates {
#   shift;
#   my $array=shift;
#   my @unique = ();
#   my %seen   = ();#
#
#   foreach my $elem ( @$array )
#   {
#     next if $seen{ $elem }++;
#     push @unique, $elem;
#   }
#   return \@unique;
#}





sub stage_HELP {
  return "stage: Sends a message to the SE to bring a copy of the file to its cache.

Usage:

\tstage [-a] <lfn>

Options:
   -a: Send a message to all the SE that have that LFN
";
}

sub stage {
  my $self=shift;
  my $options=shift;
  my $lfn=shift;
  $lfn or $self->info("Error: not enough arguments in stage:" .$self->stage_HELP()) and return;
  $self->info("Ready to stage the files $lfn @_");
  $self->{CATALOG} or $self->info("We don't have a catlogue... can't stage") and return;
  my @info= $self->{CATALOG}->f_whereis("rs", $lfn);
  my $return={};
  while (@info){
    my ($se, $pfn)=(shift @info, shift @info);
    $self->info("Staging the copy $pfn in $se");
    if ($pfn eq "auto") {
      $self->info("I'm not sure how to stage this file... The SE knows its path  Maybe I should ask the SE....");
      next;
    }
    my $url=AliEn::SE::Methods->new($pfn)
      or $self->info("Error building the url from '$pfn'") and next;
    $url->stage();
  }
  return 1;
}


sub isStaged {
  my $self=shift;
  $self->info("Ready to look if a file is staged @_");
  return 1;
}

#############################################################################################################

sub find_HELP{
  my $self=shift;
  return $self->{CATALOG}->f_find_HELP()."  -a => Put all the files in an archive - 2nd arg is archive name\n";
}

sub find {
  my $self=shift;
  $self->debug(1, "I'm in the find of LMC");
  my $options={archive=>""};
  Getopt::Long::Configure("pass_through");
  @ARGV=@_;
  Getopt::Long::GetOptions($options,"archive=s" )  or 
      $self->info("Error getting the options") and return;
  @_=@ARGV;
  Getopt::Long::Configure("default");
  my $i=0;
  while (defined $_[$i]){
    $_[$i] =~ /^-[xl]/  and $i+=2 and next;
    $_[$i] =~ /^-/ or last;
    $i++;
  }
  my $dir=$_[$i];


  my @result=$self->{CATALOG}->f_find(@_);
  if ($options->{archive}){
    $self->info("Putting the archive in $options->{archive}");
    $self->zip("-d", $dir, $options->{archive}, @result) or return;
  }
  return @result;
}
use Getopt::Std;

sub zip_HELP{
  return "zip: create a zip archive. 
Usage:
           zip [options] <archiveName> <file1> [<file2> ...]

Possible options:
        -d <directory> :remove <directory> from the beginning of the path
";
}
sub zip {
  my $self=shift;
  my %options=();
  @ARGV=@_;
  getopts("rd:",\%options);
  @_=@ARGV;
  my $lfn=shift;

  my @files;
  if ($options{r}){
    $self->debug(2, "Checking if any of the entries is a directory");
    foreach my $entry (@_){
      if ($self->{CATALOG}->isdirectory($entry)){
	push @files, $self->{CATALOG}->f_find( $entry, "*");
      }else {
	push @files, $entry;
      }
    }
  } else{
    @files=@_;
  }
  $lfn= $self->{CATALOG}->f_complete_path($lfn);

  $self->_canCreateFile($lfn) or return;


  @files or $self->info("Error not enough arguments in zip") and return;
  $self->info("We should put the files in the zip archive");
  my $zip=Archive::Zip->new();
  $options{d}  and $self->info("Taking '$options{d}' out of the path name");
  foreach my $file (@files) {
    my $lfnFile=$file;
    $options{d} and $lfnFile=~ s{$options{d}}{};
    $self->info("Getting the file $file (saving as $lfnFile)");
    my ($localfile)=$self->execute("get", "-silent", $file) 
      or $self->info("Error getting the file $file") and return;
    $zip->addFile($localfile, $lfnFile) or 
      $self->info("Error adding the file $localfile to the archive") and return;
  }
  my $myName=AliEn::TMPFile->new();
  $zip->writeToFileNamed($myName) == AZ_OK 
    or $self->info("Error creating the zip archive $myName") and return;
  $self->debug(1, "Ok, time to add the archive to the catalogue"); 
  $self->execute("add", "-silent", $lfn, $myName) or 
    $self->info("Error adding the file to the catalogue") and return;

  $self->info("Archive zip file $lfn created!! ");

  return 1;
}

sub unzip {
  my $self=shift;
  my $lfn=shift;
  my $pwd=shift;
  $self->info("Getting the file $lfn from the catalogue and unziping ");
  my ($localfile)=$self->execute("get", "-silent", $lfn)
    or $self->info("Error getting the entry $lfn from the catalogue") and return;
  my $zip=Archive::Zip->new($localfile) 
    or $self->info("Error reading the file $localfile (are you sure it is a zip archive? )") and return;

  if  ($pwd) {
    $self->info("Extracting in $pwd");
    if (! -d $pwd){
      $self->info("Creating the directory $pwd");
      my $dir;
      foreach ( split ( "/", $pwd ) ) {
	$dir .= "/$_";
	mkdir $dir, 0755;
      }
    }
    chdir $pwd or $self->info("Error going to $pwd: $!") and return;
  }

  $zip->extractTree() == AZ_OK
    or $self->info("Error extrracting the files from the archive") and return;


  $self->info("File extracted!");
  return 1;
}

sub getLog_HELP{
  return "getLog: returns the log file of the service

Syntax:
    getLog [<options>] <site> <service>

"
}
sub getLog{
  my $self=shift;
  my $options={};
  @ARGV=@_;
  Getopt::Long::GetOptions($options, "help","tail=i", "grep=s", "head=i" )
      or $self->info("Error checking the options of getLog") and return;
  @_=@ARGV;

  $options->{help} and $self->info($self->getLog_HELP()) and return 1;
  my $site=shift;
  my $service=shift;

  ($service and $site) or 
    $self->info("Error not enough arguments in getLog\n".$self->getLog_HELP()) and return ;
  $service =~ /\./ or $service.=".log";
  if (! $self->{SOAP}->{"cm_$site"}){
    $self->info("Getting the log of $site");
    my $address=$self->{SOAP}->CallSOAP("IS", "getService", $site, "ClusterMonitor")
      or $self->info("Error getting the address of $site");
    my $output=$address->result;
    my $host=$output->{HOST};
    $output->{PORT} and $host.=":$output->{PORT}";
    $host=~ m{://} or $host="http://$host";
    $output->{uri}=~ s{::}{/}g;
    $self->{SOAP}->Connect({address=>$host,
			    uri=>$output->{uri},
			    name=>"cm_$site"}) 
      or $self->info("Error connecting to $host")and return;
  }
  my $log=$self->{SOAP}->CallSOAP("cm_$site", "getFileSOAP", $service, "LOG_DIR")
    or $self->info("Error getting the log $service") and return;
  my $output=$log->result;
  $self->info("$output\n");
  return 1;


}

sub createCollection_HELP{
  return "createCollection: adds a new entry to the catalogue that is a new collection
Usage:
    createCollection <lfn> [<guid> [<xmlFile>]]

An empty collection will be created, with the guid <guid> (if specified). 
If the xmlFile was also specified, the collection will be filled with the files specified in the xml file
";

}

sub createCollection{
  my $self=shift;
  my $options={};
  @ARGV=@_;
  Getopt::Long::GetOptions($options,  "xml=s") or 
      $self->info("Error parsing the options") and return;;
  @_=@ARGV;
  my $lfn=shift;
  my $guid=shift;


  my $collection=$self->{CATALOG}->f_createCollection($lfn, $guid) or return;

  if ($options->{xml}){
    $self->info("And now, let's populate the collection");
    eval {
      my ($localFile)=$self->get($options->{xml}) or 
	die("Error getting $options->{xml}");
      $self->{DATASET} or $self->{DATASET}=AliEn::Dataset->new();
      my $dataset=$self->{DATASET}->readxml($localFile) or 
	die ("Error creating the dataset from the collection $options->{xml}");
      foreach my $entry (keys %{$dataset->{collection}->{event}}) {
	foreach my $file (keys %{$dataset->{collection}->{event}->{$entry}->{file}}){
	  my $hash=$dataset->{collection}->{event}->{$entry}->{file}->{$file};
	  my $lfn=$hash->{lfn};
	  my $info="";
	  foreach my $i (keys %{$hash}){
	    $i =~ /^(turl)|(lfn)$/ and next;
	    $info.="$i=$hash->{$i} ";
	  }
	  $self->{CATALOG}->f_addFileToCollection("-n", $lfn, $collection, $info, )
	    or die("Error adding $lfn (with info '$info') to the collection\n");
	}
      }
      $self->{CATALOG}->updateCollection("s", $collection);
    }
  };
  if ($@){
    $self->info("Error populating the collection: $@",1);
    $self->{CATALOG}->f_removeFile($collection);
    return;
  }

  return 1;
}

sub updateCollection_HELP{
  return "updateCollection: Check the consistency of a collection. 

Usage:
\tupdateCollection [<options>] <collection_name>

Possible options:
\t\t-s: silent
\t\t-c: collocate. Make sure that all the files are in one SE
By default, it checks the SE that contains all the files of the collection and the size of the collection
";
}

sub updateCollection {
  my $self=shift;
  my $options=shift;
  my $info=$self->{CATALOG}->updateCollection($options, @_) or return;
  if ($options =~ /c/){
    my $collection=$info->{collection};
    my $total=$info->{total};
    my $files=-1;
    my $bestSE;
    $self->info("Checking if a SE already contains all the files");
    foreach my $se (keys %$info){
      $se=~ /^(total)|(collection)$/  and next;

      if ( $files < $info->{$se}){
	$bestSE=$se;
	$files=$info->{$se};
      }
      $files eq $total and 
	$self->info("The se $se has all the files") and return 1;
    }
    $self->info("The SE that contains most of the files is $bestSE ($files out of $total)");
    $self->execute("mirror", "-u", $collection, $bestSE);

  }
  return 1;
}



sub why {
  my $self=shift;
  my $options=shift;
  my @because=("Why not?",
  		"You should know better",
		"It was in the DB",
		"I don't know, I'm not from here..",
		"I was an idea from Matlab");
  my $length = $#because + 1;
  $length = rand($length);
  $length = int $length;
  print "$because[$length]\n"; 
  return 1;
}

sub masterSE_HELP{
  return "masterSE: Manage SE.

Usage:

    masterSE  <SENAME> [<action> [<arguments>]]

where:
    action can be list(default), replicate, print  or erase.

Possible actions:
   masterSE <SENAME> list
       Prints statistics about the usage of the SE

   masterSE <SENAME> print [-lfn] [-md5] [-unique][-replicated] [<filename>]
       Creates a filename with all the pfns on that SE. If -lfn is present, writes the lfn 

   masterSE <SENAME> replicate  [-all]   <SE destination>
       Moves the entries from this SE to another one. By default, it copies only the entries that are not replicated already

   masterSE <SENAME> collection [<filename>]
       Creates a collection with all the entries of the SE

   masterSE <SENAME> remove [-unique] [-replicated] [-all]
       Removes all the entries of that SE in the catalogue

   masterSE  broken [-calculate] [-recover <sename>] [<dir>]
       Prints all the lfns in the catalogue that do not have a pfn

Common options:
   -unique: Do the action only for files that are not replicated in another SE
   -replicated: Do the action only for files that are replicated in another SE

";

}

sub masterSE {
  my $self=shift;
  my $sename=shift;
  my $action;
  if ($sename=~ /broken/ ){
    $action=$sename;
  } else {
    $action=shift || "list";
  }


  if ($action =~ /^list$/i){
    my $info=$self->{CATALOG}->masterSE_list($sename, @_) or return;
    $self->info("The SE $sename has:
  $info->{referenced} entries in the catalogue.
  $info->{replicated} of those entries are replicated
  $info->{broken} entries not pointed by any LFN");
    if ($info->{guids}){
	$self->info("And the guids are:".Dumper($info->{guids}));
      }
	
    return $info;
   } elsif ($action=~ /^replicate$/i){
    $self->info("Let's replicate all the files from $sename that do not have a copy");
    my $options={unique=>1,lfn=>1};
    grep (/^-all$/i, @_) and delete $options->{unique};
    my $counter=$self->executeInAllPFNEntries($sename, $options, "masterSEReplicate");
    $self->info("$counter transfers have been issued. You should wait until the transfers have been completed");

    return $counter
  } elsif ($action=~ /print/){
    my $options={};
    @ARGV=@_;
    Getopt::Long::GetOptions($options, "-lfn", "-md5", "-unique", "-replicated")
	or $self->info("Error in masterSE print. Unrecognize options") and return;
    @_=@ARGV;

    my $output=shift || "$self->{CONFIG}->{TMP_DIR}/list.$sename.txt";
    $self->info("Creating the file $output with all the entries");
    open (FILE, ">$output") or $self->info("Error opening $output") and return;
    my $counter=$self->executeInAllPFNEntries($sename, $options, "masterSEprint",\*FILE, $options);
    close FILE;
    return $counter;
  } elsif ($action =~ /broken/){
    $self->info("Printing all the broken lfn");
    my $options={};
    @ARGV=@_;
    Getopt::Long::GetOptions($options, "calculate", "recover=s", "rawdata", "nopfn")
        or $self->info("Error checking the options of masterSE broken") and return;
    @_=@ARGV;

    my $entries=$self->{CATALOG}->getBrokenLFN($options, @_);
    
    $options->{recover} and $self->masterSERecover($options->{recover},$entries, $options);
    my $t=$#$entries+1;
    $self->info("There are $t broken links in the catalogue");
    return $entries;
  } elsif ($action =~ /collection/){
    $self->info("Let's create a collection with those files (@_)");
    my $options={lfn=>1};
    @ARGV=@_;
    Getopt::Long::GetOptions($options, "-unique", "-replicated")
	or $self->info("Error in masterSE print. Unrecognize options") and return;
    @_=@ARGV;
    my $collection=shift || "collection.$sename";
    $self->execute("createCollection",$collection) or return;
    my $counter=$self->executeInAllPFNEntries($sename, $options, "masterSEcollection", $collection);
    $self->execute("updateCollection", $collection);
    $self->info("Collection created with $counter files");
    return 1;
  } elsif($action=~ /^remove$/){
    $self->info("Removing the entries from the se $sename");
    my $options={replicated=>1};
    @ARGV=@_;
    Getopt::Long::GetOptions($options, "-unique", "-replicated", "-all")
	or $self->info("Error in masterSE print. Unrecognize options") and return;
    @_=@ARGV;

    $options->{all} and delete $options->{replicated};
    my $counter=$self->executeInAllPFNEntries($sename, $options, "masterSERemove", $sename);
    $self->info("$counter entries removed");
    if ($options->{all}){
      $self->info("Since all the entries have been removed, removing also the SE");
      $self->{CATALOG}->{DATABASE}->removeSE($sename);
    }
    return $counter;
  }
  $self->info("Sorry, I don't understand 'masterSE $action'");
  return undef;

}

sub masterSERemove{ 
  my $self=shift;
  my $info=shift;
  my $sename=shift;
  $self->execute("deleteMirror", "-g", $info->{guid}, $sename, $info->{pfn});
  $self->info("Mirror deleted");
  return 1;
}
sub masterSEcollection {
  my $self=shift;
  my $guid=shift;
  my $collection=shift;

  $self->debug (1, "We have to add $guid->{lfn} to $collection");

  return $self->execute("addFileToCollection", $guid->{lfn}, $collection, "-n");

}
sub masterSERecover {
  my $self=shift;
  my $sename=shift;
  my $entries=shift;
  my $options=shift;

  $self->{GUID} or $self->{GUID}=AliEn::GUID->new();

  my $seprefix=$self->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryValue('select concat(seioDaemons,"/",seStoragePath) from SE where sename=?', undef, {bind_values=>[$sename]});

  $seprefix or $self->info("Error getting the prefix of $sename") and return;
  $seprefix =~ s{/?$}{/};
  $self->info("SE $sename -> $seprefix");

  foreach my $f(@$entries){
    $self->info("Recovering the entry $f");
    my ($guid)=$self->execute("lfn2guid", $f);
    $guid=lc($guid);

    my $pfn="";
    if ($options->{rawdata}){
      $f=~ m{([^/]*)$} or next;
      my $basename=$1;
      open (FILE, "grep $basename /tmp/list|");
      my $line=<FILE>;
      close FILE;
      print "'$line'\n";
      chomp $line;
      $pfn="root://voalice08.cern.ch/$line"
    }else{
      my $ff=sprintf("%02.2d",  $self->{GUID}->GetCHash($guid));
      my $f2=sprintf("%05.5d",  $self->{GUID}->GetHash($guid));

      $pfn="$seprefix$ff/$f2/$guid";
    }
    $self->info("Checking if $pfn exists");

    system("xrdstat $pfn > /dev/null 2>&1") and next;
    my ($size)=$self->execute("ls", "-la", $f);
    $size=~s/^[^#]*###[^#]*###[^#]*###(\d+)###.*$/$1/;
    $self->info("Ready to recover $f and $pfn and $size");
    if (! $self->execute("addMirror" , $f, $sename, $pfn)){
      $self->info("Error adding the mirror. Let's try putting the guid as well");
      if ($self->execute("register", "${f}_new", $pfn, $size, $sename, $guid)){
	$self->execute("rm", "${f}_new");
      }
    }

  }
  return 1;

}


sub masterSEReplicate{
  my $self=shift;
  my $guid=shift;
#  my ($lfn)=$self->execute("guid2lfn", $guid->{guid}) or return;

  return $self->execute("mirror", $guid->{lfn}, "alice::subatech::se");
#  return 1;

}

sub masterSEprint {
  my $self=shift;
  my $entries=shift;
  my $FILE=shift;
  my $options=shift;
  my $print =$entries->{pfn};
  ($options->{lfn}) and $print.=" $entries->{lfn}";
  ($options->{md5}) and $print.=" $entries->{md5}";
  print $FILE "$print\n";

  return 1;
}

sub executeInAllPFNEntries{
  my $self=shift;
  my $sename=shift;
  my $options=shift;
  my $function=shift;
  my $counter=0;
  my $limit=1000;
  my $repeat=1;
  my $previous_table="";

  while ($repeat){
    $self->info("Reading table $previous_table and $counter");
    (my $entries, $previous_table)=
      $self->{CATALOG}->masterSE_getFiles($sename, $previous_table,$limit, $options);
    $repeat=0;
    $previous_table and $repeat=1;
    $self->info("GOT $#$entries files");
    foreach my $g (@$entries){
      $self->debug(1, "Doing $function with $g ($counter)");
      $self->$function($g, @_);
      $counter++;
    }
  }
  return $counter;
  
}

sub fquota_HELP {
	my $self=shift;
  my $whoami=$self->{CATALOG}->{ROLE};
  if (($whoami !~ /^admin(ssl)?$/)) {
	  return "fquota: Displays information about File Quotas.
Usage: 
  fquota list [-<options>]        - show the user quota for file catalogue
Options:
  -unit = B|K|M|G: unit of file size\n";
	}

	return "fquota: Displays and modifies information about File Quotas.
Usage:
  fquota list [-<options>] <user>   - list the user quota for file catalogue
                                      use just 'fquota list' for all users
Options:
  -unit = B|K|M|G: unit of file size

  fquota set <user> <field> <value> - set the user quota for file catalogue
                                      (maxNbFiles, maxTotalSize(Byte))
                                      use <user>=% for all users\n";
}

sub fquota {
  my $self = shift;
  my $command = shift or print $self->fquota_HELP() and return;

  $DEBUG and $self->debug(1, "Calling fquota_$command");
  if (($self->{CATALOG}->{ROLE} !~ /^admin(ssl)?$/) && ($command eq "set")) {
    print STDERR "You are not allowed to execute this command!\n";
    return;
  }

  my @return;
  my $func = "fquota_$command";
  eval {
    @return = $self->$func(@_);
  };
  if ($@) {
    #If the command is not defined, just print the error message
    if ($@ =~ /Can\'t locate object method \"$func\"/) {
      $self->info( "fquota doesn't understand '$command'", 111);
      #this is just to print the error message"
      return $self->fquota();
    }
    $self->info("Error executing fquota $command: $@");
    return;
  }
  return @return;
}

sub fquota_list {
  my $self = shift;
  my $options={};
  @ARGV=@_;
  Getopt::Long::GetOptions($options, "silent", "unit=s") 
		or $self->info("Error checking the options of fquota list") and return;
  @_=@ARGV;

  #Default unit - Megabyte
	my $unit="M";
	my $unitV=1024*1024;

	$options->{unit} and $unit=$options->{unit};
	($unit !~ /[BKMG]/) and $self->info("unknown unit. use default unit: Mega Byte")
		and $unit="M";
	($unit eq "B") and $unitV=1;
	($unit eq "K") and $unitV=1024;
	($unit eq "M") and $unitV=1024*1024;
	($unit eq "G") and $unitV=1024*1024*1024;

  my $user = shift || "%";
  my $whoami = $self->{CATALOG}->{ROLE};

  # normal users can see their own information 
  if (($whoami !~ /^admin(ssl)?$/) and ($user eq "%")) {
    $user = $whoami;
  }

  if (($whoami !~ /^admin(ssl)?$/) and ($user ne $whoami)) {
    print STDERR "Not allowed to see ot
her users' quota information\n";
    return;
  }

  my $done = $self->{SOAP}->CallSOAP("Manager/Job", 'getFileQuotaList', $user);
  $done or return;
  my $result = $done->result;

  my $cnt = 0;
  printf "------------------------------------------------------------------------------------------\n";
  printf "            %12s    %12s    %42s\n", "user", "nbFiles", "totalSize($unit)";
  printf "------------------------------------------------------------------------------------------\n";
  foreach (@$result) {
    $cnt++;
		my $totalSize = ($_->{'totalSize'} + $_->{'tmpIncreasedTotalSize'}) / $unitV;
		my $maxTotalSize = $_->{'maxTotalSize'} / $unitV;;
		##Changes for unlimited file size
		if($_->{'maxTotalSize'}==-1){
		    $maxTotalSize = -1;
		}
		printf " [%04d. ]   %12s     %5s/%5s           \t %.4f/%.4f\n", $cnt, $_->{'user'}, ($_->{'nbFiles'} + $_->{'tmpIncreasedNbFiles'}), $_->{'maxNbFiles'}, $totalSize, $maxTotalSize;
  }
  printf "------------------------------------------------------------------------------------------\n";
}

sub fquota_set_HELP {
  return "Usage:
  fquota set <user> <field> <value> - set the user quota
                                      (maxNbFiles, maxTotalSize(Byte))
                                      use <user>=% for all users\n";
}

sub fquota_set {
  my $self = shift;
  my $user = shift or print STDERR $self->fquota_set_HELP() and return;
  my $field = shift or print STDERR $self->fquota_set_HELP() and return;
  my $value = shift;
  (defined $value) or print STDERR $self->fquota_set_HELP() and return;

  if ($field !~ /(maxNbFiles)|(maxTotalSize)/) {
    print STDERR "Wrong field name! Choose one of them: maxNbFiles, maxTotalSize\n";
    return;
  }

  my $done = $self->{SOAP}->CallSOAP("Manager/Job", 'setFileQuotaInfo', $user, $field, $value);
  $done and $self->fquota_list("$user");
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

  if (!$self->{PRIORITY_DB}){
    my ($host, $driver, $db) = split("/", $self->{CONFIG}->{"JOB_DATABASE"});
    $self->{PRIORITY_DB}=
      AliEn::Database::TaskPriority->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin'});
  }
  $self->{PRIORITY_DB} or $self->info("Error: couldn't connect to the priority database") and return (0, "Error connecting to the quota database.");

  my $array = $self->{PRIORITY_DB}->getFieldsFromPriorityEx("nbFiles, totalSize, maxNbFiles, maxTotalSize, tmpIncreasedNbFiles, tmpIncreasedTotalSize", "where user LIKE '$user'")
    or $self->{LOGGER}->error("Failed to get data from the PRIORITY quota table.")
    and return (0, "Failed to get data from the PRIORITY quota table. ");
  $array->[0] or $self->{LOGGER}->error("There's no entry for user $user in the PRIORITY quota table.")
    and return (-1, "There's no entry for user $user in the PRIORITY quota table.");

  my $nbFiles = $array->[0]->{'nbFiles'};
  my $maxNbFiles = $array->[0]->{'maxNbFiles'};
  my $tmpIncreasedNbFiles = $array->[0]->{'tmpIncreasedNbFiles'};
  my $totalSize = $array->[0]->{'totalSize'};
  my $maxTotalSize = $array->[0]->{'maxTotalSize'};
  my $tmpIncreasedTotalSize = $array->[0]->{'tmpIncreasedTotalSize'};
 
  $DEBUG and $self->debug(1, "size: $size");
  $DEBUG and $self->debug(1, "nbFile: $nbFiles/$tmpIncreasedNbFiles/$maxNbFiles");
  $DEBUG and $self->debug(1, "totalSize: $totalSize/$tmpIncreasedTotalSize/$maxTotalSize");
  $self->info("nbFile: $nbFiles/$tmpIncreasedNbFiles/$maxNbFiles");
  $self->info("totalSize: $totalSize/$tmpIncreasedTotalSize/$maxTotalSize");

#Implementing unlimited file quotas
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
  
  
  $self->{PRIORITY_DB}->do("update PRIORITY set tmpIncreasedNbFiles=tmpIncreasedNbFiles+1, tmpIncreasedTotalSize=tmpIncreasedTotalSize+$size where user LIKE  '$user'") or $self->info("failed to increase tmpIncreasedNbFile and tmpIncreasedTotalSize");

  $self->info("In checkFileQuota $user: Allowed");
  return (1,undef);
}


return 1;

