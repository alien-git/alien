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

use AliEn::UI::Catalogue;
use AliEn::SOAP;
use Getopt::Long;
use Compress::Zlib;
use AliEn::TMPFile;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use Data::Dumper;
use AliEn::Util;
use AliEn::PackMan;
use POSIX "isdigit";

use vars qw(@ISA $DEBUG);
@ISA = qw( AliEn::UI::Catalogue );
$DEBUG=0;
my %LCM_commands;

%LCM_commands = (
    #File Interface
		 'add' => ['$self->addFile', 0],
		 'get'      => ['$self->get', 0],
		 'access'   => ['$self->access', 2+4+8],
		 'commit'   => ['$self->commit', 0],
		 'relocate' => ['$self->relocate', 2+4+8],
		 'mirror'   => ['$self->mirror', 0],
		 'cat'      => ['$self->cat', 0],
		 'less'      => ['$self->less', 0],
		 'head'      => ['$self->head', 0],
		 'addTag'   => ['$self->addTag', 0],
		 'access'   => ['$self->access', 0],
		 'resolve'  => ['$self->resolve', 0],
		 'mssurl'   => ['$self->mssurl', 0],
		 'df'       => ['$self->df', 0],
		 'services' => ['$self->services', 0],
		 'preFetch'   => '$self->preFetch',
		 'vi'       => ['$self->vi', 0],
		 'whereis'  => ['$self->{CATALOG}->f_whereis', 66],
		 'purge'    => ['$self->purge', 0 ],
		 'erase'    => ['$self->erase', 0 ],
		 'upload'   => ['$self->upload', 0],
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
    'get'      => "\tGet a copy of the file",
    'mirror'   => "Add mirror method for a file",
    'cat'      => "\tDisplay a file on the standard output",
    'addTag'   => "Creates a new tag and asociates it with a directory",
    'add' => "\tCopies a pfn to a SE, and then registers it in the catalogue",
    'access'   => "Get an access PATH and SE for aiod access",
    'resolve'  => "Resolves the <host:port> address for services given in the <A>::<B>::<C> syntax",
    'mssurl'   => "Resolve the <protocol>://<host>:<port><path> format for the file under <path> in mss <se>",
    'df'       => "\tRetrieve Information about the usage of storage elements",
    'services' => "Retrieve Status of all services",
    'preFetch' => "Gets a list of LFN, orders them according to accessibility, and starts transfering all the files that are in remote SE",
    'vi'       => "\tGets and opens the file with vi. In case of changes the file is uploaded into the catalogue.",
    'whereis'      => "Displays the SE and pfns of a given lfn",
    'upload'   => "\tUpload a file to the SE, but does not register it in the catalog",
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


  $self->{envelopeengine} =0;
  $self->{noshuffle} = 0;


  if (defined $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'} && defined $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} && defined $ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'} and defined $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}) {
    $self->info("local private key          : $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}");
    $self->info("local public  key          : $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'}");
    $self->info("remote private key         : $ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'}");
    $self->info("remote public key          : $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}");
    require SealedEnvelope;
    
    $self->{envelopeengine} = SealedEnvelope::TSealedEnvelope->new("$ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}","$ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'}","$ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'}","$ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}","Blowfish","CatService\@ALIEN",0);
      # we want ordered results of se lists, no random
    $self->{noshuffle} = 1;


    if ($self->{MONITOR}) {
      $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","admin_readreq");
    }
    $self->{apmon} = 1;
    if (!$self->{envelopeengine}->Initialize(2)) {
      $self->info("Warning: the initialization of the envelope engine failed!!");
      $self->{envelopeengine} = 0;
    }
  }

  my $packOptions={PACKMAN_METHOD=> $options->{packman_method}|| "",
                   CATALOGUE=>$self};

  $self->{PACKMAN}= AliEn::PackMan->new($packOptions) or return;


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

Get can also be used to retrieve collections. In that case, there are some extra options:
  -c: Retrieve all the files with their original lfn name
  -b <name>: Requires -c. Remove <name> from the beginning of the lfn
  -s <se>: Retrieve the file from the se <se>

";
}
sub get {
  my $self = shift;
#  my $opt;
#  ( $opt, @_ ) = $self->Getopts(@_);
  my %options=();
  @ARGV=@_;
  getopts("gonb:clfs:", \%options) or $self->info("Error parsing the arguments of get\n". $self->get_HELP()) and  return ;
  @_=@ARGV;
  
  my $opt=join("",keys %options);
  my $file      = shift;
  my $localFile = shift;

  ($file)
    or print STDERR
"Error: not enough arguments in get\n". $self->get_HELP()
  and return;

  my $entry=$file;

  my $class="";
  $self->{CATALOG} and $class=ref $self->{CATALOG};
  my $guidInfo;
  if ($class =~ /^AliEn/ ){
    if ($options{g} ){
#      $self->debug(1, "Getting it directly from the guid '$file'");
      $guidInfo=$self->{CATALOG}->getInfoFromGUID($file)
        or return;
      $entry=$guidInfo->{guid};
    }else {
      #Get the pfn from the catalog
      #print Dumper($self->{CATALOG});
      my $info=$self->{CATALOG}->f_whereis("i",$file)
	or $self->info("Error getting the info from '$file'") and return;
      $file=$info->{lfn};
      $guidInfo=$info->{guidInfo};
      $entry=$file;
    }
  }

  my  @envelope = $self->access("-s","read",$entry) or return;

  if (!defined $envelope[0]->{envelope}) {
    $self->info( "Cannot get access to $file") and return;
  }
  $guidInfo->{guid}=$envelope[0]->{guid};
  $guidInfo->{size}=$envelope[0]->{size};
  $ENV{'IO_AUTHZ'} = $envelope[0]->{envelope};
  $guidInfo->{guid} or 
    $self->info("Error getting the guid and md5 of $file",-1) and return;

  if ($guidInfo->{type} and $guidInfo->{type} eq "c"){
    $self->info("This is in fact a collection!! Let's get all the files");
    return $self->getCollection($guidInfo->{guid}, $localFile, \%options);
  }

  
  #First, let's check the local copy of the file
  my $result=$self->{STORAGE}->getLocalCopy($guidInfo->{guid}, $localFile);
  if (! $result) {
    $self->{STORAGE}->checkDiskSpace($guidInfo->{size}, $localFile) or return;
    my $seRef = $envelope[0]->{origpfn};

    #Get the file from the LCM
    $seRef or $self->info("Error getting the list of pfns")
      and return;
    foreach my $d (@$seRef){
      $d->{seName}=$d->{se};
    }
    my (@seList ) = $self->selectClosestSE({se=>$options{'s'}}, @$seRef);
    $self->debug(1, "We can ask the following SE: @seList");

    my $sesel=1;
    while (my $entry2=shift @seList) {
      my ($se, $pfn)=($entry2->{seName}, $entry2->{pfn});
      my $start=time;
      $result = $self->{STORAGE}->getFile( $pfn, $se, $localFile, $opt, $file, $guidInfo->{guid},$guidInfo->{md5} );
      my $time=time-$start;

      if ($self->{MONITOR}){
	$self->sendMonitor('read', $se, $time, $guidInfo->{size}, $result);
      }
      $result and last;
      $self->info("Getting the copy didn't work :(. Does anybody else have the file?");
      $sesel++;
      @envelope = $self->access("-s","read",$entry, 0,0,$sesel) or return;
    }
    $result or
      $self->info("Error: not possible to get the file $file", 1) and return;
  }
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
	$self->info("error making the directory $localFile") and return;
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

sub cat {
  my $self=shift;
  my @done=$self->f_system_call("cat", @_);
  if ($done[0] =~ /1/){
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

  return $self->addFile("-v", "-md5=$md5", $reallfn, $pfn);
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
Usage: add [-v] [-g <guid>] [-s <size>] [-md5 <md5>] <lfn> <pfn> [<se>,!<se>,select=N,<qosflag>=N]\n
Possible pfns:\tsrm://<host>/<path>, castor://<host>/<path>, 
\t\tfile://<host>/<path>
If the method and host are not specified, the system will try with 'file://<localhost>'
Possible options:
\t-v:(versioning) a new version of the file is created, if it already existed\n";
#\t-c (custodial) add the file to the closest custodial se\n";
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
  Getopt::Long::GetOptions($options, "silent", "versioning", "size=i", "md5=s", "guid=s")
      or $self->info("Error checking the options of add") and return;
  @_=@ARGV;
  my $lfn   = shift;
  my $pfn   = shift;

  $pfn or $self->info("Error: not enough parameters in add\n".
                    $self->addFile_HELP(),2)  and return;
  $lineOptions=~ s/$lfn ?//;
  $lineOptions=~ s/$pfn ?//;
  $lfn = $self->{CATALOG}->f_complete_path($lfn);
  $options->{versioning} and $lineOptions=~ s/ ?-v ?/ -v=$lfn /;

  $options->{versioning} or ( $self->_canCreateFile($lfn) or return);

  my ($result)=$self->execute("upload", $pfn, $lineOptions);


  $result and $result->{status} or 
    $self->info("Error, we couldn't add/store the file on any SE!") and return;

  my $registered = $self->{CATALOG}->f_registerFile( "-f", $lfn, $result->{size},
             $result->{seref}, $result->{guid}, undef,undef, $result->{md5}, $result->{se}->{$result->{seref}}->{pfn});
   
  foreach my $se (keys(%{$result->{se}})) {
      $se ne $result->{seref} 
         and  $self->{CATALOG}->f_addMirror( $lfn, $se, $result->{se}->{$se}->{pfn}, "-c","-md5=".$result->{md5});
  }

  if ($result->{totalCount} eq scalar(keys %{$result->{se}})){
      $self->info("OK. Added the file $lfn on $result->{totalCount} SEs as specified. Superb!");
  } elsif(scalar(keys %{$result->{se}}) > 0) {
      $self->info("WARNING: Added the file to ".scalar(keys %{$result->{se}})." SEs, yet specified was to add it on $result->{totalCount}.");
  } else {
      $self->info("Error, we couldn't add/store the file on any SE!") and return;
  }
  return ($result->{status} && $registered);
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

sub addTag {
    my $self      = shift;
    ( my $opt, @_ ) = $self->Getopts(@_);
    my $directory = shift;
    my $tag       = shift;

    ($tag)
      or print STDERR
"Error: not enough arguments in addTag\nUsage: addTag  [-d] <directory> <tag name>\n\tOptions: -d create a new metadata table for this directory\n"
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
    $self->{CATALOG}->f_addTag( $directory, $tag, $description, $opt) or return;

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
  my $error=shift || "error creating the envelope";
  my $newhash;
  my @newresult;
  $newhash->{eof} = "1";
  $newhash->{error}=$error;
  push @newresult, $newhash;
  return @newresult;
}


sub getPFNforAccess {
  my $self=shift;
  my $guid=shift;
  my $se=shift;
  my $sesel=shift;
  my $lfn=shift;
  my $options=shift;

  my ($pfn, $anchor);
  my @where=$self->{CATALOG}->f_whereis("sgztr","$guid");

  if (! @where){
    $self->info("There were no transfer methods....");
    @where=$self->{CATALOG}->f_whereis("sgzr","$guid");
  }
  my @whereis=();
  foreach (@where) {
    push @whereis, $_->{se};
  }
  my $error="There where no SE for the guid '$guid'";
  $self->{LOGGER}->error_msg() and $error=$self->{LOGGER}->error_msg();
  #Get the file from the LCM
  @whereis or $self->info( "access: $error" )
    and return access_eof;

  my @closeList={};
	
  if ($se ne 0) {
    my $tmpse = "$self->{CONFIG}->{SE_FULLNAME}";
    my $tmpvo = "$self->{CONFIG}->{ORG_NAME}";
    my $tmpsite = "$self->{CONFIG}->{SITE}";
    my @tmpses = @{$self->{CONFIG}->{SEs_FULLNAME}};
    @{$self->{CONFIG}->{SEs_FULLNAME}}={};
    my ($lvo, $lsite, $lname) = split "::", $se;
    
    # change temporary the meaning of the 'local' se
    $self->{CONFIG}->{SE_FULLNAME} = $se;
    $self->{CONFIG}->{ORG_NAME} = $lvo;
    $self->{CONFIG}->{SITE} = $lsite;
    (@closeList) = $self->selectClosestSE({}, @whereis);
    $self->{CONFIG}->{SE_FULLNAME} = $tmpse;
    $self->{CONFIG}->{ORG_NAME} = $tmpvo;
    $self->{CONFIG}->{SITE} = $tmpsite;
    @{$self->{CONFIG}->{SEs_FULLNAME}}=@tmpses;
  } else {
    (@closeList) = $self->selectClosestSE({}, @whereis);
  }

  #check if the wished se is at all existing ....
  my $sefound=0;
  my $nses=scalar @closeList;
  if ($sesel > 0) {
    # the client wants a replica identified by its number
    (defined $closeList[$sesel-1]) or return access_eof;
    $se = $closeList[$sesel-1];

  } else {
    # the client wants the closest match (entry 0)
    foreach (@closeList) {
      if ( (lc $_) eq (lc $se) ) {
	$sefound=1;
	last;
      }
    }
    if (!$sefound) {
      # set the closest one
      $se = $closeList[0];
    }
    
  }

  if (! $se) {
    $self->info("access: File '$guid' does not exist in $se");
    return;
  }

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
    return ($se, $nonRoot, "", ,$lfn, $nses, \@where);
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
    $self->info("access: parsing error for $pfn [host+port]");
    return;
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
  if ($pfn=~ s/\?ZIP=(.*)$//){
    $self->info("The anchor is $1");
    $anchor=$1  
}
#  if (($urlprefix =~ /^guid/) && ($urloptions =~ /ZIP/) && ( $pfn =~ /.*\/(\w\w\w\w\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\w\w\w\w\w\w\w\w)/ )) {
#    # we got a reference guid back
#    my $newguid = $1;
#    $guid = $newguid;
    
#    $urloptions =~ /ZIP=([^\&]*)/ and $anchor=$1;
    
    
#    if ($options =~/p/) {
#      $options="ps ";
#    } else {
#      $options="s ";
#    }

#    $lfn .= "_$guid";
#    my $newanchor;
#    ($se, $pfn, $newanchor, $lfn, $nses, my $whereisr )=$self->getPFNforAccess($guid, $se, $sesel, $lfn, $options)
#      or return;
#    $self->info("The father pfn is $pfn");
#  }


  return ($se, $pfn, $anchor, $lfn, $nses, \@where);
}
sub checkPermissionsOnLFN {
  my $self=shift;
  my $lfn=shift;
  my $access=shift;
  my $perm=shift;
  
  my $filehash = $self->{CATALOG}->checkPermissions($perm,$lfn,undef, 
						    {RETURN_HASH=>1});
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
    if ($access eq "write-once")  {
      if ($self->{CATALOG}->existsEntry($lfn, $filehash->{lfn})) {
	$self->info("access: write-once but lfn $lfn exists already");
	return ;
      }
    }
    if (($access eq "write-version") && ($lfn ne "") ) {  
      if ($self->{CATALOG}->existsEntry($lfn, $filehash->{lfn})) {
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
    }
  }
  return $filehash;
}


sub access {
    # access <access> <lfn> 
    # -p create public url in case of read access 
  my $self = shift;

  if (!  $self->{envelopeengine}) {
    my $user=$self->{CONFIG}->{ROLE};
    $self->{CATALOG} and $self->{CATALOG}->{ROLE} and $user=$self->{CATALOG}->{ROLE};
    $self->info("Getting a security envelope..");
    my $info=$self->{SOAP}->CallSOAP("Authen", "createEnvelope", $user, @_)
      or $self->info("Error asking the for an envelope") and return;
    my $newhash=$info->result;
    if (!$newhash->{envelope} ){
      my $error=$newhash->{error} || "";
      $self->info("There was an error, putting into log: $error");
      $self->info($self->{LOGGER}->error_msg());
      $self->info("There is no envelope ($error)!!", 1);
      return;
     }
    $ENV{ALIEN_XRDCP_ENVELOPE}=$newhash->{envelope};
    $ENV{ALIEN_XRDCP_URL}=$newhash->{url};
    return $newhash;
  }
  $self->info("Making the envelope ourselves: @_ ");

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
  my $se      = (shift or 0);
  my $size    = (shift or "0");
  my $sesel   = (shift or 0);
  my $extguid = (shift or 0);

  my $nosize  = 0;

  if ($size eq "0") {
    $size = 1024*1024*1024;
    $nosize =1 ;
  }
  my $perm;

  if ($access eq "read") {
    $perm = "r";
  } elsif ($access =~ /^(((write)((-once)|(-version))?)|(delete))$/ ) {
    $perm = "w";
    if ($size){
      $self->info("Checking the size in $se ($size)");
      my ($info)=$self->df( $se);
      if ($info and $info->{min_size} and $info->{min_size}>$size){
	$self->info( "The file is too small!! ( only $size and it should be $info->{min_size}");
	
	return access_eof("This storage element only accepts files bigger than $info->{min_size} (and your file is $size)");
      } 
    }
  } else {
    $self->info("access: illegal access type <$access> requested");
    return access_eof;
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

    if ( $lfn =~ /(\w\w\w\w\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\w\w\w\w\w\w\w\w).*/ ) {
      $self->info("Getting the permissions from the guid");
      $guid = $1;
      $self->debug(1, "We have to translate the guid $1");
      $lfn = "";
      $filehash=$self->{CATALOG}->{DATABASE}->{GUID_DB}->checkPermission($perm, $guid, {retrieve=>"size,md5"});
      if (! $filehash){
	$self->info("access: access denied to guid '$guid'");
	return access_eof;
      }
      delete $filehash->{db};
    } else {
      $lfn = $self->{CATALOG}->f_complete_path($lfn);
    }

    if ($lfn eq "/NOLFN") {
	$lfn = "";
	$guid = $extguid;
    }
    #    print "$access $lfn $se\n";

    my $whereis;
    while(1) {
      if ( $lfn ne "") {
	$filehash=$self->checkPermissionsOnLFN($lfn,$access, $perm)
	  or return access_eof;
	$access=~ /write-version/ and $access="write-once";
      }
      $DEBUG and $self->debug(1, "We have permission on the lfn");
      if ($access =~ /^write/) {
	$se or $se = $self->{CONFIG}->{SE_FULLNAME};
		
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
	($se, $pfn, $anchor, $lfn, $nses, $whereis)=$self->getPFNforAccess($guid, $se, $sesel, $lfn, $options)
	  or return access_eof;
	$self->info("AND NOW WE HAVE $se, $pfn, $anchor, $lfn, $nses, $whereis");
	if (UNIVERSAL::isa($se, "HASH")){
	  $self->info("Here we have to return eof");
	  return access_eof;
	}
	$DEBUG and $self->debug(1, "access: We can take it from the following SE: $se with PFN: $pfn");
      }

      $ticket .= "<authz>\n";
      $ticket .= "  <file>\n";
      if ($globalticket eq "") {
	$globalticket .= $ticket;
      }

      $pfn =~ m{^((root)|(file))://([^/]*)/(.*)};
      my $pfix = $4;
      my $ppfn = $5;

      $filehash->{pfn} = "$ppfn";
      if (($lfn eq "") && ($access =~ /^write/)) {
	  $lfn = "/NOLFN";
          $self->identifyValidGUID($extguid) 
          and $guid = $extguid;
      }

      $filehash->{turl} = $pfn;

      # patch for dCache
      $filehash->{turl} =~ s/\/\/pnfs/\/pnfs/;
      $filehash->{se}   = $se;
      $filehash->{nses} = $nses;

      $filehash->{lfn}  = $lfn || $filehash->{pfn};

      #if ($access =~ /^write/) {
      $filehash->{guid} = $guid;
      #}
      
      if ((!defined $filehash->{md5}) || ($filehash->{md5} eq "")) {
	$filehash->{md5} = "00000000000000000000000000000000";
      }
      
      # the lfn has to be the first member in the <file> list
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
      $self->info("The ticket is $ticket");
      $ticket .= "  </file>\n";
      $ticket .= "</authz>\n";
      
      $self->{envelopeengine}->Reset();
      #    $self->{envelopeengine}->Verbose();
      my $coded = $self->{envelopeengine}->encodeEnvelopePerl("$ticket","0","none");
      
      my $newhash;
      $newhash->{guid} = $filehash->{guid};
      $newhash->{md5}  ="$filehash->{md5}";
      $newhash->{nSEs} = $nses;
      $newhash->{lfn}=$filehash->{lfn};
      $newhash->{size}=$filehash->{size};
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
	$newhash->{envelope} = $self->{envelopeengine}->GetEncodedEnvelope();
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
	($access =~ /^((read)|(write))/)  and $method="${1}req";
	$access =~ /^delete/ and $method="delete";

	$method and
	  $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_QUOTA","$self->{CATALOG}->{ROLE}_$method", @params); 		      
	
      }

      push @lnewresult,$newhash; 

      if (!$coded) {
	$self->info("access: error during envelope encryption");
	return access_eof;
      } else {
	(!($options=~ /s/)) and $self->info("access: prepared your access envelope");
      }
      
      ($options=~ /v/) or
	print STDERR "========================================================================
$ticket
========================================================================
",$$newresult[0]->{envelope},"
========================================================================\n", $ticket,"\n";
      last;
      
    }
  }

  if ($options =~ /g/) {
    $globalticket .= "  </file>\n";
    $globalticket .= "</authz>\n";
    
    $self->{envelopeengine}->Reset();
    my $coded = $self->{envelopeengine}->encodeEnvelopePerl("$globalticket","0","none");
    $lnewresult[0]->{genvelope} = $self->{envelopeengine}->GetEncodedEnvelope();
  }
  return @$newresult; 
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
  my $storageurl   = (shift or 0);
  my $se           = (shift or 0);
  my $guid         = (shift or 0);
  my $md5          = (shift or 0);
  my $newhash      = {};
  $newhash->{lfn}  = $lfn;

  push @$newresult,$newhash;

  $self->{envelopeengine}->Reset();
#    $self->{envelopeengine}->Verbose();
  $self->{envelopeengine}->IsInitialized();
  print STDERR "Decoding Envelope: \n $envelope \n";

    my $coded = $self->{envelopeengine}->decodeEnvelopePerl($envelope);
  if (!$coded) {
      $self->info("commit: error during envelope decryption");
      return;
  } else {
      $$newresult[0]->{authz} = $self->{envelopeengine}->GetDecodedEnvelope();
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
	      if (!$storageurl) {
		  $storageurl = $_->{storageurl};
	      }
	      if (!$se) {
		  $se = $_->{se};
	      }
	      if (!$guid) {
		  $guid = $_->{guid};
	      }
	      if ($_->{access} =~ "^write") {
		  $$newresult[0]->{$lfn} = 0;
		  $self->debug("commit: Registering file lfn=$lfn storageurl=$storageurl size=$size se=$se guid=$guid md5=$md5");
		  my $result = $self->f_registerFile("-md5=$md5",$lfn,$storageurl, $size , $se, $guid, $perm);
		  if (!$result) {
		      $self->info("commit: Cannot register file lfn=$lfn storageurl=$storageurl size=$size se=$se guid=$guid md5=$md5");
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

	my @envelope = $self->access("-s","delete","$lfn",$se);

	if ((!defined $envelope[0])||(!defined $envelope[0]->{envelope})) {
	    $self->info("Cannot get access to $lfn for deletion @envelope") and return;
	}
	$ENV{'IO_AUTHZ'} = $envelope[0]->{envelope};

	if (!$self->{STORAGE}->eraseFile($envelope[0]->{url})) {
	    $self->info("Cannot remove $envelope[0]->{url} from the storage element $se");
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

sub upload_HELP {
  return "upload: copies a file to the SE
Usage:
\t\tupload <pfn> [<se>,!<se>,select=N,<qosflag>=N,<guid>]
";
}


sub upload {
  my $self=shift;
  my $options={};
  @ARGV=@_;
  Getopt::Long::GetOptions($options, "silent", "versioning=s", 
			   "size=i", "md5=s", "guid=s")
      or $self->info("Error checking the options of add") and return;
  @_=@ARGV;
  my $pfn=shift;
  my $lfn="/NOLFN";
  my $envReq="write-once";
  if ($options->{versioning} and $options->{versioning} ne ""){
          $lfn = $options->{versioning};
          $envReq = "write-version";
   }

  my $result = {};
  $options->{guid} and $result->{guid}=$options->{guid};
 
  my @ses = ();
  my @excludedSes = ();
  my @qosList;

  $pfn or $self->info("Error not enough arguments in upload\n". $self->upload_HELP())
    and return;
  $pfn=$self->checkLocalPFN($pfn);
  my $size=AliEn::SE::Methods->new($pfn)->getSize();

  my @optentry=split(",", join(",",@_));
  foreach my $d (@optentry) {
    if ($d =~ /::/){
      my $list=\@ses;
      $d =~ s/!// and $list=\@excludedSes;
      my $n=uc($d);
      grep (/^$n$/,@$list) or push @$list, $n;
      next;
    }
    if ($d =~ /\=/){
      grep (/^$d$/, @qosList) or push @qosList, $d;
      next;
    }
    $d =~ /^\s*$/ and next;
    $self->info("WARNING: Found the following unrecognizeable option:".$d);
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
  
  foreach my $qos(keys %$qosTags){
    $self->debug(2,"Processing storage discovery qos: $qos with $qosTags->{$qos} requested elements.");
    $result = $self->putOnDynamicDiscoveredSEListByQoS($result,$pfn,$lfn,$size,$envReq,$qosTags->{$qos},$qos,$self->{CONFIG}->{SITE},\@excludedSes,1);
  }
  
  my $suppressISCheck = 0;
  if (!$result->{status} and scalar(@ses) eq 0 and $selOutOf le 0){ # if dynamic was either not specified or not successfull (not even one time, that's $result->{status} ne 1) 
    
    push @ses, $self->{CONFIG}->{SE_FULLNAME};   # and there were not SEs specified in a static list, THEN push in at least the local static LDAP entry not to loose data
    $suppressISCheck = 1;
    $totalCount = 1;
    $self->debug(2,"There was neither a user specification for the SEs to use, nor is there a default setting defined in LDAP, we use CONFIG->SE_FULLNAME: $self->{CONFIG}->{SE_FULLNAME}");
  }
  
  $self->debug(2,"Processing static SE list: @ses");
  (scalar(@ses) gt 0) and $result = $self->putOnStaticSESelectionList($result,$pfn,$lfn,$size,$envReq,$selOutOf,\@ses,1,$suppressISCheck);
  
  
  $result->{totalCount}=$totalCount;
  
  return $result;
}

sub identifyValidGUID{
   my $self=shift;
   my $guid=shift;
   my $lines = $guid;
   # guid has to be 36 chars long, containing 4 times '-', at position 9,14,19,24 and the rest needs to be hexdec
   (length($guid) eq 36)
     and $lines = substr($lines, 8, 1)
        .substr($lines, 13, 1).substr($lines, 18, 1).substr($lines, 23, 1)
     and $lines =~ s/[-]*// and (length($lines) eq 0)
     and $guid  = substr($guid, 0, 8)
        .substr($guid, 9, 4).substr($guid, 14, 4)
        .substr($guid, 19, 4).substr($guid, 24, 12)
     and $guid =~ s/[0-9a-f]*//
     and (length($guid) eq 0)
     and return 1;
     return 0;
}


sub putOnStaticSESelectionList{
   my $self=shift;
   my $result=shift;
   my $pfn=(shift || "");
   my $lfn=(shift || "");
   my $size=(shift || 0);
   my $envreq=(shift || "");
   my $selOutOf=(shift || 0);
   my $ses=(shift || "");
   my $pfnRewrite=(shift || 0);
   my $suppressISCheck=(shift || 0);


   if ($suppressISCheck eq 0) {
      for my $j(0..3) {
         my $res = $self->{SOAP}->CallSOAP("IS", "checkExclusiveUserOnSEs", $self->{CONFIG}->{ROLE}, $ses);
         $self->{SOAP}->checkSOAPreturn($res) and $ses=$res->result and last;
      }
   }

   $selOutOf eq 0 and  $selOutOf = scalar(@$ses);
   while ((scalar(@$ses) gt 0 and $selOutOf gt 0)) {
     (scalar(@$ses) gt 0) and my @staticSes= splice(@$ses, 0, $selOutOf);
     $self->debug(2,"We select out of a supplied static list the SEs to save on: @staticSes, count:".scalar(@staticSes));
     ($result, my $success, my $JustConsideredSes) = $self->registerInMultipleSEs($result, $pfn, $lfn, $size, \@staticSes, $envreq, $pfnRewrite);
		 (defined $success) and $selOutOf = $selOutOf - $success;
   }
   return $result;
}  



sub putOnDynamicDiscoveredSEListByQoS{
   my $self=shift;
   my $result=shift;
   my $pfn=(shift || "");
   my $lfn=(shift || "");
   my $size=(shift || 0);
   my $envreq=(shift || "");
   my $count=(shift || 0);
   my $qos=(shift || "");
   my $sitename=(shift || "");
   my $excludedSes=(shift || "");
   my $pfnRewrite=(shift || 0);
   my $countOutSOAP=0;

   while($count gt 0) {
     my $res = $self->{SOAP}->CallSOAP("IS", "getSEListFromSiteSECache", $count, $qos, $sitename, $excludedSes, $self->{CONFIG}->{ROLE});
     $countOutSOAP++;
     $self->{SOAP}->checkSOAPreturn($res) or ($countOutSOAP < 4 and next or last);
     my @discoveredSes=@{$res->result};
     scalar(@discoveredSes) gt 0 or $self->info("We could'nt find any of the '$count' requested SEs with qos flag '$qos' in the cache.") and last;;
     $self->debug(2,"We discovered the following SEs to save on: @discoveredSes, count:".scalar(@discoveredSes).", type flag was: $qos.");
     ($result, my $success, my $JustConsideredSes) = $self->registerInMultipleSEs($result, $pfn, $lfn, $size, \@discoveredSes, $envreq, $pfnRewrite);
		 if (defined $result) {
       push @$excludedSes, @$JustConsideredSes;
       $count = $count - $success;
		 }
  }
  return $result;
}




sub registerInMultipleSEs {
  my $self  = shift;
  my $result = (shift || {});
  my $pfn   = shift;
  my $lfn=(shift || "");
  my $size=(shift || 0);
  my $suggestedSes = ( shift || {} );
  my $envreq=(shift || "");
  my $pfnRewrite=(shift || 0);

$result->{guid} and $self->info("File has guid: $result->{guid}");
  $result->{guid} or $result->{guid} = "";


  ($pfn) or $self->{LOGGER}->warning( "LCM", "Error no pfn specified" ) and return;
  
  my $firstHit = 0;
  my $successCounter = 0;
  my @ses= ();
  my @excludedSes = ();

  my $envelopes = {};
  for my $j(0..$#{$suggestedSes}) {
     my @envelope= $self->access("-s",$envreq,$lfn, @$suggestedSes[$j], $size,0,$result->{guid});
     if(@envelope) {
         $envelopes->{@$suggestedSes[$j]}=$envelope[0]; 
         push @ses, @$suggestedSes[$j];
     } else {
         $self->debug(2,"Error getting the security envelope");
         push @excludedSes, @$suggestedSes[$j]; 
     }

     ($j eq 0) and $result->{guid} = $envelopes->{@$suggestedSes[$j]}->{guid};

  } 

  $self->debug(2,"We got envelopes for and will use the following SEs to save on: @ses, count:".scalar(@ses));

	my $user=$self->{CATALOG}->{ROLE};
  for my $j(0..$#ses) {
     $envelopes->{$ses[$j]} or $self->{LOGGER}->warning( "LCM", "Missing envelope for SE: $ses[$j]" ) and next; 
     $ENV{ALIEN_XRDCP_ENVELOPE}=$envelopes->{$ses[$j]}->{envelope};
     $ENV{ALIEN_XRDCP_URL}=$envelopes->{$ses[$j]}->{url};

     my $start=time;

     $self->debug(2, "Adding the file $pfn to $ses[$j]" );
     my $res;
     my $z = 0;
     while ($z < 5 ) {   # try five times in case of error
          $res= $self->{STORAGE}->RegisterInRemoteSE($pfn, $lfn, $envelopes->{$ses[$j]});
          $res and $z = 6 or $z++;
     }

     $res or print STDERR "ERROR storing $pfn in $ses[$j]\n" and push @excludedSes, $ses[$j] and next;

     $res->{pfn} or $self->{LOGGER}->warning( "LCM", "Error transfering the file to the SE" );

     my $time=time-$start;
     $self->sendMonitor("write", $ses[$j], $time, $size, $res);

     if($firstHit eq 0 and (! $result->{status})) {
        $result->{guid} = $res->{guid};
        $result->{md5} = $res->{md5};
        $result->{size} = $res->{size};
        $result->{pfn} = $res->{pfn};
        $result->{seref} = $ses[$j];
        $result->{status} = 1;
        $firstHit = 1;
        $self->debug(2,"Registered data for first SE, status is ok");
     }
     $result->{se}->{$ses[$j]}->{pfn}=$res->{pfn};

     if ($envelopes->{$ses[$j]}->{url} and $pfnRewrite){
          my $newPFN=$envelopes->{$ses[$j]}->{url};
          $newPFN=~ s{^([^/]*//[^/]*)//(.*)$}{$1/$envelopes->{$ses[$j]}->{url}};
          $newPFN=~ m{root:////} and $newPFN="";
          $newPFN and $self->debug(3,"Using the pfn of the security envelope '$newPFN'") and $result->{$ses[$j]}->{pfn}=$newPFN;
     }
     push @excludedSes, $ses[$j];
     $successCounter++;
  }

  return $result, $successCounter, \@excludedSes;
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
      or $self->info("Error checking the options of add") and return;
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
   masterSE  <SENAME> list
       Prints statistics about the usage of the SE

   masterSE <SENAME> print [-lfn] [-unique][-replicated] [<filename>]
       Creates a filename with all the pfns on that SE. If -lfn is present, writes the lfn 

   masterSE <SENAME> replicate  [-all]   <SE destination>
       Moves the entries from this SE to another one. By default, it copies only the entries that are not replicated already

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
    Getopt::Long::GetOptions($options, "-lfn", "-unique", "-replicated")
	or $self->info("Error in masterSE print. Unrecognize options") and return;
    @_=@ARGV;

    my $output=shift || "$self->{CONFIG}->{TMP_DIR}/list.$sename.txt";
    $self->info("Creating the file $output with all the entries");
    open (FILE, ">$output") or $self->info("Error opening $output") and return;
    my $counter=$self->executeInAllPFNEntries($sename, $options, "masterSEprint",\*FILE, $options->{lfn});
    close FILE;
    return $counter;
  } elsif ($action =~ /remove/){
    $self->info("Removing the files from the se");
    my $options={duplicate=>1, lfn=>1};
    grep (/^-all$/i, @_) and delete $options->{duplicate};
    my $counter=$self->executeInAllPFNEntries($sename, {}, "masterSEremove");
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
    my $options={};
    @ARGV=@_;
    Getopt::Long::GetOptions($options, "-lfn", "-unique", "-replicated")
	or $self->info("Error in masterSE print. Unrecognize options") and return;
    @_=@ARGV;
    my $collection=shift || "collection.$sename";
    $self->execute("createCollection",$collection) or return;
    my $counter=$self->executeInAllPFNEntries($sename, $options, "masterSEcollection", $collection);
    $self->execute("updateCollection", $collection);
    $self->info("Collection created with $counter files");
    return 1;
  }
  $self->info("Sorry, I don't understand 'masterSE $action'");
  return undef;

}

sub masterSEcollection {
  my $self=shift;
  my $guid=shift;
  my $collection=shift;

  $self->debug (1, "We have to add $guid->{guid} to $collection");
  my ($lfn)=$self->execute("guid2lfn", "-silent", $guid->{guid}) or return;

  return $self->execute("addFileToCollection", $lfn, $collection, "-n");

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

sub masterSEremove {
  my $self=shift;
  my $info=shift;

  $self->info("We are going to remove $info->{guid} (or $info->{lfn}");
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
  my $lfn=shift;
  my $print =$entries->{pfn};
  ($lfn) and $print=$entries->{lfn};

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

return 1;

