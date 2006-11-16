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
		 'addTag'   => ['$self->addTag', 0],
		 'access'   => ['$self->access', 0],
		 'resolve'  => ['$self->resolve', 0],
		 'mssurl'   => ['$self->mssurl', 0],
		 'df'       => ['$self->df', 0],
		 'services' => ['$self->services', 0],
		 'preFetch'   => '$self->preFetch',
		 'vi'       => ['$self->vi', 0],
		 'whereis'  => ['$self->whereis', 19],
		 'purge'    => ['$self->purge', 0 ],
		 'erase'    => ['$self->erase', 0 ],
		 'upload'   => ['$self->upload', 0],
		 'listTransfer'=> ['$self->{STORAGE}->listTransfer', 0],
		 'killTransfer'=> ['$self->{STORAGE}->killTransfer', 0],
		 'stage'=> ['$self->stage', 0],
		 'find'=> ['$self->find',0],
		 'zip'=> ['$self->zip',16+64],
		 'unzip'=> ['$self->unzip',0],

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

  if (defined $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'} && defined $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'} && defined $ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'} and defined $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}) {
      $self->info("local private key          : $ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}");
      $self->info("local public  key          : $ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'}");
      $self->info("remote private key         : $ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'}");
      $self->info("remote public key          : $ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}");
      require SealedEnvelope;
      
      $self->{envelopeengine} = SealedEnvelope::TSealedEnvelope->new("$ENV{'SEALED_ENVELOPE_LOCAL_PRIVATE_KEY'}","$ENV{'SEALED_ENVELOPE_LOCAL_PUBLIC_KEY'}","$ENV{'SEALED_ENVELOPE_REMOTE_PRIVATE_KEY'}","$ENV{'SEALED_ENVELOPE_REMOTE_PUBLIC_KEY'}","Blowfish","CatService\@ALIEN",0);
      # we want ordered results of se lists, no random
      $self->{noshuffle} = 1

  } else {
      $self->{enveleopengine} =0;
      $self->{noshuffle} = 0;
  }

  $self->{envelopebackdoor} = 0;

  if (!$self->{envelopeengine}) {
      $self->info("Warning: cannot create envelope sealing engine = setting backdoor\n");
      $self->{envelopebackdoor} = 1;
  } else {
    if (!$self->{envelopeengine}->Initialize(2)) {
      $self->info("Warning: cannot initialize envelope sealing engine = setting backdoor\n");
      $self->{envelopebackdoor} = 1;
    }
  }
  $self->{envelopebackdoor} and $ENV{'IO_AUTHZ'}="alien";
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
print "H", $hostport;
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

sub get {
  my $self = shift;
  my $opt;
  ( $opt, @_ ) = $self->Getopts(@_);
  my $file      = shift;
  my $localFile = shift;

  ($file)
    or print STDERR
"Error: not enough arguments in get\nUsage: get [-n] [-o]<file> [<localfile>]\n"
  and return;

  $file=$self->{CATALOG}->f_complete_path($file);
  #Get the pfn from the catalog
  my $oldSilent = $self->{CATALOG}->{SILENT};
  
  my ($md5, $guid);
  my $ret = $self->{CATALOG}->f_getMD5("sg", $file);
  $md5  = $ret->{md5};
  $guid = $ret->{guid};

  $guid or $self->info("Error getting the guid and md5 of $file",-1) and return;

  ######################################################################################
  #get the authorization envelope and put it in the IO_AUTHZ environment variable
  my @envelope = $self->access("-s","read","$file");
  if (!defined $envelope[0]->{envelope}) {
    $self->info( "Cannot get access to $file") and return;
  }

  $ENV{'IO_AUTHZ'} = $envelope[0]->{envelope};
  ######################################################################################

  #First, let's check the local copy of the file
  my $result=$self->{STORAGE}->getLocalCopy($guid, $localFile);
  if (! $result) {
    $self->{STORAGE}->checkDiskSpace($ret->{size}, $localFile) or return;
    my $seRef = $self->{CATALOG}->f_whereisFile("s$opt", $file);

    #Get the file from the LCM
    $seRef or $self->info( $self->{LOGGER}->error_msg())
      and return;

    my (@seList ) = $self->selectClosestSE(@$seRef);
    $self->debug(1, "We can ask the following SE: @seList");

    while (my $se=shift @seList) {
      my @pfns=$self->getPFNfromGUID($se, $guid);
      @pfns or next;
      foreach my $pfn (@pfns) {
	$result = $self->{STORAGE}->getFile( $pfn, $se, $localFile, $opt, $file, $guid,$md5 );
	if ($result) {
	  $self->info("And the file is $result",0,0);
	  return $result;
	}
      }
    }
    $result or return;
  }
  $self->info("And the file is $result",0,0);
  return $result;
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

sub df {
  my $self = shift;
  my $opt;
    ( $opt, @_ ) = $self->Getopts(@_);
  my $se   = (shift or $self->{CONFIG}->{SE_FULLNAME});
  my $oldsilent = $self->{CATALOG}->{SILENT};
  my @hostportsName;
  my @results = ();
  if ($opt =~/a/) {
      $se = "";
  }

  my $service="SE";
  my $function="getLVMDF";
  if ($opt=~ /c/ ){
    $service="CLC";$function="getCacheDF";
    $self->info("Cachename               1k-blocks         Used(KB)  Available Use\%    Range \  #Files",0,0);
  } else {
    $self->info("Storagename             1k-blocks         Used(KB)  Available Use\%    \#Files Type",0,0);
  }

  if ($se) {
    # query only one special SE
    $self->{CATALOG}->{SILENT} = 1;
    my $hostport = $self->resolve($se,$service);
    $self->{CATALOG}->{SILENT} = $oldsilent;
    if (! defined $hostport) {
      print STDERR "Error: $service $se is not known!\n" and return;
    }
    push @hostportsName, "$hostport###$se";

  } else {
    # query all SE from the IS
    my $response=$self->{SOAP}->CallSOAP("IS", "getAllServices",$service) or 
      $self->info("Error getting the list of $service") and return;
    $response = $response->result;
    #    print "All Service are $response->{HOSTS} and $response->{PORTS}\n";

    my $cnt = 0;
    my @hosts = split ":",$response->{HOSTS};
    my @ports = split ":",$response->{PORTS};
    my @names = split "###", $response->{NAMES};
    for (@hosts) {
      push @hostportsName, "$hosts[$cnt]:$ports[$cnt]###$names[$cnt]";
      $cnt++;
    }
  }

  for (@hostportsName) {
    my ($address, $name)=split(/###/, $_);
    $self->debug(1, "Calling $address");
    my $response = 
      SOAP::Lite->uri("AliEn/Service/$service")
	  ->proxy("http://$address",timeout => 5)
	    ->$function($name);
    
    ($response) or next;
    $self->debug(1, "Got $response");
    $response = $response->result;
    ( defined $response) or next;
    $self->debug(1, "Got $response");
    my $details = {};
    ($details->{name}, $details->{size}, $details->{used}, $details->{available}, $details->{usage}, $details->{files}, $details->{type}) 
       =split(/\s+/, $response);
    push(@results, $details);
    ( $response eq "-1") and next;
    
    $self->info("$response",0,0);
  }
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
 my @returnarray;
 $#returnarray=-1;
 foreach my $item (@_) {
   if ($item =~ /^-?z/i) {
     $returnhash=1;
     next;
   }
 
   if ($item =~ /^-?n/i) {
     $dontcall=1;
     next;
   }
   if ($item =~ /^-?co(re)?/i ) {
     push @checkservices,"Services";
     next;
   }

   if ($item =~ /^-?s(e)?/i) {
     push @checkservices,"SE";
     next;
   }
   if ($item =~ /^-?ce/i) {
     push @checkservices,"ClusterMonitor";
     next;
     }
   if ($item =~ /^-?cl(c)?/i) {
       push @checkservices,"CLC";
       push @checkservices,"CLCAIO";
       next;
     }
   if ($item =~ /^-?f(td)?/i) {
     push @checkservices,"FTD";
     next;
   }
   if ($item =~ /^-?p(ackman)?/i) {
     push @checkservices,"PackMan";
     next;
   }

   if ($item =~ /\+/) {
     $replystatus = 1;
     next;
   }
   if ($item !~ s/-?-h(elp)?//i) {
     print STDERR "Error: Don't know service flag \"$item\"\n";
   }
   print STDERR "Usage: services [+] [-][core] [-][clc] [-][ftd] [-][se] [-][ce]\n";
   print STDERR "  or   services + -co -cl -f -s -ce \n";
   return;
   
 }
 @checkservices or   
   push @checkservices, "SE","CLC","CLCAIO","ClusterMonitor","FTD","TcpRouter","Services";

 printf STDOUT "==   Service   == Servicename ============================================= Hostname ==   Status    ==";
 if ($replystatus) {
   printf STDOUT "  Vers. =  R  S  D  T  Z =\n";
 } else {
   printf STDOUT "\n";
 }
 printf STDOUT "-----------------------------------------------------------------------------------------------------------------------------\n";
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

     printf STDOUT "- [ %-9s ]   not running \n",$printservice;

     print STDOUT "-----------------------------------------------------------------------------------------------------------------------------\n";
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
     printf STDOUT "- [ %-9s ]   %-25s %40s ",$printservice, $printname,$_;
     if ((! defined $response) || (($response eq "-1") || ($response eq "") || ($response ne "1"))) {
       printf STDOUT "-- no response --\n";
       $hashresult->{servicestatus} = "noresponse";
     } else {
       $response = $response->result;
       if ( (defined $response->{'OK'}) && ($response->{'OK'} ==1) ) {
	 $hashresult->{servicestatus} = "ok";
	 if ((defined $response->{'VERSION'}) && ($replystatus)) {
	   print STDOUT "--      OK     --  $response->{'VERSION'}";
	 } else {
	   print STDOUT "--      OK     --";
	 }
       } else {
	 print STDOUT "--     down    --";
	 $hashresult->{servicestatus} = "down";
       }
       
       if ((defined $response->{'Sleep'}) && (defined $response->{'Run'}) && (defined $response->{'Trace'}) && (defined $response->{'Disk'}) && (defined $response->{'Zombie'})) {
	 printf STDOUT " %2d %2d %2d %2d %2d\n",$response->{'Run'},$response->{'Sleep'},$response->{'Disk'},$response->{'Trace'}, $response->{'Zombie'};
       } else {
	 print STDOUT " \n";
       }
     }
     push @returnarray, $hashresult;
     $cnt++;
   }
   printf STDOUT "-----------------------------------------------------------------------------------------------------------------------------\n";
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
  my ($se, @close, @site, @rest);
  while (@_) {
    my $newse  = shift;
    my $seName=$newse;
    UNIVERSAL::isa($newse, "HASH") and $seName=$newse->{se};

    $self->debug(1,"Checking $newse vs $self->{CONFIG}->{SE_FULLNAME}" );
    if ( $newse =~ /^$self->{CONFIG}->{SE_FULLNAME}$/i ){
      $se=$newse;
    }elsif( grep ( /^$newse$/i, @{ $self->{CONFIG}->{SEs_FULLNAME} } )){
      push @close, $newse;
    }elsif( grep ( /^$site/i, "$self->{CONFIG}->{ORG_NAME}::$self->{CONFIG}->{SITE}::") ){
      push @site, $newse;
    }else{
      push @rest, $newse;
    }

  }
  my @return;
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
  $self->debug(1, "After sorting we have @return");
  return @return;
}

sub cat {
  my $self=shift;
  return $self->f_system_call("cat", @_);
}
sub less {
  my $self=shift;
  return $self->f_system_call("less", @_);
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

  print "File changed, uploading...\n";

  my $pfn="file://$self->{CONFIG}->{HOST}$file";
  $pfn =~ s/\n//gs;
  my $md5=AliEn::MD5->new($file)
    or $self->info("Error calculating the md5") and return;
  my $result=$self->{STORAGE}->registerInLCM($pfn) or return;

  ($self->{CATALOG}->isFile("${reallfn}~") )
    and $self->execute("rm", "-silent", "${reallfn}~");
  $self->execute("cp", $lfn, "${reallfn}~");
  return $self->execute("update", $lfn, "-size", $result->{size}, "-guid", $result->{guid}, "-se", $self->{CONFIG}->{SE_FULLNAME}, "-md5", $md5);
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
  return ( $flags, @files );
}

sub addFile_HELP {
  return "'add' copies a file into the SE, and register an entry in the catalogue that points to the file in the SE\n\tUsage: add [-r]  <lfn> <pfn> [<SE> [<previous storage element>]]\nPossible pfns:\tsrm://<host>/<path>, castor://<host>/<path>, 
\t\tfile://<host>/<path>
If the method and host are not specified, the system will try with 'file://<localhost>'
Possible options:
\t-r:(reverse) Start an io server on the client side and let the SE fetch the file from there.
\t-v:(versioning) a new version of the file is created, if it already existed
\t-c (custodial) add the file to the closest custodial se\n";
}
#
# Check if the user can create the file
sub _canCreateFile{
  my $self=shift;
  my $lfn=shift;
  $self->{CATALOG}->checkPermissions( 'w', $lfn )  or  return;
  if ($self->{CATALOG}->f_Database_existsEntry( $lfn)) {
    $self->{LOGGER}->error("File", "file $lfn already exists!!",1);
    return;
  }
  return 1;
}

sub addFile {
  my $self  = shift;
  $self->debug(1, "UI/LCM Register @_");
  my $options={};
  @ARGV=@_;
  Getopt::Long::GetOptions($options, "silent", "reverse", "versioning",
			   "size=i", "md5=s", "custodial")
      or $self->info("Error checking the options of add") and return;
  @_=@ARGV;

  my $lfn   = shift;
  my $pfn   = shift;
  my $newSE =(shift or "");
  my $oldSE = ( shift or "" );
  my $target = (shift or "");


  $pfn or $self->info("Error: not enough parameters in add\n".
		      $self->addFile_HELP(),2)	and return;

  $lfn = $self->{CATALOG}->f_complete_path($lfn);
  if (! $options->{versioning}) {
    $self->_canCreateFile($lfn) or return;
  }

  if ($options->{custodial}){
    $self->info("Saving the file in a custodial SE");
    $newSE=$self->findCloseSE("custodial") or return;
  }
  ######################################################################################
  #get the authorization envelope and put it in the IO_AUTHZ environment variable
  my @envelope;
  if ($options->{versioning}) {
    @envelope = $self->access("-s","write","$lfn",$newSE);
  } else {
    @envelope = $self->access("-s","write-once","$lfn",$newSE);
  }
  if (!defined $envelope[0]->{envelope}) {
    $self->info( "Cannot get access to $lfn") and return;
  }

  $ENV{'IO_AUTHZ'} = $envelope[0]->{envelope};
  ######################################################################################

  $self->debug(1, "\nRegistering  $pfn as $lfn, in SE $newSE and $oldSE (target $target)");
  my $size;
  $pfn=$self->checkLocalPFN($pfn);
#  if ($options=~ /u/) {#
#
#  }

  my $data = $self->{STORAGE}->registerInLCM( $pfn, $newSE, $oldSE, $target,$lfn, $options) or return;

  $newSE or $newSE=$self->{CONFIG}->{SE_FULLNAME};
  return $self->{CATALOG}->f_registerFile( "-f", $lfn, $data->{size}, $newSE, $data->{guid}, undef,undef, $data->{md5});
}

#  This subroutine mirrors an lfn in another SE. It received the name of the lfn, and the 
#  target SE
#  It may also receive several options:
#      f : force to keep the same relative path in the new SE
#      s : change any transfer of local files to do them through soap 
#      b : (batch) do not wait for the transfer to finish
#      t : (transfer) do not attempt to get the file, but issue a transfer (this implies
#           also batch. 

sub mirror {
  my $self = shift;

  (my $options, @_)= AliEn::Catalogue->Getopts(@_);
  $self->debug(1, "UI/LCM Mirror with @_");
  $options=~ /t/ and $options.="b";
  my $lfn  = shift;
  my $se   = ( shift or "$self->{CONFIG}->{SE_FULLNAME}" );
  
  ($lfn) or print STDERR
    "Error not enough arguments mirroring a file!\nUsage: mirror [-fms] <lfn> [<se>]
Options:\t-f\t keep the same relative path
\t\t-m\t Make the new replica the master copy
\t\t-b\t Do not wait for the file transfer
\t\t-t\t Issue a transfer instead of copying the file directly (this implies also -b)\n"
      and return;
  $lfn = $self->{CATALOG}->f_complete_path($lfn);

  my $realLfn=$self->{CATALOG}->checkPermissions( 'w', $lfn )  or
    $self->info("You don't have permission to do that") and return;
  $self->{CATALOG}->isFile($lfn, $realLfn) or 
    $self->info("The entry $lfn is not a file!") and return;

  my $guid=$self->{CATALOG}->f_lfn2guid("s",$realLfn)
    or $self->info( "Error getting the guid of $lfn",11) and return;

  my @sePfnList=$self->whereis("",$realLfn);
  @sePfnList or return;

  my $pfn=$sePfnList[1];
  my $oldSE=$sePfnList[0];

#  my $mirror= $self->{CATALOG}->existsMirror( $lfn, $se );
#  $mirror and ($mirror eq "-1") and return;
#  $mirror and
#    $self->{LOGGER}->error("LCM", "That file is already mirrored at $se")
#     and return;
  
  $self->info( "Mirroring file $realLfn (from $oldSE)");
  
  
  my $transfer={"source", $pfn,                  "oldSE", $oldSE,
		"target", "",                      "TYPE", "mirror",
		"USER" => $self->{CONFIG}->{ROLE}, "LFN" =>$realLfn,
		"DESTINATION" =>$se,	             "OPTIONS" => "fm$options",
		guid=>$guid};
  
  ($options=~ /m/ ) and $transfer->{TYPE}="master";
  
  if ($options =~ /f/)    {
    $self->info( "Keeping the same relative path");
    
    my $service=$self->{CONFIG}->CheckService("SE", $se);
    $service or $self->{LOGGER}->error("UI/LCM", "SE $se does not exist!") and return;
    my $url=AliEn::SE::Methods->new($pfn) or return;
    $transfer->{target}=$url->path;
    $transfer->{target}=~ s/^$service->{SAVEDIR}//;
  }

  if ($options =~ /t/){

    my $result=$self->{SOAP}->CallSOAP("Manager/Transfer","enterTransfer",$transfer);
    $result  or return;
    #Make the remote SE get the file
    my $id=$result->result;
    $self->info("The transfer has been scheduled!! ($id)");
    return (-2, $id);
  }

  my ($done, $id)=$self->{STORAGE}->bringFileToSE($se, $transfer, $options);

  $done or return;

  ($done eq "-2") and return ($done, $id);

  $done->{transfer} and return 1;
  #Finally, add it to the catalogue
  my $newPfn=$done->{pfn};
  my $command="addMirror";

  ($options=~ /m/ ) and $command="masterCopy";

  $self->info( "Adding the mirror $pfn");
  return $self->execute($command, $realLfn, $se);
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
  @files or $self->{LOGGER}->error("LCM", "Error in preFetch. No files specified") and return; 
  $self->info("Doing preFetch of ".($#files +1)." \n@files");

  my @local=();
  my @remote=();
  foreach my $file (@files) {
    my @se=$self->{CATALOG}->isFile($file) 
      or $self->{LOGGER}->error("LCM", "Error file $file does not exist (ignoring this file)")
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

sub whereis {
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
 
 
  $lfn=$self->{CATALOG}->f_complete_path($lfn);
  my ($seList, $fileInfo);

  my $ret=$self->{CATALOG}->f_whereisFile("g$options", $lfn);
  $seList   = $ret->{selist};
  $fileInfo = $ret->{fileinfo};

  my $guid=$fileInfo->{guid};
  defined $seList or
    $self->info( "Error getting the data of $lfn", 1) and
      return;
  $self->info( "The file $lfn is in");
  my @return=();
  defined $guid or $self->info("Error getting the guid of the file") and return;
#  my $guid=$self->{CATALOG}->f_lfn2guid("s",$lfn)
#    or $self->info( "Error getting the guid of $lfn",11) and return;
  $self->info( "The guid is $guid");

  my @result;
  foreach my $se (@$seList) {
    my $format="\t$se";
    if ($options !~ /l/) {
      my $seoptions={};
      $options =~ /s/ and $seoptions={stage=>1};
      my @pfns=$self->getPFNfromGUID($se, $guid, $seoptions);
      my $found=0;
      foreach my $pfn (@pfns) {
	$found=1;
	$pfn and $format.="\t$pfn";
	if ($options =~ /z/) {
	  push @result, {se=>$se, guid=>$guid, pfn=>$pfn};
	} else {
	  push @result, $se;
	  push @result, ($pfn or "");
	}
      }
      if (!$found) {
	push @result, {"__result__"=>0};
      }
    } else {
      if ($options =~ /z/) {
	push @result, {se=>$se};
      } else {
	push @result, $se;
      }
    }
    $self->info( "$format\n", 0,0);
  }

  if ($options =~ /r/) {
    $self->debug(1, "The whereis should resolve links");
    my @temp;
    while (@result) {
      my ($se, $pfn)=(shift @result, shift @result);
      if ($options =~ /z/ ) {
	my $newhash;
	$newhash->{se} = $se;
	$newhash->{pfn} = $pfn;
	push @temp, $newhash;
      } else {
	push @temp, ($se, $pfn);
      }
    }
    @result=@temp;
  }
  if ($options=~ /i/ ){
    #We have to return also the information of the file
    return ($fileInfo, @result);
  }
  if ( (scalar @result) <= 0) {
    if ($options=~/z/) 
      {return @failurereturn;} 
    else {return}; 
  } else {
    return @result;
  }
}

# Given an SE and a guid, it returns all the pfns that the SE 
# has of that guid
sub getPFNfromGUID {
  my $self=shift;
  my $se=shift;
  my $guid=shift;
  my $options=shift || {};
  $self->debug(1,"Getting the pfn from $se");

  my ($seName, $seCert)=$self->{SOAP}->resolveSEName($se) or return;

#  $self->info( "Asking the SE at $seName");
  my $result=$self->{SOAP}->CallSOAP($seName, "getPFNFromGUID",$seName, $guid, $self->{CONFIG}->{IOMETHODS_LIST}, $options) 
    or $self->info( "Error asking the SE: $!", 1) and return;
  my @pfns=$self->{SOAP}->GetOutput($result);
  $self->debug(1, "Returning the list @pfns");
  return @pfns;
}


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
	  $self->{LOGGER}->error("LCM","access: access denied to guid $guid");
	  return;
      }
  }

  
  my $filehash = $self->{CATALOG}->checkPermissions($perm,$lfn,undef, 
						   {RETURN_HASH=>1});
  if (!$filehash) {
      $self->{LOGGER}->error("LCM","access: access denied to $lfn");
      return;
  }
  
  my $dirn = $self->{CATALOG}->f_dirname($lfn);
  my $basen = $self->{CATALOG}->f_basename($lfn);

  return $self->{CATALOG}->f_find("-z","-q","-r", $lfn,"\\");
}


#############################################################################################################
sub access_eof {
    my $newhash;
    my @newresult;
    $newhash->{eof} = "1";
    push @newresult, $newhash;
    return @newresult;
}


sub access {
    # access <access> <lfn> 
    # -p create public url in case of read access 
  my $self = shift;
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

  my $nosize  = 0;

  if ($size eq "0") {
    $size = 1024*1024*1024;
    $nosize =1 ;
  }
  my @list=();

  # for the moment we leave the backdoor open to use the old alien shell
  if ($self->{envelopebackdoor}) {
    my $newhash;
    $self->info("access: warning - we are using the backdoor ....");
    $newhash->{envelope}="alien";
#    $newhash->{guid} = $filehash->{guid};
#    $pfn =~ /^root\:\/\/([0-9a-zA-Z.-_:]*)\/\/(.*)/;
#    $newhash->{url}=$pfn;
#    push @lnewresult,$newhash; 
    return $newhash;
  }
  

  my @lfnlist = split(",",$lfns);
  my @lnewresult;
  my $newresult = \@lnewresult;

  my $globalticket="";

  foreach my $lfn (@lfnlist) {
      my $perm = "";  
      my $result;
      my $ticket = "";
      
      
      my $guid="";
      my $pfn ="";
      my $seurl =""; 
      my $nses = 0;
      if ($access eq "read") {
	  $perm = "r";
      } elsif ($access =~ /^(((write)((-once)|(-version))?)|(delete))$/ ) {
	  $perm = "w";
      } else {
	  $self->{LOGGER}->error("LCM","access: illegal access type <$access> requested");
	  return access_eof;
      }
      
      $lfn = $self->{CATALOG}->f_complete_path($lfn);
      
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
	      $self->{LOGGER}->error("LCM","access: access denied to guid $guid");
	      return access_eof;
	  }
      }
      
      
      
      
      #    print "$access $lfn $se\n";
      
      
      my $filehash = {};
      
      while(1) {
	  $filehash = $self->{CATALOG}->checkPermissions($perm,$lfn,undef, 
							 {RETURN_HASH=>1});
	  if (!$filehash) {
	      $self->{LOGGER}->error("LCM","access: access denied to $lfn");
	      return access_eof;
	  }
	  if ( ($access eq "read")) {
	      if (!$self->{CATALOG}->isFile($lfn, $filehash->{lfn})) {
		  $self->{LOGGER}->error("LCM","access: access find entry for $lfn");
		  return access_eof;
	      }
	  } else {
	      
	      if ($access eq "write-once") {
		  my $parentdir = $self->{CATALOG}->f_dirname($lfn);
		  $result = $self->{CATALOG}->checkPermissions($perm,$parentdir);
		  if (!$result) {
		      $self->{LOGGER}->error("LCM","access: parent dir missing for lfn $lfn");
		      return access_eof;
		  }
		  if ($self->{CATALOG}->existsEntry($lfn, $filehash->{lfn})) {
		      $self->{LOGGER}->error("LCM","access: write-once but lfn $lfn exists already");
		      return access_eof;
		  }
	      }
	      
	      if ($access eq "write-version") {  
		  my $parentdir = $self->{CATALOG}->f_dirname($lfn);
		  $result = $self->{CATALOG}->checkPermissions($perm,$parentdir);
		  if (!$result) {
		      $self->{LOGGER}->error("LCM","access: parent dir missing for lfn $lfn");
		      return access_eof;
		  }
		  if ($self->{CATALOG}->existsEntry($lfn, $filehash->{lfn})) {
		      $self->info( "access: lfn <$lfn> exists - creating backup version ....\n");
		      my $filename = $self->{CATALOG}->f_basename($lfn);
		      
		      $self->{CATALOG}->f_mkdir("ps","$parentdir"."/."."$filename/") or
			  $self->{LOGGER}->error("LCM","access: cannot create subversion directory - sorry") and return;
		      
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
			  $self->{LOGGER}->error("LCM","access: cannot parse the last version number of $lfn");
			  return access_eof;
		      }
		      my $pversion = sprintf "%.1f", (10.0+($version))/10.0;
		      my $backupfile = "$parentdir"."/."."$filename/v$pversion";
		      $self->info( "access: backup file is $backupfile \n");
		      if (!$self->{CATALOG}->f_mv("",$lfn, $backupfile)) {
			  $self->{LOGGER}->error("LCM","access: cannot move $lfn to the backup file $backupfile");
			  return access_eof;
		      }
		      # in the end we access a new file
		      $access="write-once";
		  } else {
		      $access="write-once";
		  }
	      }
	      
	      
	      if ($access eq "delete") {
		  if (! $self->{CATALOG}->existsEntry($lfn, $filehash->{lfn})) {
		      $self->{LOGGER}->error("LCM","access: delete of non existant file requested: $lfn");
		      return access_eof;
		  }
	      }
	  }
	  
	  if ($access =~ /^write/) {
	      if (!$se) {
		  $se = $self->{CONFIG}->{SE_FULLNAME};
	      }
	      my ($seName, $seCert)=$self->{SOAP}->resolveSEName($se) or return access_eof;
	      my $ksize=($size/1024);
	      if ($ksize<=0) {
		  $ksize=1;
	      }
	      # the volume manager deals with kbyte!
	      my $newname=$self->{SOAP}->CallSOAP($seName, "getVolumePath",$seName, $ksize)
		  or $self->{LOGGER}->error("LCM","access: Error asking $se for a filename") and return access_eof;
	      my @fileName=$self->{SOAP}->GetOutput($newname);
	      $guid=$fileName[1];
	      $pfn =$fileName[2];
	      $seurl = $fileName[3];
	      
	      $pfn=~ s/\/$//;
	      $seurl=~ s/\/$//;
	      
	      $pfn .= $lfn;
	      $pfn .= "/$guid";
	      
	      $seurl .= $lfn;
	      $seurl .= "/$guid";
	      
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
	      
	      $guid=$self->{CATALOG}->f_lfn2guid("s",$lfn)
		  or $self->info( "access: Error getting the guid of $lfn",11) and return;
	      
	    resolve_again:
	      my $whereis = $self->{CATALOG}->f_whereisFile("s", $lfn);
	      #Get the file from the LCM
	      $whereis or $self->info( "access: " . $self->{LOGGER}->error_msg())
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
		  (@closeList) = $self->selectClosestSE(@$whereis);
		  $self->{CONFIG}->{SE_FULLNAME} = $tmpse;
		  $self->{CONFIG}->{ORG_NAME} = $tmpvo;
		  $self->{CONFIG}->{SITE} = $tmpsite;
		  @{$self->{CONFIG}->{SEs_FULLNAME}}=@tmpses;
	      } else {
		  (@closeList) = $self->selectClosestSE(@$whereis);
	      }
	      #check if the wished se is at all existing ....
	      my $sefound=0;
	      $nses=scalar @closeList;
	      if ($sesel > 0) {
		  # the client wants a replica identified by its number
		  my $cnt=0;
		  if (defined $closeList[$sesel-1]) {
		      $se = $closeList[$sesel-1];
		  } else {
		      return access_eof;
		  }
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
		  $self->{LOGGER}->error("LCM","access: File $lfn does not exist in $se");
		  return access_eof;
	      }
	      
	      
	      $self->debug(1, "We can ask the following SE: $se");
	      
	      (!($options =~/s/)) and $self->info( "The guid is $guid");
	      
	      my @pfns=$self->getPFNfromGUID($se, $guid) or return;
	      $pfn=$pfns[0];
	      if (($pfn =~ /^guid\:\/\/\//) || ( $pfn =~ /.*\/\/\w\w\w\w\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\w\w\w\w\w\w\w\w\?/)) {
		  # we got a reference guid back
		  # 34ea59b5-cb8c-4ae7-8d55-06ed376afe00
		  my $newguid="";
		  if ( $pfn =~ /^guid:/ ) {
		      $pfn =~ /guid\:\/\/\/(\w\w\w\w\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\w\w\w\w\w\w\w\w).*/;
		      $newguid = $1;
		  } else {
		      $pfn =~ /.*\/\/(\w\w\w\w\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\-\w\w\w\w\w\w\w\w\w\w\w\w)\?.*/;
		      $newguid = $1;
		  }
		  print "New Guid = $newguid\n";
		  $guid = $newguid;
		  $pfn =~ /ZIP=([^\&]*)\&DB=([^\&]*)\&tableName=([^\&]*)/;
		  my $options="s ";
		  if (defined $1) {
		      $anchor = $1;
		  }
		  if (defined $2) {
		      $options .= " -db$2";
		  }
		  if (defined $3) {
		      $options .=" -table$3";
		  }
		  if ( $anchor eq "") {
		      $pfn =~ /ZIP=([^\&]*)/;
		      if (defined $1) {
			  $anchor = $1;
		      }
		  }
		  
		  print "anchor = $anchor options = $options\n";
		  my @lfns= $self->{CATALOG}->f_guid2lfn("s",$newguid);
		  if ( (defined $lfns[0]) && ($lfns[0] ne "") ) {
		      
		      $lfn = $lfns[0];
		      goto resolve_again;
		  } else {
		      $self->info( "access: Error resolving the guid reference $pfn",11) and return;
		  }
	      }
	      $DEBUG and $self->debug(1, "access: We can take it from the following SE: $se with PFN: $pfn");
	  }
	  
	  # we skip that!
	  # add the anchor to the lfn for archive files
	  #if ($anchor ne "") {
	  #$lfn .= "#" . $anchor;
	  #}
	  
	  $ticket .= "<authz>\n";
	  $ticket .= "  <file>\n";
	  if ($globalticket eq "") {
	      $globalticket .= $ticket;
	  }


	  $filehash->{lfn}  = $lfn;
	  $filehash->{turl} = $pfn;
	  $filehash->{se}   = $se;
	  $filehash->{nses} = $nses;
	  if ($access =~ /^write/) {
	      $filehash->{guid} = $guid;
	  }
	  
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
	  
	  $ticket .= "  </file>\n";
	  $ticket .= "</authz>\n";
	  
	  $self->{envelopeengine}->Reset();
	  #    $self->{envelopeengine}->Verbose();
	  my $coded = $self->{envelopeengine}->encodeEnvelopePerl("$ticket","0","none");
	  
	  my $newhash;
	  $newhash->{guid} = $filehash->{guid};
	  $newhash->{md5}  ="$filehash->{md5}";
	  $newhash->{nSEs} = $nses;

	  # the -p (public) option creates public access url's without envelopes
	  
	  if ( ($options =~ /p/) && ($access =~ /^read/) ) {
	      $newhash->{envelope} = "alien";
	      $newhash->{url}="$pfn";
	      $newhash->{se}="$se";
	      $newhash->{lfn}="$lfn";
	  } else {
	      $newhash->{envelope} = $self->{envelopeengine}->GetEncodedEnvelope();
	      $pfn =~ /^root\:\/\/([0-9a-zA-Z.\-_:]*)\/\/(.*)/;
	      if ($anchor ne "") {
		  $newhash->{url}="root://$1/$lfn#$anchor";
		  $newhash->{lfn}="$lfn#$anchor";
	      } else {
		  $newhash->{url}="root://$1/$lfn";
		  $newhash->{lfn}="$lfn";
	      }
	      $newhash->{se}="$se";
	  }
	  
	  push @lnewresult,$newhash; 
	  
	  if (!$coded) {
	      $self->{LOGGER}->error("LCM","access: error during envelope encryption");
	      return access_eof;
	  } else {
	      (!($options=~ /s/)) and $self->info("access: prepared your access envelope");
	  }
	  last;
	  
	  ($options=~ /s/) or
	      print "========================================================================
$ticket
========================================================================
",$$newresult[0]->{envelope},"
========================================================================\n";
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
  print "Decoding Envelope: \n $envelope \n";

    my $coded = $self->{envelopeengine}->decodeEnvelopePerl($envelope);
  if (!$coded) {
      $self->{LOGGER}->error("LCM","commit: error during envelope decryption");
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
		  $self->{LOGGER}->debug("LCM","commit: Registering file lfn=$lfn storageurl=$storageurl size=$size se=$se guid=$guid md5=$md5");
		  my $result = $self->f_registerFile("-md5=$md5",$lfn,$storageurl, $size , $se, $guid, $perm);
		  if (!$result) {
		      $self->{LOGGER}->error("LCM","commit: Cannot register file lfn=$lfn storageurl=$storageurl size=$size se=$se guid=$guid md5=$md5");
		  }
		  # ask at the SE, if this file is really there 
		  my ($seName, $seCert)=$self->{SOAP}->resolveSEName($se) or return;
#		  $self->{LOGGER}->debug("LCM", "commit: Asking the SE at $seName");
		  my $sepfn=$self->{SOAP}->CallSOAP($seName, "getPFNFromGUID",$se, $guid); 
		  if (!$sepfn) {
		      $self->{LOGGER}->info("LCM", "commit: Error asking the SE: $!", 1);
		      $self->{LOGGER}->error("LCM","commit: removing entry for $lfn -> not existing in the SE");
		      $self->{CATALOG}->f_removeFile("-s",$lfn);
		  } else {
		      $$newresult[0]->{$lfn} = 1;
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
        
    my (@seList ) = $self->selectClosestSE(@$seRef);

    my $failure=0;
    while (my $se=shift @seList) {
#	my $pfn=$self->getPFNfromGUID($se, $guid);
#	$pfn or next;

	my @envelope = $self->access("-s","delete","$lfn",$se);

	if ((!defined $envelope[0])||(!defined $envelope[0]->{envelope})) {
	    $self->{LOGGER}->info("UI/LCM", "Cannot get access to $lfn for deletion @envelope") and return;
	}
	$ENV{'IO_AUTHZ'} = $envelope[0]->{envelope};

	if (!$self->{STORAGE}->eraseFile($envelope[0]->{url})) {
	    $self->{LOGGER}->info("UI/LCM", "Cannot remove $envelope[0]->{url} from the storage element $se");
	    $failure=1;
	    next;
	}	
    }

    if (!$failure) {
	if (!$self->{CATALOG}->f_removeFile("-s",$lfn)) {
	    $self->{LOGGER}->info("UI/LCM", "Cannot remove lfn $lfn from the catalogue, but I deleted the file in the storage element");
	    return;
	}
    } else {
	$self->{LOGGER}->info("UI/LCM", "Could not remove all replicas of lfn $lfn - keeping catalogue entry" );
	return ;
    }
    return 1;
}

sub upload {
  my $self=shift;
  $self->debug(1, "Starting the upload with @_");

  (my $options, @_)=$self->GetOpts(@_);
  my $pfn=shift;
  my $se=shift;
  my $guid=shift || "";

  $pfn=$self->checkLocalPFN($pfn);
  my $data;
  if ($options !~ /u/ ){
    $self->info("Trying to upload the file $pfn to the se");
    $data= $self->{STORAGE}->registerInLCM( $pfn, $se, undef, undef, undef, undef, $guid) or return;
  }else {
    $self->info("Making a link to the file $pfn");

    my $url=AliEn::SE::Methods->new($pfn) 
      or $self->info( "Error creating the url of $pfn")
	and return;

    my $size=$url->getSize();
    defined $size or $self->info("Error getting the size of $pfn") 
      and return;
    
    my $md5=AliEn::MD5->new($pfn);
    my ($newguid, $sename)=$self->registerFileInSE($se, undef, $pfn, $size, {md5=>$md5}) or return;
    $data->{guid}=$newguid;
    $data->{size}=$size;
    $data->{md5}=$md5;
  }
  $self->info("The upload of $pfn worked!!"); 
  $se or $se=$self->{CONFIG}->{SE_FULLNAME};
  return {guid=>$data->{guid},
	  selist=>$se,
	  size=>$data->{size},
	  md5=>$data->{md5}
	  };
}


sub stage {
  my $self=shift;
  $self->info("Ready to stage the files @_");
  return $self->whereis("s", @_);
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
  getopts("d:",\%options);
  @_=@ARGV;
  my $lfn=shift;

  my @files=@_;
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
return 1;

