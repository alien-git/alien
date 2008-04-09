#
#  File tranport daemon for Alien
#

package AliEn::Service::FTD;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Database::TXT::FTD;
use LWP::UserAgent;

use AliEn::SE::Methods;
use AliEn::X509;
use POSIX ":sys_wait_h";
use strict;

use vars qw(@ISA);
use Classad;

use AliEn::MSS::file;
use AliEn::UI::Catalogue::LCM;
use AliEn::Service;
@ISA=qw(AliEn::Service);
# Uncomment when module is installed in alice/local...
use Filesys::DiskFree;


my $MAXIMUM_SIZE = 1024 * 1024
  ; #This is the maximum size of a "free" transfer, meaning always granted. (In bytes);

# Use this a global reference.

my $self = {};

my $error_codes = {
    TRANSFER_SUCCES           => "2",
    COMMENSE_TRANSFER         => "1",
    TRANSFER_DENIED           => "-5",
    FILE_NOT_FOUND            => "-1",
    TRANSFER_ALREDY_REQUESTED => "-2",
    TRANSFER_METHOD_NOT_KNOWN => "-3",
    TRANSFER_ALREDY_REQUESTED => "-4",
    TRANSFER_FAILED           => "-6",
    OUT_OF_DISK_SPACE         => "-7",
    TRANSFER_CORRUPTED        => "-8",
    PERMISSION_DENIED         => "-9"
};

sub initialize {
  $self=shift;

  # Initialize the logger

  $self->{METHOD} = "FTP";
  
  $self->{PORT}=$self->{CONFIG}->{'FTD_PORT'};
  $self->{HOST}=$self->{CONFIG}->{HOST};
  chomp $self->{HOST};

  $self->{SERVICE}="FTD";
  $self->{SERVICENAME}=$self->{CONFIG}->{FTD_FULLNAME};
  $self->{LISTEN}=1;
  $self->{PREFORK}=1;
  $self->{FORKCHECKPROCESS}=1;
#  $self->{PROTOCOLS}=$self->{CONFIG}->{FTD_PROTOCOL};
  # the default protocol is "bbftp"
#  $self->{PROTOCOLS} or $self->{PROTOCOLS} ="bbftp";
  my @protocols=();
  $self->{CONFIG}->{FTD_PROTOCOL_LIST} and 
    @protocols=@{$self->{CONFIG}->{FTD_PROTOCOL_LIST}};
  
  @protocols or @protocols="BBFTP";
  $self->{PROTOCOLS}=\@protocols;

  $self->info("Using configuration for " . $self->{CONFIG}->{FTD} );
  
  $self->{MAX_RETRYS} = 10;
  
  $self->{CACHE_DIR} = $self->{CONFIG}->{CACHE_DIR};    #"/tmp/Alien/CACHE";
  if ( !( -d $self->{CACHE_DIR} ) ) {
    mkdir $self->{CACHE_DIR}, 0777;
  }
  $self->{MAX_TRANSFERS} = $self->{CONFIG}->{FTD_MAXTRANSFERS} || 5;

  $self->{IS_HOST} = $self->{CONFIG}->{IS_HOST};
  $self->{IS_PORT} = $self->{CONFIG}->{IS_PORT};
  $self->checkCertificate() or return;

  $self->{JDL}=$self->createJDL();
  $self->{JDL} or return;
  $self->info($self->{JDL});
  $self->{DB} = new AliEn::Database::TXT::FTD();
  $self->{DB} or print STDERR "Error creating the TXT database\n" and return;

  $self->{NAME} = "$self->{CONFIG}->{ORG_NAME}::$self->{CONFIG}->{SITE}";
  $self->{ALIVE_COUNTS} = 0;
  
  $self->{ALLOWED_DIRS} = $self->{CONFIG}->{FTD_ALLOWEDDIRS_LIST};
  $self->info("Allowing clients to put into @{$self->{ALLOWED_DIRS}}" );
  
#  $self->createGridMapFromLdap();

  $self->{SOAP}->checkService("Broker/Transfer", "TRANSFER_BROKER", "-retry", [timeout=>50000]) or return;
  $self->{SOAP}->checkService("Manager/Transfer", "TRANSFER_MANAGER", "-retry") or return;

#  $self->{SOAP}->checkService("SE", "-retry") or return;
  $self->{CATALOGUE}=AliEn::UI::Catalogue::LCM->new({role=>$self->{CONFIG}->{CLUSTER_MONITOR_USER}}) 
    or $self->info("Error creating the catalogue in the FTD") and return;

  return $self;
}
sub createJDL {
    my $self =shift;
    my $exp={};
    
    $exp->{Name}="\"$self->{CONFIG}->{FTD_FULLNAME}\"";
    $exp->{SEName}="\"$self->{CONFIG}->{SE_FULLNAME}\"";
    $exp->{Type}="\"FTD\"";
#	$exp->{DirectAccess}="1";

    my $handle = new Filesys::DiskFree;
    $handle->df();
    my $free = $handle->avail( $self->{CACHE_DIR} );
#    my $free=`df  --block-size 1 $self->{CACHE_DIR}`;
#    $self->debug(1, "Got $free");
#    $free =~ s/^(\S+\s+){10}(\d+).*$/$2/s;

    $exp->{CacheSpace}=$self->{CACHEx_SPACE}=$free;
#    $exp->{DiskSpace}=$free;
 
    $exp->{Requirements}="other.Type==\"transfer\"";

    my @list=();

    $self->{CONFIG}->{SEs_FULLNAME} and @list=@{$self->{CONFIG}->{SEs_FULLNAME}};

    map {$_ =~ s/^(.*)$/\"$1\"/} @list;

    $exp->{CloseSE}="{". join (",", @list)."}"; 
    
    return $self->SUPER::createJDL( $exp);
}
sub checkCertificate {
  my $self=shift;
  my $certfile = "$ENV{ALIEN_HOME}/identities.ftd/cert.pem";
  my $keyfile  = "$ENV{ALIEN_HOME}/identities.ftd/key.pem";
    
  if ( !( -e $certfile ) ||  !( -e $keyfile ) ) {
    print STDERR "You do not have any certificate install in $certfile\n";
    print STDERR "I cannot start without it. Aborting\n";
    return;
  }
  my $hostcert = new AliEn::X509;
  $hostcert->load($certfile);
  my $subject = $hostcert->getSubject() ;
  if (! $subject) {
    print "ERROR: Getting the subject of the certificate\n";
    return;
  }
  $self->debug(1, "Subject $subject" );
  my $CONFSubject = $self->{CONFIG}->{FTD_CERTSUBJECT};
  if (  $subject !~ m{^$CONFSubject(/CN=((proxy)|(\d+)))*$} ){
    print "ERROR: Your certificate says:\n$subject\n";
    print "       but your configuration is $CONFSubject\n";
    print "       Are you sure you have the correct certificate?\n";
    return 0;
  }
  $ENV{X509_RUN_AS_SERVER} = "1";
  $ENV{X509_USER_CERT}     = $certfile;
  $ENV{X509_USER_KEY}      = $keyfile;
#  $ENV{X509_CERT_DIR}      = "$ENV{ALIEN_ROOT}/etc/alien-certs/certificates";
  $self->{GRIDMAP} = $ENV{GRIDMAP} = "$ENV{ALIEN_HOME}/identities.ftd/map";
  return 1;
}

sub startListening {
  my $this=shift;
  my @protocols=@{$self->{PROTOCOLS}};
  $self->info("Starting the service. Protocols @protocols");
  $self->{FTP_SERVERS}={};
  foreach my $name (@protocols) {
    my $class="AliEn::FTP::\U$name\E";
    $self->info("Trying to start method: $name");
    if (eval "require $class"){
      eval {
	$self->{FTP_SERVERS}->{$name}=$class->new($self->{CONFIG}) 
	  or die("Error creating an instance of $class");
	$self->{FTP_SERVERS}->{$name}->startListening();
      };
    }
    if ($@) {
      $self->info("Error starting $name\n$@");
      return;
    }
  }
  return $self->SUPER::startListening();
}

sub alloc {
    my $s    = shift;
    my $dir  = shift;
    my $size = shift;

    $self->info("Trying to allocate $size bytes in $dir" );

    #    print STDERR "Allocating $size bytes of data\n";

    # At the momment we can not allocate discspace

    return 0;
}


#sub generateID {
#    my $s      = shift;
#    my $retval = 0;
#    my @ID     =
#      $self->{DB}
#      ->query("SELECT ID from FILETRANSFERSNEW ORDER BY ID DESC LIMIT 1");
#    (@ID) or $ID[0] = "1";
#    $retval = $ID[0] + 1;
#    return $retval;
#}#

#sub getRoute {##
#
#    #This will probably be either a SOAP Call or checking a central database;
#    my $s         = shift;
#    my $source    = shift;
#    my $finaldest = shift;
#    my $size      = (shift or 0);
#    my $finaldestPort =shift;
#
#    $self->debug(1,
#"In getRoute, before contacting the IS at $self->{IS_HOST}:$self->{IS_PORT} (dest $finaldest : $finaldestPort"
#    );
#
#    
#    my $response =
#      SOAP::Lite->uri("AliEn/Service/IS")
#      ->proxy("http://$self->{IS_HOST}:$self->{IS_PORT}")
#      ->getRoute( $source, $finaldest,  $size, $finaldestPort );##
#
#    if ( !($response) or !( $response->result ) ) {
#       $self->{LOGGER}->warning( "FTD",
#            "The IS at $self->{IS_HOST}:$self->{IS_PORT} is not up" );
#        return ( $finaldest, "BBFTP", $finaldest, "8091" );
#    }
#    my @route = ( $response->result, $response->paramsout );
#    return @route;
#}
sub _selectPFN {
  my $self=shift;
  $self->info("Getting the best PFN from @_");
  my @methods=keys %{$self->{FTP_SERVERS}};
  $self->info("Looking for any of @methods");
  foreach my $method (@methods){
    my @pfn=grep (/^$method:/i, @_) or next;
    $self->info("The pfn @pfn is valid (taking the first one)!!");
    return shift @pfn;
  }
  if (@methods){
    $self->info("The FTD wanted '@methods', but we can only get from @_.");
    return;
  }
  $self->info("There are no favourite methods... hope that the first one will do");
  return shift @_;
}

sub findAlternativeSource{
  my $self=shift;
  my $id=shift;
  my $failedSE=shift;
  $self->info("Asking the Transfer Manager if there are alternative sources");

  my $done=$self->{SOAP}->CallSOAP("Manager/Transfer", "findAlternativeSource", $id, $failedSE)  or return;
  $done->result or $self->info("There weren't any alternative sources :( ")
    and return;
  $self->info("The manager found an alternative source :)");
  return 1;
}

sub startTransfer {
  my $s = shift;
  
  my $transfer =shift;
  
  $self->info("Starting a transfer");
  my @listPFN;
  $transfer->{FROMPFN} and @listPFN=@{$transfer->{FROMPFN}};
  my $sourceURL=$self->_selectPFN( @listPFN);
  $sourceURL=~ s/^([^:]*:)/\L$1\E/;

  my $size=$transfer->{SIZE};
  my $retries=$transfer->{RETRIES};
  my $id=$transfer->{ID};
  my $targetSE=$transfer->{TOSE};
  my $guid=$transfer->{GUID};
  my $message="";
  my $sourceCertificate=$transfer->{FROMCERTIFICATE};

  @listPFN or $message="no FROMPFN in the transfer";
  defined $size or $message="no SIZE in the transfer";
  
  $message and $self->{LOGGER}->error("FTD", "ID $id Error: $message",11) 
    and return;

  if (! $sourceURL){
    $self->info("We can't transfer from any of the elements that where proposed.");
    return $self->findAlternativeSource($id, $transfer->{FROMSE});
  }

  $self->info("ID $id Starting a transfer of  $sourceURL");
  my $toPFN=$transfer->{TOPFN};
  if (!$toPFN){

    my ($seName, $seCert)=$self->{SOAP}->resolveSEName($targetSE) or return;
  
    $self->info("ID $id asking the SE $targetSE for a new filename");
    my $result=$self->{SOAP}->CallSOAP($seName,'getFileName', $seName, $size,
				      {guid=>$guid}) 
      or return;
    $toPFN=$result->result;
    my @args=$result->paramsout;
    $self->info("We got @args") ;

    $toPFN or
      $self->{LOGGER}->error("FTD", "ID $id Error getting a new filename") 
	and return;
    $toPFN="file://$self->{HOST}$toPFN";
    
    if ($args[1]){
      $args[1]=~ /^castor/ and $toPFN=~ s/^file/castor/;
      # in case of SRM, let's keep the full host
      $args[1]=~ /^srm/ and $toPFN=$args[1];
      $args[1]=~ /^root/ and $toPFN=$args[1];
    }
    
  }
    
  $self->info("ID $id Starting a transfer to $toPFN");
 
  my $fromURL = new AliEn::SE::Methods( $sourceURL) or return;
  my $toURL = new AliEn::SE::Methods($toPFN) or return;
  my $now  = time;

    # Now, fork so child-proccess handles the transfer,
    # do Transfer($localfile, $remotefile, $host, $method, $direction); 
#    my $transferpid = fork();

      # THIS IS THE CHILD PROCESS Wait 2 seconds, so parent can update current number of transfer
      # IF THE ONE THAT EXISTS IS THE FATHER, WE WOULD HAVE A DEFUNCT PROCESS
      #Write our PID in the PID file

#    if ( $transferpid  eq 0 ) {
#	sleep(2);
#	return;#
#    }

  my $localCheck=$self->askToPut($toURL, $size, $id);
  if ( $localCheck != $error_codes->{COMMENSE_TRANSFER} ) {
    my $message =
      "Not able to get file due to local restriction (Disk space, or permissions)?";
    $self->{LOGGER}->error("FTD", "ID $id $message");
    
    return;
  }

  $self->debug(1, "ID $id Doing the transfer" );
    
  my $destPath= $fromURL->path;
 
  ($fromURL->method eq "hpss" )
    and print "Checking if we are getting from hpss" and
      $destPath = "$fromURL->{PARSED}->{VARS_HOST}$destPath";

  my $options=$transfer->{FROMFTDOPTIONS};
  ($self->{CONFIG}->{FTD_LOCALOPTIONS})
    and $options.=join (";", @{$self->{CONFIG}->{FTD_LOCALOPTIONS_LIST}});

  $self->info("ID $id Setting options ($options)");
#    my $result=$self->{MANAGER}->changeStatusTransfer($id, "TRANSFERING", {});
  my $result=$self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, "TRANSFERING", {});
  my $fromHost=$fromURL->host;
  $fromURL->port() and $fromHost.=":".$fromURL->port();
  my $toHost=$toURL->host;
  $toURL->port() and $toHost.=":".$toURL->port();


  my $error =  $self->doTransfer( $toURL->path, $destPath,  $fromURL->host, 
		       $fromURL->method, "get", $id, $options, $sourceCertificate,
		       $fromHost, $toHost);


  if ($error) {
    my $errorM="The transfer failed (w eare getting things from  ". $fromURL->method();
    $self->{LOGGER}->error_msg() and $errorM=$self->{LOGGER}->error_msg();
    $self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, "FAILED", {Reason=>$errorM});
    return ;
  }
  $self->verifyTransfer($error, $toURL, $size, $retries, $id) or return;

  $self->info("ID $id The transfer succeeded!!");
#    $self->CURRENT_TRANSFERS_DECREMENT($size);
    
  #Now kill this transferchild
  return 1;
}

sub verifyTransfer{
  my $t=shift;
  my $error=(shift or "");
  my $URL=shift;
  my $size=shift;
  my $retries=shift;
  my $id=shift;
  my $message="The transfer did not start";
  if (! $error) {
    $self->info("VerifyTransfer " );
    # Now check that file was actually received
    #
    my $result;
    $result = $self->verifyCompleteTransfer( $URL, $size, $id );
    my $status;
    my $now = time;
    if ( $result == $error_codes->{TRANSFER_SUCCES} ) {
      $self->info("ID $id Transfer finished!!!");
      my $result=$self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer","ALIEN_SOAP_RETRY", $id, "CLEANING", {"Action", "cleaning", "FinalPFN", $URL->string});
      #	    my $result=$self->{MANAGER}->changeStatusTransfer($id, "CLEANING", {"Action", "cleaning", "FinalPFN", $URL->string});
      $self->{SOAP}->checkSOAPreturn($result, "TransferManager")
	or return;
      return 1;
    }
    $message="The file was not completely transfered";
  }
  
  $self->info("ID $id Transfer failed :(\n\t\t$message");
  my $result=$self->{SOAP}->CallSOAP("Manager/Transfer",
				     "changeStatusTransfer","ALIEN_SOAP_RETRY",$id, "FAILED",
				     {"Reason", $message});
  #my $result=$self->{MANAGER}->changeStatusTransfer($id, "FAILED", 
  #						      {"Reason", $message});
  $self->{SOAP}->checkSOAPreturn($result, "TransferManager");
  return;

}


#sub checkOneTransfer {
#    my $s     = shift;
#    my $transfer = shift;
#
#    my $error;
#    my $rdir;
#    my $endstation = 0;#
#
#    my ( $ID, $file, $sourceURL, $size, $finaldestURL, $inserted, $direction,
#        $retrys )
#      = split ( "###", $transfer );#
#
#    $self->info("Checking one transfer");
#    my $sURL = new AliEn::SE::Methods($sourceURL) or return;
#    
#    my $dURL = new AliEn::SE::Methods($finaldestURL) or return;#
#
#    my $message="Starting to put $sourceURL to $finaldestURL";
#
#    ( $direction eq "get" ) and
#	$message="Starting to get $finaldestURL from $sourceURL";
#    
#    $self->info($message);
#    
#    
#    my ( $nexthost, $method, $FTDhost, $FTDport ) =
#      $self->getRoute( $sURL->host, $dURL->host, $size, $dURL->port );
#    
#    if ( !($nexthost) ) {
#        $self->{LOGGER}->error( "FTD", "No route to host $finaldestURL" );
#        return;
#    }
#    
#    $self->debug(1,
#			   "Next dest is $nexthost ($FTDhost:$FTDport) with method $method" );
#
    # Now ask the FTS checking that node if transfer is okay We should loop over all endpoints returned.#
#    my $now = time;
#    my $remoteFTD = SOAP::Lite->uri("AliEn/Services/FTD")
#    ->proxy("http://$FTDhost:$FTDport");
#
#    ($remoteFTD)
#      or $self->{LOGGER}->error( "FTD", "Error contatcting the FTD at $FTDhost:$FTDport" )
#      and return;#
#
#    my $response;
#    if ( $direction eq "put" ) {
#        #Ask remote server for permission to write $size bytes
#        $response = $remoteFTD->askToPut( $finaldestURL, $size );
#    } else  {
#        #Ask remote server for permission to read $size bytes
#        $response = $remoteFTD->askToGet($finaldestURL);
#    }
#
#    if ( !($response) ) {
#        $self->{LOGGER}->warning( "FTD", "FTD at $FTDhost:$FTDport is not up" );
#        return;
#    }
#
#    if ( $response->result == $error_codes->{COMMENSE_TRANSFER} ) {
#
#        #Now do the transfer
#        my @temp = $response->paramsout;
#        $size = $temp[0];
#        my $remoteOptions = $temp[1];
#        my @options       = ();
#        $remoteOptions and @options = ($remoteOptions);
#        $self->{CONFIG}->{FTD_LOCALOPTIONS}
#          and @options =
#          ( @options, @{ $self->{CONFIG}->{FTD_LOCALOPTIONS_LIST} } );#
#
#        my $options = join ( ";", @options );#
#
#        $self->info("Remote FTD allows the transfer of the file ($size bytes)" );
#        return $self->startTransfer( $remoteFTD, $transfer, $nexthost, $method, $size,
#				     $options );
#    }
#    if ( $response->result == $error_codes->{PERMISSION_DENIED} ) {
#	$error="was no allowed";
#    }
#    if ( $response->result == $error_codes->{OUT_OF_DISK_SPACE} ) {
#        $error="failed due to disk space problems" ;
#    }
#    if ( $response->result == $error_codes->{FILE_NOT_FOUND} ) {
#	$error="failed because file did not exist." ;
#    }
#    if ( $retrys > $self->{MAX_RETRYS} ) {
#	$error="reached maximum count";
#    }#
#
#    if ($error){
#	$self->{LOGGER}->error( "FTD", "Transportation of ". $sURL->path. " to $nexthost $error");
#	$self->{DB}->insert("UPDATE FILETRANSFERSNEW SET status='FAIL', finished='$now' where ID='$ID'");		
#	return;
#    }####
#
#    # Fail transfer if retrys is at max
#    $self->info("Transfer denied, trying next" );#
#
#    my $r = $retrys + 1;
#    $self->{DB}->insert(
#			"UPDATE FILETRANSFERSNEW SET status='WAITING', retrys='$r' where ID='$ID'"
#			    );#
#
#}
#

# This function is called automatically every minute from Service.pm 
# 
# input:  $silent. If 0, print all the things in the stdout. Otherwise, print only important things
#
# output: 1     -> everything went ok. 
#         undef -> the service will sleep for 1 minute before trying again
#

sub checkWakesUp {
  my $s = shift;
  my $silent =shift;

  my $method="info";
  my @methodData=();
  $silent and $method="debug" and push @methodData, 1;

  my $slots= $self->{MAX_TRANSFERS} - $self->CURRENT_TRANSFERS;
  if ( $slots<=0 ) {
    $self->$method(@methodData, "Already doing maximum number of transfers. Wait" );
    return;
  }

  $self->$method(@methodData,"$$ Asking the broker if we can do anything ($slots slots)");



  my ($result)=$self->{SOAP}->CallSOAP("Broker/Transfer", "requestTransfer",
				       $self->{JDL}, $slots);

  $self->info("$$ Got an answer from the broker");
  if (!$result) {
    return;
  }
  my $repeat=1;
  my @transfers=$self->{SOAP}->GetOutput($result);
  foreach my $transfer (@transfers){
    if ($transfer eq "-2"){
      $self->$method(@methodData, "No transfers for me");
      undef $repeat;
      next;
    }

    my $pid=fork();
    defined $pid or self->info("Error doing the fork");
    if ($pid){
      my @list=();
      $self->{FTD_PIDS} and @list=@{$self->{FTD_PIDS}};
      push @list, $pid;
      $self->{FTD_PIDS}=\@list;
      next;
    }
    $self->_forkTransfer($transfer);
    exit;
  }

  if ($self->{FTD_PIDS}){
    $self->info("Collecting zombies");
    sleep(5);
    my @list=@{$self->{FTD_PIDS}};
    my @newList;
    foreach (@list){
      if (CORE::kill 0, $_ and waitpid($_, WNOHANG)<=0){
	push @newList, $_
      }
    }
    $self->{FTD_PIDS}=\@newList;
  }

  return $repeat; 

}

sub _forkTransfer{
  my $self=shift;
  my $transfer=shift;

  $self->info("$$ Is checking the action");
  my $action=($transfer->{ACTION} or"");
  
  my $message="";
  $action or $message=" no action specified";
  my $id=$transfer->{ID};
  $id or $message=" transfer has no id";
  
  if ($message){
    $self->{LOGGER}->error("FTD", "Error: $message");
    return;
  }
  my $return;
  
  my $pid="";

  $self->CURRENT_TRANSFERS_INCREMENT("", $id);
  $self->{FTD_CHILDREN}=$id;

  $self->info("$$ is going to do $action");
  if ($action  eq "local copy"){
    $return=$self->makeLocalCopy($transfer);
  } elsif ($action eq "transfer") {
    $return=$self->startTransfer($transfer);
    if (!$return){
      #We want to keep the error code of FTS, and not the new one from thr SE
      my $error=$self->{LOGGER}->error_msg();
      $self->info("Let's clean the GUID from the SE");
      my ($seName, $seCert)=$self->{SOAP}->resolveSEName($transfer->{TOSE});
      if ($seName && $transfer->{GUID}){
	$self->info("Asking to delete $transfer->{GUID}");
	$self->{SOAP}->CallSOAP($seName,'unregisterFile', $seName,$transfer->{GUID});
      }
      $self->{LOGGER}->set_error_msg($error);
    }
  } elsif ($action eq "cleaning") {
    $return=$self->cleanLocalCopy($transfer);
  } else {
    $self->info("We don't know action $action :(");
  }
  $self->CURRENT_TRANSFERS_DECREMENT("", $id);

  if (! $return ) {
    $self->info("The transfer failed due to: ".$self->{LOGGER}->error_msg() );
    $self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, "FAILED", "ALIEN_SOAP_RETRY",{"Reason","The FTD at $self->{CONFIG}->{FTD_FULLNAME} got". $self->{LOGGER}->error_msg()});
  }
  $self->info("$$ returns!!");
  return 1;
}
sub makeLocalCopy {
  my $s =shift;
  my $transfer=shift;
  $self->debug(1, "Making a local copy");

  
  my @keys =keys %{$transfer};
  $self->debug(1, "Got @keys");
  my $id=$transfer->{ID};
  my $guid=$transfer->{GUID};
#  my $pfn=$transfer->{ORIGPFN};
  if (!$guid) {
    $self->{LOGGER}->error ("FTD", "ID $id Error: no GUID in makelocalcopy",1);
    return;
  }
  my @pfns=$self->getPFNfromGUID($transfer->{ORIGSE}, $transfer->{GUID});
  @pfns or $self->info ("Error getting the pfn!!") and return;
  my $pfn=shift @pfns;

  $self->info("ID $id The pfn is $pfn");
  my $file =AliEn::SE::Methods->new(
				    { "DEBUG", $self->{DEBUG}, 
				      "PFN", $pfn, "DATABASE", 
				      $self->{DATABASE} } );
  if (!$file){
    #      $self->{MANAGER}->
    $self->info("Error making the local copy (parsing $pfn)",22);
    return;
  }
  $self->info("ID $id Getting the FTPCopy of  $pfn" );
  
  my $localPfn=$file->getFTPCopy();
  if (!$pfn) {
    $self->{LOGGER}->warning( "FTD", "ID $id Error getting the FTPCopy",22 );
    return;
  }

  @pfns=();
  $self->info("ID $id The local copy is $localPfn" );

  foreach my $daemons (keys %{$self->{FTP_SERVERS}}){
    push @pfns, $self->{FTP_SERVERS}->{$daemons}->getURL($localPfn, $transfer->{ORIGSE});
  }
  $self->UpdateDiskSpace() or return;

  $self->info("ID $id Copy of $pfn done (@pfns)" ); 
  my $status="LOCAL COPY";
  my $options="";
  $self->{CONFIG}->{FTD_REMOTEOPTIONS} and 
    $options=join(";", @{ $self->{CONFIG}->{FTD_REMOTEOPTIONS_LIST}});
  $self->info("ID $id Setting options $options");

  my $info= {"FromPFN", \@pfns,
	     ORIGPFN=>$pfn,
	     "FromFTD", $self->{SERVICENAME},
	     "FromSE", $transfer->{ORIGSE},
	     "Action", "transfer",
	     "FromFTDOptions", $options,
	     "FromCertificate",  $self->{CONFIG}->{FTD_CERTSUBJECT}};

  if (!@pfns) {
    $status="FAILED";
    $info->{Reason}="Not possible to get transport URL for $localPfn";
  }
  my $result=$self->{SOAP}->CallSOAP("Manager/Transfer",
				     "changeStatusTransfer", "ALIEN_SOAP_RETRY",$id, $status, $info);
  $self->{SOAP}->checkSOAPreturn($result, "Transfer Manager") or return;
  return 1;
}
sub cleanLocalCopy {
  my $this=shift;
  my $transfer=shift;

  my $message="";

  $self->info("Cleaning the local copy");
  my $origPFN=$transfer->{ORIGPFN};
  my @listPFN;
  $transfer->{FROMPFN} and @listPFN=@{$transfer->{FROMPFN}};
  my $fromPFN=shift @listPFN;

  my $id=$transfer->{ID};


  $origPFN or $message=" missing ORIGPFN";
  $fromPFN or $message.=" missing FROMPFN";
  $id or $message.=" missing ID";

  $message and $self->{LOGGER}->error("FTD", "Error $message",2) and return;

  my $origURL=AliEn::SE::Methods->new($origPFN) or $message="Checking $origPFN";
  my $fromURL=AliEn::SE::Methods->new($fromPFN) or $message="Checking $fromPFN";

  $message and $self->{LOGGER}->error("FTD", "Error $message") and return;

  if (!($fromURL->path  eq $origURL->path)) {
    $self->info("Removing ".$fromURL->path );
    AliEn::MSS::file->rm($fromURL->path);
    }
  $self->{SOAP}->CallSOAP("Manager/Transfer", "changeStatusTransfer","ALIEN_SOAP_RETRY",$id,  "DONE");
#    $self->{MANAGER}->changeStatusTransfer($id,  "DONE");
  return 1;
}
sub UpdateDiskSpace {
    my $this=shift;

    my $handle = new Filesys::DiskFree;
    $handle->df();
    my $free = $handle->avail( $self->{CACHE_DIR} );

#    my $free=`df  --block-size 1 $self->{CACHE_DIR}`;
#    $self->debug(1, "Got $free");
#   $free =~ s/^(\S+\s+){10}(\d+).*$/$2/s;
   $self->{CACHE_SPACE}=$free;

    my $ca= Classad::Classad->new($self->{JDL});
    $ca->set_expression("CacheSpace", $free )
	   or $self->{LOGGER}->error("Transfer", "Error putting CacheSpace as $free")
	       and return;
    
    $self->{JDL}=$ca->asJDL();
    return 1;
}
sub dirandfile {
    my $fullname = shift;
    $fullname =~ /(.*)\/(.*)/;
    my @retval = ( $1, $2 );
    return @retval;
}

sub doTransfer {
    my $self       = shift;
    my $file       = shift;
    my $remotefile = shift;
    my $host       = shift;
    my $method     = shift;
    my $direction  = shift;
    my $id         = shift;

    $method="\U$method\E";
    # Now require the client, and create an instance
    my $class = "AliEn::FTP::$method";
    my $done=0;
    if ( eval "require $class; " ) {
      eval {
	$self->debug(1, "Class $class exists" );
        my $transfer = $class->new( { HOST => $host,
                                      MONITOR => $self->{MONITOR},
				      FTD_TRANSFER_ID => $id,
				      SITE_NAME => $self->{CONFIG}->{SITE},
				      FTD_FULLNAME => $self->{CONFIG}->{FTD_FULLNAME}
				  } );
        $done=$transfer->$direction( $file, $remotefile, @_ );
      }
    }
    if ($@) {
      $self->info("Error doing the transfer: $@",0);
      return -1;
    }
    $self->info("Transfer done and returning $done");
    return $done
}

sub CURRENT_TRANSFERS {
    my $s = shift;

# We are getting back count(CURRENT), 
    my $current = $self->{DB}->queryValue("SELECT count(*) from CURRENTTRANSFERS");
    $current or $current=0;
    return $current;
}

sub CURRENT_TRANSFERS_INCREMENT {
    my $s       = shift;
    my $size=  (shift or $MAXIMUM_SIZE);
    my $id=shift;

    ($size<$MAXIMUM_SIZE) and return;


    my $current = $self->CURRENT_TRANSFERS;
    $current++;
    print "INCREMENTING THE NUMBER OF TRANSFERS ($current)\n";

    my $done=$self->{DB}->do("INSERT INTO CURRENTTRANSFERS values ($id)");

    return 1;
}

sub CURRENT_TRANSFERS_DECREMENT {
    my $s       = shift;
    my $size= (shift or $MAXIMUM_SIZE);
    my $id=shift;

    ($size<$MAXIMUM_SIZE) and return;



    my $current = $self->CURRENT_TRANSFERS;
    $current--;
    print "DECRESSING THE NUMBER OF TRANSFERS ($current)\n";

    $self->{DB}->delete("CURRENTTRANSFERS","CURRENT='$id'");
    return 1;
}

sub checkIngoingTraffic {
    return 1;
}

sub chekcOutgoingTraffic {
    return 1;
}

# ################################################################
#       PUBLIC METHODS 
# ################################################################

sub transferDone {
    my $s    = shift;
    my $size = shift;

#    if ( $size > $max ) {    #$this->{LOWER_SIZE_LIMIT}) {
#    $self->CURRENT_TRANSFERS_DECREMENT($size);
#    }
    $self->debug(1,
        "Now  doing "
          . $self->CURRENT_TRANSFERS
          . " out of $self->{MAX_TRANSFERS}" );

    return 1;
}

sub failTransfer {
    my $s    = shift;
    my $ID   = shift;
    my $size = shift;

#    if ( $size > $max ) {    #$this->{LOWER_SIZE_LIMIT}) {
#    $self->CURRENT_TRANSFERS_DECREMENT($size);
#    }
    $self->debug(1,
        "Now  doing "
          . $self->CURRENT_TRANSFERS
          . " out of $self->{MAX_TRANSFERS}" );
    return 1;
}

sub verifyCompleteTransfer {
  my $s = shift;
  
  my ( $URL, $Origsize, $id ) = @_;
  $self->info("ID $id In verifyCompleteTransfer with @_" );
  
  my $size;
  eval {
    $size=$URL->getSize();
  };
  if ($@) {
    $self->info("Error getting the size of the file". $URL->string());
    return $error_codes->{TRANSFER_CORRUPTED};
  }
  
  if ( $size != $Origsize ) {
    $self->{LOGGER}->error( "FTD", "ID $id The file has size $size and should have $Origsize" );
    return $error_codes->{TRANSFER_CORRUPTED};
  }
  $self->info("ID $id Tranport success\n" );

  
  return $error_codes->{TRANSFER_SUCCES};
}

sub askToGet {
    my $s         = shift;
    my $URLString = shift;

    $self->info("Asking to get the file $URLString");
    my $URL = new AliEn::SE::Methods($URLString) 
	or return $error_codes->{FILE_NOT_FOUND};

    my $size =$URL->getSize();# -1;

    # Should now check if file exists! And check size to return to user
    my $localoptions = "";

    $self->{CONFIG}->{FTD_REMOTEOPTIONS}
      and $localoptions = join ";",@{ $self->{CONFIG}->{FTD_REMOTEOPTIONS_LIST} };
    if ( $size < $MAXIMUM_SIZE ) {    #$self->{LOWER_SIZE_LIMIT}) {
	#Always allow transfers of file less that 1 Mb.
        $self->info("Transfer of $size bytes granted" );
        my @returnv =
          ( $error_codes->{COMMENSE_TRANSFER}, $size, $localoptions );
        return @returnv;
    }
#    if ( $self->CURRENT_TRANSFERS > $self->{MAX_TRANSFERS} ) {
#        $self->info("Transfer of $size bytes denied. Already doing max ("
#              . $self->CURRENT_TRANSFERS
#              . ") transfers" );
#        return ( $error_codes->{TRANSFER_DENIED},
#            "Already executing maximum number of transfers" );
#    }

#    $self->CURRENT_TRANSFERS_INCREMENT($size);
    $self->info("Transfer of $size bytes granted. Doing "
		. $self->CURRENT_TRANSFERS
		. " out of $self->{MAX_TRANSFERS} transfers" );
    my @returnv =
	( $error_codes->{COMMENSE_TRANSFER}, $size, $localoptions );
    return @returnv;

}

sub askToPut {
    my $s         = shift;
    my $URLstring = shift;
    my $size      = shift;
    my $id        = shift;

    $self->{LOGGER}
      ->debug( "FTD", "ID $id In askToPut with $URLstring of size $size" );

    my $URL = new AliEn::SE::Methods($URLstring) 
	or return $error_codes->{FILE_NOT_FOUND} ;

    my $dir;

    #    my @allowed = @{$self->{ALLOWED_DIRS}};
    my $allo = 0;
    foreach $dir ( @{ $self->{ALLOWED_DIRS} } ) {
        if ( $URL->path =~ /^$dir/ ) {
            $allo = 1;
        }
    }
    if ( $allo == 0 ) {
        $self->info("Trying to put in an ilegal directory ("
              . $URL->path
              . ")\n Allowed= @{$self->{ALLOWED_DIRS}}" );
        return $error_codes->{PERMISSION_DENIED};
    }

    # First check if user is allowd to put the file here
    # NOT Implemented

    my $free = 1024 * 1024 * 1024;    #unlimited space... (2^32 bytes)

    if ( $URL->scheme eq "file" ) {
        my $handle = new Filesys::DiskFree;
        $handle->df();
        $free = $handle->avail( $URL->path );
        $self->debug(1,
            "System has " . ( $free / ( 1024 * 1024 ) ) . "Mb of free space" );
    }

#    if ( ($size) > $free ) {
#        $self->{LOGGER}->warning( "FTD",
#            "Not enough free disk space, trying to allocate diskscape" );
#        my ( $localdir, $localfile ) = dirandfile( $URL->path );
#        if ( !( $self->alloc( $localdir, $size ) ) ) {
#            return $error_codes->{OUT_OF_DISK_SPACE};
#        }
#    }
    if ( $size < $MAXIMUM_SIZE ) {    #$self->{LOWER_SIZE_LIMIT}) {
	#Always allow transfers of file less that 1 Mb.
        $self->info("ID $id Transfer of $size bytes granted" );
        return $error_codes->{COMMENSE_TRANSFER};

    }
#    if ( $self->CURRENT_TRANSFERS > $self->{MAX_TRANSFERS} ) {
#      $self->info("Transfer of $size bytes denied. Already doing max ("
#		  . $self->CURRENT_TRANSFERS . ") transfers" );
#      return $error_codes->{TRANSFER_DENIED};
#    }
    
#    $self->CURRENT_TRANSFERS_INCREMENT($size);
    $self->info("ID $id Transfer of $size bytes granted. Doing "
			   . $self->CURRENT_TRANSFERS
			   . " out of $self->{MAX_TRANSFERS} transfers" );
    return $error_codes->{COMMENSE_TRANSFER};

}

sub CreateLocalUniquePFN{
  my $t=shift;

  my $host=$self->{CONFIG}->{FTD_HOST};
  my $port=$self->{CONFIG}->{FTD_PORT};
  my $path=$self->{CONFIG}->{FTD_ALLOWEDDIRS};
  my $date=time;
  my $basename="FTD.$date";
  return "file://$host:$port$path/$basename";

}

# This subroutine inserts in the table of files to transfer a new entry
# It gets the local pfn, the remote pfn and the direction. If any of the 
# pfns are missing, it creates a temporary name
#

sub requestTransfer {
    my $s = shift;
    $self->debug(1, "In requestTransfer with @_" );

    my $sourceURL = (shift or $self->CreateLocalUniquePFN());
    my $destURL   = shift;

    #my $size       = shift;
    my $direction = shift;
    my $priority  = shift || 10;     # 0 means the transfer is interactive.
    my $email     = shift || "";
    my $soapData  = shift || "";
    my $origid    = shift || "-1";

    my $size = -1;                   #Means unknown

    $self->info("Requesting a transfer");
    my $sURL = new AliEn::SE::Methods($sourceURL) or return;
    my $dURL = new AliEn::SE::Methods($destURL) or return;

    my $fileName = $sURL->path;

    my $localStorageMethod = $sURL->scheme;

    # As a minimum we need to know the file, the destination and source URL

    if ( !($fileName) or !($destURL) or !($sourceURL) or !( defined $size ) ) {
        $self->{LOGGER}
          ->warning( "FTD", "Not enough arguments to requestTransfer" );
        return;
    }

    ( $direction eq "get" )
      or ( $direction eq "put" )
      or $self->{LOGGER}
      ->warning( "FTD", "Error: direction has to be either get or put" )
      and return;

    my $destfile = $dURL->path;
    my $srcfile  = $sURL->path;

    if ( $direction eq "get" ) {
        $self->info("Request to get $destURL to $sourceURL" );
        if ( $srcfile =~ /\/$/ ) {
            my ( $srcpath, $srcname ) = dirandfile($destfile);
            $sURL->path( $srcfile . $srcname );

        }
    }

    if ( $direction eq "put" ) {

        if ( $destfile =~ /\/$/ ) {
            my ( $srcpath, $srcname ) = dirandfile($srcfile);
            $dURL->path( $destfile . $srcname );
        }
        $self->info("Request to put $sourceURL to $destURL" );
    }

    my $id = $self->generateID;

    # Now we should check that we can handle files in $localStorageMethod mode.
    # Only implemented for methos  = file 

    if ( ( $localStorageMethod eq "file" ) and ( $direction eq "put" ) ) {
        if ( !( -e $sURL->path ) ) {
            $self->debug(1, "File does not exists" );
            return $error_codes->{FILE_NOT_FOUND};
        }
        $self->debug(1, "File is here" );
        my (
            $dev,   $ino,     $mode,     $nlink, $uid,
            $gid,   $rdev,    $FILEsize, $atime, $mtime,
            $ctime, $blksize, $blocks
          )
          = stat( $sURL->path );
        $size = $FILEsize;
        $self->debug(1, "The file has size $size" );
    }

    my $exists =
      $self->{DB}->queryValue( "SELECT ID from FILETRANSFERSNEW where sourceURL='$sourceURL' and finaldestURL='$destURL' and status='WAITING'" );
    if (defined $exists) {
        $self->info("File $sourceURL with destination $destURL is already scheduled for transfer" );
        return $error_codes->{TRANSFER_ALREDY_REQUESTED};
    }

    my $date=time;
    my $SQL ="INSERT INTO FILETRANSFERSNEW VALUES ('$id','$fileName','$sourceURL',
$size,$date,'WAITING', '$destURL','$email',0,0,'$direction',0)";

    $self->debug(1, "Inserting $SQL" );

    $self->{DB}->do($SQL);

    return $id;

}

sub inquireTransferByID {
    my $s  = shift;
    my $ID = shift;
    my ($stat) =
      $self->{DB}->queryRow(
"SELECT status, sourceURL, finaldestURL, direction, message from FILETRANSFERSNEW where ID='$ID'"
      );
    if ( !($stat) || ! (exists $stat->{status}) )  {
        return (-1, "The transferID $ID does not exist");
    }

    if ( $stat->{direction} eq "get" ) {
        return ( $stat->{status}, $stat->{sURL}, $stat->{message} );
    }
    if ( $stat->{direction} eq "put" ) {
        return ( $stat->{status}, $stat->{dURL}, $stat->{message} );
    }
    return -1;
}

sub checkTransfer {
    my $s = shift;
    $self->debug(1, "In checkTransfer with @_" );

    my $pfn = shift;

    if ( !$pfn ) {
        $self->{LOGGER}
          ->warning( "FTD", "Not enough arguments to checkTransfer" );
        return;
    }

    my ($stat) =
      $self->{DB}->queryRow(
"SELECT status, filename, finaldestURL from FILETRANSFERSNEW where sourceURL='$pfn'"
      );

    if ($stat && exists $stat->{status}) {
        my $uri = new AliEn::SE::Methods($stat->{finaldestURL}) or return;
        my $basename = "";
        $stat->{filename} =~ /\/([^\/]*)$/ and $basename = $1;

        $self->info("File $pfn is already scheduled for transfer $stat->{status} and $stat->{finaldestURL}" );
        ( $stat->{status} eq "DONE" ) and 
	  return $uri->path . "/$basename";

        return -2;

    }
    $self->debug(1, "The file was not there" );

    return;

}

sub getInfo {
    my $s = shift;
    $self->debug(1, "Sending info about myself" );
    my $msg = "I'm a " . ref($self) . " version $self->{CONFIG}->{VERSION}\n";
    $msg .=
      "I'm running version $DBBrowser::FTDTXTDB::VERSION on the text database\n";
}

sub senfFile {
    my $s       = shift;
    my $file    = shift;
    my $tgtHost = shift;
    my $tgtPort = shift;

    $self->debug(1, "Sending  $file to $tgtHost $tgtPort" );
    return 1;
}

# Given an SE and a guid, it returns all the pfns that the SE 
# has of that guid
sub getPFNfromGUID {
  my $self=shift;
  my $se=shift;
  my $guid=shift;
  $self->info("Asking the catalogue for the PFN of $guid");
  my @info=$self->{CATALOGUE}->execute("whereis", "-gr", $guid);
  my @pfns=();
  my $seInfo=shift @info;;
  #We don't want the se name...
  while ($seInfo){
    my $pfn= shift @info;
    if ($seInfo =~ /^$se$/){
      $self->info("This is the se that we were looking for");
      push @pfns, $pfn;
    }
    $seInfo= shift @info;
  }
  $self->info("Got @pfns");
  return @pfns;
}
#aub createGridMapFromLdap {
#   my $s = shift;#
#
#    my $mapfile   = $self->{GRIDMAP};
#    my $mapToUser = getpwuid($<);#
#
#    my $ldap = Net::LDAP->new( $self->{CONFIG}->{LDAPHOST} ) or die "$@";
#    my $base = $self->{CONFIG}->{LDAPDN};
#
#    $ldap->bind();
#    my $mesg = $ldap->search(    # perform a search
#        base   => "$base",
#        filter => "(&(objectclass=AliEnFTD))"
#    );
#    my $entry;
#    my $i;
#    $self->debug(1, "Mapfile: $mapfile" );
#    if ( open( MAP, ">$mapfile" ) ) {
#        for ( $i = 0 ; $i < $mesg->count ; $i++ ) {
#            $entry = $mesg->entry($i);
#            print MAP "\""
#              . $entry->get_value('CertSubject')
#              . "\" $mapToUser\n";
#        }
#        $self->{LOGGER}
#          ->info( "FTD", "Succesfully created gridmap-file for FTD" );
#        close MAP;
#    }
#    else {
#        $self->{LOGGER}
#          ->error( "FTD", "Unable to create gridmap-file for FTD" );
#    }
#    $ldap->unbind;
#    return 1;
#}

return 1;

__END__

=head1 NAME

AliEn::Service::FTD - Alien File Transport Daemon

=head1 SYNOPSIS


use SOAP::Transport::HTTP;

my $FTDhost = "pcepaip01.cern.ch";
my $FTDport = "8091";

my $localURL  = "file://pcepaip01.cern.ch:8091/tmp/FILE_ON_PCEPAIP01";
my $remoteURL = "file://pcepaip15.cern.ch:8091/tmp/FILE_ON_PCEPAIP15";
my $method    = "put"; # Puts $localURL to $remoteURL

my $response=SOAP::Lite
    -> uri( "AliEn/Services/FTD" )
    -> proxy("http://$FTDhost:$FTDport" )
    -> requestTransfer($sourceURL,$destURL,$method);

my $ID = $response->result;

my ($status,$srcURL,$destURL);
sleep(20);
print "Inquiring about $ID\n";
my $soap=SOAP::Lite
    -> uri( "AliEn/Services/FTD" )
    -> proxy("http://$FTDhost:$FTDport" )
    -> inquireTransferByID($ID);

my $status = $soap->result;
my $URL = $soap->paramsout;
print "Your transfer to destination $URL has status $status\n";


=head1 DESCRIPTION

The File Transfer Daemon (FTD) is the part of AliEn that can transport potentially very large files from one site to another. To the user the tranportation method is unknown, and the actual method used for the transfer is determined on the fly depending on where the file is tranported from and to and also how big the file is. 

Currently only one methods is implemented, namely bbftp, but one could imagine several other methods like GridFTP, SOAP-Transport or even mailing the file. To transfer a file from A to B, two FTDs are needed, one at A and another at  B. 

The Daemon consists of several methods, some of them can be accesed with Remote Procedure Call via SOAP. The user-application, will usually only need one method, namely requestTransfer (see Requesting filetransfers).

=head1 USAGE

Se the small example in SYNOPSIS. This example will contact the FTD at pcepaip01.cern.ch and ask to transfer $localURL on  pcepaip01.cern.ch to $remoteURL on pcepaip15. Hence the namimg local and remote, referes to the called FTD.

If instead one had set the method to get, one would ask to transfer $remoteURL on pcepaip15 to $localURL on pcepaip01.

When called with put and the URL-sheme is file://, the FTD will check that the file exists and its size. If the file does not exist, an error os returned (Se errors) and the file is not scheduled for transfer. This check is not done if called with get, since the file is (not yet) there. Hence we recommend that method put is used

=head1 SECURITY

Since filetransfer could be a security risk, every file transfer daemon has a certificate. This certificate is used to identity itself to other FTDs. All filetransferdaemons will update the local grid-map file (Only used for bbftp) based on the central AliEn LDAP-server. The update is done regularly (Every hour). This way all filetransfer-daemons can authenticate to eachother. All other FTDs will be mapped to the user that is running the FTD.

=head1 STARTING THE FTD

The FTD might need to have access to local Mass Storaga (Castor or HPSS). Hence the local user that will run the FTD should have this access. 

Since certificates are needed in order to to the acutal filetrasnfer (With bbftp) the FTD will check for a hostcertificate in $HOME/.alien/identities.ftd/cert.pem. If this does not exist it will fail to start. Also the subject on this certificate must match the subject that in LDAP for this given FTD. 

=cut




