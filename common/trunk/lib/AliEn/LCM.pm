package AliEn::LCM;
select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Logger;
use vars qw($VERSION);

use AliEn::Config;
use strict;

use AliEn::X509;
use AliEn::SOAP;
use AliEn::SE::Methods;
use AliEn::Database::LCM;
use AliEn::MSS::file;
use AliEn::Service::SubmitFile;
use AliEn::MD5;


use vars qw (@ISA $DEBUG);
push @ISA, 'AliEn::Logger::LogObject';
$DEBUG=0;

# Use this a global reference.
##############################################################################
#Private functions
##############################################################################
sub startTransferDaemon {
  my $self=shift;
  my $pfn=shift;
  my $certificate=(shift or "");
  my $use_cert=shift; 
  defined $use_cert or $use_cert=0;

  $self->{FILEDAEMON}= 
    AliEn::Service::SubmitFile->new({"pfn" => $pfn, 
				     "SEcert" => $certificate,
				    "USE_CERT"=>$use_cert,}) or return;

  return $self->{FILEDAEMON}->{pfn};
}

sub stopTransferDaemon {
  my $self=shift;

  $self->{FILEDAEMON}->stopTransferDaemon;

  return 1;
}

sub bringRemoteFile {

    my $self      = shift;
    my $pfn       = shift;
    my $SE        = shift;
    my $localFile = (shift or "");

    my $size;
    my $result = "";
    #First, ask to get the file

    $DEBUG and $self->debug(1, "Asking the SE to bring the file");

    my $response=$self->{SOAP}->CallSOAP("SE", "bringRemoteFile", $pfn, $SE, $localFile ) or return;
#    my $response=$self->{SE}->bringRemoteFile( $pfn, $SE, $localFile );


    my $transferID=$response->result;

    return $self->inquireTransfer("SE", $transferID);
}



sub getLocalCopy {
  my $self      = shift;
  my $guid       = shift;
  my $localFile = shift;

  my ($data) =
    $self->{TXTDB}->queryRow("SELECT localpfn,size,md5sum FROM LOCALGUID where guid='$guid'");
  $DEBUG and $self->debug(1, "Looking for local copies");
  if ($data and $data->{localpfn}) {
    my $file= $data->{localpfn};

    if ( -f $file ) {
      my $size= -s $file;
      if ($size eq $data->{size}) {
	my $md5sum=AliEn::MD5->new($file);
	if ( $md5sum eq $data->{md5sum}){
	  $DEBUG and $self->debug(1, "Giving back the local copy $file" );
	  $localFile or return $file;
	  $DEBUG and $self->debug(1, "Copying '$file' to '$localFile'" );
	  if ($file ne $localFile) {
	    $self->info("They are not the same ??\n");
	    (AliEn::MSS::file::cp({},$file, $localFile )) 
	      and print STDERR "ERROR copying $file to $localFile $!\n" 
		and return;
	  }
	  return $localFile;
	}else {
	  $self->info( "The localfile had a different md5sum that we expected...($md5sum instead of $data->{md5sum})");

	}	
      } else {
	$self->info( "The localfile had a different size that we expected...($size instead of $data->{size})");
      }
    }
    $DEBUG and $self->debug(1, "File $data does no longer exist"); 
    $self->{TXTDB}->delete("LOCALGUID","guid='$guid'");
  }
  return;
}

#function bringFileToSE
#input: $se         Se Name (Alice::CERN::Castor)
#       $transfer   hash including source, target, USER, DESTINATIOn, LFN, TYPE
#
# output
#       undef if error
#       -2, $id  if transfer has been schechuled
#       {"pfn"=>"", "transfer"=>""} if everything worked. transfer specifies 
#                                   if a transfer has been made( in this case, 
#                                   we don't need to do addMirror)
#
# It makes a file available in the SE specified
#
# Called from LCM->get and UI/LCM->mirror
# 

sub bringFileToSE {
  my $self=shift;
  my $se= shift;
  my $transfer=shift;
  my $options=(shift or "");

  $self->info( "In bringFiletoSE, with $se");
  my @info=$self->{SOAP}->resolveSEName($se)
    or $self->info("Error getting the address of $se") and return;
  my $result= $self->{SOAP}->CallSOAP($info[0],"getFile",$se, $transfer)
    or return;
#  my $result= $self->{SE}->getFile($se, $transfer);

  my $done=$result->result;

  if ($done eq "-2") {
    my $id =$result->paramsout;
    $self->info( "Transfer has been scheduled (transfer $id)");
    if ($options !~ /b/ ){
      $self->info( "Waiting until the transfer is completed...");
      while (1){
	sleep (20);
	$self->info( "Asking if the transfer has finished...");
	#	  $result=$self->{SE}->checkTransfer($id);
	$result=$self->{SOAP}->CallSOAP("SE","checkTransfer",$id) or return;
	
	my $status=$result->result;
	$self->info( "Got $status");
	  ($status eq "DONE") and return {"pfn", $result->paramsout,
					  "transfer", 1};
	($status eq "FAILED") and return ;
	($status eq "INCORRECT") and return ;
      }
    }
    return -2, $id;
  }

  return $done;
}


 sub inquireTransfer{
  my $self=shift;
  my $seName=shift;
  my $transferID=shift;

  $self->info( "Transfer has been scheduled...");
  $DEBUG and $self->debug(1, "waiting until it's completed (transferID $transferID)");
  my $counter=0;
  while ( 1) {
    $DEBUG and $self->debug(1, "Asking the SE at $seName about $transferID");
    my $options="";
    ($counter == "10") and $options="-f" and $counter=0;
    my $response=$self->{SOAP}->CallSOAP($seName, "inquireTransfer", $transferID, $options ) or return;


    my $status=$response->result;
    $DEBUG and $self->debug(1, "The call returned $status");
    if ($status eq -2)
      {
	print  ".";
	my $id=$response->paramsout;

       ($id ne $transferID) 
	 and $self->debug(1,"The transferID has changed!! (it was $transferID and now it is $id" )
	   and $transferID=$id;
	sleep(10);
      }
    else{
      print "done!!\n";
      my $size=$response->paramsout;
      return ({"pfn", $status, "size", $size});
    }
    $counter++;
  }
}

##############################################################################
#Public functions
##############################################################################
sub new {
    my $proto = shift;
    my $self  = {};
    $self->{CONFIG}= (shift or AliEn::Config->new() );


    my $class = ref($proto) || $proto;

    $self->{CONFIG} or print STDERR "Error getting the configuration in LCM\n" and return;

    bless( $self, $class );
    $self->SUPER::new() or return;

    $self->{SOAP}=new AliEn::SOAP;
    # Initialize the logger
    $self->{SILENT} = ( $self->{CONFIG}->{silent} or 0 );

#    $self->{DATABASE}=$self->{CONFIG}->{DATABASE};
#    $self->{DATABASE}
#      or print STDERR "Error no database specified in LC\n"
#      and return;
    $self->{X509}=new AliEn::X509;
    my $inittxt = "Initializing Local Cache Manager";
    $self->{DEBUG} = 0;
    if ( $self->{debug} ) {
        $self->{DEBUG} = 5;
        $self->{LOGGER}->debugOn($self->{debug});
        $inittxt .= " in debug mode";
    }

    $self->{CONFIG} = new AliEn::Config();
    $self->{CONFIG} or print STDERR "Error getting the config \n" and return;

    $DEBUG and $self->debug(1, $inittxt );

    $self->{CACHE_DIR} = $self->{CONFIG}->{CACHE_DIR};
    if ( !( -d $self->{CACHE_DIR} ) ) {
        mkdir $self->{CACHE_DIR}, 0777;
    }

    $self->{TXTDB} = AliEn::Database::LCM->new(
        {
            "DEBUG",  $self->{DEBUG}, "CONFIG", $self->{CONFIG},
            "LOGGER", $self->{LOGGR}
        }
    );

    $self->{TXTDB} or $self->{LOGGER}->error("LCM", "Error getting the text database") and return;

#    if ( $self->{CONFIG}->{SE} ) {
#        $DEBUG and $self->debug(1,
#"Contacting the SE at $self->{CONFIG}->{SE_HOST}:$self->{CONFIG}->{SE_PORT}"
#        );
#	$self->{SOAP}->checkService("SE") or 
#	  $self->info( "Error contacting the local SE");
#    }
    return $self;
}

sub quit {
    my $self = shift;
    $DEBUG and $self->debug(1, "Killing SE\n" );

}

sub getFile {
  my $self      = shift;
  my $pfn       = shift;
  my $SE        = ( shift or "" );
  my $localFile = ( shift or "" );
  my $opt       = ( shift or "" );
  my $lfn       = ( shift or "" );
  my $guid      = ( shift or "" );
  my $md5       = (shift or "");

  $self->info( "In getfile, with $SE ");

  ($pfn)
    or $self->{LOGGER}->warning( "LCM", "Error no file specified" )
      and return;
  $self->{SILENT} or $self->info( "Getting the file $pfn" );

  if ( $opt =~ /f/ ) {
    $DEBUG and $self->debug(1, "Deleting the local copies of $pfn" );
    $self->{TXTDB}->delete("LOCALGUID","guid='$guid'");
  }

  $DEBUG and $self->debug(1, "Evaluating $pfn" );

  my ($result, $size);
  eval {
    my $file = AliEn::SE::Methods->new({DEBUG=>$self->{DEBUG},PFN=> $pfn,
					DATABASE=> $self->{DATABASE}, 
					LOCALFILE=> $localFile });
    ($file) or die ("We are not able to parse the pfn $pfn\n");
    if ( $opt =~ /l/ ) {
      $result = $file->getLink("-s");
    }
    else {
      $result = $file->get("-s");
    }
  };

  $DEBUG and $@ and $self->debug(1,"There was an error: $@");

  if ($result){
    if ($md5 ){
      $self->debug(1,"The copy worked! Let's check if the md5 is right");
      my $newMd5=AliEn::MD5->new($result);
      $newMd5 eq $md5
	or $self->info("Error: The md5sum of the file doesn't match what it is supposed to be (it is $newMd5 instead of $md5)") and return;

    }

    $self->info( "Everything worked and got $result");
    $self->{TXTDB}->insertEntry($result, $guid);
  } else {
    $self->info("Error getting the file");
  }
  return $result;
}


#sub getFileFromSE {
#  my $self=shift;
#  my $pfn=shift;
#  my $localFile=shift;
#  my $se=shift;

#  $self->info("Getting the local copy brought by the SE $se $pfn");

#  my $seInfo=$self->{CONFIG}->CheckServiceCache("SE", $se);
#  $seInfo or $self->info("Error getting the info of $se")
#    and return;
#  my $sePort=$seInfo->{PORT};
#  $sePort and $sePort=":$sePort";
#  $DEBUG and $self->debug(1, "We have to contact in port $sePort");


#  my @possibles=($pfn);
#  if ($possibles[0]=~ /^file/){
#    push @possibles, ($pfn,$pfn);
#    $possibles[1] =~ s/^file/rfio/;
#    $possibles[2] =~ s/^file(:\/\/[^:\/]*)(:\d+)?(\/.*)$/soap$1$sePort$3?URI=SE/;
#  }
#  if ($possibles[0]=~ /^bbftp/){
#    push @possibles, ($pfn);
#    $possibles[1] =~ s/^bbftp(:\/\/[^:\/]*)(:\d+)?([^\?]*)\?.*$/soap$1$sePort$3?URI=SE/;
#  }

#  my $result="";

#  while ( (! $result) && (my $pfn=shift @possibles)){
#    $DEBUG and $self->debug(1, "TRYING WITH $pfn");
#    my $URL=AliEn::SE::Methods->new({"PFN", $pfn,
#				     "LOCALFILE", $localFile,
#				     "DEBUG", 0,
#				     SILENT=>1}) or return;
#    eval {
#      $result=$URL->get("-s");
#    };
#  }
#  #If the SE started a service for us, let's stop it
#  if ($pfn=~ /^(bb|grid)ftp:\/\/[^:\/]*:(\d+)\// ){
#    $DEBUG and $self->debug(1, "Telling the SE to stop the service in $2");
#    $self->{SOAP}->CallSOAP("SE", "stopFTPServer", $2);
#  }
#  $result or $self->info("Error transfering the local copy brought by the SE to $pfn",1000) and  return;
#  return $result; 
#}

# Register a file in a SE
#
#
#
sub registerInLCM {
  my $self  = shift;
  my $pfn   = shift;
  my $newSE = ( shift or $self->{CONFIG}->{SAVESE_FULLNAME} or $self->{CONFIG}->{SE_FULLNAME} or "");
  my $oldSE = ( shift or "" );
  my $target = (shift or "");
  my $lfn=(shift or "");
  my $options=(shift or "");
  my $reqGuid=(shift or "");
  my $envelope=(shift or "");
  ($pfn)
    or $self->{LOGGER}->warning( "LCM", "Error no pfn specified" )
      and return;

  $self->info( "Registering the file $pfn in $newSE" );

  my $result=
    $self->RegisterInRemoteSE($pfn, $newSE, $oldSE, $target, $lfn, $options, $reqGuid, $envelope);

  $result
    #       or $self->{LOGGER}->warning( "LCM", "Error contacting the SE" )
    or  return;

  ( $result eq -1 )
    and print STDERR "ERROR copying $pfn\n" . $result->paramsout . "\n"
      and return;

  $result->{pfn}
    or $self->{LOGGER}->warning( "LCM", "Error transfering the file to the SE" )
      and return;

  $self->info( "Getting the file $result->{pfn} of size $result->{size}" );
  return $result;
}

sub checkPFNisLocal {
  my $self=shift;
  my $pfn=shift or return;
  $DEBUG and $self->debug(1,"Checking if $pfn is a localfile");
  $pfn =~ /^file:\/\// or return;

  my $shortName=$self->{CONFIG}->{HOST};
  $shortName =~ s/\..*$//;

  $pfn=~ /^(localhost)|($self->{CONFIG}->{HOST})|($shortName)|(\/)/ or return;
  $DEBUG and $self->debug(1,"It is a local file");
  return 1;
}

sub RegisterInRemoteSE {
  my $self=shift;
  my $pfn=shift;
  my $newSE= (shift or "");
  my $oldSE= (shift or "");
  my $target= (shift or "");
  my $lfn=(shift or "");
  my $options=(shift or {});
  my $reqGuid=(shift or "");
  my $envelope=(shift or "");


  my $use_cert=1;
  my $repeat=1;
  my $localfile=$self->checkPFNisLocal($pfn);

  if ( $options->{reverse}  ||   not  $localfile){
    while (1) {
      $self->{SOAP} or $self->{SOAP}=new AliEn::SOAP;

      my ($seName, $seCert)=$self->{SOAP}->resolveSEName($newSE) or return;

      my $newpfn=$self->startTransferDaemon($pfn, $seCert, $use_cert)
	or return;
      my $message="Contacting SE $seName, and tell it to pick up $newpfn";
      $oldSE and $message.="(using oldSE $oldSE)";
      $self->info( $message );

      my $result =$self->waitForCopyFile($seName,{"source"=> $newpfn,
						  "oldSE"  => "$oldSE",
						  "target"=>$target,
						  "options"=> "f",
						  lfn=>$lfn});
      $message="Returned: ";
      $result and $message.=$result;
      $DEBUG and $self->debug(1, $message );
      $self->stopTransferDaemon();
      $result and return $result;
      $use_cert or $repeat=0;
      $use_cert=0;
      $newpfn=~ /^((bb)|(grid)ftp)/ or last;
      $self->info( "Ok, it didn't work... let's see if we can repeat it without $1");
    }
    $self->info( "Asking the SE to fetch the file didn't work... let's see if we can upload it");
  }
  $self->debug (1, "Trying to upload the file to the SE");
  my $url=AliEn::SE::Methods->new($pfn) 
    or $self->info( "Error creating the url of $pfn")
      and return;

  if ($url->method()=~ /^file/ and $url->host() !~ /^($self->{CONFIG}->{HOST})|(localhost)$/){
    $self->info("Error: we are in $self->{CONFIG}->{HOST}, and we can't upload a file from ". $url->host());
    return;
  }


  my $info={};
  $info->{size}=$options->{size} || $url->getSize();
  defined $info->{size} or $self->info("Error getting the size of $pfn") 
    and return;
  $info->{md5}=$options->{md5} ||AliEn::MD5->new($pfn);

  $info=$self->getPFNName($newSE, $info, $reqGuid, $envelope );
  $info or return;

  my $url2=AliEn::SE::Methods->new({PFN=>$info->{pfn},
				    LOCALFILE=>$url->path()})
    or $self->info("Error creating the url of $pfn") and return;
  
  my $done=$url2->put() or
    $self->info("Error uploading the file: ") and return;

  $self->info( "File uploaded successfuly");
  return ($info);
}

sub getPFNName {
  my $self=shift;
  my $info=$self->getPFNNameFromEnvelope(@_);
  $info and return $info;
  $self->info("Couldn't get the pfn from the envelope...");
  $self->getPFNNameFromSE(@_);
}


 sub getPFNNameFromEnvelope{
  my $self=shift;
  my $newSE=shift;
  my $info=shift;
  my $reqGuid=shift;
  my $envelope=shift;
  $self->debug(1, "Can we get the guid and the pfn from the envelope???");
  $envelope or return;
  $envelope->{guid} or return;

  $info->{guid}=$envelope->{guid};
  $info->{pfn}=$envelope->{url};
  $info->{pfn}=~ s{^([^/]*//[^/]*)//(.*)$}{$1/$envelope->{pfn}};
  $info->{pfn}=~ m{root:////} and return;
  $self->info("According to the envelope: $info->{pfn} and $info->{guid}");
  return $info;
}

 sub getPFNNameFromSE{
  my $self=shift;
  my $newSE=shift;
  my $info=shift;
  my $reqGuid=shift;

  $self->info("We don't have an envelope. Asking the SE for a filename");
  $self->{SOAP} or $self->{SOAP}=new AliEn::SOAP;
  my ($seName, $seCert)=$self->{SOAP}->resolveSEName($newSE) or return;

  my $result=$self->{SOAP}->CallSOAP($seName, "getFileName",$seName, $info->{size},{md5=>$info->{md5}, guid=>$reqGuid})
      or $self->info("Error asking for a filename") and return;
    
  my @fileName=$self->{SOAP}->GetOutput($result);
  $DEBUG and $self->debug(1, "Got @fileName");
  $info->{guid}=$fileName[4];
  $info->{pfn}=$fileName[3];

  return $info;

}

sub waitForCopyFile {
  my $self=shift;
  my $seName=shift;
  my $options=shift;

  $self->info( "Copying a file into an SE");
  my $response=$self->{SOAP}->CallSOAP($seName, "copyFile",$options)
    or $self->info( "Error talking to the SE") and return;

  my $file=$response->result;
  if ($file eq "-2"){
    my $transferID=$response->paramsout;
    $self->info( "Transfering the file (ID $transferID)... please wait");

    return $self->inquireTransfer($seName, $transferID);
  }

  ( UNIVERSAL::isa( $file, "HASH" ))
    or $self->info( "The SE did not return a hash") and return;

  $DEBUG and $self->debug(1, "Returning $file->{pfn} and $file->{size}");
  return $file;
}

sub eraseFile {
  my $self = shift;
  my $url  = shift;

  my $protocol=AliEn::SE::Methods->new({PFN=>$url,DEBUG=>$self->{DEBUG}})
      or $self->info("Error getting the method for $url") and return;

  return $protocol->remove();
}


sub listTransfer {
  my $self=shift;
  my $doSummary=grep (/^-?-summary$/, @_);
  $self->info("Checking the transfer @_");
  my $done=$self->{SOAP}->CallSOAP("Manager/Transfer", "listTransfer", @_)
    or return;

  my $result=$done->result;
 
  $result=~ /^listTransfer: returns all the transfers/ and
    $self->info($result) and     return;

  my $message="TransferId\tStatus\t\tUser\t\tDestination\t\t\tSize\t\tSource\t\tAttmpts\n";
  my $format="%6s\t\t%-8s\t%-10s\t%-15s\t\t%-12i\t\%12s\%12s";
  my @transfers = @$result;
  my $summary="";
  my $info={};
  
  foreach my $transfer (@transfers) {
#    $DEBUG and $self->debug(3, Data::Dumper($transfer));
    my (@data ) = ($transfer->{transferId},
		   $transfer->{status},
		   $transfer->{user},
		   $transfer->{destination} || "",
		   $transfer->{size} || 0,
		   $transfer->{attempts},
		   $transfer->{SE} || "",
		   $transfer->{jdl} || "",);

    $data[3] or $data[3]="";
#    #Change the time from int to string

    my $string=sprintf "$format", @data;
    $message.="$string\n";
    
    if ($doSummary){
      my $number=1;
      $info->{$transfer->{status}} and $number+=$info->{$transfer->{status}};
      $info->{$transfer->{status}}=$number;
    }
  }
  
  if ($doSummary){
    $message.="\n\nSummary of all the transfers:\n";
    $self->debug(4,"Ready to do the summary");
    foreach my $status (keys %$info){
      $message.="\t\t$status -> $info->{$status}\n";
    }
  }
  
  $self->info( $message,undef,0);

  return $result;
}

sub killTransfer {
  my $self=shift;
  $self->info("Killing the transfers @_");
  my $user=$self->{CONFIG}->{ROLE};
  my ($result) =  $self->{SOAP}->CallSOAP("Manager/Transfer", "killTransfer",$user, @_) or return;
  my ($info, @rest)=$self->{SOAP}->GetOutput($result);

  $self->info("\t".join("\n\t", @$info),0,0);

  return $result;
}

 sub checkDiskSpace{
  my $self=shift;
  my $space=shift ||0;
  my $localFile=shift;

  $self->debug(1, "Checking if we have $space bytes of diskspace to get the file");

  return 1;

}


sub resubmitTransfer {
  my $self=shift; 
  
  my $res=$self->{SOAP}->CallSOAP("Manager/Transfer", "resubmitTransfer", @_);
  
  if(!$res){
  	$self->{LOGGER}->error("LCM", "In resubmitTransfer while calling resubmitTransfer, in Manager/Transfer");
	$res=$self->{SOAP}->CallSOAP("Manager/Transfer", "resubmitTransferHelp", @_);
	return;
  }
  
   $self->info( "Transfer Resubmitted");
   return 1;
}


sub resubmitFailedTransfers {
  my $self=shift; 
  
  my $res=$self->{SOAP}->CallSOAP("Manager/Transfer", "resubmitFailedTransfers", @_);
  $self->info( "Resubmitting all failed Transfers");
   return 1;
}

return 1;

