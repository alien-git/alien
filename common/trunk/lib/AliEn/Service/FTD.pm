#
#  File tranport daemon for Alien
#

package AliEn::Service::FTD;

#use AliEn::Database::TXT::FTD;
use LWP::UserAgent;

#use AliEn::SE::Methods;
#use AliEn::X509;
use POSIX ":sys_wait_h";
use strict;

use vars qw(@ISA);
use Classad;

#use AliEn::MSS::file;

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
  
  $self->{PORT}="NO PORT";
  $self->{HOST}=$self->{CONFIG}->{HOST};
  chomp $self->{HOST};

  $self->{SERVICE}="FTD";
  $self->{SERVICENAME}=$self->{CONFIG}->{FTD_FULLNAME};
  $self->{LISTEN}=1;
  $self->{PREFORK}=1;

#  $self->{PROTOCOLS}=$self->{CONFIG}->{FTD_PROTOCOL};
  # the default protocol is "bbftp"
#  $self->{PROTOCOLS} or $self->{PROTOCOLS} ="bbftp";
  my @protocols=();
  $self->{CONFIG}->{FTD_PROTOCOL_LIST} and 
    @protocols=@{$self->{CONFIG}->{FTD_PROTOCOL_LIST}};


  #$self->{PROTOCOLS}=\@protocols;
  $self->{PROTOCOLS}=join(",", @protocols);
  foreach my $p (@protocols){
    my $test="AliEn::FTP::".lc($p);
    eval "require $test" or $self->info("Error requiring $test: $@") 
      and return;
    $self->info("initializing the $p protocol transfer");
    $self->{PLUGINS}->{lc($p)}=$test->new() or 
      $self->info("Error creating the module $test") and return;
  }


  $self->info("initialize: Using configuration for " . $self->{CONFIG}->{FTD} );

  $self->{MAX_RETRYS} = 10;

  $self->{MAX_TRANSFERS} = $self->{CONFIG}->{FTD_MAXTRANSFERS} || 5;

  $self->{JDL}=$self->createJDL();
  $self->{JDL} or return;
  $self->info($self->{JDL});

  $self->{NAME} = "$self->{CONFIG}->{ORG_NAME}::$self->{CONFIG}->{SITE}";
  $self->{ALIVE_COUNTS} = 0;


  $self->{SOAP}->checkService("Broker/Transfer", "TRANSFER_BROKER", "-retry", [timeout=>50000]) or return;
  $self->{SOAP}->checkService("Manager/Transfer", "TRANSFER_MANAGER", "-retry") or return;

  my $file="$self->{CONFIG}->{LOG_DIR}/FTD_children.pid";
  $self->info("initialize: Checking if there were any instances before");
  if (open (FILE, "<$file")){
    my @d=split (/\s+/m , join("",<FILE>));
    close FILE;
    $self->info("There are already some pids!! @d");
    $self->{FTD_PIDS}=\@d;
  }
  $self->checkOngoingTransfers();

  return $self;
}

sub checkOngoingTransfers{
  my $self=shift;
  $self->info("Checking all the transfers that we were supposed to be running");
  my $info=$self->{SOAP}->CallSOAP("Manager/Transfer","checkOngoingTransfers", $self->{CONFIG}->{FTD_FULLNAME}) or return;
  my $transfers=$info->result or return;

  $self->info("Let's see if we can recover");
  foreach my $t (@$transfers){
    my $pid=fork();
    if ($pid){
      my @list=();
      $self->{FTD_PIDS} and @list=@{$self->{FTD_PIDS}};
      push @list, $pid;
      $self->{FTD_PIDS}=\@list;
      next;
    }
    $self->_forkTransfer($t, $t->{protocolid});
    exit;
  }

}

sub createJDL {
  my $self =shift;
  my $exp={};

  $exp->{Name}="\"$self->{CONFIG}->{FTD_FULLNAME}\"";

  $exp->{Type}="\"FTD\"";
  my @protocols="none";
  $self->{CONFIG}->{FTD_PROTOCOL_LIST} 
    and @protocols=@{$self->{CONFIG}->{FTD_PROTOCOL_LIST}};


  $exp->{SupportedProtocol}='{"'.join('","',@protocols).'"}';
  $exp->{Requirements}="other.Type==\"transfer\"";

  my @list=();


  return $self->SUPER::createJDL( $exp);
}


sub startListening {
  my $this=shift;

  $self->info("In fact, this is not a service. We don't listen for anything.");
  return $self->startChecking();
}


#sub verifyTransfer{
#  my $t=shift;
#  my $error=(shift or "");
#  my $URL=shift;
#  my $size=shift;
#  my $retries=shift;
#  my $id=shift;
#  my $message="The transfer did not start";
#  if (! $error) {
#    $self->info("VerifyTransfer " );
#    # Now check that file was actually received
#    #
#    my $result;
#    $result = $self->verifyCompleteTransfer( $URL, $size, $id );
#    my $status;
#    my $now = time;
#    if ( $result == $error_codes->{TRANSFER_SUCCES} ) {
#      $self->info("ID $id Transfer finished!!!");
#      my $result=$self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer","ALIEN_SOAP_RETRY", $id, "CLEANING", {"Action", "cleaning", "FinalPFN", $URL->string});
#      #	    my $result=$self->{MANAGER}->changeStatusTransfer($id, "CLEANING", {"Action", "cleaning", "FinalPFN", $URL->string});
#      $self->{SOAP}->checkSOAPreturn($result, "TransferManager")
#	or return;
#      return 1;
#    }
#    $message="The file was not completely transfered";
#  }
  
#  $self->info("ID $id Transfer failed :(\n\t\t$message");
#  my $result=$self->{SOAP}->CallSOAP("Manager/Transfer",
#				     "changeStatusTransfer","ALIEN_SOAP_RETRY",$id, "FAILED",
#				     {"Reason", $message});
#  #my $result=$self->{MANAGER}->changeStatusTransfer($id, "FAILED", 
#  #						      {"Reason", $message});
#  $self->{SOAP}->checkSOAPreturn($result, "TransferManager");
#  return;

#}


sub checkCurrentTransfers(){
  my $self=shift;
  my $silent=shift;

  my $method="info";
  my @methodData=();

  $silent and $method="debug" and push @methodData, 1;

  $self->$method(@methodData,"$$ The father checks how many children are running");
  my $current=0;

  if ($self->{FTD_PIDS}){
    $self->info("Collecting zombies");
    sleep(5);
    my @list=@{$self->{FTD_PIDS}};
    my @newList;
    foreach (@list){
      if (CORE::kill 0, $_ and waitpid($_, WNOHANG)<=0){
	push @newList, $_;
	$current++;
      }
    }
    $self->{FTD_PIDS}=\@newList;
  } else {
    $self->{FTD_PIDS}=[];
  }

  $self->$method(@methodData,"$$ There are $current transfers: $self->{FTD_PIDS}");

  my $slots= $self->{MAX_TRANSFERS} - $current;
  if ( $slots<=0 ) {
    $self->$method(@methodData, "Already doing maximum number of transfers ($current). Wait" );
    return;
  }

  return $slots;
}


# This function is called automatically every minute from Service.pm 
# 
# input:  $silent. If 0, print all the things in the stdout. Otherwise, print only important things
#
# output: 1     -> everything went ok. 
#         undef -> the service will sleep for 1 minute before trying again
#


sub checkWakesUp{
  my $s = shift;
  my $silent =shift;

  my $method="info";
  my @methodData=();
  $silent and $method="debug" and push @methodData, 1;

  my $slots=$self->checkCurrentTransfers($silent) 
    or return;

  $self->$method(@methodData,"$$ Asking the broker if we can do anything ($slots slots)");


  my ($result)=$self->{SOAP}->CallSOAP("Broker/Transfer", "requestTransferType",$self->{JDL}, $slots);

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
    my $file="$self->{CONFIG}->{LOG_DIR}/FTD_children.pid";
    $self->info("Putting the pids into the file $file ");
    if (open (FILE, ">$file")){
      print FILE @{$self->{FTD_PIDS}};
      close FILE;
    }
  }

  return $repeat;

}

 sub _forkTransfer{
  my $self=shift;
  my $transfer=shift;
  my $recover=shift;

  my $id=$transfer->{id};
  my $n=int($id/1000);
  -d "$self->{CONFIG}->{LOG_DIR}/FTD_transfers" || mkdir "$self->{CONFIG}->{LOG_DIR}/FTD_transfers";
  -d "$self->{CONFIG}->{LOG_DIR}/FTD_transfers/$n" || mkdir "$self->{CONFIG}->{LOG_DIR}/FTD_transfers/$n";
  my $logFile="$self->{CONFIG}->{LOG_DIR}/FTD_transfers/$n/$id.log";
  $self->info("Redirecting to $logFile");

  $self->{LOGGER}->redirect($logFile);
  $self->info("$$ Is ready to do the action");

  if (not $id){
    $self->{LOGGER}->error("FTD", "Error: the transfer does not have an id");
    return;
  }

  my $ca;
  $transfer->{jdl} and $ca=Classad::Classad->new($transfer->{jdl});
  if (! $ca){
    $self->{LOGGER}->error("Error creating the classad of '$transfer->{jdl}'");
    $self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, "FAILED", "ALIEN_SOAP_RETRY",{"Reason","The FTD at $self->{CONFIG}->{FTD_FULLNAME} got". $self->{LOGGER}->error_msg()});
    return;
  }
  my $pfn;

  my ($ok, @pro)=$ca->evaluateAttributeVectorString('FullProtocolList');
  $self->info("Let's use the methods  @pro");
  $recover or
    $self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, "TRANSFERRING", "ALIEN_SOAP_RETRY");
  ($ok, my $type)=$ca->evaluateAttributeString("action");

  foreach my $line (@pro){
    my ($protocol, $source, @rest)=split(",", $line);
    if (grep(/^$protocol$/i, @{$self->{CONFIG}->{FTD_PROTOCOL_LIST}})){
      if ($type =~ /^remove$/i){
	($ok, $pfn)=$ca->evaluateAttributeString("pfn");
	$pfn or $self->info("There was no pfn in the CA",1);
	$self->info("Deleting the file $pfn");
	eval {
	  $self->{PLUGINS}->{lc($protocol)}->delete($pfn);
	};
	if ($@){
	  $self->info("Error doing the delete: $@",1);
	  undef $pfn;
	}
	last;
      }else{
	$self->info("We can get the file with '$protocol' from '$source'");
	$pfn=$self->transferFile($id, $ca, $protocol, $source, $line, $recover);
	$pfn and last;
      }
      $self->info("It didn't work :(");
    }
  }
  my $status="DONE";
  my $extra={pfn=>$pfn};
  if (! $pfn ) {
    $self->info("The transfer failed due to: ".$self->{LOGGER}->error_msg() );
    $status="FAILED_T";
    $extra={"Reason", "$self->{CONFIG}->{FTD_FULLNAME}: ". $self->{LOGGER}->error_msg()};
  }
  $self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, $status, "ALIEN_SOAP_RETRY",$extra);

  $self->info("$$ returns!!");

  return 1;
}


sub transferFile {
  my $self=shift;
  my $id=shift;
  my $ca=shift;
  my $protocol=shift;
  my $source=shift;
  my $line=shift;
  my $recover=shift ||0;

  $self->info("Ready to get the file from $source (recover $recover)");

  my ($ok, $user)=$ca->evaluateAttributeString("User");
  ($ok, my $lfn)=$ca->evaluateAttributeString("FromLFN");
  my $info=$self->{SOAP}->CallSOAP("Authen", "createEnvelope", $user, "", "read", $lfn, $source);
  $info or $self->info("Error getting the envelope to read the source") and return;
  my $sourceEnvelope=$info->result;

  ($ok, my $target)=$ca->evaluateAttributeString("ToSE");
  ($ok, my $guid)=$ca->evaluateAttributeString("GUID");
  ($ok, my $size)=$ca->evaluateAttributeInt("Size");
  $target or $self->info("Error getting the destination of the transfer")
    and return;
  $self->info("And the second envelope ( $user, , write-once, $guid, $target, $size, 0, $guid");
  $info=$self->{SOAP}->CallSOAP("Authen", "createEnvelope", $user, "", "write-once", $guid, $target, $size, 0, $guid);
  $info or $self->info("Error getting the envelope to write the target") and return;
  my $targetEnvelope=$info->result;
  $self->info("Let's start with the transfer!!!");
  my $done;
  eval{
    my $prot_id;
    if ($recover){
      $self->info("We don't issue the transfer again (is is $recover)");
      $done=2;
      $prot_id=$recover;
    } else{
      ($done, $prot_id)=$self->{PLUGINS}->{lc($protocol)}->copy($sourceEnvelope, $targetEnvelope, $line);
    }
    if ($done eq 1){
      $self->info("The transfer worked  Final pfn:'$targetEnvelope->{url}'!!!");
      $done=1;
    }elsif ($done eq 2){
      $done=0;
      $self->waitForCompleteTransfer($self->{PLUGINS}->{lc($protocol)}, $id, $prot_id)
	and $done=1;
    }
  };
  if ($@){
    $self->info("Error doing the eval: $@");
  }
  $done or return;
  my $pfn=$targetEnvelope->{url};
  $pfn=~ s{/NOLFN}{$targetEnvelope->{pfn}};

  return $pfn;
}

sub waitForCompleteTransfer{
  my $self=shift;
  my $protocol=shift;
  my $id=shift;
  my $prot_id=shift;

  my $retry=10;
  $self->info("Let's tell the Transfer Manager that we are waiting for this one");
  $self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer","ALIEN_SOAP_RETRY", $id, "TRANSFERRING", {"protocolid", $prot_id, ftd=>$self->{CONFIG}->{FTD_FULLNAME}});
  while (1){
    sleep(40);
    $self->info("Checking if the transfer $prot_id has finished");
    my $status=$protocol->checkStatusTransfer($prot_id);
    if (! $status){
      $self->info("The transfer finished!!");
      return 1;
    }
    if ($status<0){
      $self->info("Something went wrong ($status)");
      $retry+=$status;
      if ($retry <0){
	$self->info("Giving up");
	return;
      }
    }
  }

  return;
}


sub CURRENT_TRANSFERS {
    my $s = shift;

# We are getting back count(CURRENT), 
#    my $current = $self->{DB}->queryValue("SELECT count(*) from CURRENTTRANSFERS");
#    $current or $current=0;
    my $current=0;
    $self->{FTD_PIDS} and $current=$#{$self->{FTD_PIDS}}+1;
    return $current;
}


#sub verifyCompleteTransfer {
#  my $s = shift;
  
#  my ( $URL, $Origsize, $id ) = @_;
#  $self->info("ID $id In verifyCompleteTransfer with @_" );
  
#  my $size;
#  eval {
#    $size=$URL->getSize();
#  };
#  if ($@) {
#    $self->info("Error getting the size of the file". $URL->string());
#    return $error_codes->{TRANSFER_CORRUPTED};
#  }
  
#  if ( $size != $Origsize ) {
#    $self->{LOGGER}->error( "FTD", "ID $id The file has size $size and should have $Origsize" );
#    if ($self->{CONFIG}->{FTD_SKIPSIZECHECK}){
#      $self->info("But we ignore the size check due to the ldap configuration");
#    } else {
#      return $error_codes->{TRANSFER_CORRUPTED};
#    }
#  }
#  $self->info("ID $id Tranport success\n" );

  
#  return $error_codes->{TRANSFER_SUCCES};
#}


return 1;

__END__

=head1 NAME

AliEn::Service::FTD - Alien File Transport Daemon

=head1 SYNOPSIS



=cut




