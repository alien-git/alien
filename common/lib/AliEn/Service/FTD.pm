#
#  File tranport daemon for Alien
#

package AliEn::Service::FTD;

use AliEn::Database;
use LWP::UserAgent;

#use AliEn::SE::Methods;
#use AliEn::X509;
use POSIX ":sys_wait_h";
use strict;

use vars qw(@ISA $DEBUG);
use AliEn::Logger::LogObject;

push @ISA, 'AliEn::Logger::LogObject';

use AlienClassad;

#use AliEn::MSS::file;

use AliEn::Service;
use AliEn::UI::Catalogue::LCM;
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
  my $options = (shift || {});
  
  $self->{METHOD} = "FTP";
  
  $self->{PORT}="NO PORT";
  $self->{HOST}=$self->{CONFIG}->{HOST};
  chomp $self->{HOST};

  $self->{SERVICE}="FTD";
  $self->{SERVICENAME}=$self->{CONFIG}->{FTD_FULLNAME};
  $options->{role} = 'admin';
  $self->{UI} = AliEn::UI::Catalogue::LCM->new($options) or $self->info("Error getting the ui") and return;
  $self->{UI}->{CATALOG}->{envelopeCipherEngine}
    or $self->info(
    "Error! We can't create the security envelopes!! Please, define the SEALED_ENVELOPE_ environment variables")
    and return;
  
  $self->{CATALOG}=$self->{UI}->{CATALOG};
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


  $self->{RPC}->checkService("Broker/Transfer", "-retry", [timeout=>50000]) or return;
  $self->{RPC}->checkService("Manager/Transfer", "-retry") or return;

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
  my ($transfers)=$self->{RPC}->CallRPC("Manager/Transfer","checkOngoingTransfers", $self->{CONFIG}->{FTD_FULLNAME}) or return;
  $transfers or return;

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


  my ($result)=$self->{RPC}->CallRPC("Broker/Transfer", "requestTransferType",$self->{JDL}, $slots);
  $result or $self->info("The broker didn't return anything") and return;
  $self->info("$$ Got an answer from the broker");
   
  
  my $repeat=1;
  if ($result eq "-2"){
    $self->$method(@methodData, "No transfers for me");
    undef $repeat;
    
  } else {
    foreach my $transfer (@$result){
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
  my $output;
  my $status;
  
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
  $self->{LOGGER}->debug("FTD","In forkTransfers");
  my $ca;
  $transfer->{jdl} and $ca=AlienClassad::AlienClassad->new($transfer->{jdl});
  if (! $ca){
    $self->{LOGGER}->error("Error creating the classad of '$transfer->{jdl}'");
    $self->{RPC}->CallRPC("Manager/Transfer","changeStatusTransfer","-retry", $id, "FAILED",{"Reason","The FTD at $self->{CONFIG}->{FTD_FULLNAME} got". $self->{LOGGER}->error_msg()});
    return;
  }
  my $pfn;

  my ($ok, @pro)=$ca->evaluateAttributeVectorString('FullProtocolList');
  $self->info("Let's use the methods  @pro");
  $recover or
    $self->{RPC}->CallRPC("Manager/Transfer","changeStatusTransfer","-retry", $id, "TRANSFERRING");
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
	  ($pfn,$output)=$self->transferFile($id, $ca, $protocol, $source, $line, $recover);
	  $pfn and last;
	  # to debug "no file online"
	  #$output = 3;
    }
      $self->info("It didn't work :(");
    }
  }
 
#Debug
#$output = 3;
my $query = {maxtime=>0,ctime=>0,pfn=>""};
 
  my $extra={pfn=>$pfn};
  if (! $pfn ) {
    $self->info("The transfer failed due to: ".($self->{LOGGER}->error_msg() || "no LOGGER-error_msg()") );
    $status="FAILED_T";
    $extra={"Reason", "$self->{CONFIG}->{FTD_FULLNAME}: ". ($self->{LOGGER}->error_msg() || "no LOGGER-error_msg()")};
  }
  else{
        if ($output eq 3){
          $status="STAGED";
	  $query->{pfn} = $pfn;
	  $query->{maxtime} = time() + 65537;
	  $query->{ctime} = time();
          #my $query = time()+65537;
	  $self->{LOGGER}->debug("FTD","There is an error with $pfn, going to checkStaged . Max time ".$query->{maxtime});
#	  $self->{LOGGER}->debug("FTD","Update Transfer $id with ".$query);
#	  my $done = $self->{DB}->updateTransfer($id,{status=>'SATGED'} ); 
        }
        elsif ($output eq 1){
          $status = "DONE";
          $query->{pfn} = $pfn;   
        }     
        else {$status = "FAILED_T";}
  }

  #$self->{SOAP}->CallSOAP("Manager/Transfer","changeStatusTransfer",$id, $status, $query);
  $self->{LOGGER}->debug("FTD","Update??? Transfer $id with ".$query->{maxtime}." and ".$query->{ctime}." and pfn: ".$query->{pfn});
  $self->{RPC}->CallRPC("Manager/Transfer","changeStatusTransfer","-retry", $id, $status,$query);
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
  ($ok, my $target)=$ca->evaluateAttributeString("ToSE");
  $target or $self->info("Error getting the destination of the transfer")
    and return;
#  my $info=$self->{SOAP}->CallSOAP("Authen", "doOperation", "authorize", $user, "read", $lfn, $source);
  $self->{CATALOG}->f_user( "-", $user) or $self->info("Error becoming the user $user") and return;
  my @sourceEnvelopes = AliEn::Util::deserializeSignedEnvelopes($self->{CATALOG}->authorize("read", {lfn=> $lfn, wishedSE=>$source,site=>$self->{CONFIG}->{SITE}} ));

  use Data::Dumper;
  print Dumper(@sourceEnvelopes); 
  my $sourceEnvelope = shift @sourceEnvelopes;
  $sourceEnvelope or $self->info("Error getting the envelope to read the source") and return;

  #($ok, my $guid)=$ca->evaluateAttributeString("GUID");
  #($ok, my $size)=$ca->evaluateAttributeInt("Size");
  $self->info("And the second envelope ( $user, mirror, $sourceEnvelope->{guid}, $target, $sourceEnvelope->{size}, 0");
#  $info=$self->{SOAP}->CallSOAP("Authen", "doOperation", "authorize", $user, "mirror", $guid, $target, $size, 0, $guid);
  my @targetEnvelopes = AliEn::Util::deserializeSignedEnvelopes($self->{CATALOG}->authorize("mirror", {lfn=>$lfn,guidRequest=>$sourceEnvelope->{guid},wishedSE=>$target,site=>$self->{CONFIG}->{SITE}}));
  my $targetEnvelope = shift @targetEnvelopes;
  $targetEnvelope or $self->info("Error getting the envelope to mirror to the target") and return; 


#  $info or $self->info("Error getting the envelope to write the target") and return;
#  my $targetEnvelope=$info->result;
#  my $turl = 0;
#  $targetEnvelope and $turl = AliEn::Util::getValFromEnvelope($targetEnvelope,"turl");
  $targetEnvelope->{turl} or $self->info("Error getting the turl from the envelope to mirror to the target!") and return;
  
$self->info("Let's start with the transfer!!!");
  my $done = 0;
  eval{
    my $prot_id;
    if ($recover){
      $self->info("We don't issue the transfer again (is is $recover)");
      $done=2;
      $prot_id=$recover;
    } else{
      ($done, $prot_id)=$self->{PLUGINS}->{lc($protocol)}->copy($sourceEnvelope, $targetEnvelope, $line);
    }

#    $done = $self->test;
#$self->info("Done = $done"); 
    if ($done) {
      if ($done eq 1){
        $self->info("The transfer worked  Final pfn:'$targetEnvelope->{turl}'!!!");
        $done=1;
      }elsif ($done eq 2){
        $done=0;
        $self->waitForCompleteTransfer($self->{PLUGINS}->{lc($protocol)}, $id, $prot_id)
  	and $done=1;
      } elsif ($done ==3) {
	$self->info("Transfer must be STAGED, file is not online");}
    }
  };
  if ($@){
    $self->info("Error doing the eval: $@");
  }
  #$done = 3;
  $done or return;
  my $pfn=$targetEnvelope->{turl};
  if($targetEnvelope->{turl} =~ /NOLFN/) {
    my @splitturl = split (/\/\//, $targetEnvelope->{turl},3);
    $splitturl[2] and $pfn=~ s{/NOLFN}{$splitturl[2]}; # $splitturl[2] is what used to be the envelope pfn part
  }
  return ($pfn,$done);
}


sub waitForCompleteTransfer{
  my $self=shift;
  my $protocol=shift;
  my $id=shift;
  my $prot_id=shift;

  my $retry=10;
  $self->info("Let's tell the Transfer Manager that we are waiting for this one");
  $self->{RPC}->CallRPC("Manager/Transfer","changeStatusTransfer","-retry", $id, "TRANSFERRING", {"protocolid", $prot_id, ftd=>$self->{CONFIG}->{FTD_FULLNAME}});
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




