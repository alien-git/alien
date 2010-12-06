package AliEn::Service::Optimizer::Transfer::Inserting;

use strict;

use vars qw (@ISA);
use AliEn::Service::Optimizer::Transfer;

push (@ISA, "AliEn::Service::Optimizer::Transfer");



sub checkWakesUp {
  my $self=shift;
  my $silent=(shift or 0);
  my $method="info";
  my @silentData=();
  $silent and $method="debug" and push @silentData, 1;
  $self->{SLEEP_PERIOD}=10;
  $self->$method(@silentData, "Checking if there is anything to do");
  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='INSERTING'");
  $todo or return;
  $self->{DB}->updateActions({todo=>0}, "action='INSERTING'");


  my $transfers=$self->{DB}->getNewTransfers;

  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkNewTransfers error during execution of database query" )
      and return;

  @$transfers or
    $self->$method(@silentData,"In checkNewTransfers no new transfers")
      and return;

  foreach my $transfer (@$transfers) {
    $self->info( "New transfer of $transfer->{lfn}");
    if ($transfer->{options}=~ /r/){
      $self->checkFileInsideArchive($transfer) or next;
      $self->info("Now $transfer->{lfn}");
    }

    my ($size)=$self->{CATALOGUE}->execute("ls","-silent", "-l", "$transfer->{lfn}");

    if (!$size) {
      $self->{LOGGER}->error("TransferOptimizer", "In checkNewTransfers file $transfer->{lfn} does not exist in the catalogue");
      $self->{DB}->updateTransfer($transfer->{transferid},{status=>"FAILED"})
	or $self->{LOGGER}->error("TransferOptimizer", "In checkNewTransfers error updating status for transfer $transfer->{transferid}");

      next;
    }
    if ($size =~ /^c/){
      $self->info("This is in fact a collection!!");
      $self->insertCollectionTransfer($transfer);
      next;
    }
    $size =~ s/^(.*\#\#\#){3}(\d+)(\#\#\#.*){2}$/$2/;
    $self->debug(1, "In checkNewTransfers file has size $size");
    
    my $jdl=$self->createTransferJDL($transfer->{transferid}, $transfer->{lfn}, $transfer->{destination}, $size,  $transfer->{collection}, $transfer->{user}, $transfer->{options});
    $self->debug(1, "Got the jdl");
    if (!$jdl){
      my $reason=$self->{LOGGER}->error_msg() || "error creating the jdl of the transfer";
      $self->{DB}->updateTransfer($transfer->{transferid},{status=>"FAILED", Reason=>$reason})
	or $self->{LOGGER}->error("TransferOptimizer", "In checkNewTransfers error updating status for transfer $transfer->{transferid}");
      $self->{TRANSFERLOG}->putlog($transfer->{transferid}, "ERROR", "Error defining the transfer: $reason");

      next;
    }
    $self->debug(1,"In checkNewTransfers updating transfer $transfer->{transferid}. New jdl = $jdl,size = $size and status = WAITING");
    $self->{DB}->updateTransfer($transfer->{transferid},{jdl=>$jdl,
							 size=>$size,
							 lfn=>$transfer->{lfn},
							 status=>'WAITING',
							 sent=>undef,
							 started=>undef,
							 finished=>undef,})
      or $self->info( "Error updating status, jdl and size for transfer $transfer->{transferid}")
	and next;

    $self->{TRANSFERLOG}->putlog($transfer->{transferid}, "STATUS", "Transfer changed to WAITING");
    $self->info( "Transfer scheduled");
  }
  return 1;
}

sub checkFileInsideArchive{
  my $self=shift;
  my $transfer=shift;

  $self->info("If this is inside an archive, transfer the archive");
  my (@se)=$self->{CATALOGUE}->execute("whereis", "-silent", "$transfer->{lfn}");

  my $guid="";
  map { m{^guid://[^/]*/([^\?]*)\?*} and $guid=$1} @se;
  $guid or return 1;

  $self->info("This is inside an archive (guid: $guid)!!");

  my ($lfn)=$self->{CATALOGUE}->execute("guid2lfn", $guid, "-silent");
  if (! $lfn){
    $self->{TRANSFERLOG}->putlog($transfer->{transferid}, "ERROR", "In checkNewTransfers file $transfer->{lfn} is inside the archive of $guid, but that entry does not exist in the catalogue");
    $self->{DB}->updateTransfer($transfer->{transferid},{status=>"FAILED"})
      or $self->{LOGGER}->error("TransferOptimizer", "In checkNewTransfers error updating status for transfer $transfer->{transferid}");
    return;
  }
  $transfer->{lfn}=$lfn;
  $self->{TRANSFERLOG}->putlog($transfer->{transferid}, "INFO", "This is inside an archive. Transfering the archive $transfer->{lfn}");
  my $id=$self->{DB}->isScheduled($lfn,$transfer->{destination});
  if ($id){
    $self->{TRANSFERLOG}->putlog($transfer->{transferid}, "STATUS", "The transfer of the archive has already been scheduled: $id");
    $self->{DB}->updateTransfer($transfer->{transferid},{status=>"DONE"})
      or $self->{LOGGER}->error("TransferOptimizer", "In checkNewTransfers error updating status for transfer $transfer->{transferid}");

    return;
  }  
  return 1;
}



sub insertCollectionTransfer{
  my $self=shift;
  my $transfer=shift;

  my ($olduser)=$self->{CATALOGUE}->execute("whoami", "-silent");
  $self->{CATALOGUE}->execute("user","-",  $transfer->{user});

  eval {
    $self->{DB}->updateTransfer($transfer->{transferid}, {status=>'SPLITTING'}) or die("Error setting the status to SPLITTING\n");
    my ($info)=$self->{CATALOGUE}->execute("listFilesFromCollection","-silent", $transfer->{lfn}) or die("Error getting the files of the collection");
    $transfer->{options}=~ s{m|f}{}g;
    $transfer->{options}=~ /t/ or $transfer->{options}.="t";
    my $total=0;
    my @optionList= split (//, $transfer->{options});
    map {s/^/-/} @optionList;
    
    foreach my $file (@$info){
      $self->info("Now we have to send a transfer for $file->{origLFN} (options @optionList)");
      my ($done)=$self->{CATALOGUE}->execute("mirror", @optionList, $file->{origLFN}, $transfer->{destination}, "-m", $transfer->{transferid});
      $self->info("Got $done\n");
      $done or $self->info("Error mirroring $file->{origLFN}");
      $done and $total++;
    }
    $self->{DB}->updateTransfer($transfer->{transferid}, {status=>'SPLIT'})
      or die ("Error setting the status to SPLIT\n");
    $self->info("Number of subtransfers: $total");
    $total or die("There are no subtransfer for that collection\n");
  };
  my $error=$@;
  $self->{CATALOGUE}->execute("user", "-", $olduser);
  if ($error){
    $self->info("Error splitting the transfer: $@");
    $self->{DB}->updateTransfer($transfer->{transferid}, {status=>'FAILED'});
    return;
  }
  return 1;
}


sub createTransferJDL {
  my $self=shift;
  my $id=shift;
  my $lfn=shift;
  my $destination=shift;
  my $size=shift;
  my $collection=shift;
  my $user=shift;


  $self->debug(1, "In createTransferJDL creating a new jdl");

  my $exp={};
  $exp->{FromLFN}="\"$lfn\"";
  $exp->{Type}="\"transfer\"";
  $exp->{Action}="\"local copy\"";
  $exp->{ToSE}="\"$destination\"";
  $collection and $exp->{Collection}="\"$collection\"";

  $exp->{User}="\"$user\"";
  my (@info)=$self->{CATALOGUE}->execute("whereis","-silent", $lfn, );

  $self->info("The file $lfn is in @info");

  my (@se, @pfn);
  while (@info){
    push @pfn, pop @info;

    push @se, pop @info;
  }


  $exp->{OrigSE}='{"' . join('","',@se) .'"}';
  $exp->{OrigPFNs}='{"' . join('","',@pfn) .'"}';
  $exp->{Size}="\"$size\"";
  #let's round the size 


  my ($protocols, $fulllist)=$self->findCommonProtocols($destination, \@se);
  @$protocols or $self->info("There are no common protocols!!",1) and return;


  $exp->{Protocols}='{"'. join('","', @$protocols). '"}';
  $exp->{FullProtocolList}='{"'. join('","', @$fulllist). '"}';

  map {$_=~ s/^(.*)$/member\(other\.SupportedProtocol, "$1" \)/ } @$protocols;

  my $value=join("||",@$protocols);    


  $exp->{Requirements}="(other.type==\"FTD\")&&($value)";

  my ($guid)=$self->{CATALOGUE}->execute("lfn2guid", $lfn)
    or $self->info("Error getting the guid of $lfn",1) and return;

  $exp->{GUID}="\"$guid\"";



  return $self->createJDL($exp);
}


sub findCommonProtocols {
  my $self=shift;
  my $target=shift;
  my $sourceRef=shift;

  my $protocols=[];
  my $pDone={};
  my @fullList;

  foreach my $source (@$sourceRef){
    my @p=$self->{DB}->findCommonProtocols($source,$target);
    foreach my $info (@p){
      my $p=$info->{protocol};
      $pDone->{$p} or push @$protocols, $p;
      $pDone->{$p}=1;
      $info->{sourceopt} or $info->{sourceopt}="";
      $info->{targetopt} or $info->{targetopt}="";
      push @fullList, "${p},$source,$info->{sourceopt},$info->{targetopt}";
    }
  }

  return $protocols, \@fullList;
}

return 1;
