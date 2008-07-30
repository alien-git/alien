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
    
    my $jdl=$self->createTransferJDL($transfer->{transferid}, $transfer->{lfn}, $transfer->{destination}, $size, $transfer->{pfn}, $transfer->{collection});
    $self->debug(1, "Got the jdl");
    if (!$jdl){
      $self->{DB}->updateTransfer($transfer->{transferid},{status=>"FAILED"})
	or $self->{LOGGER}->error("TransferOptimizer", "In checkNewTransfers error updating status for transfer $transfer->{transferid}");
      next;
    }
    $self->debug(1,"In checkNewTransfers updating transfer $transfer->{transferid}. New jdl = $jdl,size = $size and status = WAITING");
    $self->{DB}->updateTransfer($transfer->{transferid},{jdl=>$jdl,
							 size=>$size,
							 status=>'WAITING',
							 SE=>undef,
							 sent=>undef,
							 started=>undef,
							 finished=>undef,})
      or $self->info( "Error updating status, jdl and size for transfer $transfer->{transferid}")
	and next;
    $self->info( "Transfer scheduled");
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
return 1;
