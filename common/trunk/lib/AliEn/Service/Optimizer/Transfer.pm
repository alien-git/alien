package AliEn::Service::Optimizer::Transfer;

use strict;

use vars qw (@ISA);

use AliEn::Database::Transfer;

use AliEn::Service::Optimizer;
use AliEn::UI::Catalogue::LCM;
@ISA=qw(AliEn::Service::Optimizer);


my $self;
sub initialize {
  $self=shift;
  my $options =(shift or {});

  $options->{role}="admin";

  $self->debug(1, "In initialize initializing service TransferManager" );

  $self->{SERVICE}="Transfer";

  $self->{CATALOGUE} = AliEn::UI::Catalogue::LCM->new($options);

  $self->debug(1, "In initialize creating AliEn::UI::Catalogue instance" );

  ( $self->{CATALOGUE} )
    or $self->{LOGGER}->error( "TransferOptimizer", "Error creating creating AliEn::UI::Catalogue instance" )
      and return;

  $self->{DB_MODULE}="AliEn::Database::Transfer";

  return $self->SUPER::initialize($options);

}

sub checkWakesUp {
  my $this=shift;
  my $silent=(shift or 0);
  my $method="info";
  $self->{SLEEP_PERIOD}=10;
  $silent and $method="debug";

  $self->{LOGGER}->$method("TransferOptimizer","In checkWakesUp checking if there is anything to do");

  $self->checkExpiredTransfers($silent);

  $self->checkNewTransfers($silent);

  $self->checkTransferRequirements($silent);

  undef;
}

sub checkExpiredTransfers {
  my $self=shift;
  my $silent=(shift or 0);
  my $method="info";
  $silent and $method="debug";

  $self->{LOGGER}->$method("TransferOptimizer", "In checkExpiredTransfers checking transfers that are supposed to be expired");

  $self->{DB}->updateExpiredTransfers
    or $self->{LOGGER}->warning("TransferOptimizer", "In checkExpiredTransfers error updating expired transfers");
}

sub checkTransferRequirements {
  my $self=shift;
  my $silent=(shift or 0);
  my $method="info";
  my @silentData=();
  $silent and $method="debug" and push @silentData, 1;
  

  $self->$method(@silentData,"In checkTransferRequirements checking if we can put constraints in any transfer");
  
  $self->{DB}->updateLocalCopyTransfers
    or $self->{LOGGER}->warning("TransferOptimizer", "In checkTransferRequirements error updating local copy transfers");
  
  #Updating the transfers with status 'WAITING' and only one PFN
  my $transfers=$self->{DB}->query("SELECT transferid,jdl FROM TRANSFERS WHERE (STATUS='WAITING' or STATUS='CLEANING') and SE is NULL");
  
  defined $transfers
    or $self->{LOGGER}->warning( "TransferOptimizer", "In checkTransferRequirements error during execution of database query" )
      and return;
  
  @$transfers
    or $self->$method(@silentData, "There is no transfer waiting" )
      and return 1;

  $self->$method(@silentData, "In checkTransferRequirements here are ".($#{$transfers} +1)." transfers in WAITING to check");
  foreach my $data (@$transfers) {
    my $ca=Classad::Classad->new($data->{jdl});
    $ca 
      or $self->info( "Error doing the classad of $data->{jdl}")
	and next;
    
    my ( $ok, @se)=$ca->evaluateAttributeVectorString("OrigSE");
    
    $self->debug(1, "In checkTransferRequirements possible SE: @se");
    if ($#se eq 0){
      my $dest=$se[0];
      $self->info( "Putting dest of $data->{transferid} as $dest");
      $self->{DB}->setSE($data->{transferid},$dest)
	or $self->info( "Error updating SE for transfer $data->{transferid}")
	  and next;
    }
  }

  return 1;
}
sub createTransferJDL {
  my $self=shift;
  my $id=shift;
  my $lfn=shift;
  my $destination=shift;
  my $size=shift;
  my $pfn =shift;


  $self->debug(1, "In createTransferJDL creating a new jdl");

  my $exp={};
  $exp->{FromLFN}="\"$lfn\"";
  $exp->{Type}="\"transfer\"";
  $exp->{Action}="\"local copy\"";
  $exp->{ToSE}="\"$destination\"";
  $pfn and $exp->{ToPFN}="\"$pfn\"";


  my (@se)=$self->{CATALOGUE}->execute("whereis","-silent", $lfn, "-l");
  $self->info("The file $lfn is in @se");
  map {$_= "\"$_\"" } @se;

  $exp->{OrigSE}="{" . join(",",@se) ."}";

  map {$_=~ s/^(.*)$/member\(other\.CloseSE, $1 \)/ } @se;
  my $value=join("||",@se);    
  $exp->{Requirements}="(other.type==\"FTD\")&&($value)&&((other.DirectAccess==1)||(other.CacheSpace>$size))";

  my ($guid)=$self->{CATALOGUE}->execute("lfn2guid", $lfn)
    or $self->info("Error getting the guid of $lfn") and return;

  $exp->{GUID}="\"$guid\"";


  return $self->createJDL($exp);
}

sub checkNewTransfers {
  my $this=shift;
  my $silent=(shift or 0);
  my $method="info";
  my @silentData=();
  $silent and $method="debug" and push @silentData, 1;
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
    $size =~ s/^(.*\#\#\#){3}(\d+)(\#\#\#.*){2}$/$2/;
    $self->debug(1, "In checkNewTransfers file has size $size");
    my $jdl=$self->createTransferJDL($transfer->{transferid}, $transfer->{lfn}, $transfer->{destination}, $size, $transfer->{pfn});
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
							 finished=>undef})
      or $self->info( "Error updating status, jdl and size for transfer $transfer->{transferid}")
	and next;

    $self->info( "Transfer scheduled");
  }
  return 1;
}

return 1;
