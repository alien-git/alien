package AliEn::Service::Optimizer::Transfer;

use strict;

use vars qw (@ISA);

use AliEn::Database::Transfer;

use AliEn::Service::Optimizer;
use AliEn::UI::Catalogue::LCM;
use POSIX qw(ceil);
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
  $self->SUPER::initialize(@_) or return;

  $self->StartChildren('Assigned', 'Archive', 'Inserting', 'Merging', 'SE', 'Agent', "No_se") or return;
#  $self->StartChildren('No_se') or return;


  return 1;

}

sub checkWakesUp {
  my $this=shift;
  my $silent=(shift or 0);
  my $method="info";
  $self->{SLEEP_PERIOD}=10;
  $silent and $method="debug";

  $self->{LOGGER}->$method("TransferOptimizer","In checkWakesUp checking if there is anything to do");

  $self->checkExpiredTransfers($silent);

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


  my (@info)=$self->{CATALOGUE}->execute("whereis","-silent", $lfn, );

  $self->info("The file $lfn is in @info");
  map {$_= "\"$_\"" } @info;

  my (@se, @pfn);
  while (@info){
    push @pfn, pop @info;

    push @se, pop @info;
  }


  $exp->{OrigSE}="{" . join(",",@se) ."}";
  $exp->{OrigPFNs}="{" . join(",",@pfn) ."}";

  map {$_=~ s/^(.*)$/member\(other\.CloseSE, $1 \)/ } @se;
  #let's round the size 

  $size= 1000*ceil($size/1000);
  my $value=join("||",@se);    
  $exp->{Requirements}="(other.type==\"FTD\")&&($value)&&((other.DirectAccess==1)||(other.CacheSpace>$size))";

  my ($guid)=$self->{CATALOGUE}->execute("lfn2guid", $lfn)
    or $self->info("Error getting the guid of $lfn") and return;

  $exp->{GUID}="\"$guid\"";


  return $self->createJDL($exp);
}

return 1;
