package AliEn::Service::Broker::Transfer;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Database::Transfer;

use AliEn::Service::Broker;
use strict;

use vars qw (@ISA);

@ISA=("AliEn::Service::Broker");
use Classad;

my $self = {};

sub initialize {
  $self     = shift;
  my $options =(shift or {});

  $self->debug(1, "In initialize initializing service TransferBroker" );

  $self->{SERVICE}="Transfer";

  $self->{DB_MODULE}="AliEn::Database::Transfer";

  return $self->SUPER::initialize($options);
}


sub findTransfers {
  my $this    = shift;
  my $site_ca = shift;
  my $slots   = shift;
  
  my ($ok, @se)=$site_ca->evaluateAttributeVectorString("CloseSE");
  map {$_="SE='$_'"} @se;
  my $se=join (" or ", ("SE is NULL", @se));
  $self->debug(1, "Finding a transfer for '$se'");

  my ($list) = $self->{DB}->getWaitingTransfersBySE($se,"priority desc");

  defined $list
    or $self->{LOGGER}->warning( "TransferBroker", "In findTransfer error during execution of database query" )
      and return;
  
  @$list  or return ();
  
  return $self->match("transfer", $site_ca, $list, undef, undef, undef, $slots , "getTransferFromAgentId");
}
sub getTransferFromAgentId {
  my $self=shift;
  my $agentId=shift;
  my $cache=shift;

  if (!$cache){
    $self->info("Getting the jobids for jobagent '$agentId'");
    my $data=AliEn::Util::returnCacheValue($self, "WaitingTransfersFor$agentId");
    if (! $data){
      $data=$self->{DB}->query("select transferid as id, jdl from TRANSFERS where agentid=? and (STATUS='WAITING' or STATUS='LOCAL COPY' or STATUS='CLEANING') order by transferid", undef, {bind_values=>[$agentId]});
    }
    $self->info("There are $#$data entries for that jobagent");
    return @$data;
  }
  $self->info("For the next time that this thing is called, putting the info in the cache");
  ( $#$cache>100) or $cache=undef;
  AliEn::Util::setCacheValue($self, "WaitingTransfersFor$agentId", $cache);
  return 1;
}
sub requestTransfer {
  my $this = shift;
  my $jdl=shift;
  my $slots=shift || 1;


  $jdl
    or $self->{LOGGER}->warning( "TransferBroker", "In requestTransfer no classad for the host received" )
      and return ( -1, "no classad received" );
  $self->debug(1, "The jdl is $jdl");
  $self->setAlive();

  my $ca = Classad::Classad->new($jdl);
  $self->debug(1, "Classad created");
  my ($ok, $host)=$ca->evaluateAttributeString("Name");
  $self->info("New transfer requested from $host!!");


  my @ids=$self->findTransfers($ca, $slots);
  my @toReturn;
  while (@ids){
    my ( $transferId, $transfer_ca, $id2 ) = (shift @ids, shift @ids, shift @ids);
    $self->info("WE ARE GOIND TO RETURN TRANSFER $transferId, with ". $transfer_ca->asJDL() );
    push @toReturn, $self->getTransferArguments($transferId,  $transfer_ca, $ca );

  }
  @toReturn or $self->info("Nothing to do") and return (-2);
  
  return @toReturn;
}


sub getTransferArguments {
    my $this =shift;
    my $id   =shift;
    my $transfer_ca   =shift;
    my $ftd_ca=shift;

    my $transfer={};


    $transfer->{ID}=$id;
    $self->info("Defining the transfer");

    my @args=("Action","ToPFN", "ORIGPFN", "FromFTDOptions", "GUID", "FromCertificate", "ToSE", "FromSE" );

    my $ok;

    foreach my $arg (@args) {
	my $name="\U$arg\E";
	($ok, $transfer->{$name})=
	    $transfer_ca->evaluateAttributeString($arg);
	$self->debug(1, "$arg -> $transfer->{$name}");
    }
#    (my $ok, $transfer->{ACTION})=
#	$transfer_ca->evaluateAttributeString("Action");
#    ($ok, $transfer->{FROMPFN})= 
#	$transfer_ca->evaluateAttributeVectorString("FromPFN");
#    ($ok, $transfer->{TOPFN})= 
#	$transfer_ca->evaluateAttributeVectorString("ToPFN");
#    ($ok, $transfer->{FROMFTDOPTIONS})= 
#	$transfer_ca->evaluateAttributeVectorString("FromFTDOptions");

#    if ($transfer->{ACTION} eq "local copy") {
    ( $ok, my @se)=$transfer_ca->evaluateAttributeVectorString("OrigSE");
    ( $ok, my @pfns)=$transfer_ca->evaluateAttributeVectorString("OrigPFNs");
    ($ok, my @fromPFN)=$transfer_ca->evaluateAttributeVectorString("FromPFN");
    if (@fromPFN) {
      $transfer->{FROMPFN}=\@fromPFN;
    }
    ($ok, my @allSE)=$ftd_ca->evaluateAttributeVectorString("CloseSE");

    my $found;
    #Checking which one of the SE close to the FTD has the file
    foreach my $se (@allSE) {
      $self->info("Checking $se");
      my @tempSE=grep (/^$se$/i, @se) or next;
      $found=shift @tempSE;
      $found and last;
    }

    #Let's also check the PFN that we have to use
    foreach my $se (@se){
      if ($found =~ /^$se$/){
	$transfer->{ORIGPFN}=shift @pfns;
	last;
      }
      shift @pfns;
    }
    $transfer->{ORIGSE}=$found; 
    $self->debug(1, "In getTransferArguments OrigPFN it from $transfer->{ORIGPFN}");


    ($transfer->{SIZE})=$self->{DB}->getSize($id)
		or $self->{LOGGER}->error("TransferBroker", "In getTransferArguments couldn't get size of transfer $id")
		and return;

    $self->debug(1, "In getTransferArguments FromPFN it from $transfer->{FROMPFN} (size $transfer->{SIZE})");

    ($ok, my $host)=$ftd_ca->evaluateAttributeString("Name");

    $self->info("Sending transfer $id to $host");
    use Data::Dumper;
    print Dumper($transfer);
    return $transfer;
}



return 1;








