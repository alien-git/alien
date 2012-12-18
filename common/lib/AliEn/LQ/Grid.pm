package AliEn::LQ::Grid;

use AliEn::LQ;
use AliEn::Config;
@ISA = qw( AliEn::LQ );

use strict;
use AliEn::Database::TXT::EDG;

sub initialize {
  my $self=shift;
  $self->{TXT}= new AliEn::Database::TXT::EDG;

  $self->{TXT} or return;

  $ENV{GLOBUS_LOCATION}="/opt/globus";

  delete $ENV{X509_CERT_DIR};


  return 1;

}

sub GetJobRequirements {    
    my $self=shift;

    my $allreq=( $self->{JDL_REQ} or "");
    my $requirements=( $self->{JDL_SPECIAL_REQ} or "");

    $requirements and $self->debug(1, "Adding $requirements") 
      and $allreq .= "&& $requirements";

    return "$allreq ;";
}


sub getContactByQueueID {
  my $self = shift;
  my $queueid = shift;
  $queueid or return;
  my ($contact )= $self->{TXT}->queryValue("SELECT contact from JOBS where queueid=$queueid");
  $contact or $self->{LOGGER}->error("Grid", "The job $queueid is not here") and return;
  return $contact;
}

sub getJobStatus {
  my $self = shift;
  my $queueid = shift;
  
  $queueid or return;
  my ($contact )= $self->getContactByQueueID($queueid);
  $contact or return;
  my $user = getpwuid($<);
  my @args=();
  $self->{CONFIG}->{CE_STATUSARG} and 
    @args=split (/\s/, $self->{CONFIG}->{CE_STATUSARG});
  
  open( OUT, "dg-job-status -noint @args \"$contact\"|" );
  my @output = <OUT>;
  close(OUT);
  return @output;
}

sub getStatus {
    return 'QUEUED';
}

sub kill {
    my $self    = shift;
    my $queueid = shift;

    $queueid or return;
    my ($contact )= $self->getContactByQueueID($queueid);
    $contact or return;
    return ( system( "dg-job-cancel",  "--noint","$contact" ) );
}

return 1;




