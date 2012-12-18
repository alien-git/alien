package AliEn::ProxyRouter::FwFromDB;

use strict;
use AliEn::ProxyRouter;
use AliEn::Database;
use AliEn::Database::TXT::ClusterMonitor;
use AliEn::Config;
use AliEn::SOAP;

use vars qw(@ISA);

@ISA = ( "AliEn::ProxyRouter" );

my $self = {};

######################################################
# select a running process from the process databases,
# move it's status to INTERACTIV, and return it's
# host+portnumber.
######################################################

sub init {
    my $self = shift;

    $self->{CONFIG} = new AliEn::Config;
    $self->{CONFIG} or return;

    print "Initializing SOAP ...\n";
    $self->{SOAP} = new AliEn::SOAP;
    
    $self->{SOAP}->{CLUSTERMONITOR}=SOAP::Lite
	->uri("AliEn/Service/ClusterMonitor")
	->proxy("http://$self->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}");
    
    $self->{SOAP}->{CPUSERVER}     =SOAP::Lite
	->uri("AliEn/Service/Manager/Job")
     ->proxy("http://$self->{CONFIG}->{QUEUE_HOST}:$self->{CONFIG}->{QUEUE_PORT}"); 

    # install the .roodpass from LDAP
    my $rootdpass = $self->{CONFIG}->{PROOF_CRYPT_PASSWORD};
    if (!$rootdpass) {
	print STDERR "Could not find the LDAP password for PROOF\n";
    } else {
	print "Installing $rootdpass in .rootdpass\n";
	system("echo \"$rootdpass\" > ~/.#rootdpass; mv ~/.#rootdpass ~/.rootdpass");
    }
    return 1;
}


# for the TcpRouter Service, we never want to die !
sub ShallDie {
  return 0;
}

sub GetNewForwardAddress {
    my $self=shift;
    my $fwaddr;
    my $nodeName;
    my $queueId;

    # select a free interactive slot

    $self->{TXTDB} or return;

    my $data = $self->{SOAP}->CallSOAP("ClusterMonitor", "getIdleProcess", $self->{EXEC});

    if (!$data) {
	print STDERR "Error contacting the ClusterMonitor\n Going to the Manager/Job";
	return ;
    }

    $data and $data = $data->result;

#    my ($data) =
#      $self->{TXTDB}
#	->query("SELECT nodeName,queueId from PROCESSES where status='IDLE' and command='$self->{EXEC}' LIMIT 1");
#    print "SELECT nodeName,queueId from PROCESSES where status='IDLE' and command='$self->{EXEC}' LIMIT 1\n";

    if ($data) {
      ( $nodeName,$queueId ) = split "###", $data;
      if ( (defined $nodeName) && (defined $queueId) ) {
	$fwaddr->{HOST} = $nodeName;
	$fwaddr->{PORT} = $self->{EXECPORT};
	$fwaddr->{ID}   = $queueId;
      }
    } else {
      return;
    }

    # $self->changeStatus($queueId,"%", "INTERACTIV");

    # misses a check here, if this could be inserted ....
    #    ($data) = 
    #  $self->{TXTDB}
    #->insert("UPDATE PROCESSES set status='INTERACTIV' where queueId='$queueId'");
    
    printf "[$$] OK: FW-Addr: %s:%s\n",$fwaddr->{HOST}||'undef',$fwaddr->{PORT}||'undef';
    return ($fwaddr->{HOST},$fwaddr->{PORT},$fwaddr->{ID});
}

######################################################
# remove forwarding socket and move the status from
# INTERACTIV to IDLE
# 
######################################################

sub changeStatus {
  my $self = shift;
  my $queueId = shift;
	my $oldStatus=shift;
  my $status  = shift;

  my $done = $self->{SOAP}->CallSOAP("ClusterMonitor", "changeStatusCommand", $queueId ,$oldStatus, $status);
  $done and $done = $done->result;
  if (!$done) {
      print STDERR "Error contacting the ClusterMonitor\n Going to the Manager/Job";
      $done = $self->{SOAP}->CallSOAP("Manager/Job", "changeStatusCommand", $queueId ,$oldStatus, $status);
      $done and $done = $done->result;
      if (!$done) {
	  print STDERR "Error contacting the Manager/Job\n Status will be inconsistant!\n";
	  return 1;
      }
  }
  return 1;
}

sub RemoveID {
  my $self = shift;
  my $queueId = shift;

  my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR", "changeStatusCommand", $queueId ,"%", "IDLE");
  $done and $done = $done->result;
  if (!$done) {
      print STDERR "Error contacting the ClusterMonitor\n Going to the Manager/Job";
      my $done = SOAP::Lite->uri("AliEn/Service/Manager/Job")
	  ->proxy($self->{CPUSERVERURI})
	  ->changeStatusCommand( $queueId,"%", "IDLE");
  }
				     
#  my ($data) = 
#    $self->{TXTDB}
#      ->insert("UPDATE PROCESSES set status='IDLE' where queueId='$queueId'");

  return 1;
}

sub FaultyID {
  my $self = shift;
  my $queueId = shift;

  $self->changeStatus($queueId, "%", "FAULTY");

#  my ($data) =
#    $self->{TXTDB}
#      ->insert("UPDATE PROCESSES set status='FAULTY' where queueId='$queueId'");
  return 1;
}

sub RemoveForwardSocket {
  my $self   = shift;
  my $socket = shift;
  my $testsocket;
  my $cnt=0;

  $self->{TXTDB} or return;

  $self->{DEBUG} and print "[$$] OK: RemoveFowardSocket\n";
  foreach $testsocket (@{$self->{MUX}}) {
    if ( ($testsocket->{'wsocket'} == $socket ) || ($testsocket->{'rsocket'} == $socket) ) {
      my $queueId = $testsocket->{'ID'};
      # change status in the processes table to IDLE
      # misses a check here, if this could be inserted ....

      #my ($data) = 
      #	$self->{TXTDB}
      #->insert("UPDATE PROCESSES set status='IDLE' where queueId='$queueId'");

      $self->changeStatus($queueId, "%", "IDLE");

      splice(@{$self->{MUX}},$cnt,1);
      $cnt--;
    }
    $cnt++;
  }
}

return 1;

