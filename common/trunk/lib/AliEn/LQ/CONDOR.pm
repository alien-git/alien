package AliEn::LQ::CONDOR;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;
use AliEn::X509;

use strict;

sub submit {
  my $self = shift;
  my $classad=shift;
  my ( $command, @args ) = @_;
  
  my $arglist = join " ", @args;
  
  my $error=-2;
  local $SIG{PIPE} =sub {
    print "Error submitting the job: sig pipe received!\n";
    $error=-1;
  };
  $self->{X509} or $self->{X509}=AliEn::X509->new();
  $self->{X509}->checkProxy();
  $self->{COUNTER} or $self->{COUNTER}=0;
  my $cm="$self->{CONFIG}->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}";

  my $submit="executable = $command
arguments = $arglist
output = $self->{PATH}/$ENV{ALIEN_LOG}.out
error  = $self->{PATH}/$ENV{ALIEN_LOG}.err
log    = $self->{PATH}/$ENV{ALIEN_LOG}.log
$self->{SUBMIT_ARG}
environment=ALIEN_CM_AS_LDAP_PROXY=$cm;ALIEN_JOBAGENT_ID=$$.$self->{COUNTER};ALIEN_ALICE_CM_AS_LDAP_PROXY=$cm
queue
";
  $self->{COUNTER}++;
  eval {
    
    open( BATCH,"| $self->{SUBMIT_CMD}") or print  "Can't send batch command: $!" and return -2;
    $self->debug(1, "Submitting the command:\n$submit");
    print BATCH $submit;
    close BATCH or return -1;
    $error=0
  };
  if ($@) {
    print "The command died!!\n";
    return -2;
  }
  
  return $error;
}

sub getStatus {
    return 'QUEUED';
}

sub initialize() {
  my $self = shift;

  $self->{PATH} = $self->{CONFIG}->{LOG_DIR};
  $self->{X509}=AliEn::X509->new();


  $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "condor_submit" );
  $self->{SUBMIT_ARG}="";

  if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
    my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
    foreach (@list) {
      $self->{SUBMIT_ARG}.="$_\n";
    }
  }
  $self->{KILL_CMD} = ( $self->{CONFIG}->{CE_KILLCMD} or "condor_rm" );
  $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "condor_q" );

  $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD}";
  if ( $self->{CONFIG}->{CE_STATUSARG} ) {
    $self->{GET_QUEUE_STATUS}.=" @{$self->{CONFIG}->{CE_STATUSARG_LIST}}"
  }
  return 1;
}

sub kill {
    my $self    = shift;
    my $queueId = shift;
    my ( $id, @rest ) = split ( ' ', `$self->{STATUS_CMD} | grep $queueId\$` );
    $id or print "Command $queueId not found in condor!!\n" and return -1;
    print STDERR "In CONDOR, killing process $queueId (id $id)\n";
    return ( system(" $self->{KILL_CMD} $id") );
}
sub removeKilledProcesses{
  my $self=shift;
  my @jobs=();
  foreach my $line (@_){
    my ($id, $user,$date, $time, $cpu, $status, $rest)=split (/\s+/, $line);
    ($status  and  ($status eq "X"))
      or  push @jobs, $line;
  }
  return @jobs;
}


return 1;
