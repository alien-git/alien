package AliEn::LQ::CONDOR;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;
use AliEn::X509;
use AliEn::TMPFile;
use Data::Dumper;
use POSIX;
use strict;

sub submit {
  my $self = shift;
  my $classad=shift;
  my ( $command, @args ) = @_;
  
  my $arglist = join " ", @args;
  $self->debug(1,"*** CONDOR.pm submit ***");

  my $error=-2;
  local $SIG{PIPE} =sub {
    $self->info("Error submitting the job: sig pipe received!\n");
    $error=-1;
  };
  $self->{X509} or $self->{X509}=AliEn::X509->new();
  $self->{X509}->checkProxy();
  $self->{COUNTER} or $self->{COUNTER}=0;
  my $cm="$self->{CONFIG}->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}";

  my @argstring=$self->{SUBMIT_ARG};

  $self->debug(1,"new status command it $self->{STATUS_CMD} \n");

#--> define temporary files for log, out & err   
  my $n=AliEn::TMPFile->new({ttl=>'24 hours', base_dir=>$self->{PATH},filename=>$ENV{ALIEN_LOG}} );

  my $submit="executable = $command
arguments = $arglist
@argstring
output = $n.out
error  = $n.err
log    = $n.log
environment=CONDOR_ID=\$(Cluster).\$(Process);ALIEN_CM_AS_LDAP_PROXY=$cm;ALIEN_JOBAGENT_ID=$$.$self->{COUNTER};ALIEN_ALICE_CM_AS_LDAP_PROXY=$cm
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
    $self->info("CONDOR submit command died - use debug to evaluate");
    return -2;
  }
  
  return $error;
}

# Usually, we only want the jobs that have something like 'alien'
# or 'agent' in their names (in case the same user is submitting
# some other jobs)
sub _filterOwnJobs {
  my $self     = shift;
  my @queueids = ();
  my $rcount=0;
  foreach (@_) {
    if($_ =~ /undefined/){ next; }
    if($_ =~ /HoldReason/){ next; }
    if(($_ =~ /((alien)|(agent.startup))/i)) {
       $rcount++; 
       my ($i1,$i2) = split /\ /,$_,2;
       $i1=~s/^\s*//;
       push @queueids, $i1;
    }
  }
  return @queueids;
}

# Sometimes condor is down or unresponsive leading to the appearance
# that 0 jobs are running.  If jobs=0 call this function to test health.
sub checkCondorHealth{
  my $self = shift;
  my $healthquery="condor_q";
  my $healthcheck=system($healthquery);
  if($healthcheck){
    $self->info("CONDOR healthcheck Error = $healthcheck in condor_q call, Condor might be down");
    return undef;
  }
  return 1;
}


sub getNumberRunning{
  my $self = shift;
  my $jobquery="condor_q -format \"%s \" GridJobId -format \"HoldReason=%s \" HoldReasonCode -format \"%s \n\" GridJobStatus | grep -v undef | grep -v HoldReason | wc -l";
  open(JOBS,"$jobquery |") or $self->info("error doing $jobquery");
  my $njobs=<JOBS>;
  close(JOBS);
  $njobs=~s/\n//;
  if($njobs == 0 && (!defined $self->checkCondorHealth())){
    $self->info("Error getting total number of jobs, unable to check condor queue");
    return undef;
  }
  return $njobs;
}

sub getNumberQueued{
  my $self = shift;
#  my $jobquery="condor_q -format \"%s \" GridJobId -format \"%s \n\" GridJobStatus | grep PENDING | wc -l";
  my $jobquery="condor_q -format \"%s \" GridJobId -format \"HoldReason=%s \" HoldReasonCode -format \"%s \n\" GridJobStatus | grep PENDING | grep -v HoldReason | wc -l";

  open(JOBS,"$jobquery |") or $self->info("error doing $jobquery");
  my $njobs=<JOBS>;
  close(JOBS);
  $njobs=~s/\n//;
  if($njobs == 0 && (!defined $self->checkCondorHealth())){
     $self->info("Error getting number of queued jobs, unable to check condor queue");
     return undef;
  } 
  return $njobs;
} 


sub getStatus {
  return 'QUEUED';
}

sub initialize() {
  my $self = shift;

  $self->{PATH} = $self->{CONFIG}->{LOG_DIR};
  $self->{X509}=AliEn::X509->new();


  $self->debug(1,"In CONDOR.pm initializex");
  $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "condor_submit" );
  $self->{SUBMIT_ARG}="";


  if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
    my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
    foreach (@list) {
      $self->debug(1,"CE::CONFIG Submit arg --> $_");
      my $entry=$_;
        if($entry=~m/grid_resource/){
          $entry=~s/\=/\=gt2 /;
        }
	if($entry=~m/GlobusScheduler/){  # old style ... backwords compatible
	    my @tmp=split("=",$entry);
            $entry="grid_resource=gt2 ".$tmp[1];
	}
        
      $self->{SUBMIT_ARG}.="$entry\n";
    }
  }
  $self->{KILL_CMD} = ( $self->{CONFIG}->{CE_KILLCMD} or "condor_rm" );
  $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "condor_q -format \"%d \" ClusterId -format \"HoldReason=%s \" HoldReasonCode -format \"%s \" GridJobId -format \"%s \\n\" Cmd" );
  $self->debug(1,"CONDOR SUBMIT ARG = $self->{SUBMIT_ARG}");
  $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD}";
  if ( $self->{CONFIG}->{CE_STATUSARG} ) {
    $self->{GET_QUEUE_STATUS}.=" @{$self->{CONFIG}->{CE_STATUSARG_LIST}}"
  }

  $self->debug(1,"CONDOR intialize finished");
  return 1;
}

sub kill {
    my $self    = shift;
    my $queueId = shift;
    my ( $id, @rest ) = split ( ' ', `$self->{STATUS_CMD} | grep $queueId\$` );
    $id or $self->info("Command $queueId not found in condor!!") and return -1;
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
