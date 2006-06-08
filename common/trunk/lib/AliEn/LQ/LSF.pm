package AliEn::LQ::LSF;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use IPC::Open2;

use strict;

sub updateClassAd {
  my $self=shift;
  my $classad=shift;

  open(FILE, "lshosts |") or
    $self->info("Error using lshosts") and return;
  my @data=<FILE>;
  close FILE;
  my ($maxMemory, $maxSwap)=(0,0);
  foreach my $entry (@data) {
    chomp $entry;
    $self->debug(1, "Checking the entry $entry");
    $entry=~ /^\s*(\S+\s+){5}(\d+M?)\s+(\d+M?)\s/ or next;
    my ($memory, $swap)=($2, $3);
    $memory=~ s/M// and $memory*=1024;
    $swap=~ s/M// and $swap*=1024;
    $memory>$maxMemory and $maxMemory=$memory;
    $swap>$maxSwap and $maxSwap=$swap;
  }
  $self->info("Setting the maximum memory to $maxMemory and the maximum swap to $maxSwap");
  $classad->set_expression("Memory", $maxMemory);
  $classad->set_expression("FreeMemory", $maxMemory);
  $classad->set_expression("Swap", $maxSwap);
  $classad->set_expression("FreeSwap", $maxSwap);

  
  return $classad;
}

sub submit {
  my $self    = shift;
  my $classad = shift;


  my $LSFarguments="";
  my ($ok, $memory)=$classad->evaluateExpression("Memory");
  if ($ok and $memory) {
    $LSFarguments="mem=$memory";
  }
  ($ok, my $swap)=$classad->evaluateExpression("Swap");
  if ($ok and $swap) {
    $LSFarguments and $LSFarguments.=":";
    $LSFarguments.="swap=$swap";
  }
  $LSFarguments and $LSFarguments="#BSUB -R rusage[$LSFarguments]";

  my $command = join " ", @_;

  $command =~ s/"/\\"/gs;

  my $execute=$command;
  $execute =~ s{^.*/([^/]*)$}{$ENV{HOME}/$1};

  my $message = "#BSUB -o $self->{PATH}/$ENV{ALIEN_LOG}.out
#BSUB -J $ENV{ALIEN_LOG}
#BSUB -f \"$command > $execute\"
$LSFarguments
$self->{SUBMIT_ARG}
" . $self->excludeHosts() . "
$execute\n";

  #".$self->excludeHosts()."
  #$command\n";
  
  $self->debug(1, "USING $self->{SUBMIT_CMD}\nMessage \n$message");
  my $pid = open2(*Reader, *Writer, "$self->{SUBMIT_CMD}" )
    or print STDERR "Can't send batch command: $!"
      and return -1;
  print Writer "$message\n";
  my $error=close Writer ;
  my $got = <Reader>;
  close Reader;
  waitpid $pid, 0;

  $self->info("$got");
  $got =~ /Job <([0-9]*)> is submitted/ and $self->{LAST_JOB_ID}=$1;
  $error or return -1;
  return 0;
}
sub getBatchId {
  my $self=shift;
  return $self->{LAST_JOB_ID};
}
sub kill {
    my $self    = shift;
    my $queueId = shift;
    print STDERR "Removig process $queueId from LSF...";
    my $error = system("$self->{KILL_CMD} $self->{KILL_ARG} -J AliEn-$queueId");

    ($error) or print STDERR "ok\n";
    ($error) and print STDERR "error $!\n";

    return 1;
}


sub getStatus {
  my $self = shift;
  my $queueId = shift;
  my $refoutput = shift;
  my @output;
  my $user = getpwuid($<);
  
  if (!$refoutput) {
    @output = $self->getQueueStatus();
  } else {
    @output = @{$refoutput};
  }
  
  
  my @line = grep ( /AliEn-$queueId/, @output );
  if ($line[0] ) {
    $line[0] =~ /(\d+)\s+(\S+)\s+(\S+)\s+(.*)/;
    my ($id, $name, $stat, $rest) = ($1,$2,$3,$4);
    if ( $stat =~ /PEND/ ) {
      return 'QUEUED';
    }
    
    if ( $stat =~ /RUN/ ) {
      return 'RUNNING';
    }
    return 'QUEUED';
  }
  return 'DEQUEUED';
}

sub getExpired {
    return 86400;
}

sub excludeHosts {
  my $self = shift;
  return "";
  $self->{CM_HOST} or return "";
  #my @hosts = @_;
  my $response =
    SOAP::Lite->uri("AliEn/Service/ClusterMonitor")
	->proxy("http://$self->{CM_HOST}:$self->{CM_PORT}")->getExcludedHosts();
  
  ($response) or return "";
  
  my @hosts = ( $response->result, $response->paramsout );
  
  my $host;
  my $string = "";
  if (@hosts) {
    $string = "#BSUB -R \"";
    while ( $host = shift (@hosts) ) {
      $string .= " hname != '$host' &&";
    }
    
    $string =~ s/\&\&$//;
    $string .= " \"\n";
  }
  return $string;
}

sub initialize() {
    my $self = shift;

    $self->{PATH}       = $self->{CONFIG}->{LOG_DIR};
    $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "bsub" );
    $self->{SUBMIT_ARG} = ( $self->{CONFIG}->{CE_SUBMITARG} or "" );

    $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "qstat" );
    $self->{STATUS_ARG} = ( $self->{CONFIG}->{CE_STATUSARG} or "" );

    $self->{KILL_CMD}   = ( $self->{CONFIG}->{CE_KILLCMD} or "bkill" );
    $self->{KILL_ARG}   = ( $self->{CONFIG}->{CE_KILLARG} or "");


    if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
        my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
        map { $_ = "#BSUB $_\n" } @list;
        $self->{SUBMIT_ARG} = "@list";
    }

    $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD} $self->{STATUS_ARG}";

    return 1;
}

sub getAllBatchIds {
  my $self=shift;
  my @output=$self->getQueueStatus() or return;

  $self->debug(1,"Checking the jobs from  @output");
  @output= grep (s{^\s*[0-9]+:\s+\S+\s+([0-9]+)\s.*$}{$1}s, @output);

  $self->debug(1, "Returning @output");

  return @output;

}
return 1
