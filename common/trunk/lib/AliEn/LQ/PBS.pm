package AliEn::LQ::PBS;

use AliEn::LQ;
@ISA = qw( AliEn::LQ );

use IPC::Open2;

use strict;

sub submit {
  my $self = shift;
  my $classad=shift;
  my $command = join " ", @_;
  $command =~ s/"/\\"/gs;

  my $name=$ENV{ALIEN_LOG};
  $name =~ s{\.JobAgent}{};
  $name =~ s{^(.{14}).*$}{$1};

  my $execute=$command;
  $execute =~ s{^.*/([^/]*)$}{$ENV{HOME}/$1};

  system ("cp",$command, $execute);
  my $message = "#PBS -o $self->{PATH}/$ENV{ALIEN_LOG}.out
#PBS -e $self->{PATH}/$ENV{ALIEN_LOG}.err
#PBS -V
#PBS -N $name
#PBS -W stagein=$execute\@$self->{CONFIG}->{HOST}:$command
$self->{SUBMIT_ARG}
" . $self->excludeHosts() . "
$execute\n";

  $self->debug(1, "USING $self->{SUBMIT_CMD}\nWith  \n$message");

  my $pid = open2(*Reader, *Writer, "$self->{SUBMIT_CMD} -C '#PBS'" )
    or print STDERR "Can't send batch command: $!"
      and return -1;
  print Writer "$message\n";
  my $error=close Writer ;
  my $got = <Reader>;
  close Reader;
  waitpid $pid, 0;

  chomp($got);
  $got =~ /\d+.*/ and $self->{LAST_JOB_ID}=$got;
  $error or return -1;
  return 0;

}
sub getBatchId {
  my $self=shift;
  return $self->{LAST_JOB_ID};
}

sub getQueueStatus {
  my $self = shift;
  open (OUT, "$self->{GET_QUEUE_STATUS} |") or print "Error doing $self->{GET_QUEUE_STATUS}\n" and return "Error doing $self->{GET_QUEUE_STATUS}\n";
  #    while (<OUT>) {
#	push @output, $_;
#    }

  my @output = <OUT>;
  close(OUT) or print "Error doing $self->{GET_QUEUE_STATUS}\n" and return "Error doing $self->{GET_QUEUE_STATUS}\n";
  @output= grep ( /^\d+\S*\s+\S+.*/, @output);
  push @output,"DUMMY";
  return @output;

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
    

    my @line = grep ( /AliEn.*/, @output );
    @line = grep ( /^$queueId/, @line );
    if ($line[0] ) {
# JobID Username Queue Jobname SessID NDS TSK Memory Time Status Time Nodes
	my @opts = split $line[0];
        if ( $opts[9] =~ /Q/ ) {
          return 'QUEUED';
        }
        if ( $opts[9] =~ /R/ ) {
          return 'RUNNING';
        }
    
	return 'QUEUED';
    }
    return 'QUEUED';
#    return 'DEQUEUED';
}

sub kill {
    my $self    = shift;
    my $queueId = shift;

    my $user = getpwuid($<);

    open( OUT, "$self->{STATUS_CMD}  |" );
    my @output = <OUT>;
    close(OUT);
    my @line = grep ( /AliEn.*/, @output );
    @line = grep ( /^$queueId/, @line );
    if ( $line[0] ) {
	$line[0] =~ /(\w*)\..*/;
        return ( system("qdel $1") );
    }
    return (2);
}

sub initialize() {
    my $self = shift;

    $self->{PATH}       = $self->{CONFIG}->{LOG_DIR};
    $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "qsub" );
    $self->{SUBMIT_ARG} = ( $self->{CONFIG}->{CE_SUBMITARG} or "" );
    $self->{STATUS_ARG} = ( $self->{CONFIG}->{CE_STATUSARG} or "" );
    $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "qstat -n -1" );

    if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
        my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
        map { $_ = "#PBS $_\n" } @list;
        $self->{SUBMIT_ARG} = "@list";
    }


    $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD} $self->{STATUS_ARG}";
    return 1;
}

sub getNumberQueued {
  my $self = shift;
  my $status = "' Q '";

  if(open(OUT, "$self->{STATUS_CMD} -u ".getpwuid($<)." | grep $status |")){
	my @output = <OUT>;
	close OUT;
	$self->info("We have ".($#output + 1)." jobs $status");
	return $#output + 1;
  }else{
	$self->info("Failed to get number of $status jobs");
  }
  return 0;
}
sub getAllBatchIds {
  my $self=shift;
  my @output=$self->getQueueStatus() or return;

  $self->debug(1,"Checking the jobs from  @output");
  @output= grep (s/\s+.*//s, @output);

  $self->debug(1, "Returning @output");

  return @output;

}

return 1;

