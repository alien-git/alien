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
##PBS -W stagein=$execute\@$self->{CONFIG}->{HOST}:$command
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

  $error or return -1;
  return 0;

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
    my @line = grep ( /AliEn-$queueId/, @output );
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
    $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "qstat" );

    if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
        my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
        map { $_ = "#PBS $_\n" } @list;
        $self->{SUBMIT_ARG} = "@list";
    }


    $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD} $self->{STATUS_ARG}";
    return 1;
}

sub getNumberJobsInState {
  my $self = shift;
  my $status = shift;

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


sub getNumberQueued {
  my $self = shift;
  return $self->getNumberJobsInState("' Q '");
}

sub getNumberRunning {
  my $self = shift;
  return $self->getNumberJobsInState("-v ' Q '  | grep ".getpwuid($<));
}

return 1;

