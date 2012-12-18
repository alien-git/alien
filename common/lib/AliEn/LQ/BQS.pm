package AliEn::LQ::BQS;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use strict;

sub submit {
  my $self = shift;
  my $classad=shift;
  my $command = join " ", @_;
#  $command =~ s/"/\\"/gs;

  my $execute=$command;
  $execute =~ s{^.*/([^/]*)$}{$ENV{HOME}/$1};
  system ("cp",$command, $execute);


    my $message = "#BSUB -o $self->{PATH}/$ENV{ALIEN_LOG}.out
#BSUB -e $self->{PATH}/$ENV{ALIEN_LOG}.err
#BSUB -N $ENV{ALIEN_LOG}
$self->{SUBMIT_ARG}
" . $self->excludeHosts() . "
$execute\n";

    #    $self->{DEBUG}>5 and 
    print "USING $self->{SUBMIT_CMD} -V \nWith  \n$message\n";

    open( BATCH, "| $self->{SUBMIT_CMD} -V -C '#BSUB'" )
      or print STDERR "Can't send batch command: $!"
      and return -1;
    print BATCH "$message";
    my $error = close BATCH;

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
    return 'DEQUEUED';
}

sub kill {
    my $self    = shift;
    my $queueId = shift;

    print STDERR "In BQS, killing process $queueId\n";
    return ( system( "$self->{KILL_CMD}", "AliEn-$queueId" ) );
}

sub initialize() {
    my $self = shift;
    $self->{PATH}       = $self->{CONFIG}->{LOG_DIR};
    $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "qjob" );
    $self->{STATUS_ARG} = ( $self->{CONFIG}->{CE_STATUSARG} or "-l" );
    $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "qsub" );
    $self->{SUBMIT_ARG} = ( $self->{CONFIG}->{CE_SUBMITARG} or "" );

    $self->{KILL_CMD} = ( $self->{CONFIG}->{CE_KILLCMD} or "qdel" );

    if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
        my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
        map { $_ = "#BSUB $_" } @list;
        $self->{SUBMIT_ARG} = join "\n", @list;
    }

    $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD} $self->{STATUS_ARG}";
    return 1;
}

return 1;

