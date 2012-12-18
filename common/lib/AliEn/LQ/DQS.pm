package AliEn::LQ::DQS;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use strict;

sub submit {
  my $self = shift;
  my $classad=shift;
  my $command = join " ", @_;
  $command =~ s/"/\\"/gs;

  my $message = "#BSUB -o $self->{PATH}/$ENV{ALIEN_LOG}.out
#BSUB -e $self->{PATH}/$ENV{ALIEN_LOG}.err
#BSUB -V
#BSUB -N $ENV{ALIEN_LOG}
$self->{SUBMIT_ARG}
" . $self->excludeHosts() . "
$command\n";

    $self->{DEBUG} > 5
      and print STDERR "USING $self->{SUBMIT_CMD}\nWith  \n$message\n";

    open( BATCH, "| $self->{SUBMIT_CMD} -C '#BSUB'" )
      or print STDERR "Can't send batch command: $!"
      and return -1;
    print BATCH "$message";
    my $error = close BATCH;

    $error or return -1;
    return 0;

}

sub getStatus {
    return 'QUEUED';
}

sub kill {
    my $self    = shift;
    my $queueId = shift;

    print STDERR "In DQS, killing process $queueId\n";
    return ( system("qdel -J AliEn-$queueId") );
}

sub initialize() {
    my $self = shift;
    $self->{PATH}       = "/tmp";
    $self->{PATH}       = $self->{CONFIG}->{LOG_DIR};
    $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "qsub" );
    $self->{SUBMIT_ARG} = ( $self->{CONFIG}->{CE_SUBMITARG} or "" );

    if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
        my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
        map { $_ = "#BSUB $_\n" } @list;
        $self->{SUBMIT_ARG} = "@list";
    }
    my $user = getpwuid($<);
    $self->{GET_QUEUE_STATUS}="qstat -u $user";

    return 1;
}

return 1;

