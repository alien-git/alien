package AliEn::LQ::SGE;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use strict;

sub submit {
    my $self    = shift;
    my $classad=shift;
    my $command = join " ", @_;

    $command =~ s/"/\\"/gs;

    open FILE, "<", $command;
    my $text = join("",<FILE>);
    close FILE;

    my $message = "#BSUB -V
#BSUB -N $ENV{ALIEN_LOG}
#BSUB -o $self->{PATH}/$ENV{ALIEN_LOG}.out
$self->{SUBMIT_ARG}
" . $self->excludeHosts() . "
cd \$TMPDIR
$text\n";

     $self->debug(2, "USING $self->{SUBMIT_CMD}\nThe message is \n$message");
    open( BATCH, "| $self->{SUBMIT_CMD} -C '#BSUB'" )
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
    my $user = getpwuid($<);
    my @output;

    if (!$refoutput) {
	@output = $self->getQueueStatus();
    } else {
	@output = @{$refoutput};
    }
	

    my @line = grep ( /AliEn-$queueId/, @output );
    if ($line[0] ) {
	$line[0] =~ /(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.*)/;
	my ($id, $prior, $name, $user, $stat, $rest) = ($1,$2,$3,$4,$5,$6);
	if ( $stat =~ /Eqw/ ) {
	    return 'ERROR_S';
	}

	if ( $stat =~ /qw/ ) {
	    return 'QUEUED';
	}
	
	if ( $stat =~ /r/) {
	    return 'RUNNING';
	}
	# default, if found in the queue, return queued ...
	return 'QUEUED';
    }
    return 'DEQUEUED';
}

sub kill {
    my $self    = shift;
    my $queueId = shift;

    my $user = getpwuid($<);
    my $statusargs="";

    if (!($self->{STATUS_ARG}=~/.*\S+.*/)) {
	$statusargs = "-u $user";
    }

    open( OUT, "$self->{STATUS_CMD} $statusargs |" );

    my @output = <OUT>;
    close(OUT);
    my @line = grep ( /AliEn-$queueId/, @output );
    if ( $line[0] ) {
        my ( $id, $rest ) = split ( " ", $line[0] );
        system("$self->{KILL_CMD} $self->{KILL_ARG} $id");
	return 1;
    }
    return (2);
}

sub initialize() {
    my $self = shift;

    $self->{PATH}       = $ENV{TMPDIR};
    $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "qsub" );
    $self->{SUBMIT_ARG} = ( $self->{CONFIG}->{CE_SUBMITARG} or "" );

    $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "qstat" );
    $self->{STATUS_ARG} = ( $self->{CONFIG}->{CE_STATUSARG} or "" );
    $self->{KILL_CMD}   = ( $self->{CONFIG}->{CE_KILLCMD} or "qdel" );
    $self->{KILL_ARG}   = ( $self->{CONFIG}->{CE_KILLARG} or "");

    if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
        my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
        map { $_ = "#BSUB $_\n" } @list;
        $self->{SUBMIT_ARG} = join("", @list);
    }

    my $user = getpwuid($<);

    my $statusargs="";
    if ((!($self->{STATUS_ARG}) ||(!($self->{STATUS_ARG}=~/.*\S+.*/)))) {
	$statusargs = "-u $user";
    }
    $self->{GET_QUEUE_STATUS}=" $self->{STATUS_CMD} $statusargs"; 

    return 1;
}

return 1;

