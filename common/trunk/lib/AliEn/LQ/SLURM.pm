package AliEn::LQ::SLURM;

use AliEn::LQ;
@ISA = qw( AliEn::LQ );

use IPC::Open2;

use strict;

sub submit {
  my $self = shift;
  my $classad=shift;
  my $command = join " ", @_;
#  $command =~ s/"/\\"/gs;

#  my $name="alien.JobAgent";
  my $name=$ENV{ALIEN_LOG};
  $name =~ s{\.JobAgent}{};
  $name =~ s{^(.{14}).*$}{$1};

  my $slurm_tmp="/tmp/AliEn-slurm-tmp-\${SLURM_JOB_ID}.sh";

#  my $job_out_base="/tmp/AliEn-\${SLURM_JOB_ID}";
#  my $job_w_dir="/tmp";
#  my $job_out_base="$self->{PATH}/$ENV{ALIEN_LOG}-\${SLURM_JOB_ID}";
  my $job_out_base="$self->{PATH}/$ENV{ALIEN_LOG}";
  my $job_w_dir="$self->{PATH}";

  my $message = "#!/bin/sh
#SBATCH -o /dev/null
#SBATCH -e /dev/null
#SBATCH -J \"$name\"
#SBATCH -D \"/tmp\"
#SBATCH --no-requeue\n";

  $message.="#SBATCH $self->{SUBMIT_ARG}\n";

  $message.="cat<<__EOF__ | base64 -d > $slurm_tmp\n";

  open (OUT, "base64 \"$command\" |") or print "Error encoding $command\n" and return -1;

  my $pid = open2(*Reader, *Writer, "$self->{SUBMIT_CMD}" );
  if ( !$pid ) {
    print STDERR "Can't send batch command: $!";
    close(OUT);
    return -1;
  }

  print Writer "$message";

  while($message = <OUT>){
    print Writer "$message";
  }

  print Writer "__EOF__
chmod a+x $slurm_tmp
mkdir -p \"$self->{PATH}\"
mkdir -p \"$job_w_dir\"
srun -D \"$job_w_dir\" --open-mode=truncate -o \"$job_out_base.out\" -o \"$job_out_base.err\" \"$slurm_tmp\"
rm -f \"$slurm_tmp\"
rm -f \"$job_out_base.out\"
rm -f \"$job_out_base.err\"
\n";

  my $error=close Writer ;
  my $got = <Reader>;
  close Reader;
  waitpid $pid, 0;

  chomp($got);
#  $self->debug(1,"got=$got");
  if ( $got =~ /.*batch\s+job\s+\d+$/ ) {
    $got =~ s/.*batch[\s]+job[\s]+//;
    $self->{LAST_JOB_ID} = $got;
  }
#  $self->debug(1,"LAST_JOB_ID=$self->{LAST_JOB_ID}");
  $error or return -1;
  return 0;
}

sub getBatchId {
  my $self=shift;
  return $self->{LAST_JOB_ID};
}

sub getQueueStatus {
  my $self = shift;
  my $statcmd = "$self->{GET_QUEUE_STATUS} -t all $self->{QUEUE_DEF_OPTS}";

  open (OUT, "$statcmd |") or print "Error doing $statcmd\n" and return "Error doing $statcmd\n";

  my @output = <OUT>;
  close(OUT);
  push @output,"DUMMY";
  return @output;
}

sub getStatus {
    my $self = shift;
    my $queueId = shift;
    my $refoutput = shift;
    my @output;
    if (!$refoutput) {
	@output = $self->getQueueStatus();
    } else {
	@output = @{$refoutput};
    }
    

    my @line = grep ( /^$queueId\s/, @output );
    if ($line[0] ) {
# JobID Status JobName
	my @opts = split $line[0];
        if ( $opts[1] =~ /(PD|CF)/ ) {
          return 'QUEUED';
        }
        if ( $opts[1] =~ /(R|S|CG)/ ) {
          return 'RUNNING';
        }
        if ( $opts[1] =~ /(CA|F|NF|TO)/ ) {
          return 'DEQUEUED';
        }
    
	return 'QUEUED'; # should never happen
    }
#    return 'QUEUED';
    return 'DEQUEUED'; # job is already purged from slurm db
}

sub kill {
    my $self    = shift;
    my $queueId = shift;

    open( OUT, "$self->{GET_QUEUE_STATUS} -j $queueId $self->{QUEUE_DEF_OPTS} |" );
    my @output = <OUT>;
    close(OUT);
    my @line = grep ( /^$queueId/, @output );
    if ( $line[0] ) {
	$line[0] =~ /(\w*)\..*/;
        return ( system("scancel --ctld -Q --signal $queueId") ); # use --signal in order to have the temporary files deleted by the batch script
    }
    return (2);
}

sub initialize() {
    my $self = shift;

    $self->{PATH}       = $self->{CONFIG}->{LOG_DIR};
    $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "sbatch" );
    $self->{SUBMIT_ARG} = ( $self->{CONFIG}->{CE_SUBMITARG} or "" );
    $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "squeue" );
    $self->{STATUS_ARG} = ( $self->{CONFIG}->{CE_STATUSARG} or "" );

    my $user=getpwuid($<);
    $self->{QUEUE_DEF_OPTS}="-h -o \"%i %t %j\" -u \"$user\"";

    $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD} $self->{STATUS_ARG}";
    return 1;
}

sub getNumberQueued {
  my $self = shift;
#  my $status = "PD,R,S,CG,CF";
  my $status = "PD,CF";

  if(open(OUT, "$self->{GET_QUEUE_STATUS} -t $status $self->{QUEUE_DEF_OPTS} |")){
	my @output = <OUT>;
	close OUT;
	$self->info("We have ".($#output+1)." jobs queued");
	return $#output+1;
  }else{
	$self->info("Failed to get number of queued jobs");
  }
  return 0;
}

sub getAllBatchIds {
  my $self=shift;
  my @output=$self->getQueueStatus() or return;

  $self->debug(1,"Checking the jobs from  @output");
  @output= grep (!/^DUMMY$/, @output);
  @output= grep (s/\s+.*//, @output);

  $self->debug(1, "Returning @output");

  return @output;
}

return 1;

