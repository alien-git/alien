package AliEn::LQ::OCCI;

use AliEn::LQ;
@ISA = qw( AliEn::LQ );

use IPC::Open2;

use strict;

my $endpoint = "http://cloud-4.bitp.kiev.ua:8787";
my $mixin = "os_tpl#151e86f3-b2c0-428d-8f3c-5e9d8724b92b";

sub submit {
  my $self = shift;

=pod
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
#PBS -N $name\n";
  if (not $self->{NOT_STAGE_FILES}) {
    $message.="#PBS -W stagein=$execute\@$self->{CONFIG}->{HOST}:$command\n";
  }
  $message.="$self->{SUBMIT_ARG}
" . $self->excludeHosts() . "
$execute\n";

  $self->debug(1, "USING $self->{SUBMIT_CMD}\nWith  \n$message");

  my $pid = open2(*Reader, *Writer, "$self->{SUBMIT_CMD} " )
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
=cut

  my $command = "occi --endpoint $endpoint --action create \\
--resource compute --attribute occi.core.title=\"OCCI-VM\" --mixin \\
$mixin --mixin resource_tpl#m1-small \\
--auth x509 --user-cred $ENV{X509_USER_PROXY} --voms";

  $self->info( $command );
  system( $command );

  return 0;

}


sub getBatchId {
  my $self=shift;
=pod
  return $self->{LAST_JOB_ID};
=cut

  return undef;
}



sub getQueueStatus {
  my $self = shift;

=pod
  open (OUT, "$self->{GET_QUEUE_STATUS} |") or print "Error doing $self->{GET_QUEUE_STATUS}\n" and return "Error doing $self->{GET_QUEUE_STATUS}\n";
  #    while (<OUT>) {
#	push @output, $_;
#    }

  my @output = <OUT>;
  close(OUT) or print "Error doing $self->{GET_QUEUE_STATUS}\n" and return "Error doing $self->{GET_QUEUE_STATUS}\n";
  @output= grep ( /^\d+\S*\s+\S+.*/, @output);
  push @output,"DUMMY";
  return @output;
=cut
   
  return undef;
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

=pod
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
=cut

    my $command = "occi --endpoint $endpoint --action delete \\
--resource $queueId --auth x509 \\
--user-cred $ENV{X509_USER_PROXY} --voms";
    $self->info($command);
    system($command);

    return 2;
}


sub initialize() {
    my $self = shift;

=pod
    $self->{PATH}       = $self->{CONFIG}->{LOG_DIR};
    $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "qsub" );
    $self->{SUBMIT_ARG} = ( $self->{CONFIG}->{CE_SUBMITARG} or "" );
    $self->{STATUS_ARG} = ( $self->{CONFIG}->{CE_STATUSARG} or "" );
    $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "qstat -n -1" );

    if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
      my @list = @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} };
      grep (/^alien_not_stage_files$/i, @list) and $self->{NOT_STAGE_FILES}=1;
      @list =grep (! /^alien_not_stage_files$/i, @list);
      map { $_ = "#PBS $_\n" } @list;
      $self->{SUBMIT_ARG} = "@list";
    }


    $self->{GET_QUEUE_STATUS}="$self->{STATUS_CMD} $self->{STATUS_ARG}";
=cut

    return 1;
}


sub getNumberRunning {
  my $self = shift;
  
  my ($running, $queued) = $self->getAllBatchIds();
  return $running;
}


sub getNumberQueued {
  my $self = shift;

=pod
  my $status = "' Q '";

  if(open(OUT, "$self->{STATUS_CMD} -u ".getpwuid($<)." | grep $status |")){
	my @output = <OUT>;
	close OUT;
	$self->info("We have ".($#output + 1)." jobs $status");
	return $#output + 1;
  }else{
	$self->info("Failed to get number of $status jobs");
  }
=cut

  return 0;
}


sub getOCCIQueueStatus{
  my $self = shift;
  my $command = "occi --endpoint $endpoint --action list --resource \\
	compute --auth x509 --user-cred $ENV{X509_USER_PROXY} --voms | wc -l";
  
  $self->info( $command );
  my $out = `$command`;
  $self->info("We got $out machines running");

  return $out;  
}


sub getAllBatchIds {
  my $self=shift;
  my $output=$self->getOCCIQueueStatus(); # or return;


  $self->debug(1, "Returning $output");

  return ($output, 0);
}

return 1;

