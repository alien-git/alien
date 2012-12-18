package AliEn::LQ::STATIC;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use strict;

my $hostlist;

sub initialize() {
    my $self= shift;
    my $dbfile    = "$self->{CONFIG}->{LOG_DIR}/ClusterMonitor.db/PROCESSES";
    $hostlist = $self->{CONFIG}->{CE_STATUSARG_LIST};
    my $procid   = $self->{CONFIG}->{CE_STATUSCMD};
    my $CENAME   = $self->{CONFIG}->{CE_FULLNAME};
#    open(DBOUT,">$dbfile");
#    printf DBOUT "queueId,port,nodeName,started,finished,command,received,status,queue,runtime,runtimes,cpu,mem,cputime,rsize,vsize,ncpu,cpufamily,cpuspeed,cost,maxrsize,maxvsize\n";
    
    foreach (@{$hostlist}) {
	print "Static Job at:", $_,"\n";
#	printf DBOUT "$procid,0000,$_,1000000000,,/bin/proofd/,1000000000,STARTUP,$CENAME,00:00:00,1000,0,0,0,0,0,1000,0,0,0,,\n";
	$procid++;
    }
 #   close DBOUT;
    return 1;
}
sub getDefaultStatus {
	return '';
}

# return always the dummy status QUEUED
sub getStatus {
    return 'QUEUED';
}

sub submit {
  my $self = shift;
  $self->debug(1, "In LQ submit");
  my $classad=shift;
    my $debugMode=$self->{LOGGER}->getMode();
    my (@args) = @_;
    my @cmd = (@args);
    if ($debugMode =~ s/^debug\s*//){
	$self->debug(1, "Passing debug mode");
	$debugMode=~ s/\s/,/g;
	$debugMode or $debugMode=5;
	@cmd=( @cmd, "-debug", $debugMode);
    }
    
    my $newhost = shift @{$hostlist};

    @cmd=("ssh $newhost", @cmd);

    $self->{LOGGER}->info("LQ", "Submitting @cmd");
		my $done=system(@cmd) ;
		$self->{LOGGER}->info("LQ", "Got $done");
    return 0;
}

return 1

