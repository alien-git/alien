#Developement
package AliEn::LQ;

use strict;
use AliEn::Config;
use AliEn::Logger::LogObject;
use vars qw (@ISA);
push @ISA, 'AliEn::Logger::LogObject';

sub new {
    my ($this) = shift;
    my $class = ref($this) || $this;
    my $self = {};
    bless $self, $class;
    $self->SUPER::new() or return;

    my $options = shift;

    $self->{USER}  = $options->{user};
    $self->{DEBUG} = ( $options->{debug} or 0 );

    $self->{CM_HOST} = $options->{CM_HOST};
    $self->{CM_PORT} = $options->{CM_PORT};

    $self->{CONFIG} = new AliEn::Config();
    ( $self->{CONFIG} )
      or print STDERR "Error getting the configuration\n"
      and return;
    $self->{LOGGER} =AliEn::Logger->new();
    $self->{LOGGER} or return;
    $self->{COUNTER}=0;
    $self->initialize()or return;
    return $self;
}

sub kill {
    my $self    = shift;
    my $Jobname = shift;
    print STDERR "Kill not implemented for $self \n";
    return 1;
}

sub submit {
  my $self = shift;
  my $classad=shift;
  $self->debug(1, "In LQ submit");
  my $debugMode=$self->{LOGGER}->getMode();
  my (@args) = @_;
  my @cmd = (@args);
  if ($debugMode =~ s/^debug\s*//){
    $self->debug(1, "Passing debug mode");
      $debugMode=~ s/\s/,/g;
    $debugMode or $debugMode=5;
    @cmd=(@cmd, "-debug", $debugMode);
  }
  my $date=time;
  my $out="$self->{CONFIG}->{LOG_DIR}/$ENV{ALIEN_LOG}.out";
  $self->{LOGGER}->info("LQ", "Submitting @cmd (output $out)");
  $self->{COUNTER}++;
  $self->{LOGGER}->redirect($out);
  my $done=system(@cmd) ;
  $self->{LOGGER}->redirect();
  $self->{LOGGER}->info("LQ", "Got $done");
  return 0;
}

sub getBatchId {
  return;
}
sub initialize {
	return 1;
}

sub getQueueStatus {
  my $self = shift;
  $self->{GET_QUEUE_STATUS} or 
    return "Sorry, not implemented for queue " . ref($self) . "\n";
  
  open (OUT, "$self->{GET_QUEUE_STATUS} |") or print "Error doing $self->{GET_QUEUE_STATUS}\n" and return "Error doing $self->{GET_QUEUE_STATUS}\n";
  #    while (<OUT>) {
#	push @output, $_;
#    }

  my @output = <OUT>;
  close(OUT) or print "Error doing $self->{GET_QUEUE_STATUS}\n" and return "Error doing $self->{GET_QUEUE_STATUS}\n";
  push @output,"DUMMY";
  return @output;

}

sub getQueuedJobs {
  my $self=shift;
  my @queuestatus = $self->getQueueStatus();
   ( @queuestatus) or return;

  $queuestatus[0]=~ /Sorry, not implemented/ and return;
  $queuestatus[0]=~ /^Error doing / and return;

  @queuestatus=$self->removeKilledProcesses(@queuestatus);

  my @queueids;
  push @queueids,"0";
  foreach (@queuestatus) {
    if ($_ =~ /(alien)|(agent.startup)/i) {
      push @queueids,$1;
    }
  }
  
  return @queueids;
}
#This subroutine parses the output of getQueueStatus, and 
#it is supposed to remove the lines of jobs that have been killed
#By default, it doesn't do anything. It should be overloaded in the 
#different LQ implementations./ 
sub removeKilledProcesses{
  my $self=shift;
  
  return @_;
}
sub getStatus {
  my $self=shift;
  my $queueId = shift;
  my $refoutput = shift;

  print "WARNING!!!! Not implemented for queue " . ref($self) . "\n";
  return $self->getDefaultStatus();

}
sub getNumberQueued {
  my $self=shift;
  return 0;
}
sub getNumberRunning {
  my $self=shift;
  my @ids=$self->getQueuedJobs();
  @ids or $self->{LOGGER}->info("LQ", "Error getting the number of jobs") and return;
  $self->{LOGGER}->info("LQ", "There are $#ids jobs right now");
  return $#ids;

}
sub getDefaultStatus {
    return 'QUEUED';
}

sub getExpired {
    return 86400;
}

sub statusChange {
    return 1;
}
    
sub excludeHosts {
    return "";
}

sub getAllBatchIds {
  return;
}
sub updateClassAd {
  my $self=shift;
  $self->debug(1, "This LQ doesn't modify the classad");
  return shift;
}
sub getFreeSlots {
  return 0;
}
return 1;

