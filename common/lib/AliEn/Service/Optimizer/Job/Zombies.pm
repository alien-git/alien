package AliEn::Service::Optimizer::Job::Zombies;

use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";

  $self->{LOGGER}->$method("Zombies", "The zombies optimizer starts");
  
  # Adding deletion of MESSAGES - the MESSAGES are avoiding ZOMBIES by killing JA payloads
  my $time = time;
  $self->{DB}->delete("MESSAGES", "Expires !=0 and Expires < ?", {bind_values=>[$time]});

  # produce ZOMBIES
  $self->checkTransition($method, "(statusId=6)", "ZOMBIE", 600);
  $self->checkTransition($method, "(statusId=10 or statusId=7)", "ZOMBIE");
  $self->checkTransition($method, "(statusId=11)", "ZOMBIE", 3600*3);

  # remove ZOMBIES
  $self->checkTransition($method, "statusId=-15", "EXPIRED"); #ZOMBIE

  return;
}

sub checkTransition{
  my $self=shift;
  my $method=shift;
  my $status=shift;
  my $newStatus=shift;
  my $period=shift || 3600;

  my $now = time;
  
  $self->info("Going to check jobs in $status to $newStatus with period $period");

  my $query = $self->{DB}->getJobOptimizerZombies($status,$period);
  my $pct = $self->{DB}->getFieldsFromQueueEx("p.procinfotime,statusId,p.queueId,now()-lastupdate as lastupdate",$query);

  defined $pct
    or $self->{LOGGER}->warning( "Zombies", "In checkJobs error during execution of database query" ) and return;

  if (!@$pct ) {
    $self->{LOGGER}->$method( "Zombies", "There are no jobs, which were ZOMBIES" );
    return 1;
  }

  foreach my $job (@$pct) {
    # no new status since more than the zombiewaittime, make the Zombie to a Failed Job
    $self->info("Process $job->{queueId} with status $job->{status} didn't update since $job->{lastupdate} seconds");
    $self->{DB}->updateStatus($job->{queueId},"%",$newStatus, {procinfotime=>$now});
    my $message = sprintf "Job state transition from to $newStatus  (by the optimizer) |=| ";
    $self->putJobLog($job->{queueId},"state", $message);
  }
  return 1;
}

1
