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

  # produce ZOMBIES
  $self->checkTransition($method, "(status='RUNNING' or status='ASSIGNED' or status='STARTED' or status='SAVING')", "ZOMBIE");

  # remove ZOMBIES
  $self->checkTransition($method, "status='ZOMBIE'", "EXPIRED");

  return;

}

sub checkTransition{
  my $self=shift;
  my $method=shift;
  my $status=shift;
  my $newStatus=shift;

  my $now = time;


  my $pct = $self->{DB}->getFieldsFromQueueEx("procinfotime,status,queueId,site","where $status ");

  defined $pct
    or $self->{LOGGER}->warning( "Zombies", "In checkJobs error during execution of database query" ) and return;

  if (!@$pct ) {
    $self->{LOGGER}->$method( "Zombies", "There are no jobs, which were ZOMBIES" );
    return 1;
  }

  foreach (@$pct) {
    my $zombiewaittime = 12000;
    my $updatediff =99999 ;
    if ($_->{procinfotime}) { 
      $updatediff = ($now - $_->{procinfotime});
    }
    ( $updatediff > $zombiewaittime )  or next;
    # no new status since more than the zombiewaittime, make the Zombie to a Failed Job
    $self->info("Process $_->{queueId} at $_->{site} with status $_->{status} didn't update since $updatediff seconds");
    $self->{DB}->updateStatus($_->{queueId},"%",$newStatus, {procinfotime=>$now});
    my $message = sprintf "Job state transition from to $newStatus  (by the optimizer) |=| ";
    $self->putJobLog($_->{queueId},"state", $message);
  }
  return 1;
}

1
