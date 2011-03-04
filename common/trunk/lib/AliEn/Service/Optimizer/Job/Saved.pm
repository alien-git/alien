package AliEn::Service::Optimizer::Job::Saved;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::GUID;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  $self->{SLEEP_PERIOD}=10;
  my $method="info";
  $silent and $method="debug";
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
  $self->{INSERTING_COUNTING}++;
  if ($self->{INSERTING_COUNTING}>10){
    $self->{INSERTING_COUNTING}=0;
  }else {
    $method="debug";
  }

  $self->{LOGGER}->$method("Zombies", "The saved optimizer starts");

  my $dosth=0; 

  my $todoone = $self->{DB}->queryValue("SELECT todo from ACTIONS where action='SAVED'");
  $todoone and $self->{DB}->update("ACTIONS", {todo=>0}, "action='SAVED'");

  my $todotwo = $self->{DB}->queryValue("SELECT todo from ACTIONS where action='SAVED_WARN'");
  $todotwo and $self->{DB}->update("ACTIONS", {todo=>0}, "action='SAVED_WARN'");
 
  ($todoone or $todotwo) or return; 

  my $done=$self->checkJobs($silent, "SAVED", "checkSavedJob");
  $self->checkJobs($silent, "SAVED_WARN", "checkSavedJob");

  return;

}

sub checkSavedJob{
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;
  my $status=shift;
  my $now = time;

  $self->info("********************************\n\tWe should do something with job $queueid");

  my @fails = $self->{CATALOGUE}->registerOutput($queueid, $self); 
  my $success = shift @fails;


  if ($success and scalar(@fails) eq 0){
    $self->putJobLog($queueid,"trace", "Output files registered successfully in $success");
    $self->putJobLog($queueid,"state", "Job state transition from $status to DONE");
  } else {
    $self->{DB}->updateStatus($queueid,$status, "ERROR_RE");
    foreach (@fails) { $self->putJobLog($queueid,"error", "Error registering: $_"); }
    $self->putJobLog($queueid,"state", "Job state transition from $status to ERROR_RE");
  }
 
  return $success;
}

1

