package AliEn::Service::Optimizer::Job::Killed;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::Database::Admin;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  $self->{SLEEP_PERIOD}=60;
  my $method="info";
  $silent and $method="debug";
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
  $self->{INSERTING_COUNTING}++;
  if ($self->{INSERTING_COUNTING}>10){
    $self->{INSERTING_COUNTING}=0;
  }else {
    $method="debug";
  }
  $self->{LOGGER}->$method("Inserting", "The Killed  optimizer starts");
 
  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='KILLED'");
  $todo or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='KILLED'");
  $self->{DB}->lock("QUEUEJDL write, QUEUEPROC write, QUEUE write, SITEQUEUES");

  $self->{DB}->do("delete from QUEUEJDL using QUEUEJDL join QUEUE using (queueid) where status='KILLED'");
  $self->{DB}->do("delete from QUEUEPROC using QUEUEPROC join QUEUE using (queueid) where status='KILLED'");
  $self->{DB}->do("delete from QUEUE where status='KILLED'");
  $self->{DB}->do("update SITEQUEUES set killed=0");
  $self->{DB}->unlock();
  my $done=$self->checkJobs($silent, "KILLED", "updateKilled");

  $self->info( "The inserting optimizer finished");
  return;
}


1
