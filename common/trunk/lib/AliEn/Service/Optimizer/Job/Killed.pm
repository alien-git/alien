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
 
  #We take the max timestamp to make sure that jobs killed while we are doing this are processed in 
  #the next iteration 
	my $time=$self->{DB}->queryValues("select max(timestamp) from QUEUE where status='KILLED'");
  $self->{DB}->do("delete from a using QUEUEJDL a join QUEUE using (queueid) where status='KILLED' and mtime<=?",
                  {bind_values=>[$time]});
  $self->{DB}->do("delete from a using QUEUEPROC a join QUEUE using (queueid) where status='KILLED' and mtime<=?",
                  {bind_values=>[$time]});
  $self->{DB}->do("delete from a using FILES_BROKER a join QUEUE q on (a.split=q.queueid) where status='KILLED' and mtime<=?",
                  {bind_values=>[$time]});
  $self->{DB}->do("delete from QUEUE where status='KILLED' and mtime<=?",
                  {bind_values=>[$time]});
  $self->{DB}->do("update SITEQUEUES set killed=0");
  

  $self->info( "The inserting optimizer finished");
  return;
}


1
