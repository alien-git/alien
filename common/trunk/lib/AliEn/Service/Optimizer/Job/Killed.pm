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

  my $done=$self->checkJobs($silent, "KILLED", "updateKilled");

  $self->info( "The inserting optimizer finished");
  return;
}

sub updateKilled {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  my $status="WAITING";
  print "\n";
  $self->info( "Job $queueid has been killed. Removing it from the queue" );
  
  my $info=$self->{DB}->getFieldsFromQueue($queueid,"submitHost,site") 
    or $self->info("Error getting the info of that job") and return; 
  my $submitHost=$info->{submitHost}
    or $self->info("Error getting the owner of that job") and return; 
  my $site=$info->{site} || "";

  $self->{DB}->delete("QUEUE", "queueId=$queueid");
  $self->info("Reducing the number of jobs in $site");
  $self->{DB}->do("UPDATE SITEQUEUES set KILLED=KILLED-1 where site='$site'");
  my $procDir=AliEn::Util::getProcDir(undef,$submitHost,  $queueid);
  $procDir or $self->info("Error getting the directory") and return;

  $self->{CATALOGUE}->execute("rmdir", "-rf", $procDir);
  return 1;
}


1
