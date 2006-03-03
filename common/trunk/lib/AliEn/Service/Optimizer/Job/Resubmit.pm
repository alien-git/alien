package AliEn::Service::Optimizer::Job::Resubmit;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::SOAP;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

my $self;

sub checkWakesUp {
  $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";

  $self->{LOGGER}->$method("Resubmit", "The splitting optimizer starts");
  my $done2=$self->checkJobs($silent, "FAILED", "updateResubmit");

  $self->{LOGGER}->$method("Resubmit", "The splitting optimizer finished");
  return;
}

sub updateResubmit {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  $self->info("\nJob $queueid failed. Let's try to resubmit it");

  $self->info("Creating a new jobtoken");
   $self->{SOAP}->CallSOAP("Authen", "recreateJobToken", $queueid) or return;
  $self->info("The job was resubmitted");

  $self->info( "Putting the status of $queueid to 'WAITING'");
  $self->{DB}->updateStatus($queueid,"%","WAITING")
    or $self->info( "Error updating status for job $queueid" )
      and die("Error changing the status\n");;
  $self->putJobLog($queueid,"state", "Job state transition to WAITING (job resubmitted)", "state");
  return 1;
}


1
