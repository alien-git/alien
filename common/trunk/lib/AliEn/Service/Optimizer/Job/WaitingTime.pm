package AliEn::Service::Optimizer::Job::WaitingTime;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::GUID;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp  {
  my $self=shift;
  my $date = time;
  
  $self->info( "Checking if there are any jobs that have to expire, time: $date");  
   
  my $done=$self->{DB}->queryColumn("SELECT queueId from QUEUE where statusId=5 and expires!='null' and expires+received < ?", 
  undef, {bind_values=>[$date]}); #WAITING
  
  foreach my $job (@$done){
  	$self->info("Going to expire job $job");
    $self->{DB}->updateStatus($job, "WAITING", "ERROR_EW");
    $self->putJobLog($job,"state", "Job state transition from WAITING to ERROR_EW, stayed in the queue longer than MaxWaitingTime");
  }

  return;

}

1

