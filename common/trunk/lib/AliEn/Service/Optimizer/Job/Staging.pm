package AliEn::Service::Optimizer::Job::Staging;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::GUID;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  #$self->{SLEEP_PERIOD}=10;
  my $method="info";
  $silent and $method="debug";
  my @data=();
  $silent and push @data, 1;
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
  $self->{INSERTING_COUNTING}++;
  if ($self->{INSERTING_COUNTING}>10){
    $self->{INSERTING_COUNTING}=0;
  }else {
    $method="debug";
    @data=1;
  }

  $self->info("The staging optimizer starts");

  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='STAGING'");
  if ($todo){
    $self->{DB}->update("ACTIONS", {todo=>0}, "action='STAGING'");
    my $done=$self->checkJobs($silent, 19, "checkStagingJob"); # STAGING
  }
  $self->info("And now, let's check the jobs that were waiting to be staged");
 
  my $info=$self->{DB}->query("select q.queueId as qid, qj.origJdl as jdl from QUEUE q join QUEUEJDL qj using (queueId) where statusId=19");
  foreach my $entry (@$info){
    $self->checkAlreadyStaged($entry->{qid}, $entry->{jdl});
  }

  return;

}

sub checkStagingJob{
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  $self->info("Inserting job $queueid in STAGING table");
  eval {
  	$self->{DB}->insert("STAGING", {queueid=>$queueid});
  }; 
  $@ and $self->info("Job $queueid already in STAGING table ($@)");
  
  return 1;
}


sub checkAlreadyStaged {
  my $self=shift;
  my $queueid=shift;
  my $jdl=shift;
  $self->info("Checking if job $queueid has staged all its files");
  
  my $ca=AlienClassad::AlienClassad->new($jdl);
  my ($ok, my @inputData) = $ca->evaluateAttributeVectorString("InputData"); 
  
  $self->copyInputCollection($ca, $queueid, \@inputData)
    or $self->info("Error copying the inputCollection") 
      and $self->{DB}->updateStatus($queueid, "TO_STAGE", "FAILED") 
      and $self->putJobLog($queueid,"state", "Job state transition from TO_STAGE to FAILED")
      and return;
            
  foreach my $file ( @inputData) {
     $file =~ s/,nodownload$//; $file =~ s/^LF://i;
     $self->{CATALOGUE}->isStaged($file) or $self->info("File $file not staged yet") and return; 
  }
  $self->info("All staged ($queueid)");

  ($ok, my $req)=$ca->evaluateExpression("Requirements");  
  my $agentreq=$self->getJobAgentRequirements($req, $ca);
  my $set={};
  $set->{agentid}=$self->{DB}->insertJobAgent($agentreq);
  $self->{DB}->updateStatus($queueid, "STAGING", "WAITING", $set);
  $self->{DB}->delete("STAGING", "queueid=$queueid");
 
  return 1;
}
1
