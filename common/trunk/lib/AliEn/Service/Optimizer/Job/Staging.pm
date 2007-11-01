package AliEn::Service::Optimizer::Job::Staging;

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

  $self->$method(@data, "The saved optimizer starts");

  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='STAGING'");
  if ($todo){
    $self->{DB}->update("ACTIONS", {todo=>0}, "action='STAGING'");
    
    my $done=$self->checkJobs($silent, "STAGING", "checkStagingJob");

  }
  $self->$method(@data, "And now, let's check the jobs that were waiting to be staged");

  
  my $info=$self->{DB}->query("select s.queueid, jdl from STAGING s, QUEUE q where s.queueid=q.queueid and timestampadd(MINUTE, 5, staging_time)<now()");
  foreach my $entry (@$info){
    $self->checkAlreadyStaged($entry->{queueid}, $entry->{jdl});
  }
  return;

}

sub checkStagingJob{
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;
  my $now = time;

  $self->info("********************************\n\tWe should do something with job $queueid");


  $self->{DB}->insert("STAGING", {queueid=>$queueid});
  return 1;
}


sub checkAlreadyStaged {
  my $self=shift;
  my $queueid=shift;
  my $jdl=shift;
  $self->info("And now we put the job $queueid to WAITING");
  $self->info("*****WE SHOULD CHECK THE REQUIREMENTS!!!!!");
  
  my $ca=Classad::Classad->new($jdl);
  $self->info("Got the ca");
  my ($ok, $req)=$ca->evaluateExpression("Requirements");
  ($ok, my $stage)=$ca->evaluateAttributeString("StageCE");
  $req.=" && other.CE==\"$stage\"";

  $self->info("The new requirements are '$req'");
  $ca->set_expression("Requirements", $req);
  $jdl=$ca->asJDL();
  my $agentreq=$self->getJobAgentRequirements($req, $ca);

  my $set={jdl=>$jdl};
  $set->{agentid}=$self->{DB}->insertJobAgent($agentreq);
  $self->{DB}->updateStatus($queueid, "STAGING", "WAITING", $set);

  $self->{DB}->delete("STAGING", "queueid=$queueid");
 
  return 1;
}
1
