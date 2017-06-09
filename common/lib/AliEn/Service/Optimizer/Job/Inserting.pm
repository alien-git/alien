package AliEn::Service::Optimizer::Job::Inserting;

use strict;

use AliEn::Service::Optimizer::Job;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  $self->{SLEEP_PERIOD}=10;
  my $method="info";
  $silent and $method="debug";
  my @data;
  $silent and push @data, 1;
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
  $self->{INSERTING_COUNTING}++;
  if ($self->{INSERTING_COUNTING}>10){
    $self->{INSERTING_COUNTING}=0;
  }else {
    $method="debug";
    @data=(1);
  }
  $self->$method(@data, "The inserting optimizer starts");
#  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='INSERTING'");
#  $todo or return;
  $self->{DB}->queryValue("SELECT count(1) as c from QUEUE where statusId=1")
    or $self->info("Returned in Count") and return;
#  $self->{DB}->update("ACTIONS", {todo=>0}, "action='INSERTING'");
  my $q = "1' and upper(origjdl) not like '\% SPLIT = \"\%";
  $self->{DB}->{DRIVER}=~/Oracle/i and $q = "1 and REGEXP_REPLACE(upper(origjdl), '\\s*', '') not like '\%SPLIT=\"\%";
  my $done=$self->checkJobs($silent,$q, "updateInserting", 15, 15);

  $self->$method(@data, "The inserting optimizer finished");
  return;
}

sub updateInserting {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  my $status="WAITING";

  $self->info( "\n\nInserting a new job $queueid" );

  my $user= $self->{DB}->queryValue("select user from QUEUE join QUEUE_USER using (userid) where queueid=?",
                     undef, {bind_values=>[$queueid]})
    or $self->info( "Job $queueid doesn't exist" )
      and return;

  my $set={};
  eval {
    if ( !$job_ca->isOK() ) {
      die("incorrect JDL input");
    }
    
    my ($okspt, $strategy) = $job_ca->evaluateAttributeString("Splitted");
  	my ($ok, $req)=$job_ca->evaluateExpression("Requirements");
	    ($ok and $req) or
	      die("error getting the requirements of the jdl");
	      
    my $done = {};
    
    if (!$okspt || $strategy !~ /^se$/i){
	    $done=$self->copyInput($queueid, $job_ca, $user) or 
	      die("error copying the input\n");
	
	    $self->debug(1,  "Let's create the entry for the jobagent");
	    $req =~ s{ \&\& \( other.LocalDiskSpace > \d+ \)}{}g;
	
	    $done->{requirements} and $req.=" && $done->{requirements}";
    }

    ($ok, my $stage)=$job_ca->evaluateExpression("Prestage");
    if ($stage){
      $self->putJobLog($queueid, "info", "The job asks for its data to be pre-staged");
      $status="TO_STAGE";
      $req.="  && other.TO_STAGE==1 ";
    }
    
    # Call add CVMFS_Revision to requirements
    my ($code,$new_job_ca, $newreqs) = $self->addCVMFSRevision($job_ca, $req) 
      or die("Error adding the CVMFS_Revision requirement\n");
        
    ($code and $code==2 and $job_ca=$new_job_ca and $req = $newreqs)
      or ($ok=$job_ca->set_expression("Requirements", $req) or 
	        die("ERROR SETTING THE REQUIREMENTS TO $req"));
	      
	$set->{origjdl}=$job_ca->asJDL();

    ($status) = $self->checkRequirements($req,$queueid,$status);

    if($status ne "FAILED"){
	    $req=$self->getJobAgentRequirements($req, $job_ca);
	    $set->{agentId}=$self->{DB}->insertJobAgent($req)
	      or die("error creating the jobagent entry\n");
    }
  };
  my $return=1;
  if ($@) {
    $self->info( "Error inserting the job: $@");
    $status="ERROR_I";
    # $self->{DB}->deleteJobToken($queueid);
    $self->putJobLog($queueid,"error", "There were problems analyzing the job requirements: $@");
    undef $return;
  }
  if (! $self->{DB}->updateStatus($queueid,"INSERTING", $status, $set)) {
    $self->{DB}->updateStatus($queueid,"INSERTING", "ERROR_I");
    $self->info( "Error updating status for job $queueid" );
    return;
  }
  $self->putJobLog($queueid,"state", "Job state transition from INSERTING to $status");

  $return and $self->debug(1, "Command $queueid inserted!" );
  return $return


}


1
