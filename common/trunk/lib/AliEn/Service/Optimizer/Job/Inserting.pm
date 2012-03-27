package AliEn::Service::Optimizer::Job::Inserting;

use strict;

use AliEn::Service::Optimizer::Job;
#use AliEn::Database::Admin;
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
  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='INSERTING'");
  $todo or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='INSERTING'");
  my $q = "INSERTING' and upper(origjdl) not like '\% SPLIT = \"\%";
  $self->{DB}->{DRIVER}=~/Oracle/i and $q = "INSERTING' and REGEXP_REPLACE(upper(origjdl), '\\s*', '') not like '\%SPLIT=\"\%";
  my $done=$self->checkJobs($silent,$q, "updateInserting");

  $self->$method(@data, "The inserting optimizer finished");
  return;
}

sub updateInserting {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  my $status="WAITING";

  $self->info( "\n\nInserting a new job $queueid" );

  my ($host)= $self->{DB}->getFieldFromQueue($queueid,"submitHost")
    or $self->info( "Job $queueid doesn't exist" )
      and return;

  my $user = "";
  ( $host =~ /^(.*)\@/ ) and ( $user = $1 );
  my $set={};
  eval {
    if ( !$job_ca->isOK() ) {
      die("incorrect JDL input");
    }

    my $done=$self->copyInput($queueid, $job_ca, $user) or 
      die("error copying the input\n");

#    $self->info("Updating the ADMIN table for $queueid" );
#    $self->insertToken($queueid, $user);

    my ($ok, $req)=$job_ca->evaluateExpression("Requirements");
    ($ok and $req) or
      die("error getting the requirements of the jdl");
    $self->debug(1,  "Let's create the entry for the jobagent");
    $req =~ s{ \&\& \( other.LocalDiskSpace > \d+ \)}{}g;

    $done->{requirements} and $req.=" && $done->{requirements}";

    $ok=$job_ca->set_expression("Requirements", $req) or 
      die("ERROR SETTING THE REQUIREMENTS TO $req");
    $set->{origjdl}=$job_ca->asJDL();
#    print "The jdl is $set->{jdl}\n";

    ($ok, my $stage)=$job_ca->evaluateExpression("Prestage");
    if ($stage){
      $self->putJobLog($queueid, "info", "The job asks for its data to be pre-staged");
      $status="TO_STAGE";
      $req.="  && other.TO_STAGE==1 ";
    }

    $req=$self->getJobAgentRequirements($req, $job_ca);


    $set->{agentId}=$self->{DB}->insertJobAgent($req)
      or die("error creating the jobagent entry\n");
  };
  my $return=1;
  if ($@) {
    $self->info( "Error inserting the job: $@");
    $status="ERROR_I";
    $self->{DB}->deleteJobToken($queueid);

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


#sub insertToken{
#  my $self=shift;
#  my $queueid=shift;
#  my $user=shift;
#  
#  if ($self->{addbh}) {
#    $self->info("Inserting the jobtoken in the database");
#    $self->{addbh}->insertJobToken($queueid,$user,-1) and return 1;
#  }
#  $self->info("Talking to the Authen");
#  my $result =$self->{SOAP}->CallSOAP("Authen", "insertJob", $queueid, $user );
#  if (!$result) {
#    $self->info( "Talking to the Authen failed... trying again");
#    $self->{SOAP}->CallSOAP("Authen", "insertJob", $queueid, $user )
#      or die("error inserting the job token\n");
#  }
#  return 1;
#}

1
