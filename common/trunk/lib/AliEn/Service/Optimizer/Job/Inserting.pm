package AliEn::Service::Optimizer::Job::Inserting;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::Database::Admin;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  $self->{SLEEP_PERIOD}=10;
  $self->{addbh} or $self->{addbh} = new AliEn::Database::Admin();    
  $self->{addbh} or 
    $self->info("Error getting the admin database... we will have to talk to the Authen");
  my $method="info";
  $silent and $method="debug";
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
  $self->{INSERTING_COUNTING}++;
  if ($self->{INSERTING_COUNTING}>10){
    $self->{INSERTING_COUNTING}=0;
  }else {
    $method="debug";
  }
  $self->{LOGGER}->$method("Inserting", "The inserting optimizer starts");
  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='INSERTING'");
  $todo or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='INSERTING'");

  my $done=$self->checkJobs($silent, "INSERTING' and jdl not like '\% Split = \"\%", "updateInserting");

  $self->info( "The inserting optimizer finished");
  return;
}

sub updateInserting {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  my $status="WAITING";
  print "\n";
  $self->info( "Inserting a new job" );

  my ($host)= $self->{DB}->getFieldFromQueue($queueid,"submitHost")
    or $self->info( "Job $queueid doesn't exist" )
      and return;

  my $user = "";
  ( $host =~ /^(.*)\@/ ) and ( $user = $1 );
  my $set={};
  eval {
    my $done=$self->copyInput($queueid, $job_ca, $user) or 
      die("error copying the input\n");

    $self->info("Updating the ADMIN table for $queueid" );
    $self->insertToken($queueid, $user);

    my ($ok, $req)=$job_ca->evaluateExpression("Requirements");
    ($ok and $req) or
      die("error getting the requirements of the jdl");
    $self->info( "Let's create the entry for the jobagent");
    $req =~ s{ \&\& \( other.LocalDiskSpace > \d+ \)}{}g;

    $done->{requirements} and $req.=" && $done->{requirements}";

    $ok=$job_ca->set_expression("Requirements", $req) or 
      die("ERROR SETTING THE REQUIREMENTS TO $req");
    $set->{jdl}=$job_ca->asJDL();
#    print "The jdl is $set->{jdl}\n";


    $req = "Requirements= $req;\n";

    foreach my $entry ("user", "memory", "swap", "localdisk") {
      my ($ok, $info)=$job_ca->evaluateExpression($entry);
      ($ok and $info) or next;
      $req.=" $entry =$info;\n";
    }

    $self->{DB}->insertJobAgent($req)
      or die("error creating the jobagent entry\n");
  };
  my $return=1;
  if ($@) {
    $self->info( "Error inserting the job: $@");
    $status="ERROR_I";
    undef $return;
  }
  if (! $self->{DB}->updateStatus($queueid,"INSERTING", $status, $set)) {
    $self->{DB}->updateStatus($queueid,"INSERTING", "ERROR_I");
    $self->info( "Error updating status for job $queueid" );
    return;
  }
  $self->putJobLog($queueid,"state", "Job state transition from INSERTING to $status");

  $return and $self->info( "Command $queueid inserted!" );
  return $return


}


sub insertToken{
  my $self=shift;
  my $queueid=shift;
  my $user=shift;
  
  if ($self->{addbh}) {
    $self->info("Inserting the jobtoken in the database");
    $self->{addbh}->insertJobToken($queueid,$user,-1) and return 1;
  }
  $self->info("Talking to the Authen");
  my $result =$self->{SOAP}->CallSOAP("Authen", "insertJob", $queueid, $user );
  if (!$result) {
    $self->info( "Talking to the Authen failed... trying again");
    $self->{SOAP}->CallSOAP("Authen", "insertJob", $queueid, $user )
      or die("error inserting the job token\n");
  }
  return 1;
}

1
