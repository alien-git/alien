package AliEn::Service::Broker::Job;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Database::TaskQueue;

#use AliEn::TokenManager;

use AliEn::Service::Broker;
use strict;
use AliEn::Database::Admin;

use AliEn::Util;

use vars qw (@ISA);

@ISA=("AliEn::Service::Broker");
use Classad;

my $self = {};

sub initialize {
  $self     = shift;
  my $options =(shift or {});

  $self->debug(1, "In initialize initializing service TransferBroker" );

  $self->{SERVICE}="Job";

  $self->{DB_MODULE}="AliEn::Database::TaskQueue";
#  $self->{TOKENMAN} = AliEn::TokenManager->new($self->{CONFIG});

  $self->{addbh} = new AliEn::Database::Admin();    

  $self->forkCheckProcInfo() or return;

  $self->SUPER::initialize($options) or return;

}

#
# This function is called when a jobAgent starts, and tries to get a task
#

sub getJobAgent {
  my $this    = shift;
  my $user    =shift;
  my $host    = shift;
  my $site_jdl = shift;
  my $site_stage_jdl =shift;

  my $date = time;

  #DO NOT PUT ANY print STATEMENTS!!! Otherwise, it doesn't work with an httpd container

  $self->redirectOutput("JobBroker/$host");
  $self->info( "In findjob finding a job for $host");

  $self->{DB}->updateHost($host,{status=>'ACTIVE', date=>$date})
    or $self->{LOGGER}->error("JobBroker", "In findjob error updating status of host $host")
      and return;

  my ($queueName, $params)=$self->extractClassadParams($site_jdl);
  $queueName eq '-1' and return $queueName, $params;
  use Data::Dumper;
  $self->info("We have the parameters:". Dumper($params));

  $params->{returnId}=1;
  my $agentid=$self->{DB}->getNumberWaitingForSite($params);
  
  if (!$agentid){
  	$self->info("Let's check if we need a package");
  	delete $params->{packages};
  	delete $params->{returnJob};
  	$params->{returnPackages}=1;
  	my $packages=$self->{DB}->getNumberWaitingForSite($params);
  	if (not $packages){
      $self->info( "In findjob no job to match" );
      return {execute=> [-2, "No jobs waiting in the queue"]};
    } else {
      $self->info("Telling the site to install packages");	
      return {execute=> [-3, split(",",$packages )]};
    }
  }
  my ($queueid, $jdl)=$self->{DB}->getWaitingJobForAgentId($agentid);
  $queueid or $self->info("There were no jobs waiting for agentid!") and return {execute=> [-2, "No jobs waiting in the queue"]};
  
  $self->putlog($queueid,"state","Job state transition from WAITING to ASSIGNED ");

  $self->info("Getting the token");
  my $result=$self->getJobToken($queueid);
  
  $self->info("I got as token $result"); 
  if ((! $result) || ($result eq "-1")) {
      $self->{DB}->updateStatus($queueid, "%", "ERROR_A");
      $self->putlog($queueid,"state","Job state transition from ASSIGNED to ERRROR_A");
      $self->info("In requestCommand error getting the token" );
      return -1, "getting the token of the job $queueid" ;    
    }
  my $token   = $result->{token};
  my $jobUser = $result->{user};
  $self->debug(1, "In requestCommand $jobUser token is $token" );
  $self->info(  "Command $queueid sent !" );
  return {execute=> [{queueid=>$queueid, token=>$token, jdl=>$jdl, user=>$jobUser}]};
}

sub checkQueueOpen {
  my $self=shift;
  my $site_ca=shift;
  my $queue_name=shift;
  if (!$queue_name) {
    (my $ok, $queue_name)=$site_ca->evaluateAttributeString("CE");
    if (! $queue_name) {
      $self->info("Error getting the queue name from the classad");
      return ("", "Error getting the queue name from the classad");
    }
  }
  my $open=$self->{DB}->queryValue("select count(*) from SITEQUEUES where blocked='open' and site='$queue_name'");
  if (!$open) {
    $self->{DB}->setSiteQueueStatus($queue_name,"closed-blocked", $site_ca->asJDL());
    return ("", "The queue is locked ");
  }
  return (1,"");
}

# ***************************************************************
# Creates a new token randomly. Alway 32 caracters long.
# ***************************************************************
my $createToken = sub {
    my $token = "";
    my @Array = (
        'X', 'Q', 't', '2', '!', '^', '9', '5', '3', '4', '5', 'o',
        'r', 't', '{', ')', '}', '[', ']', 'h', '9', '|', 'm', 'n', 'b', 'v',
        'c', 'x', 'z', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', ':', 'p',
        'o', 'i', 'u', 'y', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
        'A', 'S', 'D', 'F', 'G', 'H', 'J', 'Z', 'X', 'C', 'V', 'B', 'N', 'M'
    );
    my $i;
    for ( $i = 0 ; $i < 32 ; $i++ ) {
        $token .= $Array[ rand(@Array) ];
    }
    return $token;
};

sub getJobToken {
  my $self=shift;
  my $procid = shift;
  
  $self->info("Getting  job $procid" );
  
  ($procid)
    or $self->info("Error: In getJobToken not enough arguments" )
      and return;
  
  my ($data) = $self->{addbh}->getFieldsFromJobToken($procid,"jobToken, userName");

  ($data)
    or $self->{LOGGER}->error( "CatalogDaemon", "Database error fetching fields for $procid" )
      and return;

  my ( $token, $user ) = ( $data->{jobToken}, $data->{userName});

  ( $token eq '-1' )
    or $self->info("Job $procid already given.." )
      and return;

  $token = $createToken->();

  $self->{addbh}->setJobToken($procid,$token)
    or $self->{LOGGER}->warning( "CatalogDaemon","Error updating jobToken for user $user" ) 
      and return (-1, "error setting the job token");

  $self->info("Sending job $procid to $user" );
  return { "token" => $token, "user" => $user };
}

# Checks if there are any agents needed that fulfill the requirements
# It returns an array of arrays of jobagents and requirements.
#

sub extractClassadParams{
  my $self=shift;
  my $ca_text=shift;
  my $params={};

  $self->debug(1, "Creating the classad");
  my $classad = Classad::Classad->new($ca_text);
  $self->debug(1, "Classad created");

  my $site="";
  my ($ok,$queueName)=$classad->evaluateAttributeString("CE");
  my @jobAgents;
  ($ok, my $msg)=$self->checkQueueOpen($classad, $queueName);
  $ok or return (-1, $msg);

  $queueName =~ /::(.*)::/ and $site=$1;
  ($ok, my $ttl)=$classad->evaluateAttributeString("TTL");
  $params->{ttl} = $ttl || 84000;
  ($ok, $params->{disk})=$classad->evaluateExpression("LocalDiskSpace");
  ($ok, my @pack)=$classad->evaluateAttributeVectorString("Packages");
  $params->{packages}=",". join(",", sort @pack ) .",";
  ($ok, @pack)=$classad->evaluateAttributeVectorString("Partition");
  $params->{partition}=",". join(",", sort @pack ) .",";

  return ($queueName, $params);
}

sub offerAgent {
  shift;
  my $user=shift;
  my $host=shift;
  my $ca_text=shift;
  my $free_slots=( shift or 0);
  
  $self->redirectOutput("JobBroker/$host");
   $self->info( "And now Checking if there are any agents that can be started in the machine $host (up to a maximum of $free_slots)");

  $self->setAlive();

  $free_slots or $self->info( "Not enough resources")
    and return (-1, "Not enough resources");

  my ($queueName, $params)=$self->extractClassadParams($ca_text);
  $queueName eq '-1' and return $queueName, $params;
  
  my $waiting=$self->{DB}->getNumberWaitingForSite($params);
  
  $self->info("We could run $waiting jobs there");
  $waiting > $free_slots and $waiting=$free_slots;
  if ($waiting){
    $self->info("Telling the site to start $waiting job agents");
    $self->{DB}->setSiteQueueStatus($queueName,"open-matching", $ca_text);
    return [$waiting, '[Type="Job";Requirements = other.Type == "machine" ]' ];
  }
  delete $params->{packages};
  $params->{returnPackages}=1;
  $waiting=$self->{DB}->getNumberWaitingForSite($params);
  if ($waiting){
    $self->info("The site could run some jobs if it installed '$waiting'");
    $self->{DB}->setSiteQueueStatus($queueName,"open-install-p", $ca_text);    
    return -3, split(",", $waiting);
  }
  
  $self->info( "There is nothing for this host ($host)");
  $self->{DB}->setSiteQueueStatus($queueName,"open-no-match", $ca_text);
  return -2;
}

sub putlog {
  my $self=shift;
  my $queueId=shift;
  my $status=shift;
  my $message=shift;
  return $self->{DB}->insertJobMessage($queueId, $status,$message,0);
}




sub invoke {
  my $other=shift;
  my $op=shift;


  if (!$self->{TASK_DB}) {

  $self->{PASSWD} = ( $self->{LOCALJOBDB}->{PASSWD} or "" );

    my ($host, $driver, $db) =
      split("/", $self->{CONFIG}->{"JOB_DATABASE"});


    $self->{TASK_DB}=
        AliEn::Database::TaskQueue->new({PASSWD=>"$self->{PASSWD}",DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin', SKIP_CHECK_TABLES=> 1});
    $self->{TASK_DB} or $self->{LOGGER}->error( "CE", "In initialize creating TaskQueue instance failed" )
      and return;

#    $self->{TASK_DB}->setSiteQueueTable();
  }

  $self->info("$$ Ready to do a task operation (and $op '@_')");

  my $mydebug=$self->{LOGGER}->getDebugLevel();
  my $params=[];

  (my $debug,$params) = AliEn::Util::getDebugLevelFromParameters(@_);
  $debug and $self->{LOGGER}->debugOn($debug);
  $self->{LOGGER}->keepAllMessages();
#  $op = "$self->{TASK_DB}->".$op;
  my @info = $self->{TASK_DB}->$op(@_);

  my @loglist = @{$self->{LOGGER}->getMessages()};

  $debug and $self->{LOGGER}->debugOn($mydebug);
  $self->{LOGGER}->displayMessages();
  $self->info("$$ invoke DONE with OP: $op (and @_)");#, rc = $rc");
  $self->info("$$ invoke result: @info".scalar(@info));
  return { #rc=>$rc,
     rcvalues=>\@info, rcmessages=>\@loglist};
}





1;
