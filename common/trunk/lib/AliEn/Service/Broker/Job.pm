package AliEn::Service::Broker::Job;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Database::TaskQueue;

#use AliEn::TokenManager;
use AliEn::JOBLOG;

use AliEn::Service::Broker;
use strict;
use AliEn::Database::Admin;

use AliEn::Database::CE;

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
  $self->{JOBLOG} = new AliEn::JOBLOG();
  $self->{addbh} = new AliEn::Database::Admin();    

  $self->{LOCALJOBDB}=new AliEn::Database::CE or return;

  $self->forkCheckProcInfo() or return;

  $self->SUPER::initialize($options) or return;

}

sub getJobIdFromAgentId {
  my $self=shift;
  my $agentId=shift;
  my $cache=shift;

  if (!$cache){
    $self->info("Getting the jobids for jobagent '$agentId'");
    my $data=AliEn::Util::returnCacheValue($self, "WaitingJobsFor$agentId");
    if (! $data){
      $data=$self->{DB}->query("select queueid as id, jdl from QUEUE where agentid=? and (STATUS='WAITING' or STATUS='TO_STAGE') order by queueid", undef, {bind_values=>[$agentId]});
    }
    $self->info("There are $#$data entries for that jobagent");
    return @$data;
  }
  $self->info("For the next time that this thing is called, putting the info in the cache");
  ( $#$cache>100) or $cache=undef;
  AliEn::Util::setCacheValue($self, "WaitingJobsFor$agentId", $cache);
  return 1;
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

  $self->info( "Getting the list of jobs");
  my $list= $self->{DB}->getWaitingJobAgents();

  defined $list                              
    or $self->{LOGGER}->warning( "JobBroker", "In findjob error during execution of database query" )
      and return;

  my $number=@$list;
   if($self->{MONITOR}){
    $self->{MONITOR}->sendParams(("WAITING_jobs", $number));
  }
 
  if (!@$list){
    $self->info( "In findjob no job to match" );
    return {execute=> [-2, "No jobs waiting in the queue"]};
  }
  $self->info( "Starting the match, with $number elements");
  
  my $site_ca = Classad::Classad->new($site_jdl);
  $self->{SITE_CA}=$site_ca;
  my ($ok, $msg)=$self->checkQueueOpen($site_ca);
  if (!$ok){
    return {execute=>[-1, $msg]};
  }
  ($ok, my $wn)= $site_ca->evaluateAttributeString("WNHost");
  $self->info("The worker node is $wn");
  my ($queueId, $job_ca, $jdl)=$self->match( "agent", $site_ca, $list, $user, $host , "checkPackagesToInstall", 0,"getJobIdFromAgentId");

  my $to_stage;
  if ($site_stage_jdl and 0){
    $self->info("Checking if it can stage anything ($site_stage_jdl)");
    my $site_stage_ca = Classad::Classad->new($site_stage_jdl);
    if (($site_stage_ca) and 0 ){
      my ($stageId, $stage_ca, $stage_jdl)=$self->match("agent", $site_stage_ca, $list, $user, $host."_alienSTAGE", undef, 0,"getJobIdFromAgentId");
      $self->info("AFTER CHECKING, got $stageId, $stage_ca and $stage_jdl");

      if ($stageId>0){
	$self->info("The JobAgent could stage $stageId");
	$to_stage={queueid=>$stageId, jdl=>$stage_jdl};
      }
    } else{
      $self->info("The stage JDL has the wrong syntax!!"); 
      
    }
  }
  my @return=();
  eval {
    if (!$queueId){
      push @return,-2, "No jobs waiting in the queue";
      die("No job matches '$site_jdl'\n");
    }
    if ($queueId eq "-3"){
      my @packages=@$job_ca;
      $self->info("Before we can assign the job, the WN has to install some packages (@packages)");
      $self->putlog($queueId, "debug", "Site needs to install @packages before retrieving the job");
      push @return, -3, @packages;
      die("Telling the site to install @packages");
    }
    $self->putlog($queueId,"state","Job state transition from WAITING to ASSIGNED ($host)");

    $self->info("Getting the token");
    my $result=$self->getJobToken($queueId);
    #  my $result = $self->{TOKENMAN}->getJobToken($queueId);
    $self->info("I got as token $result"); 
    if ((! $result) || ($result eq "-1")) {
      $self->{DB}->updateStatus($queueId, "%", "ERROR_A");
      $self->putlog($queueId,"state","Job state transition from ASSIGNED to ERRROR_A");
      $self->{LOGGER}->error( "Broker", "In requestCommand error getting the token" );
      push @return, -1, "getting the token of the job $queueId" ;
      die ("Error getting the token of the job");
    }
    my $token   = $result->{token};
    my $jobUser = $result->{user};
    $self->debug(1, "In requestCommand $jobUser token is $token" );
    $self->info(  "Command $queueId sent to $host" );
    push @return, {queueid=>$queueId, token=>$token, jdl=>$jdl, user=>$jobUser};
  };

  $self->info("Returning the new format");
  return {execute=>\@return,stage=>$to_stage};
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

#  $self->info("Changing the ownership of the directory" );

#  my $procDir = AliEn::Util::getProcDir($user, undef, $procid);
#  if (!($self->{cat}->f_chown( $user, $procDir ))) {
#    $self->{LOGGER}->warning("Broker",
#			     "Error changing the privileges of the directory $procDir in the catalogue"
#			    );
#    $self->{LOGGER}->warning("Broker","Making a new database connection ");
#    $self->{cat} = AliEn::Catalogue::Server->new($self->{options});
#    $self->{LOGGER}->warning("Broker","Now I have a new database connection");
#    if (!($self->{cat}->f_chown( $user, $procDir ))) {
#      $self->{LOGGER}->critical(
#				"Broker",
#				"Error changing the privileges of the directory $procDir in the catalogue 2nd time"
#			       );
#      return ( -1, "changing the privileges" );
#    }
#  }
  
  
  $self->info("Sending job $procid to $user" );
  return { "token" => $token, "user" => $user };
}



 sub checkPackagesToInstall{
  my $self=shift;
  my $job_ca=shift;
  my $host_ca=$self->{SITE_CA};
  my @packages;
  $self->debug(1, "Checking if the host has all the packages installed");
  my ($ok, @host_packages)=$host_ca->evaluateAttributeVectorString("InstalledPackages");
  ($ok, my @host_defined)=$host_ca->evaluateAttributeVectorString("Packages");
  ($ok, my @job_packages)=$job_ca->evaluateAttributeVectorString("Packages");
  $self->debug(1, "Checking if the site has @job_packages");
  foreach my $package (@job_packages){
    $self->debug(1,"Checking $package");
    $package =~ /@/ or $package=".*\@$package";
    $package =~ /::/ or $package="${package}::.*";
    grep (/^$package$/, @host_packages) and next;
    my @solution=grep (/^$package$/, @host_defined);
    $self->info("Telling the jobagent to install $solution[0]");
    push @packages, $solution[0];
    
  }
  (@packages) and  return -3, \@packages;
  
  $self->debug(1, "All the packages are installed");
  return 1;
}

# Checks if there are any agents needed that fulfill the requirements
# It returns an array of arrays of jobagents and requirements.
#
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
  $self->debug(1, "Creating the classad");
  my $site_ca = Classad::Classad->new($ca_text);
  $self->debug(1, "Classad created");
  my ($ok,$queueName)=$site_ca->evaluateAttributeString("CE");
  my @jobAgents;
  ($ok, my $msg)=$self->checkQueueOpen($site_ca, $queueName);
  $ok or return (-1, $msg);


  my ($dbAgents)=$self->{DB}->getWaitingJobAgents();

  if (!$dbAgents  ){
    $self->info( "Error getting the entries from the queue");
    die("Error selecting the jobAgent entries\n");
  }
  $self->info( "Got the list of needed agents ($#$dbAgents +1 elements)");
  foreach my $element (@$dbAgents) {
    $self->debug(1, "Checking the jdl of $element->{jdl}");
    my $job_ca = Classad::Classad->new($element->{jdl});
    if ( !$job_ca->isOK() ) {
      $self->info( "Error creating the jdl ");
      next;
    }
    my ( $match, $rank ) = Classad::Match( $job_ca, $site_ca );
    $match or next;
    $self->info( "There is a match");
    $self->{SITE_CA}=$site_ca;
    my ($ok, @packages)=$self->checkPackagesToInstall($job_ca);
    if ($ok eq -3){
      return ($ok, @packages);
    }
    #If after starting all these job agents, we still have some free slots,
    #we have to keep doing the matching
    if ($element->{counter}<$free_slots) {
      push @jobAgents, [$element->{counter}, $element->{jdl}];
      $free_slots-=$element->{counter};
    }else {
      push @jobAgents, [$free_slots, $element->{jdl}];
      last;
    }
  }
  if (!@jobAgents) {
    $self->info( "There is nothing for this host ($host)");
    $self->{DB}->setSiteQueueStatus($queueName,"open-no-match", $ca_text);
    return -2;
  }
  $self->{DB}->setSiteQueueStatus($queueName,"open-matching", $ca_text);

  foreach my $job (@jobAgents) {
    my ($num, $jdl)=@$job;
    $self->info( "Starting: $num for $jdl in $host");
  }
  $self->info("The broker returns @jobAgents");
  return @jobAgents;
}

sub putlog {
  my $self=shift;
  my $queueId=shift;
  my $status=shift;
  my $message=shift;
  return $self->{LOCALJOBDB}->insertMessage($queueId, $status,$message,0);
}


1;
