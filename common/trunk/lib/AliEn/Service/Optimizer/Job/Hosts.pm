package AliEn::Service::Optimizer::Job::Hosts;

use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");


sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";
  my @data=();
  $silent and push @data, "1";
  $self->{DBIS} or 
    $self->{DBIS}=new AliEn::Database::IS(
					  {
					   DB=>$self->{CONFIG}->{IS_DATABASE},
					   HOST=>$self->{CONFIG}->{IS_DB_HOST},
					   DRIVER=>$self->{CONFIG}->{IS_DRIVER},
					   DEBUG  => $self->{DEBUG},
					   ROLE => "admin"
					  }
					 );
  $self->{DBIS} or $self->{LOGGER}->info("Hosts", "Error getting the database");

  $self->$method(@data, "The hosts optimizer starts");
  my $done5=$self->checkHosts($silent);
  $self->unblockQueue($silent);
  $self->$method(@data, "The hosts optimizer finished");
  return;
}

sub unblockQueue {
  my $self=shift;
  my $silent=shift;
  $self->{DB}->do("UPDATE SITEQUEUES set timeblocked=now() where timeblocked is null and blocked='locked-err'") or $self->info("Error setting the time when the queue was blocked") and return;

  $self->info("Opening the queues that have been closed for more than 15 minutes");
  $self->{DB}->do("UPDATE SITEQUEUES set blocked='open', timeblocked=null where blocked='locked-err' and adddate(timeblocked,interval 15 minute)<now()");
  
  
  return 1;
}

sub  checkHosts {
  my $self=shift;
  my $silent=shift;
  my $status=shift;
  my $function=shift;


  my $method="info";
  my @data=();
  $silent and $method="debug" and push @data, 1;


  $self->$method(@data, "In checkHosts checking the number of jobs of each host");
  my $hosts=$self->{DB}->getMaxJobsMaxQueued;

  defined $hosts
    or $self->{LOGGER}->warning( "Hosts", "In checkHosts error during execution of database query" )
      and return;

  @$hosts
    or $self->info("There are no hosts" )
      and return;		#check if it's ok to return undef here!!


  foreach my $data (@$hosts) {
    $self->$method(@data,"====> $data->{hostname}");
    # translate the hostname into service name
    my $serviceblock= $self->{DBIS}->getServiceNameByHost("ClusterMonitor",$data->{hostname});
    my $site = $serviceblock->[0]->{name} or
      $self->info("Failed to resolve CM service name of host $data->{hostname}") and next;
    $self->$method(@data,"Getting the maxjobs of $data->{hostname}");

    my ($newJobs, $newQueued)=$self->{CONFIG}->GetMaxJobs($data->{hostname});
    $newJobs or next;
    my $set ={};
    if (( $data->{maxjobs} eq $newJobs) && ($data->{maxqueued} eq  $newQueued)) {
      $self->$method(@data, "Still the same number ($data->{maxjobs} and $data->{maxqueued})");
    } else {

      $self->info("In checkHosts updating maxjobs and maxqueued in database (to $newJobs and $newQueued)");

      $self->{DB}->updateHost($data->{hostname},{maxjobs=>$newJobs, maxqueued=>$newQueued})
	or $self->{LOGGER}->warning( "Hosts", "In checkHosts error updating maxjobs and maxqueued for host $data->{hostname}" );

    }
					   
    # update also in the sitequeue table and calculate the load value for this site

    $set->{'maxqueued'}  = $newQueued;
    $set->{'maxrunning'} = $newJobs;
    my $queueload = $self->{DB}->getFieldFromSiteQueue("$site","( RUNNING + QUEUED + ASSIGNED + STARTED + IDLE + INTERACTIV + SAVING ) as LOADALL");
    my $runload = $self->{DB}->getFieldFromSiteQueue("$site","( RUNNING + STARTED + INTERACTIV + SAVING ) as LOADALL");
    defined $queueload or 
      $self->info("No info of the queued/running processes for site $site of SITEQUEUES table");
    defined $runload or
      $self->info("No info of the running processes for site $site of SITEQUEUES table ");
    $set->{'queueload'} = "-0.0";
    $set->{'runload'}   = "-0.0";
    $queueload and $newJobs and $set->{'queueload'} = sprintf "%3.02f", 100.0 * $queueload / $newJobs;
    $runload and $newJobs and $set->{'runload'} = sprintf "%3.02f", 100.0 * $runload / $newJobs;
    $self->$method(@data,"Updating site $site with QL $set->{'queueload'} and RL $set->{'runload'}");
    my $done=$self->{DB}->updateSiteQueue($set,"site='$site'") or
      $self->{LOGGER}->warning( "Hosts","In checkHosts error updating maxjobs and maxqueued for host $data->{hostname} in site $site in SITEQUEUETABLE");
    if ($done eq "") {
      $self->info("The site didn't exist... Let's insert it");
      $set->{'site'} = "$site";
      $set->{'blocked'} = "open";
      $self->{DB}->insertSiteQueue($set);
    }
  }
  return 1;
}


1
