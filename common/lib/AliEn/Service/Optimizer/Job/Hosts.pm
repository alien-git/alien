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

  $self->$method(@data, "The hosts optimizer starts");
  my $done5=$self->checkHosts($silent);
  $self->unblockQueue($silent);
  $self->$method(@data, "The hosts optimizer finished");

  $self->{COUNTER} or $self->{COUNTER}=0;
  $self->{COUNTER}++;
  if ($self->{COUNTER}>60){
    $self->{DB}->resyncSiteQueueTable();
    $self->{COUNTER}=0;
  }
  
  return;
}

sub unblockQueue {
  my $self=shift;
  my $silent=shift;
  $self->{DB}->do("UPDATE SITEQUEUES set timeblocked=now() where timeblocked is null and blocked='locked-err'") or $self->info("Error setting the time when the queue was blocked") and return;

  $self->info("Opening the queues that have been closed for more than 15 minutes");
  $self->{DB}->do("UPDATE SITEQUEUES set blocked='open', timeblocked=null where blocked='locked-err' and  timeblocked+interval '15' minute<now()");
  
  
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

#  $self->info(Dumper($hosts));
#  use Data::Dumper;
#  return 1;

  foreach my $data (@$hosts) {
    $self->$method(@data,"====> $data->{hostname}, $data->{cename}");

    my ($newJobs, $newQueued)=$self->{CONFIG}->GetMaxJobs($data->{hostname});
    $newJobs or next;
    my $set ={};
    
    my $time = time;
    my $upd = $self->{DB}->do("update HOSTS set status='LOST' where $time-date>24*3600 and status!='LOST' and hostname=?",{bind_values=>[$data->{hostname}]} ); 
    $upd>0 and $self->info( "In checkHosts set status=LOST to $data->{hostname}, time: $time" ) 
            or $self->info( "In checkHosts host $data->{hostname} keeps its status, time: $time" );
    
    if (( $data->{maxjobs} eq $newJobs) && ($data->{maxqueued} eq  $newQueued)) {
      $self->$method(@data, "Still the same number ($data->{maxjobs} and $data->{maxqueued})");
    } else {

      $self->info("In checkHosts updating maxjobs and maxqueued for $data->{cename} (to $newJobs and $newQueued)");

      $self->{DB}->updateHost($data->{hostname},{maxjobs=>$newJobs, maxqueued=>$newQueued})
	or $self->{LOGGER}->warning( "Hosts", "In checkHosts error updating maxjobs and maxqueued for host $data->{hostname}" );
	
    }
					   
    # update also in the sitequeue table and calculate the load value for this site

    $set->{'maxqueued'}  = $newQueued;
    $set->{'maxrunning'} = $newJobs;
    my $queueload = $self->{DB}->getFieldFromSiteQueue($data->{cename},"( RUNNING + ASSIGNED + STARTED + IDLE + INTERACTIV + SAVING ) as LOADALL");
    my $runload = $self->{DB}->getFieldFromSiteQueue($data->{cename},"( RUNNING + STARTED + INTERACTIV + SAVING ) as LOADALL");
    defined $queueload or 
      $self->info("No info of the queued/running processes for site $data->{cename} of SITEQUEUES table");
    defined $runload or
      $self->info("No info of the running processes for site $data->{cename} of SITEQUEUES table ");
    $set->{'queueload'} = "-0.0";
    $set->{'runload'}   = "-0.0";
    $queueload and $newJobs and $set->{'queueload'} = sprintf "%3.02f", 100.0 * $queueload / $newJobs;
    $runload and $newJobs and $set->{'runload'} = sprintf "%3.02f", 100.0 * $runload / $newJobs;
    $self->$method(@data,"Updating site $data->{cename} with QL $set->{'queueload'} and RL $set->{'runload'}");
    my $done=$self->{DB}->updateSiteQueue($set,"site=?", {bind_values=>[$data->{cename}]}) or
      $self->{LOGGER}->warning( "Hosts","In checkHosts error updating maxjobs and maxqueued for host $data->{hostname} and $data->{cename} in SITEQUEUETABLE");
    if ($done eq "") {
      $self->info("The site didn't exist... Let's insert it");
      $self->{DB}->insertSiteQueue($data->{cename});
    }
  }
  return 1;
}


1
