package AliEn::Service::Optimizer::Transfer::Agent;

use strict;

use vars qw (@ISA);
use AliEn::Service::Optimizer::Transfer;

push (@ISA, "AliEn::Service::Optimizer::Transfer");



sub checkWakesUp {
  my $self=shift;
  my $silent=(shift or 0);
  my $method="info";

  $self->info("In checkTransferRequirements checking that the AGENTS are in sync");

  $self->info("First, let's take a look at the missing jobagents");

  my $jobs=$self->{DB}->query("select jdl, agentid from TRANSFERS_DIRECT q join (select min(transferid) as q from TRANSFERS_DIRECT left join AGENT_DIRECT on agentid=entryid where entryid is null  and status='WAITING'  group by agentid) t  on transferid=q") or $self->info("Error getting the transfers without agents") and return;
  
  foreach my $job (@$jobs){
    $self->info("We have to insert an agent for $job->{jdl}");
    $job->{jdl} =~ /(requirements[^;]*)/i or 
      $self->info("Error getting the requirements from $job->{jdl}") and next;
    my $req="[ $1 ; Type=\"Transfer\"]";

    $self->{DB}->insert("AGENT_DIRECT", {counter=>30, entryid=>$job->{agentid}, 
					 requirements=>$req});
  }



  $self->info("Now, update the jobagent numbers");
  $self->{DB}->do("update AGENT_DIRECT j  set currentTransfers=0, counter=(select count(*) from TRANSFERS_DIRECT where status='WAITING' and agentid=entryid)");
  $self->{SLEEP_PERIOD}=3600;


  return ;
}
