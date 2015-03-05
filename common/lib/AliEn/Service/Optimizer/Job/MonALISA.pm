package AliEn::Service::Optimizer::Job::MonALISA;

use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA );


push (@ISA, "AliEn::Service::Optimizer::Job");

use AliEn::Util;

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  $self->{SLEEP_PERIOD}=300;
  my $method="info";
  $silent and $method = "debug";
    
  $self->{LOGGER}->$method("MonALISA",  "MonALISA optimizer starts");
    
  #get all job states from QUEUE table 
  my $table =$self->{DB}->query("SELECT statusId, count(*) from QUEUE group by statusId");
  $table or return;

  #initialize resulting hash %res
  my %res;
  my $states = AliEn::Util->JobStatus();
  foreach  (@$states){
    $res{$_} = 0;	
  }
  $res{'WAITING_IN_JOBAGENT'} = 0;
  
  
  for (my $i = 0; $i<  @$table ; $i++)
    {
      $res{AliEn::Util::statusName($table->[$i]->{"statusId"})} = $table->[$i]->{"count(*)"};
    }
  #get number of entries from JOBAGENT table (supposed to be equal to number of 'WAITING' jobs from QUEUE)
  $table =$self->{DB}->queryValue("SELECT sum(counter) from JOBAGENT");
  $table and $res{WAITING_IN_JOBAGENT} = $table;
  
  #send everything to MonALISA
  $self->{MONITOR}->sendParameters($self->{CONFIG}->{ORG_NAME}.'_AllJobs', 'States', %res);
  $self->{LOGGER}->$method("MonALISA",  "Sent info to local MonALISA agent");
  
  return;
}



1
