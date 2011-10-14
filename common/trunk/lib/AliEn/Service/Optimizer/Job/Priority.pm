package AliEn::Service::Optimizer::Job::Priority;

use strict;

use AliEn::Service::Optimizer::Job;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
    my $self = shift;
    my $silent = shift;
    $self->{SLEEP_PERIOD}=3600;
    
    my $method="info";
    my @data;
    $silent and $method="debug" and push @data, 1;

    $self->$method(@data, "The priority optimizer starts");

    $self->{CATALOGUE}->execute("resyncJobAgent");
    $self->info("First, let's get all the users");
    my $userColumn=$self->{DB}->userColumn;
$self->{DB}->optimizerJobPriority($userColumn);
    $self->info("Now, compute the number of jobs waiting and priority per user");
    my $update = $self->{DB}->getPriorityUpdate($userColumn);
    $self->info("Doing $update");
    $self->{DB}->do($update);

    $self->info("Finally, let's update the JOBAGENT table");
  # $update="UPDATE JOBAGENT j set j.priority=(SELECT computedPriority-(min(queueid)/(SELECT ifnull(max(queueid),1) from QUEUE)) from PRIORITY p, QUEUE q where j.entryId=q.agentId and status='WAITING' and $userColumn=p.".$self->{DB}->reservedWord("user")." group by agentId)";
    $update = $self->{DB}->getJobAgentUpdate($userColumn);
    $self->info("Doing $update");
    $self->{DB}->do($update);

    $update = "UPDATE JOBAGENT j set j.priority=j.priority * (SELECT ifnull(max(price),1) FROM QUEUE q WHERE q.agentId=j.entryId)";
   
    $self->info("Doing $update");
    $self->{DB}->do($update);


    $self->$method(@data, "The priority optimizer finished");

    return;
}



1
