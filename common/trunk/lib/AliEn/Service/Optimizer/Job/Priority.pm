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

    $self->info("First, let's get all the users");
    my $userColumn="SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 )";
    $self->{DB}->do("INSERT IGNORE INTO PRIORITY(user, priority, maxparallelJobs, nominalparallelJobs) SELECT distinct $userColumn, 1,200, 100 from QUEUE");

    $self->info("Now, compute the number of jobs waiting and priority per user");
    my $update="update PRIORITY p  set 
waiting= (select count(*) from QUEUE where status='WAITING' and p.user=$userColumn),
running=(select count(*) from QUEUE where (status='RUNNING' or status='STARTED' or status='SAVING') and p.user= $userColumn ),
userload=running/maxparallelJobs,
computedpriority=if(running<maxparallelJobs,
                    if((2-userload)*priority>0,50.0*(2-userload)*priority,0),0)";
    $self->info("Doing $update");
    $self->{DB}->do($update);

    $self->info("Finally, let's update the JOBAGENT table");
    $update="UPDATE JOBAGENT j set j.priority=(SELECT computedPriority-(min(queueid)/(SELECT ifnull(max(queueid),1) from QUEUE)) from PRIORITY p, QUEUE q where j.entryId=q.agentId and $userColumn=p.user group by agentId)";

    $self->info("Doing $update");
    $self->{DB}->do($update);

    $self->$method(@data, "The priority optimizer finished");

    return;
}



1
