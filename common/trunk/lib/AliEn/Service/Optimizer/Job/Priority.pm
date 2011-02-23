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
    my $userColumn="SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 )";
    $self->{DB}->do("INSERT IGNORE INTO PRIORITY(user, priority, maxparallelJobs, nominalparallelJobs) SELECT distinct $userColumn, 1,200, 100 from QUEUE");

    $self->info("Now, compute the number of jobs waiting and priority per user");
    my $update="update PRIORITY p left join 
(select SUBSTRING( submitHost, 1, POSITION('@' in submitHost)-1 ) user ,count(*) w from QUEUE where status='WAITING' group by SUBSTRING( submitHost, 1, POSITION('@' in submitHost)-1 ) )  b using (user)
 left join (select SUBSTRING( submitHost, 1, POSITION('@' in submitHost)-1 ) user,count(*) r from QUEUE where (status='RUNNING' or status='STARTED' or status='SAVING') group by SUBSTRING( submitHost, 1, POSITION('@' in submitHost)-1 ) ) b2 using (user) 
 set waiting=coalesce(w,0), running=COALESCe(r,0) , 
userload=(running/maxparallelJobs), 
computedpriority=(if(running<maxparallelJobs, if((2-userload)*priority>0,50.0*(2-userload)*priority,1),1))" ;

    $self->info("Doing $update");
    $self->{DB}->do($update);

    $self->info("Finally, let's update the JOBAGENT table");
   $update="UPDATE JOBAGENT j set j.priority=(SELECT computedPriority-(min(queueid)/(SELECT ifnull(max(queueid),1) from QUEUE)) from PRIORITY p, QUEUE q where j.entryId=q.agentId and status='WAITING' and $userColumn=p.user group by agentId)";

    $self->info("Doing $update");
    $self->{DB}->do($update);

    $update = "UPDATE JOBAGENT j set j.priority=j.priority * (SELECT max(price) FROM QUEUE q WHERE q.agentId=j.entryId)";
    $self->info("Doing $update");
    $self->{DB}->do($update);


    $self->$method(@data, "The priority optimizer finished");

    return;
}



1
