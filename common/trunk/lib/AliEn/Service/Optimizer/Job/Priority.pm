package AliEn::Service::Optimizer::Job::Priority;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::Database::TaskPriority;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
    my $self = shift;
    my $silent = shift;
    
    my $method="info";
    my @data;
    $silent and $method="debug" and push @data, 1;
    if (!$self->{PRIORITY_DB}) {
      $self->{PRIORITY_DB}=AliEn::Database::TaskPriority->new({ROLE=>"admin"}) or die("Error getting a copy of the taskPriority database");
    }

    $self->$method(@data, "The priority optimizer starts");
    my $done=$self->setPriority($method);
    
    $self->$method(@data, "The priority optimizer finished");

    return;
}
    
sub setPriority {
    my $self = shift;
    my $method = shift;
    my @data;
    $method eq "debug" and push @data, 1;
    # select the user's having jobs in waiting state
    $self->$method(@data,"DB: Getting userjobs");
    my $userjobs = $self->{DB}->getFieldsFromQueueEx("SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, queueId","where status='WAITING'");
    $self->$method(@data,"DB: Getting usernames and waiting jobs");
    my $usernames = $self->{DB}->getFieldsFromQueueEx("SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, count(SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 )) as njobs","where status='WAITING' group by user");
    $self->$method(@data,"DB: Getting usernames and running jobs");
    my $runvalues = $self->{DB}->getFieldsFromQueueEx("SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, count(SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 )) as njobs","where status='RUNNING' or status='SAVING' or status='QUEUED' or status='STARTED' group by user");

    my %userset=();

    $userset{all}=();
    $userset{all}->{user}="all";

    foreach (@$usernames) {
	$self->$method(@data,"======================================================");
#	print "$_->{user}, $_->{njobs} waiting\n";
	$self->{PRIORITY_DB}->checkPriorityValue($_->{user});
	my $priorityset=$self->{PRIORITY_DB}->getFieldsFromPriority("$_->{user}","*");
	my $pkeys;
	foreach $pkeys ( keys %$priorityset ) {
#	    printf "Set=> %-16s = %-16s |$pkeys|\n", $pkeys, $priorityset->{$pkeys};
	    $userset{$_->{user}}{$pkeys} = $priorityset->{$pkeys};
	    if ($pkeys ne "user" ) {
		$userset{all}{$pkeys}+= $priorityset->{$pkeys};
	    }
	    $self->$method(@data,"Setting userset $_->{user} key $pkeys to $priorityset->{$pkeys}");
	}
	$userset{$_->{user}}{waiting} = $_->{njobs};
	$userset{all}{waiting}+= $_->{njobs};
    }


    my $allset={};
    $allset->{user}="all";
#    push @$usernames,$allset;

    foreach (@$usernames) {
	$userset{$_->{user}}{running} = 0;
    }
	
    foreach (@$runvalues) {
#	print "$_->{user}, $_->{njobs} running\n";
	$userset{$_->{user}}{running} = $_->{njobs};
    }

    ############################################################################
    # here we have a nice hashref with all the information per user
    ############################################################################

    my $userref=\%userset;
    
    my $vset={};
    $vset->{waiting} = 0;
    $vset->{running} = 0;
    $vset->{userload} = 0;
    $vset->{computedpriority} = 0;
    $self->{PRIORITY_DB}->updatePriority($vset,"1")
	or print STDERR "Error updating the computed priority value for all\n";

    foreach (@$usernames) {
	my $pkeys;
	my $lset;
	my $priorityoffset = 0;

	$lset = $userref->{$_->{user}};

	if ($lset->{priority} > 1) {
	    $self->$method(@data,"You should set the user priority for $_->{user} between 0 <= priority < 1.0 => forcing to 1.0!");
	    $lset->{priority} = 1.0;
	}

	if ($lset->{priority} < 0) {
            $self->$method(@data,"You should set the user priority for $_->{user} between 0 <= priority < 1.0 => forcing to 0!");
            $lset->{priority} = 0;
	    $priorityoffset = -1;
        }

	$self->$method(@data,"======================================================");
	$self->$method(@data,"User is $_->{user}");
	foreach $pkeys ( keys %$lset ) {
	    my $log = sprintf "Set=> %-16s = %-16s |$pkeys|", $pkeys, $lset->{$pkeys};
	    $self->$method(@data,"$log");
	}

	####################################################################
	# user load value is 1.0 if a user run's his maximum number of jobs,
	# 0, if he does not run any job
	my $userload;
	if ($lset->{nominalparallelJobs}>0) {
	    $userload = (1.0 * ($lset->{running})/$lset->{nominalparallelJobs});
	    $self->$method(@data,"Userload [$_->{user}]\t: $userload");
	}
	
	my $computedpriority = 100.0*(1 - $userload) * $lset->{priority} + $priorityoffset;
	$computedpriority>0 or $computedpriority=0;
	my $pset={};
	$pset->{waiting} = $userset{$_->{user}}{waiting};
	$pset->{running} = $lset->{running};
	$pset->{userload} = $userload;
	$pset->{computedpriority} = int($computedpriority);
	$self->{PRIORITY_DB}->updatePriority($pset,"user like '$_->{user}'")
	    or print STDERR "Error updating the computed priority value for $_->{user}\n";

	$self->$method(@data, "------------------------------------------------------");

	# count the jobs id's found in status waiting
	my $found_waiting=0;
	my $ljob;
	my @user_queueIds;

	foreach $ljob (@$userjobs) {
	    if ($_->{user} eq $ljob->{user}){
		$found_waiting++;
		push @user_queueIds,$ljob->{queueId};
	    }
	}
	
	if ($found_waiting < $lset->{waiting}) {
	    $lset->{waiting} = $found_waiting;
	}
	my $set = {};
	$set->{priority} = $computedpriority;
	$self->{DB}->lock($self->{DB}->{QUEUETABLE});
	$self->$method(@data,"Job: @user_queueIds   \t Priority: $computedpriority");
	$self->{DB}->updateJobs($set,@user_queueIds) or
	  $self->info("Error updating the jobs");
	$self->{DB}->unlock($self->{DB}->{QUEUETABLE});
    }		   
    
    
    # Set effective priority which is 'priority'*'price'
        my $set   = {};
        my $where;
        my $options = {};

	   $set->{effectivePriority} = "priority * price";                                          
	   $where = "status='WAITING'";
           $options->{noquotes} = "1";           

	$self->{DB}->lock($self->{DB}->{QUEUETABLE});

        	if ($self->{DB}->updateQueue($set, $where, $options )){
	 	$self->info("effectivePriority has been set for all waiting jobs");	
		}
		else {
		$self->info("Error setting effectivePriority"); 	 
		}

        $self->{DB}->unlock($self->{DB}->{QUEUETABLE});
    
}



1
