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


    $self->$method(@data, "The priority optimizer finished");

    return;
}



1
