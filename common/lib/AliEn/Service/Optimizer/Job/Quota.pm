package AliEn::Service::Optimizer::Job::Quota;

use strict;

use AliEn::Service::Optimizer::Job;
use Data::Dumper;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self = shift;
  my $silent = shift;
  $self->{SLEEP_PERIOD} = 600; #60
  
  $self->{NOT_FIRST} 
    or sleep(20) and $self->{NOT_FIRST}=1;

  my $method="info";
  my @data;
  $silent and $method="debug" and push @data, 1;
 
  $self->info(@data, "The Job/Quota optimizer starts");
  $self->{CATALOGUE}->execute("calculateJobQuota"); #, $silent);
  $self->info(@data, "The Job/Quota optimizer finished");

  return;
} 

1;
