package AliEn::Service::JobAgent::Local;


use strict;

use vars qw(@ISA);

push @ISA, 'AliEn::Service::JobAgent';

use AliEn::Service::JobAgent;

sub initialize {
  my $self=shift;
  my $options=shift || {};
  $self->info("HELLO WE ARE IN A LOCAL JOBAGENT");
  $ENV{ALIEN_CM_AS_LDAP_PROXY}="NO_HOST:NO_PORT";
  $ENV{ALIEN_PROC_ID}=$self->{QUEUEID}=$$;
  $self->{PORT}=1999;
  $self->SUPER::initialize($options) or return;
  $self->info("And putting our options");

  $self->{CA}=$options->{CA};
  $self->{CA} or $self->info("Error: the job classad was not specified while creating a local jobagent!") and return;
  
  $self->{JOB_USER}=$self->{CONFIG}->{ROLE};
  return $self;
}

sub changeStatus {
  my $self=shift;
  my @print=@_;
  map { defined $_ or $_='undef'} @print;
  $self->info("Status changed to @print (and we don't notify anybody)");

  return 1;
}

sub getBatchId {
  my $self=shift;
  $self->info("The batch id is our current pid: $$");
  return $self->{QUEUEID};
}

sub putJobLog{
  my $self=shift;
  $self->info("We don't put in the joblog: @_");
}

return 1;
