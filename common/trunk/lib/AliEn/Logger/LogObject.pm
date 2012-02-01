package AliEn::Logger::LogObject;

use AliEn::Logger;
use vars qw (@ISA $DEBUG);
use strict;
use Class::ISA;

$DEBUG=0;

sub new {
  my $self=shift;

  $self->{LOGGER} or 
    $self->{LOGGER}= new AliEn::Logger(@_) 
      or return;
  $self->{LOG_REF}=ref $self;
  foreach my $class (Class::ISA::self_and_super_path($self->{LOG_REF})){
    $self->{LOGGER}->{LOG_OBJECTS}->{$class}=$AliEn::Logger::DEBUG_LEVEL;
  }
  return $self;
}

sub debug{
  my $self=shift;
  my $level=(shift || 0);
  my $message=shift;

  if (not $self->{LOG_REF} ) {
    $self->{LOG_REF}=ref $self;
  }
  $self->{LOGGER}->{LOG_OBJECTS}->{$self->{LOG_REF}} or return 1;

  $level> $self->{LOGGER}->{LOG_OBJECTS}->{$self->{LOG_REF}} and return 1;

  return $self->{LOGGER}->display("debug", $self->{LOG_REF},"$level $self->{LOG_REF} -> $message", @_);
}

sub raw{
  my $self=shift;

  ($self->{LOGGER}->{LEVEL}>1 and $#_<1) and return 1;
  return $self->{LOGGER}->display("raw", $self->{LOG_REF},@_);
}

sub info{
  my $self=shift;

  ($self->{LOGGER}->{LEVEL}>1 and $#_<1) and return 1;
  return $self->{LOGGER}->display("info", $self->{LOG_REF},@_);
}

sub error{
  my $self=shift;

  return $self->{LOGGER}->display("error", $self->{LOG_REF},@_);
}

sub notice{
  my $self=shift;
  return $self->{LOGGER}->display("notice", $self->{LOG_REF},@_);
}



return 1;
