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
  my $level=shift;
  my $message=shift;
  $level> $self->{LOGGER}->{LOG_OBJECTS}->{$self->{LOG_REF}} and return 1;

  return $self->{LOGGER}->display("debug", $self->{LOG_REF},"$level $self->{LOG_REF} -> $message", @_);
}

sub info{
  my $self=shift;
  $self->{LOGGER}->{LEVEL}>1 and return 1;
  return $self->{LOGGER}->display("info", $self->{LOG_REF},@_);
}

return 1;
