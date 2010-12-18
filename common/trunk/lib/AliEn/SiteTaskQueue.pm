package AliEn::SiteTaskQueue;

#use AliEn::Catalogue;
use strict;

use vars qw(@ISA);

@ISA = ('AliEn::Logger::LogObject', @ISA);

sub new {
  my $proto   = shift;
  my $class   = ref($proto) || $proto;
  my $self    = {};
  my $options = shift;
  bless( $self, $class );
  if((defined $options->{user}) and !(defined $options->{role})) {
    $options->{role} = $options->{user};
  }
  $options->{DEBUG}  = $self->{DEBUG}  = ( $options->{debug}  or 0 );
  $options->{SILENT} = $self->{SILENT} = ( $options->{silent} or 0 );
  $self->{LOGGER} = new AliEn::Logger;
  $self->{CONFIG} = new AliEn::Config($options);
  $self->{ROLE}=$options->{role} || $options->{ROLE} || $self->{CONFIG}->{ROLE}; 
  $self->{SOAP} = new AliEn::SOAP
    or print "Error creating AliEn::SOAP $! $?" and return;

  $self->{SILENT} = $options->{silent} || 0;

  return $self;
}

sub callBroker {
  my $self = shift;

  $self->{LOGGER}->getDebugLevel() and push @_, "-debug=".$self->{LOGGER}->getDebugLevel();
  return $self->{SOAP}->CallAndGetOverSOAP("Broker/Job", "invoke", @_);
}

return 1;
__END__
