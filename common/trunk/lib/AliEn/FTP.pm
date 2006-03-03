package AliEn::FTP;

use strict;
use AliEn::Logger::LogObject;

use vars qw(@ISA);

push @ISA, 'AliEn::Logger::LogObject';
sub new {
  my ($this) = shift;
  my $class = ref($this) || $this;
  my $options = shift;
  my $self = (shift or {}) ;

  bless $self, $class;
  $self->{DESTHOST} = $options->{HOST};

  $self->SUPER::new() or return;

  $self->initialize($options) or return;
  return $self;
}

sub initialize{
  return 1;
}

sub getURL{
  my $self=shift;
  my $pfn=shift;
  $self->info("In the FTP method, returning directly $pfn");
  return $pfn;
}

1;
