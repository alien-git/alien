package AliEn::URL;

use strict;

sub new {
  my $proto = shift;
  my $URL   = shift;
  my $self  = {};
  bless($self, (ref($proto) || $proto));
  $URL =~ /([a-zA-Z0-9]*):\/\/([:a-zA-Z0-9\._-]*)\/(.*)/;
  $self->{METHOD} = $1;
  $self->{HOST}   = $2;
  $self->{PATH}   = "/$3";

  if (!($self->{METHOD}) && !($self->{HOST}) && !($self->{PATH})) {
    print STDERR "Error: Malformed URL exception";
  }

  ($self->{HOST}, $self->{PORT}) = split(":", $self->{HOST});
  ($self->{PORT}) or $self->{PORT} = 9001;
  $self->{URL} = $URL;

  #Split into file and directory
  $self->{PATH} =~ /(.*)\/(.*)/;
  $self->{DIR}  = $1;
  $self->{FILE} = $2;

  return $self;
}

sub recreateURL {
  my $self = shift;
  $self->{URL} = sprintf "%s://%s:%s%s", $self->{METHOD}, $self->{HOST}, $self->{PORT}, $self->{PATH};
  return $self->{URL};
}

sub getURL {
  my $self = shift;
  return $self->{URL};
}
return 1;
