package AliEn::SE::Methods::gridftp;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA );
@ISA = ("AliEn::SE::Methods::Basic");

use strict;

sub initialize {
    my $self = shift;
}
sub get {
  my $self=shift;
  $self->{LOGGER}->info("GRIDFTP", "In gridftp, getting the file $self->{PARSED}->{ORIG_PFN}");
  my $remoteSubject=$self->{PARSED}->{VARS_SUBJECT};
  $remoteSubject =~ s{//}{=}g;
  $remoteSubject =~ s{/CN=proxy}{}g;

  my $command="globus-url-copy -p 5 -tcp-bs 5000 -ss \"$remoteSubject\" gsiftp://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}$self->{PARSED}->{PATH} gsiftp://$self->{CONFIG}->{HOST}$self->{LOCALFILE}";


  $self->{LOGGER}->info("GRIDFTP", "Doing $command");
  my $error = system($command);

  $error and return;
  return $self->{LOCALFILE};
}
sub getSize {
  my $self=shift;
  $self->{LOGGER}->info("GRIDFTP", "Asking for the size");
  return;
}

sub put {
  my $self=shift;
  $self->{LOGGER}->info("GRIDFTP", "In gridftp, putting the file $self->{PARSED}->{ORIG_PFN}");
  my $remoteSubject= ($self->{PARSED}->{VARS_SUBJECT} || "");
  $remoteSubject =~ s{//}{=}g;
  $remoteSubject =~ s{/CN=proxy}{}g;

  my $command="globus-url-copy -p 5 -tcp-bs 5000 -ds \"$remoteSubject\" $self->{LOCALFILE} gsiftp://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}$self->{PARSED}->{PATH}";

  $self->{LOGGER}->info("GRIDFTP", "Doing $command");
  my $error = system($command);

  $error and return;
  return $self->{LOCALFILE};
}
return 1;
__END__

=head1 NAME

AliEn::SE::Methods::gridftp

=head1 DESCRIPTION

AliEn::SE::Methods::gridftp module extends AliEn::SE::Methods::Basic 
