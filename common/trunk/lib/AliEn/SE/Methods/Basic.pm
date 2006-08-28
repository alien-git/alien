package AliEn::SE::Methods::Basic;

use AliEn::SE::Methods;

use strict;

sub getLink {
    my $self = shift;
    print "There is no getLink for $self\n";
    return $self->get(@_);

}

sub getFTPCopy {
  my $self = shift;
  $self->{LOCALCOPY} or return $self->path(); 
  $self->info("This method requires a cache copy of the file");
  return ( $self->getLink(@_) );
}
return 1;

