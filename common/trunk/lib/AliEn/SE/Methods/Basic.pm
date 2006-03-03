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
    return ( $self->getLink(@_) );
}
return 1;

