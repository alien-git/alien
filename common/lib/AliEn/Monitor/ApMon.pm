package AliEn::Monitor::ApMon;

use strict;

#use ApMon::ApMon;

sub new {
    my ($class, @params) = @_;
    return ApMon::ApMon->new(@params);
}

1;
