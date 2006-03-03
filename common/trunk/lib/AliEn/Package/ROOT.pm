package AliEn::Package::ROOT;

use AliEn::Package;
use strict;
use vars qw(@ISA);
@ISA = qw(AliEn::Package);

sub Initialize {
    my $self = shift;

    #    $self->{provide}->{"package"} = "ROOT";
    #    return $self;
    return $self;
}

sub Require {

}

sub Configure {
    my $self = shift;

    my $version = $self->{version};
    $ENV{ROOTSYS} = $self->{path};

    $ENV{PATH} = "$ENV{ROOTSYS}/bin:$ENV{PATH}";

    ( $ENV{LD_LIBRARY_PATH} ) or $ENV{LD_LIBRARY_PATH} = "";

    $ENV{LD_LIBRARY_PATH} = "$ENV{ROOTSYS}/lib:$ENV{LD_LIBRARY_PATH}";
    print STDOUT "Setting Package AliEn ROOT $version\n";
    print STDOUT "PATH -> $ENV{PATH}\n";
    print STDOUT "LD_LIBRARY_PATH -> $ENV{LD_LIBRARY_PATH}\n";
    print STDOUT "ROOTSYS -> $ENV{ROOTSYS}\n\n";

}

return 1;
