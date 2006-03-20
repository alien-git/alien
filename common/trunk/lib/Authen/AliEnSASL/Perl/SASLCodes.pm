package Authen::AliEnSASL::Perl::SASLCodes;
use strict;

sub _call {
    my ( $self, $name ) = @_;

    my $cb = $self->{callback}{$name};

    if ( ref($cb) eq 'ARRAY' ) {
        my @args = @$cb;
        $cb = shift @args;
        return $cb->( $self, @args );
    }
    elsif ( ref($cb) eq 'CODE' ) {
        return $cb->($self);
    }

    return $cb;
}

# These codes correspond to the ones defined in sasl.h of the Cyrus sasl package

sub SASL_INTERACT {
    return;
}

sub SASL_CONTINUE {
    return 1;
}

sub SASL_OK {
    return 0;
}

sub SASL_FAIL {
    return -1;
}

sub SASL_NOMEM {
    return -2;
}

sub SASL_BUFOVER {
    return -3;
}

sub SASL_NOMECH {
    return -4;
}

sub SASL_BADPROT {
    return -5;
}

sub SASL_NOTDONE {
    return -6;
}

sub SASL_BADPARAM {
    return -7;
}

sub SASL_TRYAGAIN {
    return -8;
}

sub SASL_BADMAC {
    return -9;
}

sub SASL_BADSERV {
    return -10;
}

sub SASL_WRONGMECH {
    return -11;
}

sub SASL_NEWSECRET {
    return -12;
}

sub SASL_BADAUTH {
    return -13;
}

sub SASL_TOOWEAK {
    return -14;
}

sub SASL_ENCRYPT {
    return -16;
}

sub SASL_TRANS {
    return -17;
}

sub SASL_EXPIRED {
    return -18;
}

sub SASL_DISABLED {
    return -19;
}

sub SASL_NOUSER {
    return -20;
}

sub SASL_PWLOCK {
    return -21;
}

sub SASL_BADVERS {
    return -22;
}

sub SASL_NOPATH {
    return -23;
}
1;
