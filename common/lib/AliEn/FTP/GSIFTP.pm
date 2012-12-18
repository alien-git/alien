package SE::FTD::GSIFTP;

use strict;

sub new {
    my $proto   = shift;
    my $options = shift;
    my $class   = ref($proto) || $proto;
    my $self    = {};
    bless( $self, $class );
    $self->{DESTHOST} = $options->{HOST};

    $self->{CONFIG} = new Config::Config();

    return $self;
}

sub transfer {
    my $self = shift;
    my $file = shift;
    my $rdir = shift;

    my $error = system("gsincftpput $self->{DESTHOST} $rdir $file");
    $error = $error / 256;
    return $error;
}
return 1;
