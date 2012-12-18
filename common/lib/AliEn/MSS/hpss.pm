package AliEn::MSS::hpss;

use AliEn::MSS;

use strict;

use vars qw(@ISA);
@ISA = ( "AliEn::MSS" );


sub _System {
    open( OUTPUT, "|@_ > /dev/null 2>&1 " );
    print OUTPUT "y\ny\n";
    close OUTPUT;
    return ($?);
}

sub mkdir {
    my $self = shift;
    my (@args) = @_;

    my @cmd = ( "rfstat", @args );
    my $error = system(@cmd);

    $error or return (0);

    @cmd = ( "rfmkdir", "-p", @args );
    return ( system(@cmd) );
}

sub cp {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "rfcp", @args );
    return ( system(@cmd) );
}

sub mv {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "rfrename", @args );
    return ( system(@cmd) );
}

sub rm {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "rfstat", @args );
    _System(@cmd);
    ($?) and return (0);
    @cmd = ( "rfrm", "-r", @args );
    return ( _System(@cmd) );
}

sub lslist {
  my $self=shift;
  my @fileInSE;
  return \@fileInSE;
}

sub sizeof {
    my $self = shift;
    my $file = shift;

    print "CALLING SIZEOF $file\n";
    my $size = `rfdir $file`;
    $size =~ s/^(\S+\s+){4}(\S+)\s.*$/$2/s;

    return $size;
}

sub url {
    my $self = shift;
    my $file = shift;

    my $host = $self->{HOST};

    $file =~ s/^(.*):// and $host = $1;

    return "hpss://$host$file";
}

return 1;

