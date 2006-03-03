package AliEn::MSS::dmf;

@ISA = qw( AliEn::MSS );

use AliEn::MSS;

use strict;

sub mkdir {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "mkdir", "-p", @args );
    return ( system(@cmd) );
}

sub cp {
    my $self = shift;
    my ( $from,  $to ) = @_;
    my @cmd = ("dmf", "-c", "$from $to");
    return (system(@cmd));
}

sub mv {
    my $self = shift;
    my ( $from, $to ) = @_;
    my @cmd = ("dmf", "-m", "$from $to");
    return (system(@cmd));
}

sub rm {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "rm", "-f", @args );
    return ( system(@cmd) );
}

sub url {
    my $self = shift;
    my $file = shift;

    return "dmf://$self->{HOST}$file";
}

sub lslist {
  my $self=shift;
  my @fileInSE;
  return \@fileInSE;
}

sub sizeof {
    my $self = shift;
    my $file=shift;

    my $size = (-s "$file");
    return $size;
}

return 1;
