package AliEn::MSS::hsi;
use strict;

use AliEn::MSS;

use vars qw(@ISA);
@ISA = ( "AliEn::MSS" );

sub initialize {
  my $self=shift;
  $self->{FTP_LOCALCOPIES}=1;
  return 1;
}

sub mkdir {
    my $self = shift;
    my (@args) = @_;

    my @cmd = ( "hsi", "-q", "mkdir", "-p", @args );
    return ( system(@cmd) );
}

sub get {
    my $self = shift;
    my ($from, $to)=@_;

    open (OUTPUT, "which hsi >& /dev/null|");
    my $done=close(OUTPUT);
    
    $done or return 1;


    $self->debug(1, "Doing get $to : $from");
    return system( "hsi","-q", "get", "$to : $from ");
}


sub put {
    my $self = shift;
    my ( $from, $to ) = @_;
    $self->debug(1, "Doing put $from : $to");
    my @cmd = ( "hsi", "-q", "put", "$from : $to" );
    return ( system(@cmd) );
}

sub mv {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "hsi", "-q", "rename", @args );
    return ( system(@cmd) );
}

sub rm {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "hsi", "-q", "rm", @args );
    return ( system(@cmd) );
}

sub lslist {
  my $self=shift;
  my @fileInSE;
  return \@fileInSE;
}

sub sizeof {
  my $self=shift;
  return 1;
  }

sub url {
    my $self = shift;
    my $file = shift;

    return "hsi://$self->{HOST}$file";
}

return 1;
