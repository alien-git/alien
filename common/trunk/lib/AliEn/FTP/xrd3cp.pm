package AliEn::FTP::xrd3cp;

use strict;
use vars qw(@ISA);

use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

sub initialize {
  my $self=shift;
  $self->info("HEllo, creating a new xrd3cp package");

  if (!open(FILE, "which xrd3cp 2>&1 |")){
    $self->info("Error: xrd3cp is not in the path");
    return;
  }
  my @info=<FILE>;
  if (! close FILE){
    $self->info("Error: xrd3cp is not in the path (closing) @info and $?");
    return;
  };
  return $self;
}

sub copy {
  my $self=shift;
  my $source=shift;
  my $target=shift;
  $self->info("Ready to copy $source into $target");
  use Data::Dumper;
  print Dumper($source);
  print Dumper($target);

  
  return;
}

1;
