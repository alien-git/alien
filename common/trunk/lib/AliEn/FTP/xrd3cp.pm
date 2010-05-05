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
  my @args=("-m $source->{url} $target->{url}  \"authz=$source->{envelope}\" \"authz=$target->{envelope}\" ");

#  -OS\\\&authz=\"$source->{envelope}\" $target->{url} -OD\\\&authx=\"$target->{envelope}\" ");

  if(system("xrd3cp  @args")){
    $self->info("Error doing the xrd3cp @args",1);
    return;
  }
  $self->info("The transfer worked!!");
  return 1;
}

1;
