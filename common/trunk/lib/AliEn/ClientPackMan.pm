package AliEn::ClientPackMan;

use AliEn::PackMan;
use AliEn::ClientCatalogue;
use AliEn::Config;
use strict;
use AliEn::Util;
use vars qw(@ISA);

@ISA = ('AliEn::PackMan',  @ISA);

sub initialize {
  my $self=shift;
  $self->{RPC}=AliEn::RPC->new() or return;
  return $self;
}
sub registerPackageInDB{
  my $self=shift;
#  $self->info("THIS SHOULD BE DONE OVER AUTHEN");
  return $self->callOverRPC( 'registerPackageInDB', @_);
}
sub deletePackageFromDB {
 my $self = shift;
 return $self->callOverRPC( 'deletePackageFromDB', @_);

}
sub getListPackagesFromDB {
  my $self=shift;
  my @l=$self->callOverRPC( 'getListPackagesFromDB', @_);
  @l or return;  
  return $l[0];
}
# sub recomputePackages {
#  my $self=shift;
#  $self->info("Ready to call the packman");
#  return $self->callOverRPC( "recomputePackages",@_);
# }

sub findPackageLFNInternal{
  my $self=shift;
  my @s=$self->callOverRPC("findPackageLFNInternal", @_);
  @s or return;
  #This one is tricky. An empty hash over soap gets converted into an empty string. Here we change it back.
  $s[1] or $s[1]={};
  return @s;
}
sub callOverRPC {
  my $self = shift;
  my $user = $self->{ROLE};
  if ($ENV{ALIEN_PROC_ID} and $ENV{ALIEN_JOB_TOKEN}) {
    $user = "alienid:$ENV{ALIEN_PROC_ID} $ENV{ALIEN_JOB_TOKEN}";
  }

  if ($_[0] =~ /^-user=([\w]+)$/) {
    shift;
    $user = $1;
  }
  $self->{LOGGER}->getDebugLevel() and push @_, "-debug=" . $self->{LOGGER}->getDebugLevel();
  return $self->{RPC}->CallRPCAndDisplay($self->{SILENT}, "Authen", "doPackMan", $user,  @_);
 }

return 1;
__END__
