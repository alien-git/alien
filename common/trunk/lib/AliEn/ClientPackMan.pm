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
  $self->info("WE HAVE A CLIENTPACKMAN INSTANCE");
  return $self;
}
sub registerPackageInDB{
  my $self=shift;
  $self->info("THIS SHOULD BE DONE OVER AUTHEN");
  return $self->callOverSOAP( 'registerPackageInDB', @_);
}

sub getListPackagesFromDB {
  my $self=shift;
  my @l=$self->callOverSOAP( 'getListPackagesFromDB', @_);
  @l or return;  
  return $l[0];
}
sub recomputePackages {
  my $self=shift;
  $self->info("Ready to call the packman");
  return $self->callOverSOAP( "recomputePackages",@_);
}
sub findPackageLFNInternal{
  my $self=shift;
  my @s=$self->callOverSOAP("findPackageLFNInternal", @_);
  @s or return;
  #This one is tricky. An empty hash over soap gets converted into an empty string. Here we change it back.
  $s[1] or $s[1]={};
  return @s;
}
### BASIC COMMANDS ###
sub callOverSOAP {
  my $self = shift;
  my $user = $self->{ROLE};
  if ($ENV{ALIEN_PROC_ID} and $ENV{ALIEN_JOB_TOKEN}) {
    $user = "alienid:$ENV{ALIEN_PROC_ID} $ENV{ALIEN_JOB_TOKEN}";
  }

  if ($_[0] =~ /^-user=([\w]+)$/) {
    shift;
    $user = $1;
  }
  $self->info("Asking the server");
  $self->{LOGGER}->getDebugLevel() and push @_, "-debug=" . $self->{LOGGER}->getDebugLevel();
  return $self->{SOAP}->CallAndGetOverSOAP($self->{SILENT}, "Authen", "doPackMan", $user,  @_);
 }





return 1;
__END__
