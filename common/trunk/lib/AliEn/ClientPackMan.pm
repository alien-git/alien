package AliEn::ClientPackMan;

use AliEn::PackMan;
use AliEn::ClientCatalogue;
use AliEn::Config;
use strict;
use AliEn::Util;
use vars qw(@ISA);

@ISA = ('AliEn::Logger::LogObject','AliEn::PackMan',  @ISA);

sub new {
  my $proto   = shift;
  my $class   = ref($proto) || $proto;
  my $self    = {};
  my $options = shift;
  bless($self, $class);

  if ((defined $options->{user}) and !(defined $options->{role})) {
    $options->{role} = $options->{user};
  }

  $self->{CATALOGUE} = $options->{CATALOGUE} or return ;
  $options->{DEBUG}  = $self->{DEBUG}  = ($options->{debug}  or 0);
  $options->{SILENT} = $self->{SILENT} = ($options->{silent} or 0);
  $self->{LOGGER} or $self->{LOGGER} = $options->{LOGGER} || new AliEn::Logger;

  $self->{CONFIG} or $self->{CONFIG} = new AliEn::Config() or return;

  $self->{SOAP} = new AliEn::SOAP or print "Error creating AliEn::SOAP $! $?" and return;

  $self->{ROLE} = $options->{role} || $options->{ROLE} || $self->{CONFIG}->{ROLE};

  $self->{INSTALLDIR}=$self->{CONFIG}->{PACKMAN_INSTALLDIR} || "$ENV{ALIEN_HOME}/packages";
  -d $self->{INSTALLDIR} or mkdir $self->{INSTALLDIR};
  if (not -d $self->{INSTALLDIR}) {
    $self->{INSTALLDIR}="$ENV{ALIEN_HOME}/packages";
    -d $self->{INSTALLDIR} or mkdir $self->{INSTALLDIR};
    -d $self->{INSTALLDIR} or return;
  }
  $self->{LIST_FILE_TTL} or $self->{LIST_FILE_TTL} = 7200;
  $self->{REALLY_INST_DIR} or $self->{REALLY_INST_DIR}=$self->{INSTALLDIR};
  $self->info("WE HAVE A CLIENTPACKMAN INSTANCE");
  return $self;
}
sub registerPackageInDB{
  my $self=shift;
  $self->info("THIS SHOULD BE DONE OVER AUTHEN");
  return $self->{CATALOGUE}->{CATALOG}->callAuthen("packman", 'registerPackageInDB', @_);
}

sub getListPackagesFromDB {
  my $self=shift;
  $self->info("WE HAVE TO ASK THE SERVER");
  my @d=$self->{CATALOGUE}->{CATALOG}->callAuthen("packman", 'getListPackagesFromDB', @_);
  return $d[0];
}
sub recomputePackages {
  my $self=shift;
  $self->info("Ready to call the packman");
  return $self->{CATALOGUE}->{CATALOG}->callAuthen("packman", "recompute",@_);
}
sub findPackageLFN{
  my $self=shift;
  $self->info("FINDING THE LFN IN THE SERVER");
  my @s=$self->{CATALOGUE}->{CATALOG}->callAuthen("packman", "findPackageLFN", @_);
  print Dumper($s[0]);
  print "THAT' ALL\n";
  use Data::Dumper;
  return $s[0];
}
### BASIC COMMANDS ###



return 1;
__END__
