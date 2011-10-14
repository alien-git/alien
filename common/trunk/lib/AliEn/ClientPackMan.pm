package AliEn::ClientPackMan;

use AliEn::PackMan;
use AliEn::ClientCatalogue;
use AliEn::Config;
use strict;
use vars qw(@ISA);

@ISA = ('AliEn::Logger::LogObject', @ISA);

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

  return $self;
}

sub f_packman_HELP{
  return AliEn::PackMan::f_packman_HELP(@_);
}

sub f_packman {
  my $self = shift;
  my @arg = @_;
  my $operation = shift @arg;
  $operation or $self->info("The operation is not specified!") and return;

  if ($operation =~ /^l(ist)?$/) {
     return $self->getListPackages(@_);
  }
  elsif ($operation =~ /^listI(nstalled)?$/) {
     return $self->getListInstalledPackages($self, @_);
  }
  elsif ($operation =~ /^i(nstall)?$/) {
     return $self->installPackage(@_);
  }
  elsif ($operation =~ /^t(est)?$/) {
     return $self->testPackage(@_);
  }
 # elsif ($operation =~ /^d(efine)?$/) {
 #   return $self->definePackage($self, @_);
 # }
 # elsif ($operation =~ /^u(ndefine)?$/) {
 #   return  $self->undefinePackage(@_);
 # }
  elsif ($operation =~ /^r(emove|m)?$/) {
    return $self->removePackage( @_);
  }
  elsif ($operation =~ /^synchronize$/) {
    return $self->synchronizePackages($self, @_);
  }
  elsif ($operation =~ /^dependencies$/) {
    return $self->getDependencies( @_);
  }
  elsif ($operation =~ /^installLog?$/) {
    return $self->getInstallLog(@_);
  }
#  elsif ($operation =~ /^recompute?$/) {
#    $self->info("And deleting any local caches");
#    my $dir = ($self->{CONFIG}->{PACKMAN_INSTALLDIR} || '$ALIEN_HOME/packages');
#    system("rm -f $dir/alien_list_*");
#    return $self->recomputeListPackages($self, @_);
#    return $self->recomputeListPackages( @_);
#  }
  else {
   return AliEn::PackMan::f_packman($self, @_);
  }
return 1;
}

### BASIC COMMANDS ###

sub getListPackages{
  my $self = shift;
  return $self->{CATALOGUE}->{CATALOG}->callAuthen("packman", @_);
}

sub installPackage{
  my $self = shift;
  return $self->{CATALOGUE}->{CATALOG}->callAuthen("packman", @_);
}

sub getDependencies {
  my $self = shift;
  return $self->{CATALOGUE}->{CATALOG}->callAuthen("packman", @_);
}

sub getInstallLog {
  my $self = shift;
  return $self->{CATALOGUE}->{CATALOG}->callAuthen("packman", @_);
}

sub testPackage {
  my $self = shift;
  return $self->{CATALOGUE}->{CATALOG}->callAuthen("packman", @_);
}


sub definePackage{
  return AliEn::PackMan::definePackage(@_); 
}

sub undefinePackage{
  return AliEn::PackMan::undefinePackage(@_);
}

sub synchronizePackages{
  return AliEn::PackMan::synchronizePackages(@_);
}

sub removePackage{
  my $self = shift;
  return $self->{CATALOGUE}->{CATALOG}->callAuthen("packman", @_);
# return AliEn::PackMan::removePackage(@_);
}

sub recomputeListPackages{
  return AliEn::PackMan::recomputeListPackages(@_);
}

#############################

sub printPackages {
  return AliEn::PackMan::printPackages(@_);
}

sub getListInstalled_Internal {
  return  AliEn::PackMan::getListInstalled_Internal(@_);
} 

sub getListInstalledPackages{
  return  AliEn::PackMan::getListInstalledPackages(@_);
} 

sub getListInstalledPackages_ {
  return  AliEn::PackMan::getListInstalledPackages_(@_);
}

sub getSubDir {
  return AliEn::PackMan::getSubDir (@_);
} 

sub readPackagesFromFile {
  return AliEn::PackMan::readPackagesFromFile(@_);
}

sub isPackageInstalled{
  return AliEn::PackMan::isPackageInstalled(@_);
}

sub existsPackage{
  return AliEn::PackMan::existsPackage(@_);
}

sub findPackageLFN{
  return AliEn::PackMan::findPackageLFN(@_);
}

return 1;
__END__
