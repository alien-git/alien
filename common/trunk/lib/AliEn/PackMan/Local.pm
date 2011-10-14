package AliEn::PackMan::Local;

use AliEn::Config;
use strict;
use vars qw(@ISA);
use AliEn::UI::Catalogue::LCM;
use Filesys::DiskFree;
use AliEn::Util;
use AliEn::SOAP;
use Data::Dumper;
push @ISA, 'AliEn::Logger::LogObject', 'AliEn::PackMan';


sub initialize{
  my $self=shift;

  ($self->{INST_DIR}) or 
    $self->{INST_DIR}=($self->{CONFIG}->{PACKMAN_INSTALLDIR} || "$ENV{ALIEN_HOME}/packages");
  
  $self->info("Using $self->{INST_DIR} as the installation directory");
  while ($self->{INST_DIR} =~  s/\$([^\/]*)/$ENV{$1}/ ) {
    $self->debug(1, "The installdir contains \$$1. Let's replace it with $ENV{$1}");
    $ENV{$1} or $self->info("Error: the environment variable $1 is not defined. Using the home directory to install the packages") and $self->{INST_DIR}= "$ENV{ALIEN_HOME}/packages";
  }
  if (! -d $self->{INST_DIR}) {
    $self->info( "$$ Creating the directory $self->{INST_DIR}");
    require  AliEn::MSS::file;;
    AliEn::MSS::file::mkdir($self, $self->{INST_DIR}) and return;
  }
  $self->info("CREATING THE CATALOGUE");
  $self->{CATALOGUE}=AliEn::UI::Catalogue::LCM->new({no_catalog=>1})  or return;
  $self->SUPER::initialize() or return;
  $self->{REALLY_INST_DIR} or $self->{REALLY_INST_DIR}=$self->{INST_DIR};

  $self->createListFiles() or
    $self->info("WARNING! We couldn't create the list of packages")
      and return

  return 1;
}

return 1;
