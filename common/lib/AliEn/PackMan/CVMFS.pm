package AliEn::PackMan::CVMFS;

use AliEn::Config;
use strict;
use vars qw(@ISA);
use AliEn::UI::Catalogue::LCM;
use AliEn::Util;
use Data::Dumper;

push @ISA, 'AliEn::Logger::LogObject', 'AliEn::PackMan';

sub initialize{
  my $self=shift;

  $self->SUPER::initialize() or return;

  $self->info("In  AliEn::PackMan::CVMFS");

  return 1;
}

sub removeLocks{
    my $self=shift;
    $self->info("No locks needed for CVMFS");
}

sub getListInstalledPackages {
  my $self = shift;
  my ($status, @packages) = $self->getListPackages();
  grep (/^-s(ilent)?$/, @_)
    or $self->printPackages({input => \@_, text => " installed"}, @packages);
  return ($status, @packages);
}

sub getListPackages {
  my $self = shift;

  $self->info("AliEn::PackMan::CVMFS - getListPAckages");

  my @packages;

  foreach my $pkg (`alienv q --packman`) {
      chomp $pkg;
      push @packages, $pkg;
  }

  grep (/^-s(ilent)?$/, @_)
    or $self->printPackages({input => \@_}, @packages);
  return (1,@packages);
}

sub readPackagesFromFile {
    my $self = shift;
    my $file = shift;
    $self->info("AliEn::PackMan::CVMFS - Ignore readPackagesFromFile");
}


sub installPackage {
  my $self    = shift;
  my $user    = shift;
  my $package = shift;
  my $version = shift;

  my $args = $package;
  
  if (defined($version)) {
      $args .= "/$version";
  }

  my $source = `alienv printenv $args`;

  $source =~ s/(.*)=(.*)/$1=\"$2\"/g;
 
  return (1, "$source");
} 

1;
