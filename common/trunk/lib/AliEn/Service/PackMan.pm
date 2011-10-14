package AliEn::Service::PackMan;

=head1 B<NAME>

AliEn::Service::PackMan

=head1 B<SYNOPSIS>

  my $packman=AliEn::Service::PackMan->new();
  $packman->startListening()

=head1 B<DESCRIPTION>

This is the Service that implements the Package Manager. It inherits from AliEn::Service

The public methods that it includes are:

=over

=cut

use AliEn::Service;
use AliEn::Util;
use AliEn::PackMan;
use Cwd;
use Data::Dumper;
use vars qw(@ISA $DEBUG);

@ISA=qw(AliEn::Service);


$DEBUG=0;

use strict;


# Use this a global reference.

my $self = {};

=item C<removePackage($user,$package,$version)>

If the package was installed, this method removes it from the disk

=cut 

sub removePackage {
  shift;
  my ($done, $error)=$self->{PACKMAN}->removePackage(@_);
  $self->info("PackMan Service === sub remove arguments === @_ ====");
  $self->info("The packman returned $done ,and $error");
  return ($done, $error);
}


sub recomputeListPackages {
  shift;
  my $done=$self->{PACKMAN}->recomputeListPackages(@_);
  return $done;
}

=item C<getListInstalledPackages()>

Returns a list of all the packages installed in the machine
Each entry is in the format "<user>::<package>::<version>"

=cut


sub getListInstalledPackages {
  shift;

  grep (/^-?-force$/, @_)
    and  AliEn::Util::deleteCache($self);

  $self->info( "$$ Giving back the list of packages that have been installed");
  my $cache=AliEn::Util::returnCacheValue($self, "installedPackages");
  if ($cache) {
    $self->info( "$$ $$ Returning the value from the cache (@$cache)");
    return (1, @$cache);
  }

  my ($status, @allPackages)=$self->{PACKMAN}->getListInstalledPackages();
  $self->info( "$$ Returning @allPackages");
  AliEn::Util::setCacheValue($self, "installedPackages", \@allPackages);
  return ($status, @allPackages);
}


#
##
# PRIVATE FUNCTIONS
#
#

sub initialize {
  $self = shift;
  my $options =(shift or {});

  $self->debug(1, "Creatting a PackMan" );

  $self->{PORT}=$self->{CONFIG}->{PACKMAN_PORT} || "9991";
  $self->{HOST}=$self->{CONFIG}->{PACKMAN_HOST} || $self->{CONFIG}->{HOST};
  if ($self->{CONFIG}->{PACKMAN_ADDRESS}){
    $self->{CONFIG}->{PACKMAN_ADDRESS}=~ /^(.*):\/\/([^:]*):(\d+)/ and
      ($self->{HOST},$self->{PORT})=($2,$3);
  }

  $self->{SERVICE}="PackMan";
  $self->{SERVICENAME}=$self->{CONFIG}->{PACKMAN_FULLNAME};
  $self->{LISTEN}=1;
  $self->{PREFORK}=5;

  $self->{CACHE}={};
  #Remove all the possible locks;
  $self->info( "$$ Removing old lock files");
  my $method= $self->{CONFIG}->{PACKMAN_METHOD} || "Local";

  $self->{PACKMAN}=AliEn::PackMan->new({PACKMAN_METHOD=>$method, 
					LIST_FILE_TTL=>600,
					LIST_FILE_CREATION=> 1,
					SOAP_SERVER=>"PackManMaster"}) or return;

  $self->{PACKMAN}->removeLocks();
  $self->{INST_DIR}=$self->{PACKMAN}->{INST_DIR};

  return $self;

}

return 1;


