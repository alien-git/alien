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
  $self->info("The packman returned $done ,and $error");
  return ($done, $error);
}


sub recomputeListPackages {
  shift;
  my $done=$self->{PACKMAN}->recomputeListPackages(@_);
  return $done;
}

=item C<getListPackages()>

Returns a list of all the packages defined in the system

=cut


sub getListPackages{
  shift;

  $self->info( "$$ Giving back all the packages defined (options @_)");

  grep (/^-?-force$/, @_)
    and  AliEn::Util::deleteCache($self);

  my $platform=AliEn::Util::getPlatform($self);

  if(  grep (/^-?-all$/, @_)) {
    $self->info("Returning the info of all platforms");
    $platform="all";
  }

  my $cache=AliEn::Util::returnCacheValue($self, "listPackages-$platform");
  if ($cache) {
    $self->info( "$$ $$ Returning the value from the cache (@$cache)");
    return (1, @$cache);
  }

  my ($status, @packages)=$self->{PACKMAN}->getListPackages($platform, @_);

  $self->info( "$$ $$ RETURNING @packages");
  AliEn::Util::setCacheValue($self, "listPackages-$platform", \@packages);

  return ($status,@packages);
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

=item C<testPackage($user,$package,$version)>

Checks if a package is installed, and the environment that it would produce.
It returns: $version=> $version of the package installed
            $info  => dependencies and information of the package
            $list  => The directory where the package is installed
            $environment => environment that will result after configuring the package

=cut

sub testPackage{
  shift;
  $self->info( "$$ Checking if the package is installed: @_");
  my @all=$self->{PACKMAN}->testPackage(@_);
  $self->info("The PackMan returns @all");
  return @all;

}

=item C<installPackage($user,$package,$version,[$dependencies])>

 This method is going to install a package

=cut

sub installPackage{
  shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;

  $self->info( "$$ Checking package $package for $user and $version");
  my $cacheName="package_${package}_${version}_${user}";
  my $cache=AliEn::Util::returnCacheValue($self, $cacheName);
  if ($cache) {
    $self->info( "$$ Returning the value from the cache (@$cache)");
    return (@$cache);
  }
  my ($done,@rest )=$self->{PACKMAN}->isPackageInstalled($user, $package, $version);
  my $exit=0;
  if (! $done){
    if (-f "$self->{PACKMAN}->{INST_DIR}/$user.$package.$version.InstallLock"){
      $self->info("Someone is already installing the package");
      return (-1, "Package is being installed");
    }
    $self->info("The package is not installed. Forking and installing it");
    fork() and return (-1, "Package is being installed");
    $exit=1;
  }

  my ($done, $psource, $dir)=$self->{PACKMAN}->installPackage($user, $package, $version);


  my @list= ($done, $psource, $dir);
  AliEn::Util::setCacheValue($self, $cacheName, \@list);
  $self->info("The PackMan service returns @list (and we exit $exit)");
  $exit and    exit(0);

  return ($done, $psource, $dir);
}


=item C<getInstallLog($user,$package, $version,)>

Gets the installation log of the package

=cut

sub getInstallLog{
  shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $options=shift;
  $self->info( "$$ Getting the installation log of $package, $user and $version");
  my ($lfn, $info)=$self->{PACKMAN}->findPackageLFN($user, $package, $version);

  $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
    and ($version)=($1);
  my $logFile="$self->{INST_DIR}/$user.$package.$version.InstallLog";

  if ($options){
    $logFile= "$self->{CONFIG}->{LOG_DIR}/packman/$package.$version.$options.$self->{CONFIG}->{HOST}";
    $self->info("$$ Getting the log with option $options and $logFile");

  }

  open (FILE, "<$logFile" ) or die ("Error opening $logFile\n");
  my @content=<FILE>;
  close FILE;
  $self->{LOGGER}->info( "$$ $$ Returning the file");
  return join("", @content);
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
  $self->{PACKMAN}=AliEn::PackMan->new({PACKMAN_METHOD=>"Local", 
					SOAP_SERVER=>"PackManMaster"}) or return;

  $self->{PACKMAN}->removeLocks();
  $self->{INST_DIR}=$self->{PACKMAN}->{INST_DIR};

  return $self;

}


#sub findPackageLFN{
#  my $self=shift;
#  my $user=shift;
#  my $package=shift;
#  my $version=shift;
  
#  my $platform=AliEn::Util::getPlatform($self);
#  $self->info("$$ Looking for the lfn of $package ($version) for the user $user");

#  my $result=$self->{SOAP}->CallSOAP("PackManMaster", "findPackageLFN", $user, $package, $version, $platform)
#    or $self->info("Error talking to the PackManMaster") and return;

#  my @info=$self->{SOAP}->GetOutput($result);
#  if (  $info[0]=-2){
#    my $message="The package $package (v $version) does not exist for $platform \n";
#    $self->info($message);
#    die $message;
#  }
#  use Data::Dumper;
#  print Dumper(@info);
#  return @info;
#}


sub getDependencies {
  my $this=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;


  my $cacheName="dep_package_${package}_${version}_${user}";

  my $cache=AliEn::Util::returnCacheValue($self, $cacheName);
  if ($cache) {
    $self->info( "$$ Returning the value from the cache $cacheName (@$cache)");
    return (@$cache);
  }

  my ($lfn, $info)=$self->{PACKMAN}->findPackageLFN($user, $package, $version);
  
  AliEn::Util::setCacheValue($self, $cacheName, [1,$info]);
  $self->info("Giving back the dependencies of $package");

  return (1, $info);
}

return 1;


