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
  my ($done, $error)=$self->{PACKMAN}->removePackage();
  $self->info("The packman returned $done ,and $error");
  return ($done, $error);
}

=item C<getListPackages()>

Returns a list of all the packages defined in the system

=cut

sub getPlatform()
{
    my $sys1 = `uname -s`;
    chomp $sys1;
    $sys1 =~ s/\s//g; #remove spaces
    my $sys2 = `uname -m`;
    chomp $sys2;
    $sys2 =~ s/\s//g; #remove spaces
    my $platform="$sys1-$sys2";

    return $platform;
}


sub getListPackages{
  shift;
  $self->info( "$$ Giving back all the packages defined (options @_)");

  grep (/^-?-force$/, @_)
    and  AliEn::Util::deleteCache($self);

  my $platform=$self->getPlatform();
  my $platformPattern="(($platform)|(source))";
  if(  grep (/^-?-all$/, @_)) {
    $self->info("Returning the info of all platforms");
    $platform="all";
    $platformPattern=".*";
  }

  my $cache=AliEn::Util::returnCacheValue($self, "listPackages-$platform");
  if ($cache) {
    $self->info( "$$ $$ Returning the value from the cache (@$cache)");
    return (1, @$cache);
  }
  my ($status, @packages)=$self->{PACKMAN}->getListPackages(@_);

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

  my ($done, $psource, $dir)=$self->{PACKMAN}->installPackage($user, $package, $version, undef, {fork=>1});
  if (! $done or $done eq "-1"){
    $self->info("Error trying to install the package $psource");
    return (-1, $psource);
  }
  my @list= ($done, $psource, $dir);
  AliEn::Util::setCacheValue($self, $cacheName, \@list);
  $self->info("The PackMan service returns @list");
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
  my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);

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
  $self->{SERVICENAME}="PackMan\@$self->{HOST}";
  $self->{LISTEN}=1;
  $self->{PREFORK}=5;

  $self->{CACHE}={};
  #Remove all the possible locks;
  $self->info( "$$ Removing old lock files");
  $self->{PACKMAN}=AliEn::PackMan->new({PACKMAN_METHOD=>"Local", 
				       CREATE_CATALOGUE=>1}) or return;

  $self->{PACKMAN}->removeLocks();
  $self->{INST_DIR}=$self->{PACKMAN}->{INST_DIR};

  return $self;

}


sub findPackageLFN{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  
  my @dirs=("$self->{CONFIG}->{USER_DIR}/". substr( $user, 0, 1 ). "/$user/packages",
	    "/\L$self->{CONFIG}->{ORG_NAME}/packages",);
  my $lfn;
  my $platform=$self->getPlatform();
  $self->info("$$ Looking for the lfn of $package ($version) for the user $user");

  foreach (@dirs){
    $self->info("Looking in the directory $_");
    my @files=$self->{PACKMAN}->{CATALOGUE}->execute("find", 
#					  "-silent",
					  "$_/$package", $platform) or next;
    $self->info("$$ Got @files");
    if ($version) {
      @files=grep (/$package\/$version\// , @files);
      print "After the version, we have @files\n";
      @files or next;
    }
    $lfn=shift @files;
    last;
  }

  if (!$lfn){  
    $self->info("$$ So far, we didn't get the lfn. Looking for source packages");
    #Ok, let's look for the package source
    foreach (@dirs){
      my @files=$self->{PACKMAN}->{CATALOGUE}->execute("find", "-silent","$_/$package", "source") or next;
      print "Got @files\n";
      if ($version) {
	@files=grep (/$package\/$version\// , @files);
	print "After the version, we have @files\n";
	@files or next;
      }
      $lfn=shift @files;
      last;
    }
    if (!$lfn) {
      $version or $version="";
      my $message="The package $package (v $version) does not exist for $platform \n";
      $self->info($message);
      die $message;
    }
  }
  $self->info( "$$ Using $lfn");
  my (@dependencies)=$self->{PACKMAN}->{CATALOGUE}->execute("showTagValue", "-silent",$lfn, "PackageDef");
  my $item={};
  @dependencies and $dependencies[1]  and $item=shift @{$dependencies[1]};

  $self->info( "$$ Metadata of this item");
  use Data::Dumper;
  print Dumper($item);
  return ($lfn, $item);
}


sub getDependencies {
  my $this=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;


  my $cacheName="dep_package_${package}_${version}_${user}";

  my $cache=AliEn::Util::returnCacheValue($self, $cacheName);
  if ($cache) {
    $self->info( "$$ Returning the value from the cache (@$cache)");
    return (@$cache);
  }

  my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);
  
  AliEn::Util::setCacheValue($self, $cacheName, [1,$info]);
  $self->info("Giving back the dependencies of $package");
  return (1, $info);
}

return 1;


