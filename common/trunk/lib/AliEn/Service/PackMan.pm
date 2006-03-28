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
use Cwd;

use vars qw(@ISA $DEBUG);

@ISA=qw(AliEn::Service);


$DEBUG=0;

use strict;

use AliEn::UI::Catalogue::LCM;

# Use this a global reference.

my $self = {};

=item C<removePackage($user,$package,$version)>

If the package was installed, this method removes it from the disk

=cut 

sub removePackage {
  shift;
  $self->info( "$$ Removing the package @_");
  my $user=shift;
  my $package=shift;
  my $versionUser=shift;

  my ($done, $lfn, $info, $version)=$self->isPackageInstalled($user,$package,$versionUser);
  
  $done or return (-1, "Package is not installed");


  my $dir="$self->{INST_DIR}/$user/$package/$version";
  if (($dir=~ /\.\./) or ($dir=~ /\s/)) {
    $self->info( "$$ Error: someone is trying to delete another directory '$dir'");
    die("Error trying to delete $dir: this is not the directory where the package is installed\n");
  }

  system ("rm","-rf","$dir") and 
    $self->info( "$$ Error deleting the package")
      and die("Error deleting the directory $dir\n");
  $self->info( "$$ Package $package ($version) removed");
  AliEn::Util::deleteCache($self);
  return 1;
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
  $self->info( "$$ Giving back all the packages defined");

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


  my $silent="";
  $DEBUG or $silent="-silent";

  my @userPackages=$self->{CATALOGUE}->execute("find", $silent, $self->{CONFIG}->{USER_DIR}, "/packages/*");
  my @voPackages=$self->{CATALOGUE}->execute("find", $silent, "\L/$self->{CONFIG}->{ORG_NAME}/packages", "*");
  my @packages;
  foreach my $pack (@userPackages, @voPackages) {
    $self->debug(2,  "FOUND $pack");
    if ($pack =~ m{^$self->{CONFIG}->{USER_DIR}/?./([^/]*)/packages/([^/]*)/([^/]*)/$platformPattern$}) {
      grep (/^$1\@${2}::$3$/, @packages) or
	push @packages, "$1\@${2}::$3";
      next;
    }
    if ($pack =~ m{^\L/$self->{CONFIG}->{ORG_NAME}\E/packages/([^/]*)/([^/]*)/$platformPattern$}) {
      grep (/^VO\@${1}::$2$/, @packages) or
	push @packages, "VO\@${1}::$2";
      next;
    }
    $self->debug(2, "Ignoring $pack");
  }
  $self->info( "$$ $$ RETURNING @packages");
  AliEn::Util::setCacheValue($self, "listPackages-$platform", \@packages);

  return (1,@packages);
}


=item C<getListInstalledPackages()>

Returns a list of all the packages installed in the machine
Each entry is in the format "<user>::<package>::<version>"

=cut

sub getListInstalledPackages {
  shift;
  $self->info( "$$ Giving back the list of packages that have been installed");
  my $cache=AliEn::Util::returnCacheValue($self, "installedPackages");
  if ($cache) {
    $self->info( "$$ $$ Returning the value from the cache (@$cache)");
    return (1, @$cache);
  }
  my @allPackages=();
  eval {
    my $dir="$self->{INST_DIR}";
    $self->debug(1, "Checking $dir");
    foreach my $user ($self->getSubDir($dir)) {
      $self->debug(1, "Checking $dir/$user");
      foreach my $package ($self->getSubDir("$dir/$user")){
	$self->debug(1, "Checking $dir/$user/$package");
	foreach my $version ($self->getSubDir("$dir/$user/$package")){
	  $self->debug(1, "Checking $dir/$user/$package/$version");
	  push @allPackages, "${user}\@${package}::$version";
	}
      }
    }

  };
  if ($@) {
    $self->info( "$$ We couldn't find the packages ");
    die ($@);
  }
  $self->info( "$$ Returning @allPackages");
  AliEn::Util::setCacheValue($self, "installedPackages", \@allPackages);
  return (1, @allPackages);
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
  my $user=shift;
  my $package=shift;
  my $versionUser=shift;

  my ($done, $lfn, $info, $version)=$self->isPackageInstalled($user,$package,$versionUser);

  $done or  return (-1, "Package is not installed");
  $self->info( "$$ Ok, the package is installed");
  ($done, my $source, my $installDir)= 
    $self->installPackage($user, $package, $version);
  $self->info( "$$ We should do $done and $source");
  my $env="";
  if ($source) {
     $self->info( "$$ Lets's call $source env");
    $env=`$source env`;
    my $env2=`$source echo "The AliEn Package $package is installed properly"`;
    if ($env2 !~ /The AliEn Package $package is installed properly/s ){
      $self->info( "$$ Warning!!! The package has to source $source, but this script doesn't seem to execute commands");
      $env.="================================================
Warning!!!!
The package is supposed to do: $source
This script will receive as the first argument the directory where the package is installed, and then the command to execute. Please, make sure that the script finishes with a line that calls the rest of the arguments (something like \$*).";
    }
  }

  my $directory=`ls -la $installDir`;
  return ($version, $info, $directory, $env);
}

=item C<installPackage($user,$package,$version,[$dependencies])>

 This method is going to install a package

=cut

sub installPackage{
  shift;

  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $dependencies=(shift or {});
  $self->info( "$$ Checking package $package for $user and $version");

  my $cacheName="package_${package}_${version}";

  my $cache=AliEn::Util::returnCacheValue($self, $cacheName);
  if ($cache) {
    $self->info( "$$ Returning the value from the cache (@$cache)");
    return (@$cache);
  }
  my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);

  $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
    and ($version)=($1);
  my $source="";

  if ($lfn =~ m{^/$self->{CONFIG}->{ORG_NAME}/packages/}i ) {
    $self->info( "$$ This package is defined by the VO. Let's put the user to VO");
    $user=uc("VO_$self->{CONFIG}->{ORG_NAME}");
  }

  #First, let's try to install all the dependencies
  print "Ready to install $package and $version\n";
  $dependencies->{"${package}::$version"}=1;

  if ($info) {
    $self->info( "$$ Installing the dependencies of $package");
    foreach (split(/,/,$info->{dependencies})){
      my ($pack, $ver)=split(/::/, $_, 2);
      my $pack_user=$user;
      $pack=~ s/^(.*)\@// and $pack_user=$1;
      #let's install the packages without configuring them
      if ($dependencies->{"${pack}::$ver"} ) {
	$self->info( "$$ Package $pack $ver already configured");
	next;
      }
      my ($ok, $depsource, $dir)=$self->installPackage($user, $pack, $ver, $dependencies);
      $depsource and $source="$source $depsource";
    }
  }
  
  $self->info( "$$ Installing the package $package for $user");

  $self->InstallPackage($lfn, $user, $package, $version,$info, $source);
  my ($done, $psource, $dir)= $self->ConfigurePackage($user, $package, $version, $info);
  $source and $psource="$source $psource";
  $self->info( "$$ Returning $done and ($psource)\n");

  my @list= ($done, $psource, $dir);
  AliEn::Util::setCacheValue($self, $cacheName, \@list);
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
  $self->info( "$$ Getting the installation log of $package, $user and $version");
  my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);

  $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
    and ($version)=($1);
  my $logFile="$self->{INST_DIR}/$user.$package.$version.InstallLog";

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
  $self->{CATALOGUE}=AliEn::UI::Catalogue::LCM->new()
    or return;

  $self->{CACHE}={};
  $self->{INST_DIR}=($self->{CONFIG}->{PACKMAN_INSTALLDIR} || "$ENV{ALIEN_HOME}/packages");
  if (! -d $self->{INST_DIR}) {
    $self->info( "$$ Creating the directory $self->{INST_DIR}");
    require  AliEn::MSS::file;;
    AliEn::MSS::file::mkdir($self, $self->{INST_DIR}) and return;
  }
  #Remove all the possible locks;
  $self->info( "$$ Removing old lock files");
  system ("rm -f $self->{INST_DIR}/*.InstallLock");
  return $self;

}

sub getSubDir{
  my $self=shift;
  my $dir=shift;
  opendir (DIR, $dir) or $self->info( "$$ Error reading $dir\n")
    and die("Error reading $dir");
  my @entries = grep { ( ! /^\./ ) && -d "$dir/$_" } readdir(DIR);
  closedir DIR;
  return @entries;
}

sub ConfigurePackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $info=shift;

  $self->info( "$$ Configuring the package $package (v $version)");
  my $dir="$self->{INST_DIR}/$user/$package/$version";
  chdir $dir or 
    return $self->installPackage($user, $package, $version);
  my $sourceFile= ($info->{config} || ".alienEnvironment");
  $info->{path} and $dir.="/$info->{path}";

  my $source="";
  if (-f "$dir/$sourceFile"){
    $self->{LOGGER}->info( "$$ PacKMan","Testing if $dir/$sourceFile is executable");
    if (! -x "$dir/$sourceFile") {
      $self->info( "$$ The file wasn't executable");
      chmod 0755 ,"$dir/$sourceFile";
    }
    $source="$dir/$sourceFile $dir ";
  }
  return (1,$source);
}
sub existsPackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $info=shift;

  $self->info( "$$ Checking if $package is already installed");

  my $dir="$self->{INST_DIR}/$user/$package/$version";
  if (!-d $dir) {
    $self->debug("Checking among the VO packages");
    $dir="$self->{INST_DIR}/VO_\U$self->{CONFIG}->{ORG_NAME}\E/$package/$version";
    (-d $dir) or return;
  } 

  $self->info( "$$ Checking the size of $dir");
  my $size;
  if (-l $dir) {
    $self->info( "$$ This is installed in the common area... let's ignore it for the time being");
  }else {
#    open (FILE, "du -s $dir|") or 
#      $self->info( "$$ Error getting the size of the directory")
#	and return;
#    $size=<FILE>;
#    close FILE;
#    $size=~ s/^\s*(\d+)\s+.*$/$1/s;
#    if ( $size eq "0") {
#      $self->info( "$$ The size of the package is 0");
#      system("rm -rf $dir");
#    return;
#    }
  }
  $info and chomp $info->{size};

  $self->info( "$$ Size $size (has to be $info->{size})");
  if (  $info->{size} and ($size ne $info->{size}) ){
    $self->info( "$$ The size of the package does not correspond (has to be $info->{size} and is $size)");
    system("rm -rf $dir");
    return;
  }
  if ($info->{md5sum}) {
    $self->info( "$$ Checking the md5sum of $info->{executable}");
    chdir $dir;
    system("md5sum -c .alienmd5sum") and
      $self->info( "$$ Error checking the md5sumlist")
	and return;
  }
  $self->info( "$$ The package is already installed (in $dir)");
  return $dir;
}
sub findPackageLFN{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  
  my @dirs=("/\L$self->{CONFIG}->{ORG_NAME}/packages",
	    "$self->{CONFIG}->{USER_DIR}/". substr( $user, 0, 1 ). "/$user/packages");
  my $lfn;
  my $platform=$self->getPlatform();
  $self->info("$$ Looking for the lfn of $package ($version)");

  foreach (@dirs){
    $self->info("Looking in the directory $_");
    my @files=$self->{CATALOGUE}->execute("find", "-silent","$_/$package", $platform) or next;
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
      my @files=$self->{CATALOGUE}->execute("find", "-silent","$_/$package", "source") or next;
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
  my (@dependencies)=$self->{CATALOGUE}->execute("showTagValue", "-silent",$lfn, "PackageDef");
  my $item={};
  @dependencies and $dependencies[1]  and $item=shift @{$dependencies[1]};

  $self->info( "$$ Metadata of this item");
  use Data::Dumper;
  print Dumper($item);
  return ($lfn, $item);
}

sub InstallPackage {
  my $self=shift;
  my $lfn=shift;
  my ($user, $package, $version, $info,$depConf)=(shift, shift, shift,shift);


  my $dir="$self->{INST_DIR}/$user/$package/$version";
  my $lock="$self->{INST_DIR}/$user.$package.$version.InstallLock";
  my $logFile="$self->{INST_DIR}/$user.$package.$version.InstallLog";

  ( -f $lock) and $self->info( "$$ Package being installed\n")
    and  die ("Package is being installed\n");


  $self->existsPackage($user, $package, $version,$info) and return 1;
  $self->info( "$$ Ready to install the package (output in $logFile) ");
  
  open FILE, ">$lock" 
    and close FILE
    or $self->info( "$$ Error creating $lock")
    and die ("Error creating $lock\n");

  $self->{LOGGER}->redirect($logFile);
  eval {
    if (! $info->{shared}) {
      AliEn::MSS::file::mkdir($self, $dir) and
	  $self->info( "$$ Error creating $dir") and
	    die("Error creating $dir\n");
      chdir $dir or die ("Error changing to $dir $!\n");
    } else {
      my $shared="$self->{INST_DIR}/$user/alien_shared";
      $self->info( "$$ This package has to be installed in a shared directory");
      AliEn::MSS::file::mkdir($self,$shared,"$self->{INST_DIR}/$user/$package/") and 
	  $self->info( "$$ Error creating the directory $shared") and die ("Error creating the directory $shared $!");
      system ("ln -s $shared $dir") and $self->info( "$$ Error creating the link") and die ("Error creating the link\n");
    }
  };
  if ($@) {
    system ("rm -rf $dir $lock");

    $self->info( "$$ Error $@");
    $self->{LOGGER}->redirect();
    $self->info( "$$ Error $@") and die ("Error $@\n");
  }
  $self->info( "$$ Installing package $package (V $version)");
  my $pid=fork();
  if (!$pid){
    $self->info( "$$ Let's tell the client to retry in sometime...");
    $self->{LOGGER}->redirect();
    die ("Package is being installed\n");
  }

  eval {
    $self->_doAction($package, $version, $dir, $info, "pre_install", $depConf);
    $self->_Install($dir, $lfn, $info);
    $self->_doAction($package, $version, $dir, $info, "post_install", $depConf);

  };
  my $error=$@;

  system ("rm $lock");
  if ($error) {
    $self->{LOGGER}->redirect();
    system ("rm -rf $dir");
    $self->info( "$$ Error $@") and die ("Error $@\n");
  }
  $self->info( "$$ Package $package installed successfully!");
  $self->{LOGGER}->redirect();
  AliEn::Util::deleteCache($self);
  return 1;
}

sub _doAction {
  my $self=shift;
  my ($package, $version)=(shift, shift);
  my $dir=shift;
  my $metadata=shift;
  my $action=shift;
  my $depConf=shift ||"";
  my $script=$metadata->{$action} or return 1;
  $self->info( "$$ Doing the $action with $script");
  my ($file)=$self->{CATALOGUE}->execute("get", $script)
    or die("Error getting the file $script for the $action\n");
  chmod(0755, $file);
  $self->info( "$$ Calling $file $dir");

#  open SAVEOUT,  ">&STDOUT";
#  open SAVEOUT2, ">&STDERR";

#  open SAVEOUT,  ">&STDOUT";
#  open SAVEOUT2, ">&STDERR";
  my $log="$self->{CONFIG}->{LOG_DIR}/packman/$package.$version.$action.$self->{CONFIG}->{HOST}";
  $self->{LOGGER}->redirect($log);
  

#  require AliEn::MSS::File;
#  AliEn::MSS::file::mkdir($self, "$self->{CONFIG}->{LOG_DIR}/packman");
#  if ( !open STDOUT, ">$log" ) {
#    open STDOUT, ">&SAVEOUT";
#    die "stdout not opened!!";
#  }
#  open( STDERR, ">&STDOUT" ) or die ("Error opening STDERR");
  my @todo=($file, $dir);
  $depConf and @todo=(split(/ /, $depConf), $file, $dir);
  my $error=system(@todo);
#  close STDOUT;

#  open STDOUT, ">&SAVEOUT";
#  open STDERR, ">&SAVEOUT2";

  $self->{LOGGER}->redirect();
  $self->info( "$$ $action done with $error (log $log)!!");
  return 1;
}
sub _Install {
  my $self=shift;
  my $dir=shift;
  my $lfn=shift;
  my $metadata=shift;

   my ($file)=$self->{CATALOGUE}->execute("get", $lfn)
      or die("getting the file $lfn\n");
  $self->info( "$$ Starting the uncompress...");
  system("tar zxf $file") and die("uncompressing $lfn ($file)\n");
  return 1;
}

sub isPackageInstalled {
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;

  my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);

  $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
    and ($version)=($1);

  $self->existsPackage($user, $package, $version,$info)  or
    $self->info( "$$ The package is not installed :D") and 
      return ;
  return (1, $lfn, $info, $version)
}
return 1;


