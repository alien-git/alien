
package AliEn::PackMan;

use AliEn::Config;
use strict;
use Data::Dumper;
use AliEn::Util;
use Getopt::Long;
use Time::HiRes;
use AliEn::UI::Catalogue;
use AliEn::Database::Catalogue;

use vars qw (@ISA $DEBUG);

push @ISA, 'AliEn::Logger::LogObject';
$DEBUG = 0;

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = (shift or {});
  bless($self, $class);
  $self->SUPER::new();
 
  $self->{CONFIG} or $self->{CONFIG} = AliEn::Config->new();
  my $role = ($self->{role} or $self->{ROLE} or $self->{user} );
  
  $self->{SOAP} = new AliEn::SOAP or print "Error creating AliEn::SOAP $! $?" and return;

  if (! $self->{DB}){
    if ($role eq 'admin'){
#      $self->info("= $proto =====WE ARE CREATING DATABASE CONNECTION");
      $self->{UI}=AliEn::UI::Catalogue->new({role=>$role}) or return;
      $self->{DB}=$self->{UI}->{CATALOG}->{DATABASE} or $self->info("Error getting the database") and return;
    }
    else{
      $self->{DB}=AliEn::Database::Catalogue->new() or return;
    }
  }

  $self->{LIST_FILE_TTL} or $self->{LIST_FILE_TTL} = 7200;

  $self->initialize(@_) or return;
  
  return $self;
}

sub initialize {
  
  my $self = shift;

  $self->{INSTALLDIR} = $self->{CONFIG}->{PACKMAN_INSTALLDIR} || "$ENV{ALIEN_HOME}/packages";
   -d $self->{INSTALLDIR} or mkdir $self->{INSTALLDIR};
  if (not -d $self->{INSTALLDIR})  {
    $self->{INSTALLDIR}="$ENV{ALIEN_HOME}/packages"; 
    -d $self->{INSTALLDIR} or mkdir $self->{INSTALLDIR};
    -d $self->{INSTALLDIR} or return;
  }
  $self->{REALLY_INST_DIR} or $self->{REALLY_INST_DIR}=$self->{INSTALLDIR};




  return $self;
}

sub f_packman_HELP {
  return "packman: talks to the Package Manager. By default, it talks to the closest PackMan. 
Usage:

   packman [-name <PackManName>] [-everywhere] <command> [<command options and arguments>]

Global options:
     -everywhere:  Do the command in all the packmans currently available
     -name <PackManName>' to talk to a specific instance. By default, it will talk to the closest 

Possible commands:
\tpackman list [-retry number]:\treturns all the packages defined in the system
\tpackman listInstalled:\treturns all the packages that the service has installed
\tpackman test <package>: tries to configure a package. Returns the metainformation associated with the package, a view of the directory where the package is installed, and an environment that the package would set
\tpackman install <package>: install a package (and all its dependencies) in the local cache of the PackMan
\tpackman installLog <package>: get the installation log of the package
\tpackman dependencies <name>: gives the list of dependencies of a package
\tpackman remove  <package>: removes a package from the local cache
\tpackman define <name> <version> <tar file> [<package options>]
\tpackman undefine <name> <version>
#\tpackman recompute: (only for admin) recompute the list of packages.
\tpackman synchronize [-retry number]:\tinstalls all the existing packages, and removes the packages locally installed that do not exist anymore.
Package options: -platform source, else the default for the local system is used
		 -retry number specifies a number of retries if the command cannot get the list of packages
                  post_install <script> where the script should be given with the full catalogue path
The format of the string <package> is:
    [<user>\@]<PackageName>[::PackageVersion}
For instance, 'ROOT', 'ROOT::4.1.3', 'psaiz\@ROOT', 'psaiz\@ROOT::4.1.2' comply with the format of <package>
";
}

sub f_packman {
  my $self = shift;
  $DEBUG and $self->debug(1, "Talking to the PackMan: @_");
  
  my $silent     = grep (/^-s$/,           @_);
  my $returnhash = grep (/^-z$/,           @_);
  my $allPackMan = grep (/^-everywhere$/i, @_);
  my @arg        = grep (!/^-z$/,          @_);
  @arg = grep (!/^-s$/,          @arg);
  @arg = grep (!/^-everywhere$/, @arg);

  my $string = join(" ", @arg);


  
  $self->info("*** calling PackMan with arguments $string");

  if ($allPackMan) {
    $self->info("We are going to call all the packman");
    my $response = $self->{SOAP}->CallSOAP("IS", "getAllServices", "PackMan")
    or return;

    $response = $response->result;

    #    print Dumper($response);
    my @n = split(/###/, $response->{NAMES});
    $silent and $string .= " -s";
    foreach my $n (@n) {
      $self->info("Checking $n");

      $self->f_packman("-name $n $string");
    }
    return 1;
  }

  my $direct = 0;
  $string =~ s{-?-silent\s+}{} and $silent = 1;
  if ($string =~ s{-?-n(ame)?\s+(\S+)}{}) {
    my $name = $2;

    $self->info("Talking to the packman $name");

    my $done = $self->{CONFIG}->CheckServiceCache("PACKMAN", $name)
      or $self->info("Error looking for the packman $name")
      and return;
#    $self->{SOAP}->Connect(
#      { address => "http://$done->{HOST}:$done->{PORT}",
#        uri     => "AliEn/Service/PackMan",
#        name    => "PackMan_$name",
#        options => [ timeout => 5000 ]
#      }
#    ) or return;
#    $serviceName = "PackMan_$name";

    @arg = split(" ", $string);
  }
#########################################################################
  my $operation = shift @arg;
  $operation
    or $self->info($self->f_packman_HELP(), 0, 0)
    and return;
  my $callfunction;
  my $requiresPackage = 0;

  if ($operation =~ /^l(ist)?$/) {
    $callfunction  = "getListPackages";
    $operation = "list";
    $direct = 1;
  } elsif ($operation =~ /^listI(nstalled)?$/) {
    $callfunction  = "getListInstalledPackages";
    $operation = "listInstalled";
    $direct = 1;
  } elsif ($operation =~ /^t(est)?$/) {
    $requiresPackage = 1;
    $callfunction        = "testPackage";
    $operation       = "test";
  } elsif ($operation =~ /^i(nstall)?$/) {
    $requiresPackage = 1;
    $callfunction        = "installPackage";
    $operation       = "install";
  } elsif ($operation =~ /^r(emove|m)?$/) {
    $requiresPackage = 1;
    $callfunction        = "removePackage";
    $operation       = "remove";
  } elsif ($operation =~ /^d(efine)?$/) {
    return $self->definePackage(@arg);
  } elsif ($operation =~ /^u(ndefine)?$/) {
    return $self->undefinePackage(@arg);
  } elsif ($operation =~ /^dependencies$/) {
    $callfunction        = "getDependencies";
    $requiresPackage = 1;
  } elsif ($operation =~ /^installLog?$/) {
    $callfunction        = "getInstallLog";
    $requiresPackage = 1;
  } 
  #elsif ($operation =~ /^recompute?$/) {
  #  $callfunction = "recomputeListPackages";
  #  $self->info("And deleting any local caches");
  #  my $dir = $self->{INSTALLDIR};
    
    #system("rm -f $dir/alien_list_*");
  #}
    elsif ($operation =~ /^synchronize$/) {
    return $self->synchronizePackages(@_);
  } else {
    $self->info("I'm sorry, but I don't understand $operation");
    $self->info($self->f_packman_HELP(), 0, 0);
    return;
  }

  if ($requiresPackage) {
    my $package = shift @arg;
    $package or $self->info("Error not enough arguments in 'packman $operation") 
      and $self->info($self->f_packman_HELP(), 0, 0) and return;

    my $version = "";
    my $user    = $self->{CATALOGUE}->{CATALOG}->{ROLE};
    $package =~ s/::([^:]*)$//  and $version = $1;
    $package =~ s/^([^\@]*)\@// and $user    = $1;

    @arg = ($user, $package, $version, @arg);
  }

  my (@result, $done);

  if ($direct) {
    $self->info("Calling directly $callfunction (@_)");
    return $self->$callfunction(@_);
    
  }
  else {
    $silent or $self->info("Let's do $operation (@arg)");

    ($done, @result) = $self->$callfunction( @arg);
    if (!$done or $done < 0) {
      my $print = 0;
      my $error =  shift @result;
      $error or $error = "Error talking to the PackMan";
      $error =~ /Package is being installed/ or $print = 1 + $print;
  
      ($callfunction eq "installPackage")
      and $error =~ /Package is being installed/
      and $error = "Don't PANIC!! The previous message is not a real error. The package is being installed.\n\t\tYou can use \"installLog\" to check the status of the 
installation";
      #If the error message is that the package is being installed, do not print an error
      $self->info($error);
      return;
    }

    $done or $self->info("Error asking for the packages") and return;

  }

  my $return = 1;


 if ($operation =~ /^list(installed)?/i) {
    my $message = "The PackMan has the following packages";
    $1 and $message .= " $1";
    $silent or $self->info(join("\n\t", "$message:", @result));

    if ($returnhash) {
      my @hashresult;
      map {
        my $newhash = {};
        my ($user, $package) = split '@', $_;
        $newhash->{user}    = $user;
        $newhash->{package} = $package;
        push @hashresult, $newhash;
      } @result;
      return @hashresult;
    }
   $return = \@result; 
}  elsif ($operation =~ /^t(est)?$/) {
    $silent
      or $self->info(
      "The package (version $done) has been installed properly\nThe package has the following metainformation\n"
        . Dumper(shift @result));
    my $list = shift @result;
    $silent or $self->info("This is how the directory of the package looks like:\n $list");
    my $env = shift @result;
    $env and $self->info("The package will configure the environment to something similar to:\n$env");
  }
  elsif ($operation =~ /^r(emove|m)$/) {
    $self->info("Package removed!!\n");
  }
  elsif ($operation =~ /^dependencies$/) {
    my $info = shift @result;
    $info or $self->info("The package doesn't have any dependencies\n") and return;
    $self->info("The information of the package is:");
    foreach (keys %$info) {
      /entryId/ and next;
      $info->{$_} and $self->info("\t$_:\t\t$info->{$_}", 0, 0);
    }
  } elsif ($operation =~ /^installLog$/) {
      $self->info(
      "The installation log is\n
=========================================================
$done
=========================================================
\n"
    );
  }

  return $return;
}

############GetlisInstalledPackages_ 


sub getListInstalledPackages_ {

  my $self=shift;
  my @allPackages=();
#  eval {
    my $dir = $self->{INSTALLDIR};
    $DEBUG and $self->debug(1, "Checking $dir");
    foreach my $user ($self->getSubDir($dir)) {
      $DEBUG and $self->debug(1, "Checking $dir/$user");
      foreach my $package ($self->getSubDir("$dir/$user")){
      $DEBUG and $self->debug(1, "Checking $dir/$user/$package");
        foreach my $version ($self->getSubDir("$dir/$user/$package")){
         $DEBUG and $self->debug(1, "Checking $dir/$user/$package/$version");
          (-f "$dir/$user.$package.$version.InstallLock") and
           $DEBUG and $self->debug(1, "The package is being installed") and next;
          push @allPackages, "${user}\@${package}::$version";
        }
      }
    }

 # };
  if (!@allPackages) {
    $self->info( "$$ We couldn't find the packages ") and return;
   }
  
   $self->info( "$$ We could find the packages ");
   return  1, @allPackages;

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


#######################################################################
sub getListInstalledPackages {
  my $self = shift;
  
  my @packages = $self->getListInstalled_Internal(@_) or return;
  
  grep (/^-s(ilent)?$/, @_)
    or $self->printPackages({input => \@_, text => " installed"}, @packages);
  
  return @packages;
}

#This is the method that has to be overwritten in other implemetations
sub getListInstalled_Internal {
  my $self = shift;
   
  my ($status, @list) = (0, undef);
  if (!grep (/-force/, @_)) {
#    ($status, @list) = $self->readPackagesFromFile("alien_list_installed");
  ($status, @list) = $self->readPackagesFromFile("alien_listInstalled_packages_");
  
  }
  $DEBUG and $self->debug(1, "Asking the PackMan for the packages that it has installed");

  if (!$status or $status != 1) {
  AliEn::Util::deleteCache($self);
     my $cache=AliEn::Util::returnCacheValue($self, "installedPackages");
     if ($cache and $cache->[0]){
    $self->info("This is for test the returned cache (@$cache)");
    return (1, @$cache);
    }
     ($status, @list) = $self->getListInstalledPackages_() or return;
     AliEn::Util::setCacheValue($self, "installedPackages", \@list);    
   } elsif ($status < 0) {
      $self->info("Well, the info is old, but it is better than nothing");
      $status = 1;
    }

  $DEBUG and $self->debug(2, "The list of installed is @list");
  return $status, @list;

}
#################################################################
=item C<getListPackages()>
Returns a list of all the packages defined in the system
=cut
####### VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV #######

sub getListPackages {
  my $self = shift;
  my $platform = AliEn::Util::getPlatform($self);
  grep (/^-all/, @_) and $platform = "all";

  my $retry = 1;
  my $maxRetry = 25;
  my $options={retry=>1};
  my @old=@ARGV;
  @ARGV  = @_;
  Getopt::Long::Configure("pass_through");
  Getopt::Long::GetOptions($options, "retry=i");
  Getopt::Long::Configure("default");
  @_=@ARGV;
  @ARGV=@old;
  if($options->{retry} <= $maxRetry){
    $retry = $options->{retry} if ($options->{retry} > 0);
  }else{
    $retry = $maxRetry;
  }

  my ($status, @packages) = (0, undef);
  if (!grep (/-force/, @_)) {
    ($status, @packages) = $self->readPackagesFromFile("alien_list_packages_$platform");
  }
  if ($status != 1 or $#packages == 0)
  {
    while (1)
      {
        $self->info("Asking for the list of all the packages defined in the system");

        grep (/^-?-force$/, @_)
        and  AliEn::Util::deleteCache($self);

        my $platform=AliEn::Util::getPlatform($self);

        if (grep (/^-?-all$/, @_)) {
            $self->info("Returning the info of all platforms");
            $platform="all";
        }
        my $cache=AliEn::Util::returnCacheValue($self, "listPackages-$platform");
        if ($cache and $cache->[0]) {
           $self->info( "Returning the value from the cache (@$cache)");
           return (1, @$cache);
        }

        $self->info("Retrieving the list of Packages (@_)");
        my $query="SELECT distinct fullPackageName from PACKAGES";
        my $bind=[];

       if( $platform ne  "all") {
         $self->info("Returning the info of the platform $platform");
         $query.=" where  (platform=?  or platform='source')";
         $bind=[$platform];
       }
       $self->info("Let's do query $query");
       my $packages=$self->{DB}->queryColumn($query,undef, {bind_values=>$bind})
         or $self->info("Error doing the query") and return;
       my ($done, @pack) = ( 1, @$packages);

       AliEn::Util::setCacheValue($self, "listPackages-$platform", \@pack);
       if ($done and $done == 1 and $#pack >= 0 ){
          ($status, @packages) = ($done, @pack);
          last;
       }
       elsif ($status < 0 and $#pack >= 0) {
          $self->info("Well, the info is old, but it is better than nothing");
          $status = 1;
          last;
      }
       elsif (!$done or !$pack[0]) {
          $retry--;
          $retry or $self->info("Can't get the list of packages!!!\n") and return;
          $self->info("Can't get the list of packages. Let's sleep for some time and try again");
          sleep(2*$retry);
       }
       else{
         $self->info("Can't get the list of packages!!!\n") and return;
       }
     }
  }
  grep (/^-s(ilent)?$/, @_) or $self->printPackages({input => \@_}, $status, @packages);
  return $status, @packages;
}

####### ^^^^^^^^^^^^^^^^^ getListPackages ^^^^^^^^^^^^^^^^^^^^^^^ #######

sub readPackagesFromFile {
  my $self = shift;
  my $file = shift;

  my $dir = $self->{INSTALLDIR} ;
  
  $file = "$dir/$file";

  $DEBUG and $self->debug(1, "Checking if the file $file exists...");
  use File::stat;
  my $st = stat($file);

  if (!$st  or -z $file){
  $self->createListFiles() or return 0; 
  }
  if ($st){

  $DEBUG and $self->debug(2, "Reading from the file $file!");

  open(FILE, "<$file") or $self->info("Error opening the file $file") and return 0;
  my @packages = <FILE>;
  close FILE;
  chomp @packages;
  
  $self->info("File '$file' read");
  my $return = 1;

  my $time = time;

  if ($time - $st->mtime > $self->{LIST_FILE_TTL}) {
    $self->info("The file is older than $self->{LIST_FILE_TTL} seconds... use at your own risk");
    $return = -2;

  }
  return $return, @packages;
}
#  return @packages;
}

#################################################################
=item C<installPackage($user,$package,$version,[$dependencies])>
 This method is going to install a package
=cut
####### VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV #######

sub installPackage {
  my $self    = shift;
  my $user    = shift;
  my $package = shift;
  my $version = shift;

  $self->info("Asking the PackMan to install");

  my ($done, $psource, $dir);
  my $retry = 5;
  while (1) {
    $self->info("Asking the package manager to install $package as $user");

    my $cacheName="package_${package}_${version}_${user}";
    my $cache=AliEn::Util::returnCacheValue($self, $cacheName);
    if ($cache and $cache->[0]) {
      $self->info( "Returning the value from the cache (@$cache)");
      return (@$cache);
    }
    my ($done2,@rest )=$self->isPackageInstalled ($user, $package, $version);
    my $exit=0;
    if (! $done2){
      if (-f "$self->{INSTALLDIR}/$user.$package.$version.InstallLock"){
        $self->info("Someone is already installing the package");
        return (-1, "Package is being installed");
      }
      $self->info("Forking to install the package");
      fork() and return (-1, "Package is being installed");
      $exit=1;
  }

  my @list = ($done, $psource, $dir) = $self->PackageInstaller($user, $package, $version);

    AliEn::Util::setCacheValue($self, $cacheName, \@list);
   if ($done){
   my $dir3 =  ($self->{INSTALLDIR} ||  "$ENV{ALIEN_HOME}/packages");

   my $file = "$dir3/alien_listInstalled_packages_";
   $self->info("==After install removing $file===");
   system("rm -f $file");
   }

    $self->info("The PackMan returns @list (and we exit $exit)");
    $exit and exit(0);

    $done and last;

    my $message = $AliEn::Logger::ERROR_MSG;
    $self->info("The reason it wasn't installed was $message");
    $message =~ /Package is being installed/ or $retry--;
    $retry or last;
    $self->info("Let's sleep for some time and try again");
    sleep(30);
  }

  if (!$done) {
    $self->info("The package has not been instaled!!");
    return;
  }
  $self->info("The PackMan returned '$done' and '$psource'");

  return ($done, $psource);
}

sub PackageInstaller{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $dependencies=shift ||{};
  my $options=shift ||{};
 
  $self->debug (2,"checking if we have to install the package");
  my $source="";
  my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);
  if (!$lfn or $lfn < 0) {
    $self->info("Error installing $package: $info");
    return (0, "Error finding the lfn for $package");
  }

  $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
    and ($version)=($1);
  if ($lfn =~ m{^/$self->{CONFIG}->{ORG_NAME}/packages/}i ) {
    $self->info( "$$ This package is defined by the VO. Let's put the user to VO");
    $user=uc("VO_$self->{CONFIG}->{ORG_NAME}");
  }

  #First, let's try to install all the dependencies
  $self->info("Ready to install $package and $version and $user (from $lfn)");
  
  while (1){
    my $done=$self->createLock($user, $package, $version);
    $done and last;
    defined ($done) or $self->info("Error creating the lock")
      and return (0, "Error creating the lock");
    $self->info("Sleeping for a while");
    sleep(20);
  }

  $dependencies->{"${package}::$version"}=1;

  if ($info && $info->{dependencies}) {
	
    $self->info( "$$ Installing the dependencies of $package (without forking");

    foreach (split(/,/,$info->{dependencies})){
      my ($pack, $ver)=split(/::/, $_, 2);
      my $pack_user=$user;
      $pack=~ s/^(.*)\@// and $pack_user=$1;
      #let's install the packages without configuring them
      if ($dependencies->{"${pack}::$ver"} ) {
	        $self->info( "$$ Package $pack $ver already configured");
	        next;
      }
      my ($ok, $depsource, $dir)=$self->PackageInstaller($user, $pack, $ver, $dependencies, $options);
      if (! $ok or $ok eq '-1'){
          $self->removeLock($user, $package, $version);
          return (-1, $depsource);
      }
      $depsource and $source="$source $depsource";
    }
  }

  $self->debug(2,  "Ready to do the installPackage $package for $user");
  #Let's put the files public

  umask 0022;
  my ($done2, $error)=$self->InstallPackage($lfn, $user, $package, $version,$info, $source, $options);
  umask 0027;

  $self->removeLock($user, $package, $version);
 
  $self->info("The installation of $user, $package, $version finished with $done2 and $error");
  $done2 or return (-1, $error);
  $done2 eq '-1' and return (-1, $error);

  my ($done, $psource, $dir2)= $self->ConfigurePackage($user, $package, $version, $info);
  $psource and $source="$source $psource";
  $self->info( "$$ Returning $done and ($source)\n");

  $self->info("Everything is ready. We just have to do $source");
  return ($done, $source, $dir2);
}

sub InstallPackage {
  my $self=shift;
  my $lfn=shift;
  my ($user, $package, $version, $info,$depConf)=(shift, shift, shift,shift,shift);
  my $options=shift || {};

  my $dir="$self->{REALLY_INST_DIR}/$user/$package/$version";

  my $logFile="$self->{REALLY_INST_DIR}/$user.$package.$version.InstallLog";

  $self->existsPackage($user, $package, $version,$info) and return 1;
  $self->info( "$$ Ready to install the package (output in $logFile) ");

  system("mv",$logFile, "$logFile.back");
  $self->{LOGGER}->redirect($logFile);
  eval {
    if (! $info->{shared}) {
      AliEn::MSS::file::mkdir($self, $dir) and
	  $self->info( "$$ Error creating $dir") and
	    die("Error creating $dir\n");
      chdir $dir or die ("Error changing to $dir $!\n");
    } else {
      my $shared="$self->{REALLY_INST_DIR}/$user/alien_shared";
      $self->info( "$$ This package has to be installed in a shared directory");
      AliEn::MSS::file::mkdir($self,$shared,"$self->{REALLY_INST_DIR}/$user/$package/") and 
	  $self->info( "$$ Error creating the directory $shared") and die ("Error creating the directory $shared $!");
      system ("ln -s $shared $dir") and $self->info( "$$ Error creating the link") and die ("Error creating the link\n");
    }
    if (!$self->checkDiskSpace($dir, $info->{installedSize} )){
      die("Error checking for diskspace to install");
    }
  };

  if ($@) {

    $self->info( "$$ Error $@");
    $self->{LOGGER}->redirect();
    $self->info( "$$ Error $@");
    return (-1, "Error $@\n");
  }

  $self->info( "Installing package $package (V $version)");

  eval {
    $self->_doAction($package, $version, $dir, $info, "pre_install", $depConf);
   # $self->_Install($dir, $lfn, $info);
    my ($file)=$self->{CATALOGUE}->execute("get", "-s", "no_se", $lfn)
      or die("getting the file $lfn\n");
    $self->info( "$$ Starting the uncompress...");
    system("tar zxf $file") and die("uncompressing $lfn ($file)\n");
    $self->_doAction($package, $version, $dir, $info, "post_install", $depConf);
  };
  my $error=$@;
  if ($error) {
    $self->info("Something failed. Removing the directory");
    $self->{LOGGER}->redirect();
    system ("rm -rf $dir");
    $self->info( "$$ Error $@");
    return (-1, "Error $@\n");
  }
  $self->info( "$$ Package $package installed successfully!");
  $self->{LOGGER}->redirect();
  AliEn::Util::deleteCache($self);
  return 1;
}

####### ^^^^^^^^^^^^^^^^^ installPackage ^^^^^^^^^^^^^^^^^^^^^^^ #######

sub synchronizePackages {
  my $self = shift;
  my $cmd  = shift;
  $self->info("Ready to synchronize the packages with the catalogue (@_)");
  my $retry = 1;
  my $maxRetry = 25;
  my $options={retry=>1};
  my @arg             = @_;
  my $optionsPackages = {};
  my @old=@ARGV;
  @ARGV = @arg;
  Getopt::Long::Configure("pass_through");
  Getopt::Long::GetOptions($options, "retry=i");
  Getopt::Long::GetOptions($optionsPackages, "packages=s");
  Getopt::Long::Configure("default");

  #      or $self->info("Error checking the options of packman synchronize") and return;
  @arg = @ARGV;
  @ARGV=@old;
  $retry = $options->{retry} if ($options->{retry} > 0);
  $optionsPackages->{packages} and $self->info("Doing only the packages '$optionsPackages->{packages}'");
  my $pattern = "(" . join(")|(", split(/,/, $optionsPackages->{packages} || "")) . ")";
#  my ($ok1, @packages) = $self->getListPackages("-s", "-retry", "$retry");
  my ($ok1, @packages) = $self->f_packman("list", "-s", "-retry", "$retry");
  $ok1 or $self->info("Error getting the list of packages") and return;
  my ($ok, @installed) = $self->f_packman("listInstalled", "-s", @arg);
  $ok or $self->info("Error getting the list of packages") and return;

  if (@packages != 0){
    foreach my $p (@packages) {
      if (!grep (/^$p$/, @installed)) {
        if (grep(/^$pattern/, $p)) {
          $self->info("  We have to install $p");
          $self->f_packman("install", "-s", $p, @arg);
        }
      }
      @installed = grep (!/^$p$/, @installed);
    }
    foreach my $p (@installed) {
      grep(/^$pattern/, $p) or next;
      $self->info("  And we have to delete $p");
      $self->f_packman("remove", "-s", $p, @arg);
    }
  }else{
    $self->info("=== The list of defined packages is empty ===");
  }
  return 1;
}

sub definePackage {
  my $self        = shift;
  my $packageName = shift;
  my $version     = shift;
  my $tar         = shift;
  my $message     = "";

  $self->info("Adding a new package");

  $packageName or $message .= "missing Package Name";
  $version     or $message .= "missing version";
  $tar         or $message .= "missing tarfile";
  (-f $tar)    or $message .= "the file $tar doesn't exist";

  $message and $self->info("Error: $message", 100) and return;

  my @args   = ();
  my $se     = "";
  my $lfnDir = lc($self->{CATALOGUE}->{CATALOG}->GetHomeDirectory() . "/packages");
  my $sys1   = `uname -s`;
  chomp $sys1;
  my $sys2 = `uname -m`;
  chomp $sys2;
  my $platform = "$sys1-$sys2";
  
  while (my $arg = shift) {

    if ($arg =~ /^-?-se$/) {
      $se = shift;
      next;
    }
    if ($arg =~ /^-vo$/) {
      $lfnDir = lc("/$self->{CONFIG}->{ORG_NAME}/packages");
      next;
    }
    if ($arg =~ /^-?-platform$/) {

      $platform = shift;
      next;
    } else {
      push @args, $arg;
    }
  }
  my $topDir = $lfnDir;
  $lfnDir .= "/$packageName/$version";

  my $lfn = "$lfnDir/$platform";

  $self->{CATALOGUE}->{CATALOG}->isFile($lfn)
    and $self->info("The package $lfn already exists")
    and return;
  $self->{CATALOGUE}->execute("mkdir", "-p", $lfnDir)
    or $self->info("Error creating the directory $lfnDir")
    and return;
  $self->{CATALOGUE}->execute("addTag", "$topDir/$packageName", "PackageDef")
    or $self->info("Error creating the tag definition")
    and return;
  $self->{CATALOGUE}->execute("add", $lfn, $tar, $se)
    or $self->info("Error adding the file $lfn from $tar $se")
    and return;

  if (@args) {
    if (!$self->{CATALOGUE}->execute("addTagValue", $lfnDir, "PackageDef", @args)) {
      $self->info("Error defining the metainformation of the package");
      $self->{CATALOGUE}->execute("rm", "-rf", $lfn);
      return;
    }
  }
    $lfn =~ s{//+}{/}g;;
     
  
#####################################################################

 my @packages;
 my $org="$self->{CONFIG}->{ORG_NAME}";

    if ($lfn =~ m{^$self->{CONFIG}->{USER_DIR}/?./([^/]*)/packages/([^/]*)/([^/]*)/([^/]*)$}) {
      push @packages,{'fullPackageName'=> "$1\@${2}::$3",
                      packageName=>$2,
                      username=>$1,
                     packageVersion=>$3,
                      platform=>$4,
                      lfn=>$lfn};
     
    }elsif ($lfn =~ m{^/$org/packages/([^/]*)/([^/]*)/([^/]*)$}) {
      push @packages,{'fullPackageName'=> "VO_\U$org\E\@${1}::$2",
                     packageName=>$1,
                      username=>"VO_\U$org\E",
                      packageVersion=>$2,
                      platform=>$3,
                      lfn=>$lfn};
     }else {
      $self->info("Don't know what to do with $lfn");
    }


  $self->info("PackMan === READY TO INSERT @packages DB = $self->{DB} ===\n");
#####################################################################
  $self->{DB}->insert('PACKAGES', @packages) or return;

  $self->info("Package $lfn added!!");

 my $dir3 = ($self->{CONFIG}->{PACKMAN_INSTALLDIR} ||  "$ENV{ALIEN_HOME}/packages");
 my $file = "$dir3/alien_list_packages_*";
 $self->info("After define removing the $file==");
 system("rm -f $file");
  # $self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->do("update ACTIONS set todo=1 where action='PACKAGES'");
  #$self->f_packman("recompute");
  return 1;
}

sub undefinePackage {
  my $self        = shift;
  my $packageName = shift;
  my $version     = shift;
  my $message     = "";
  $self->info("Undefining a package");
  $packageName or $message .= "missing Package Name";
  $version     or $message .= "missing version";

  $message and $self->info("Error: $message", 100) and return;

  my $arguments = join(" ", @_);
  my $sys1 = `uname -s`;
  chomp $sys1;
  my $sys2 = `uname -m`;
  chomp $sys2;
  my $platform = "$sys1-$sys2";
  if (($arguments =~ s{-?-platform\s+(\S+)}{})) {
    $platform = $1;
    @_ = split(" ", $arguments);
  }

  my $lfnDir = $self->{CATALOGUE}->{CATALOG}->GetHomeDirectory() . "/packages/$packageName/$version";
  $arguments =~ s{-vo\s}{} and $lfnDir = "/" . lc($self->{CONFIG}->{ORG_NAME}) . "/packages/$packageName/$version";
  my $lfn = "$lfnDir/$platform";

  $self->{CATALOGUE}->{CATALOG}->isFile($lfn)
    or $self->info("The package $lfn doesn't exist")
    and return;
  $self->{CATALOGUE}->execute("rm", $lfn)
    or $self->info("Error removing $lfn")
    and return;

   $lfn =~ s{//+}{/}g;;
   $self->{DB}->delete('PACKAGES', "lfn = '$lfn'");
   $self->info("Package $lfn undefined!!");

    my $dir = ($self->{CONFIG}->{PACKMAN_INSTALLDIR} || "$ENV{ALIEN_HOME}/packages");
    my $file = "$dir/alien_list_packages_*";
    $self->info("After undefine removing the $file");
    system("rm -f $file");

 return 1;
}

#in
sub printPackages {
  my $self       = shift;
  my $options    = shift || {};
  my @packages   = @_;
  my $silent     = 0;
  my $returnhash = 0;
  if ($options->{input}) {
    grep (/-s/, @{$options->{input}}) and $silent     = 1;
    grep (/-z/, @{$options->{input}}) and $returnhash = 1;
  }

  #remove the status
  my $status  = shift @packages;
  my $message = "The PackMan has the following packages";
  $options->{text} and $message .= "$options->{text}";
  $silent or $self->info(join("\n\t", "$message:", @packages));

  if ($returnhash) {
    my @hashresult;
    map {
      my $newhash = {};
      my ($user, $package) = split '@', $_;
      $newhash->{user}    = $user;
      $newhash->{package} = $package;
      push @hashresult, $newhash;
    } @packages;
    return @hashresult;
  }

  return $status, @packages;
}

sub getDependencies {
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;

  my $cacheName="dep_package_${package}_${version}_${user}";

  my $cache=AliEn::Util::returnCacheValue($self, $cacheName);
  if ($cache and $cache->[0]) {
    $self->info( "Returning the value from the cache $cacheName (@$cache)");
    return (@$cache);
  }

  my ($lfn, $info) = $self->findPackageLFN($user, $package, $version);

  AliEn::Util::setCacheValue($self, $cacheName, [1,$info]);
  $self->info("Giving back the dependencies of $package");

  return (1, $info);
}

=item C<getInstallLog($user,$package, $version,)>

Gets the installation log of the package

=cut

sub getInstallLog{
  my $self = shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $options=shift;

     my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);

     if (!$lfn or $lfn < 0) {
        return $info ;
     }

     $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
    and ($version)=($1);
  my $logFile="$self->{INSTALLDIR}/$user.$package.$version.InstallLog";

  if ($options){
    $logFile= "$self->{CONFIG}->{LOG_DIR}/packman/$package.$version.$options.$self->{CONFIG}->{HOST}";
  }

  open (FILE, "<$logFile" ) or die ("Error opening $logFile\n");
  my @content=<FILE>;
  close FILE;
  $self->{LOGGER}->info( "Returning the file");
  return join("", @content);
}



sub isPackageInstalled {
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;

  my ($lfn, $info)=$self->findPackageLFN($user, $package, $version);

  $version or $lfn =~ /\/([^\/]*)\/[^\/]*$/
    and ($version)=($1);

  my $vo=$self->{CONFIG}->{ORG_NAME};
  $lfn =~ m{^/$vo/packages/}i and $user=uc("VO_$vo");

  $self->existsPackage($user, $package, $version,$info)  or
    $self->info( "$$ The package is not installed :D") and 
      return ;
  return (1, $lfn, $info, $version)
}

sub findOldPackages {
  my $self=shift;
  open (FILES, "find  -name .alien_last_used|") or
    $self->info("Error doing the find") and return;
  my @files=<FILES>;
  close FILES;
  my @list;
  my $now=time;
  foreach my $file (@files){
    chomp $file;
    $self->info("Checking if the package '$file' is old");
    my (@info)=stat $file;


    if  ($info[9]+3600*24*7 <$now) {
      $self->info( "The file $file hasn't been accessed in one week");
      $file =~ m{/([^/]*)/([^/]*)/([^/]*)/\.alien_last_used$} or
                 print "Error getting the information out of the link\n" and next;
      my ($user, $package, $version)=($1,$2,$3);

      push @list, "$user\@${package}::$version";
    }
  }
  $self->info("Returning @list");
  return @list;
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
  chmod(0750, $file);

  my @todo=($file, $dir);
  $depConf =~ s{^\s*}{};
  $depConf =~ s{\s*$}{};

  $depConf and @todo=(split(/ /, $depConf), $file, $dir);
  $self->info( "$$ Calling '@todo'");

  my $error=system(@todo);
  $self->info( "$$ $action done with $error!!");
  $error and die("Error doing the $action!!\n");
  return 1;
}

sub checkDiskSpace {
  my $self=shift;
  my $dir=shift;
  my $requestedSpace=shift;
  my $options=shift || {};

  my $handle=Filesys::DiskFree->new();
  $handle->df_dir($dir);
  my $size=$handle->avail($dir);
  if (! $size){
     $self->info("Probably '$dir' is a link... getting the size in a different way");
     $handle->df();
     $size=$handle->avail($dir);
  }

  #if there is enough space, just install
  if ($requestedSpace <$size){
    $self->debug(3, "There is enough space to install the file");
    return 1;
  }
  $options->{no_clean} and return;
  $self->info("Let's try with some clean up");
  my @packages=$self->findOldPackages();
  foreach my $package (@packages){
    $self->info("Lets's delete $package");
    $package=~ /^(.*)\@([^:]*)::(.*)$/
      or  $self->info("Error getting the info from $package") and next;
    my ($user, $package, $version)=($1,$2,$3);
    $self->removePackage($user, $package, $version);
  }
  $options->{no_clean}=1;
  return $self->checkDiskSpace($dir, $requestedSpace, $options);
}

sub ConfigurePackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $info=shift;

  $self->info( "Configuring the package $package (v $version)");
  my $dir="$self->{INSTALLDIR}/$user/$package/$version";
  chdir $dir or
    return $self->installPackage($user, $package, $version);
  my $sourceFile= ($info->{config} || ".alienEnvironment");
  $info->{path} and $dir.="/$info->{path}";

  my $source="";
  my $dirw="$self->{REALLY_INST_DIR}/$user/$package/$version";
  system("touch $dirw/.alien_last_used");
  if (-f "$dir/$sourceFile"){
    $self->{LOGGER}->info( "PacKMan","Testing if $dir/$sourceFile is executable");
    if (! -x "$dir/$sourceFile") {
      $self->info( "$$ The file wasn't executable");
      chmod 0750 ,"$dir/$sourceFile";
    }
    $source="$dir/$sourceFile $dir ";
  }
  return (1,$source);
}

sub createListFiles {
  my $self=shift;
 # $self->{LIST_FILE_CREATION}
  #  or $self->debug(1, "In fact, we don't do any lists")
  #    and return 1;
  my $options=shift || {};

  my $platform=AliEn::Util::getPlatform($self);
  $self->{PACKMAN_PLATFORM} and $platform=$self->{PACKMAN_PLATFORM};

  my @list=( {function=>"list", arguments=>$platform},
             {function=>"list", arguments=>"all"},
             {function=>"listInstalled", arguments=>""});

  $options->{only_installed} and @list=$list[2];

  foreach my $e (@list){
 #   my $file="$self->{REALLY_INST_DIR}/alien_list_$e->{function}packages_$e->{arguments}";
     my $file="$self->{REALLY_INST_DIR}/alien_$e->{function}_packages_$e->{arguments}";

    if (!-e $file or -z $file){
    $self->info("Making the list of all the $e->{function} packages in $file");

    open (FILE, ">$file")
      or $self->info("Error creating the file $file}") and return;
    #my $fun="getList$e->{function}Packages";
    #my ($ok,@list)=$self->$fun($e->{arguments}, "-force");
    my ($ok,@packages)=$self->f_packman($e->{function}, $e->{arguments}, "-force");
    @packages or $self->info("Error getting the list of  packages") and return;
    print FILE join ("\n", @packages);
    close FILE;
    }
}
  return 1;
}

sub removeLocks{
  my $self=shift;
  open (FILE, "ls $self->{REALLY_INST_DIR}/*.InstallLock 2>/dev/null |") or
    $self->info("Error removing the locks");
  while (<FILE>){
    my $lock=$_;
    $self->info("Ready to remove $lock !");
    if ($lock =~ /^(.*)\.([^\.]*)\.([^\.]*).InstallLock$/){
      $self->info("Removing the directory  $1/$2/$3");
      system ("rm -rf $1/$2/$3");
    }
    system ("rm -f $lock");
  }
  close FILE;

}

sub createLock{
  my $self=shift;
  my ($user, $package, $version)=(shift, shift, shift);

  my $dir="$self->{INSTALLDIR}/$user/$package/$version";
  my $lock="$self->{REALLY_INST_DIR}/$user.$package.$version.InstallLock";
  (-f $lock)
    and return 0;

  (-d $dir) and return 1;

  $self->info("Locking the file $lock");

  open FILE, ">$lock"
    and close FILE
      or die("$$ Error creating $lock");

  return 1;
}

sub removeLock {
  my $self=shift;
  my ($user, $package, $version)=(shift, shift, shift);

  my $lock="$self->{REALLY_INST_DIR}/$user.$package.$version.InstallLock";
  system ("rm", "-rf", $lock);
}

### This is from AliEn::PackMan::Local ###

sub removePackage{
  my $self=shift;
  $self->info( "$$ Removing the package @_");
  my $user=shift;
  my $package=shift;
  my $versionUser=shift;

  my ($done, $lfn, $info, $version)=$self->isPackageInstalled($user,$package,$versionUser);

  $done or return (-1, "Package is not installed");

  my $dir="$self->{REALLY_INST_DIR}/$user/$package/$version";
  if (($dir=~ /\.\./) or ($dir=~ /\s/)) {
    $self->info( "$$ Error: someone is trying to delete another directory '$dir'");
    die("Error trying to delete $dir: this is not the directory where the package is installed\n");
  }

  system ("rm","-rf","$dir") and
    $self->info( "$$ Error deleting the package")
      and die("Error deleting the directory $dir\n");
  $self->info( "$$ Package $package ($version) removed");
  AliEn::Util::deleteCache($self);
  my $dir3 =  ($self->{INSTALLDIR} ||  "$ENV{ALIEN_HOME}/packages");
   
   my $file = "$dir3/alien_listInstalled_packages_";
   $self->info("After remove removing the $file====");
   system("rm -f $file");

  return 1;
}

### This is from AliEn::PackMan::Local ###

sub findPackageLFN{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;

  my $platform=AliEn::Util::getPlatform($self);
  $self->info("Looking for the lfn of $package ($version) for the user $user");

  my $cacheName="lfn_${user}_${package}_${version}";
  my $cache=AliEn::Util::returnCacheValue($self, $cacheName);
  if ($cache and $cache->[0]) {
    $self->info("Returning from the cache $cacheName (@$cache)");
    return @$cache ;
  }

  my $vo_user=uc("VO_$self->{CONFIG}->{ORG_NAME}");
  my $query="SELECT lfn from PACKAGES where packageName=? and (platform=? or platform='source') and (username=? or username=?)";
  my @bind=($package, $platform, $user, $vo_user);
  my @bind_source=($package, $platform, $user, $vo_user);

  if ($version) {
    $query.=" and packageVersion=? ";
    push @bind, $version;
    push @bind_source, $version
  }

  my $result=$self->{DB}->queryColumn($query, undef, {bind_values=>\@bind})
    or die ("Error doing the query $query");

  if (! @$result){
    $self->info("The package doesn't exist for that platform. Let's look for source");
    $result=$self->{DB}->queryColumn($query, undef, {bind_values=>\@bind_source})
      or die ("Error doing the query $query");
  }
  $self->info("We got $#$result and @$result");

  if ($#$result <0 ){
    return -2, "The package $user, $package, $version, $platform doesn't exist";
  }
  my $lfn=$$result[0];
 
  my (@dependencies)=$self->{CATALOGUE}->execute("showTagValue", "-silent",$lfn, "PackageDef");
  my $item={};

  @dependencies and $dependencies[1] and $item = shift @{$dependencies[1]};

  my @info = ($lfn, $item);

  if (  $info[0] eq /^-2$/ ){
    return undef,"The package $package (v $version) does not exist for $platform \n";
  }

  $self->info(Dumper(@info));
  if ( $info[1] =~ /^$/) {
    $self->info("The metadata is empty????");
    $info[1]={};
  }

  AliEn::Util::setCacheValue($self, $cacheName, \@info);
  return @info;
}

sub existsPackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $info=shift;

  $self->debug(2, "Checking if $package is already installed");

  my $dir="$self->{INSTALLDIR}/$user/$package/$version";
  (-d $dir) or return;

###???
  my $lock="$self->{REALLY_INST_DIR}/$user.$package.$version.InstallLock";
  (-f $lock) and $self->info("The lock exists. Someone is installing the package") and return  ;
  #  if (!-d $dir) {
  #    $self->debug("Checking among the VO packages");
  #    $dir="$self->{INSTALLDIR}/VO_\U$self->{CONFIG}->{ORG_NAME}\E/$package/$version";
  #
  #  }
  my $dirw="$self->{INSTALLDIR}/$user/$package/$version";

  $self->debug(2,  "$$ Checking the size of $dir");

  if (system("find $dir -name .alien_last_checked -mtime -1 | grep last_checked > /dev/null 2>&1")){
    my $size="";
    $self->info("****** THE PACKAGE HAS NOT BEEN CHECKED IN THE LAST 24 HOURS***");
    if (-l $dir) {
      $self->info( "This is installed in the common area... let's ignore it for the time being");
    }else {
      open (FILE, "du -s $dir|") or
        $self->info( "Error getting the size of the directory")
          and return;
      $size=<FILE>;
      close FILE;
      $size=~ s/^\s*(\d+)\s+.*$/$1/s;
      $self->info("The size of the package is $size bytes");
      if ( $size eq "0") {
        $self->info( "The size of the package is 0");
        system("rm -rf $dirw");
        return;
      }
    }

    $info and $info->{size} and chomp $info->{size};
    $info->{size} or $info->{size}="";
    $self->debug(2,  "$$ Size $size (has to be $info->{size})");
    if (  $info->{size} and ($size ne $info->{size}) ){
      $self->info( "The size of the package does not correspond (has to be $info->{size} and is $size)");
      system("rm -rf $dirw");
      return;
    }
    if ($info->{min_size}){
      $self->info("Checking the minimum size of the package");
      if ($info->{min_size}>$size){
        $self->info("$$ The package is too small!! It is only $size, and it should be at least $info->{min_size}");
        system("rm -rf $dirw");
        return;
      }
    }
  }

  if ($info->{md5sum}) {
    $self->info( "$$ Checking the md5sum of $info->{executable}");
    chdir $dir;
    system("md5sum -c .alienmd5sum") and
      $self->info( "$$ Error checking the md5sumlist")
        and return;
  }
  $self->info( "The package is already installed (in $dir)!!");
  system("touch", "$dirw/.alien_last_checked");
  return $dir;
}

#################################################################
=item C<testPackage($user,$package,$version)>
Checks if a package is installed, and the environment that it would produce.
It returns: $version=> $version of the package installed
            $info  => dependencies and information of the package
            $list  => The directory where the package is installed
            $environment => environment that will result after configuring the package
=cut
####### VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV #######

sub testPackage{
  my $self=shift;
  $self->info( "Checking if the package is installed: @_");
  my $user=shift;
  my $package=shift;
  my $versionUser=shift;
  my $command=join(" ", @_);
  $command or $command="env";

  my ($done, $lfn, $info, $version)=$self->isPackageInstalled($user,$package,$versionUser);

  $done or  return (-1, "Package is not installed");
  $self->info( "$$ Ok, the package is installed");
  ($done, my $source, my $installDir)=
    $self->installPackage($user, $package, $version);
  $self->info( "$$ We should do $done and $source");
  my $env="";
  if ($source) {
     $self->info( "$$ Lets's call $source $command");
    $env=`$source $command`;
    my $env2=`$source echo "The AliEn Package $package is installed properly"`;
    if ($env2 !~ /The AliEn Package $package is installed properly/s ){
      $self->info( "$$ Warning!!! The package has to source $source, but this script doesn't seem to execute commands");
      $env.="================================================
Warning!!!!
The package is supposed to do: $source
This script will receive as the first argument the directory where the package is installed, and then the command to execute. Please, make sure that the script finishes with a line 
that calls the rest of the arguments (something like \$*).";
    }

  }

  my $dir=$installDir || ".";
  my $md5file="$dir/.alien_md5_list";
  $self->info("And let's check the md5 sum");
  if (not -f "$md5file.sorted"){
    if (not -f "$md5file.lock"){
      system("touch $md5file.lock");
      system("rm", "-rf", $md5file);
      system("find $dir -type f -not -path '/.*' -exec md5sum {} >> $md5file \\;");
      system("sort $md5file > $md5file.sorted");
      system("md5sum $md5file.sorted > $md5file.total");
    }
  }
  if (-f "$md5file.total"){
    if(open (FILE, "<$md5file.total")){
      $env=join("", "And the total md5 is:", <FILE>, $env);
      close FILE;
    }
  }
  my $directory=`ls -la $dir`;
  $self->info( "We have $env");

  my @all = ($version, $info, $directory, $env);
  $self->info("The PackMan returns @all");
  return @all;

}

####### ^^^^^^^^^^^^^^^^^ testPackage ^^^^^^^^^^^^^^^^^^^^^^^ #######

return 1;
