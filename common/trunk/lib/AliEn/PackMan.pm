package AliEn::PackMan;

use AliEn::Config;
use strict;
use vars qw(@ISA);
use Data::Dumper;
use AliEn::Util;
#use AliEn::PackMan::Local;
use Time::HiRes;
push @ISA, 'AliEn::Logger::LogObject';

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = (shift or {});
  bless($self, $class);
  $self->SUPER::new();
  $self->{CONFIG} or $self->{CONFIG} = AliEn::Config->new();
  if ($self->{PACKMAN_METHOD}) {
    $self->info("This packman uses the method $self->{PACKMAN_METHOD}");
    my $name = "AliEn::PackMan::$self->{PACKMAN_METHOD}";
    
    eval "require $name";
    if ($@) {
      $self->info("Error requiring $name: $@");
      return;
    }
    bless($self, $name);
  }
  $self->{SOAP} = AliEn::SOAP->new() or return;
  $self->{LIST_FILE_TTL} or $self->{LIST_FILE_TTL} = 7200;
  $self->initialize(@_) or return;
  
  return $self;
}

sub initialize {
  
  my $self = shift;
  $self->{SOAP_SERVER} or $self->{SOAP_SERVER} = "PackMan";
  
  $self->debug(1, "We will talk to the $self->{SOAP_SERVER}");
  
  return $self;
}


############GetlisInstalledPackages_ 

sub getListInstalledPackages_ {

  my $self=shift;
  my $before = Time::HiRes::time();
  
  $self->info("Checking the packages that we have installed locally");
  my @allPackages=();
  eval {
    my $dir = "$ENV{'ALIEN_HOME'}/packages";
    $self->{CONFIG}->{PACKMAN_INSTALLDIR}  and $dir="$self->{CONFIG}->{PACKMAN_INSTALLDIR}";
    $self->debug(1, "Checking $dir");
    foreach my $user ($self->getSubDir($dir)) {
      $self->debug(1, "Checking $dir/$user");
      foreach my $package ($self->getSubDir("$dir/$user")){
       $self->debug(1, "Checking $dir/$user/$package");
        foreach my $version ($self->getSubDir("$dir/$user/$package")){
         $self->debug(1, "Checking $dir/$user/$package/$version");
          (-f "$dir/$user.$package.$version.InstallLock") and
           $self->debug(1, "The package is being installed") and next;
          push @allPackages, "${user}\@${package}::$version";
        }
      }
    }

  };
  if ($@) {
    $self->info( "$$ We couldn't find the packages ");


   die ($@);
  }
  my $time = Time::HiRes::time() - $before;
  print "The Time is $time\n";
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


############################
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
  ($status, @list) = $self->readPackagesFromFile("alien_list_Installedpackages_");
  
  }
  $self->debug(1, "Asking the PackMan for the packages that it has installed");

  if ($status != 1) {
#   my ($done) = $self->{SOAP}->CallSOAP("PackMan", "getListInstalledPackages", "ALIEN_SOAP_SILENT", @_) or return;
     AliEn::Util::deleteCache($self);
     my $cache=AliEn::Util::returnCacheValue($self, "installedPackages");
     if ($cache){
    $self->info("This is for test the returned cache (@$cache)");
    return (1, @$cache);
    }
     ($status, @list) = $self->getListInstalledPackages_() or return;
     AliEn::Util::setCacheValue($self, "installedPackages", \@list);    
   } elsif ($status < 0) {
      $self->info("Well, the info is old, but it is better than nothing");
      $status = 1;
    }

  $self->debug(2, "The list of installed is @list");
  return $status, @list;

}

sub getListPackages {
  my $self = shift;

  my $platform = AliEn::Util::getPlatform($self);
  grep (/^-all/, @_) and $platform = "all";

  my ($status, @packages) = (0, undef);
  if (!grep (/-force/, @_)) {
    ($status, @packages) = $self->readPackagesFromFile("alien_list_packages_$platform");
  }

  if ($status != 1) {
    $self->info("Asking the $self->{SOAP_SERVER} for the packages that it knows");
    my ($done, @pack);
    eval {
      ($done) = $self->{SOAP}->CallSOAP($self->{SOAP_SERVER}, "getListPackages", @_) or return;
      ($done, @pack) = ($done->result, $done->paramsout);
    };
    if ($@) {
      $self->info("Error contacting the packman: $@");
    }

    if ($done and $done == 1) {
      ($status, @packages) = ($done, @pack);
    } elsif ($status < 0) {
      $self->info("Well, the info is old, but it is better than nothing");
      $status = 1;
    }
  }
  grep (/^-s(ilent)?$/, @_)
    or $self->printPackages({input => \@_}, @packages);
  return $status, @packages;
}

sub readPackagesFromFile {
  my $self = shift;
  my $file = shift;
  

  my $dir = ($self->{CONFIG}->{PACKMAN_INSTALLDIR} || '$ALIEN_HOME/packages');
  $dir =~ s{\$([^/]*)}{$ENV{$1}}g;
  
  $file = "$dir/$file";

  $self->debug(1, "Checking if the file $file exists...");
  use File::stat;
  my $st = stat($file) or return 0;
  $self->debug(2, "Reading from the file $file!");

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
#  return @packages;
}

sub installPackage {
  my $self    = shift;
  my $user    = shift;
  my $package = shift;
  my $version = shift;

  $self->info("Asking the PackMan to install");

  my $result;
  my $retry = 5;
  while (1) {
    $self->info("Asking the package manager to install $package as $user");

    $result = $self->{SOAP}->CallSOAP("PackMan", "installPackage", $user, $package, $version) and last;
    my $message = $AliEn::Logger::ERROR_MSG;
    $self->info("The reason it wasn't installed was $message");
    $message =~ /Package is being installed/ or $retry--;
    $retry or last;
    $self->info("Let's sleep for some time and try again");
    sleep(30);
  }
  if (!$result) {
    $self->info("The package has not been instaled!!");
    return;
  }
  my ($ok, $source) = $self->{SOAP}->GetOutput($result);
  $self->info("The PackMan returned '$ok' and '$source'");
  return ($ok, $source);

}

sub f_packman_HELP {
  return "packman: talks to the Package Manager. By default, it talks to the closest PackMan. 
Usage:

   packman [-name <PackManName>] [-everywhere] <command> [<command options and arguments>]

Global options:
     -everywhere:  Do the command in all the packmans currently available
     -name <PackManName>' to talk to a specific instance. By default, it will talk to the closest 

Possible commands:
\tpackman list:\treturns all the packages defined in the system
\tpackman listInstalled:\treturns all the packages that the service has installed
\tpackman test <package>: tries to configure a package. Returns the metainformation associated with the package, a view of the directory where the package is installed, and an environment that the package would set
\tpackman install <package>: install a package (and all its dependencies) in the local cache of the PackMan
\tpackman installLog <package>: get the installation log of the package
\tpackman dependencies <name>: gives the list of dependencies of a package
\tpackman remove  <package>: removes a package from the local cache
\tpackman define <name> <version> <tar file> [<package options>]
\tpackman undefine <name> <version>
\tpackman recompute: (only for admin) recompute the list of packages.
\tpackman synchronize:\tinstalls all the existing packages, and removes the packages locally installed that do not exist anymore

Package options: -platform source, else the default for the local system is used
                  post_install <script> where the script should be given with the full catalogue path
The format of the string <package> is:
    [<user>\@]<PackageName>[::PackageVersion}
For instance, 'ROOT', 'ROOT::4.1.3', 'psaiz\@ROOT', 'psaiz\@ROOT::4.1.2' comply with the format of <package>
";
}

sub f_packman {
  my $self = shift;
  $self->debug(1, "Talking to the PackMan: @_");
  
  my $silent     = grep (/^-s$/,           @_);
  my $returnhash = grep (/^-z$/,           @_);
  my $allPackMan = grep (/^-everywhere$/i, @_);
  my @arg        = grep (!/^-z$/,          @_);
  @arg = grep (!/^-s$/,          @arg);
  @arg = grep (!/^-everywhere$/, @arg);

  my $string = join(" ", @arg);
  
  
  my $serviceName = "PackMan";

  #FIXME: TEST
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
    $self->{SOAP}->Connect(
      { address => "http://$done->{HOST}:$done->{PORT}",
        uri     => "AliEn/Service/PackMan",
        name    => "PackMan_$name",
        options => [ timeout => 5000 ]
      }
    ) or return;
    $serviceName = "PackMan_$name";
    
    @arg = split(" ", $string);
  }

  my $operation = shift @arg;
  $operation
    or $self->info($self->f_packman_HELP(), 0, 0)
    and return;
  my $soapCall;
  my $requiresPackage = 0;

  my $local = 0;
  my $optionsLocal = {PACKMAN_METHOD => "Local"};
  if (grep (/^-local$/i, @arg)) {
    $local = 1;
    @ARGV  = @arg;
    Getopt::Long::Configure("pass_through");
    Getopt::Long::GetOptions($optionsLocal, "local", "dir=s");
    Getopt::Long::Configure("default");

    #      or $self->info("Error checking the options of packman -local") and return;
    $optionsLocal->{dir} and $optionsLocal->{INST_DIR} = $optionsLocal->{dir};
    @arg = @ARGV;
  }
  if ($operation =~ /^l(ist)?$/) {
    $soapCall  = "getListPackages";
    $operation = "list";
    $serviceName eq "PackMan" and $direct = 1;
    $local = 0;
  } elsif ($operation =~ /^listI(nstalled)?$/) {
    $soapCall  = "getListInstalledPackages";
    $operation = "listInstalled";
    $serviceName eq "PackMan" and $direct = 1;
  } elsif ($operation =~ /^t(est)?$/) {
    $requiresPackage = 1;
    $soapCall        = "testPackage";
    $operation       = "test";
  } elsif ($operation =~ /^i(nstall)?$/) {
    $requiresPackage = 1;
    $soapCall        = "installPackage";
    $operation       = "install";
  } elsif ($operation =~ /^r(emove|m)?$/) {
    $requiresPackage = 1;
    $soapCall        = "removePackage";
    $operation       = "remove";
  } elsif ($operation =~ /^d(efine)?$/) {
    return $self->definePackage(@arg);
  } elsif ($operation =~ /^u(ndefine)?$/) {
    return $self->undefinePackage(@arg);
  } elsif ($operation =~ /^dependencies$/) {
    $soapCall        = "getDependencies";
    $requiresPackage = 1;
  } elsif ($operation =~ /^installLog?$/) {
    $soapCall        = "getInstallLog";
    $requiresPackage = 1;
  } elsif ($operation =~ /^recompute?$/) {
    $soapCall = "recomputeListPackages";
    $self->info("And deleting any local caches");
    my $dir = ($self->{CONFIG}->{PACKMAN_INSTALLDIR} || '$ALIEN_HOME/packages');
    
    system("rm -f $dir/alien_list_*");
  } elsif ($operation =~ /^synchronize$/) {
    return $self->synchronizePackages(@_);
  } else {
    $self->info("I'm sorry, but I don't understand $operation");
    $self->info($self->f_packman_HELP(), 0, 0);
    return;
  }

  if ($requiresPackage) {
    my $package = shift @arg;
    $package
      or $self->info("Error not enough arguments in 'packman $operation")
      and $self->info($self->f_packman_HELP(), 0, 0)
      and return;
    my $version = "";
    my $user    = $self->{CATALOGUE}->{CATALOG}->{ROLE};
    $package =~ s/::([^:]*)$//  and $version = $1;
    $package =~ s/^([^\@]*)\@// and $user    = $1;
    if ($operation =~ /^r(emove|m)?$/) {
      if ($user ne $self->{CATALOGUE}->{CATALOG}->{ROLE}) {

        #	$self->{CATALOGUE}->{CATALOG}->{ROLE} eq "admin" or
        #	  $self->info( "You can't uninstall the package of someone else") and return;
      }
    }
    @arg = ($user, $package, $version, @arg);
  }

  my (@result, $done);
  if ($local) {
    $self->info("Doing the operation '$soapCall' ourselves");

    my $p = AliEn::PackMan->new($optionsLocal)
      or $self->info("Error getting an instance of packman")
      and return;
    return $p->$soapCall(@arg);

  } elsif ($direct) {
    $self->info("Calling directly $soapCall (@_)");
    return $self->$soapCall(@_);
    
  } else {
    $silent or $self->info("Let's do $operation (@arg)");
    my $result = $self->{SOAP}->CallSOAP($serviceName, $soapCall, @arg);

    if (!$result) {
      my $print = 0;
      my $error = $self->{LOGGER}->error_msg();
      $error =~ /Package is being installed/ or $print = 1 + $print;
      $error or $error = "Error talking to the PackMan";

      ($soapCall eq "installPackage")
        and $error =~ /Package is being installed/
        and $error =
"Don't PANIC!! The previous message is not a real error. The package is being installed.\n\t\tYou can use installLog to check the status of the installation";

      #If the error message is that the package is being installed, do not print an error

      $self->info($error);

      return;
    }
    ($done, @result) = $self->{SOAP}->GetOutput($result);
    $done
      or $self->info("Error asking for the packages")
      and return;
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
  } elsif ($operation =~ /^t(est)?$/) {
    $silent
      or $self->info(
      "The package (version $done) has been installed properly\nThe package has the following metainformation\n"
        . Dumper(shift @result));
    my $list = shift @result;
    $silent or $self->info("This is how the directory of the package looks like:\n $list");
    my $env = shift @result;
    $env and $self->info("The package will configure the environment to something similar to:\n$env");
  } elsif ($operation =~ /^r(emove|m)$/) {
    $self->info("Package removed!!\n");
  } elsif ($operation =~ /^dependencies$/) {
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

sub synchronizePackages {
  my $self = shift;
  my $cmd  = shift;
  $self->info("Ready to synchronize the packages with the catalogue (@_)");
  my @arg             = @_;
  my $optionsPackages = {};
  @ARGV = @arg;

  Getopt::Long::Configure("pass_through");
  Getopt::Long::GetOptions($optionsPackages, "packages=s");
  Getopt::Long::Configure("default");

  #      or $self->info("Error checking the options of packman synchronize") and return;
  @arg = @ARGV;
  $optionsPackages->{packages} and $self->info("Doing only the packages '$optionsPackages->{packages}'");
  my $pattern = "(" . join(")|(", split(/,/, $optionsPackages->{packages} || "")) . ")";

  my ($ok1, @packages) = $self->getListPackages("-s");
  $ok1 or self->info("Error getting the list of packages") and return;
  my ($ok, @installed) = $self->f_packman("listInstalled", "-s", @arg);
  $ok or self->info("Error getting the list of packages") and return;

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

  # $self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->do("update ACTIONS set todo=1 where action='PACKAGES'");
  $self->f_packman("recompute");
  $self->info("Package $lfn added!!");
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

  #$self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->do("update ACTIONS set todo=1 where action='PACKAGES'");
  $self->f_packman("recompute");
  $self->info("Package $lfn undefined!!");
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

return 1;


