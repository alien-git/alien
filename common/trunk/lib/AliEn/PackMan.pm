package AliEn::PackMan;

use AliEn::Config;
use strict;
use vars qw(@ISA);
use Data::Dumper;

push @ISA, 'AliEn::Logger::LogObject';

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = ( shift or {} );
  bless( $self, $class );
  $self->SUPER::new();
  $self->{CONFIG} or $self->{CONFIG}=AliEn::Config->new();
  if ($self->{PACKMAN_METHOD}){
    $self->info("This packman uses the method $self->{PACKMAN_METHOD}");
    my $name="AliEn::PackMan::$self->{PACKMAN_METHOD}";
    eval "require $name";
    if ($@){
      $self->info("Error requiring $name: $@");
      return;
    }
    bless($self, $name);
  }
  $self->{SOAP}=AliEn::SOAP->new() or return;

  $self->initialize() or return;
  return $self;
}

sub initialize{
  my $self=shift;

  $self->{SOAP_SERVER} or $self->{SOAP_SERVER}="PackMan";
  $self->info("We will talk to the $self->{SOAP_SERVER}");

  return $self;
}

sub getListInstalledPackages{
  my $self=shift;

  my @packages=$self->getListInstalled_Internal(@_) or return;
  grep (/^-s(ilent)?$/, @_) or 
    $self->printPackages({input=>\@_, text=>" installed"}, @packages);
  return @packages;
}

#This is the method that has to be overwritten in other implemetations
sub getListInstalled_Internal {
  my $self=shift;
  $self->debug(1,"Asking the PackMan for the packages that it has installed");
  my ($done)=$self->{SOAP}->CallSOAP("PackMan","getListInstalledPackages","ALIEN_SOAP_SILENT", @_) or return;
  my $status=$done->result;
  my @list=$done->paramsout;
  $self->debug(2, "The list of installed is @list");
  return  $status, @list;

}

sub getListPackages {
  my $self=shift;
  $self->debug(1,"Asking the $self->{SOAP_SERVER} for the packages that it knows");
  my ($done)=$self->{SOAP}->CallSOAP($self->{SOAP_SERVER},"getListPackages", @_) or return;

  my @packages=($done->result, $done->paramsout);
  grep (/^-s(ilent)?$/, @_) or 
    $self->printPackages({input=>\@_},@packages);
  return @packages;
}


sub installPackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
 

  $self->info("Asking the PackMan to install");
  
  my $result;
  my $retry=5;
  while (1) {
    $self->info("Asking the package manager to install $package as $user");
      
    $result=$self->{SOAP}->CallSOAP("PackMan", "installPackage", $user, $package, $version) and last;
    my $message=$AliEn::Logger::ERROR_MSG;
    $self->info("The reason it wasn't installed was $message");
    $message =~ /Package is being installed/ or $retry--;
    $retry or last;
    $self->info("Let's sleep for some time and try again");
    sleep (30);
  }
  if (! $result){
    $self->info("The package has not been instaled!!");
    return;
  }
  my ($ok, $source)=$self->{SOAP}->GetOutput($result);
  $self->info("The PackMan returned '$ok' and '$source'");
  return ($ok, $source);

}


sub f_packman_HELP {return  "packman: talks to the Package Manager. By default, it talks to the closest PackMan. 
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
Package options: -platform source, else the default for the local system is used
                  post_install <script> where the script should be given with the full catalogue path
The format of the string <package> is:
    [<user>\@]<PackageName>[::PackageVersion}
For instance, 'ROOT', 'ROOT::4.1.3', 'psaiz\@ROOT', 'psaiz\@ROOT::4.1.2' comply with the format of <package>
";
}


sub f_packman {
  my $self=shift;
  $self->debug(1, "Talking to the PackMan: @_");
  my $silent     = grep ( /^-s$/, @_ );
  my $returnhash = grep ( /^-z$/, @_ );
  my $allPackMan = grep (/^-everywhere$/i, @_);
  my @arg        = grep ( !/^-z$/, @_ );
  @arg        = grep ( !/^-s$/, @arg );
  @arg        = grep ( !/^-everywhere$/, @arg );

  my $string=join(" ", @arg);
  my $serviceName="PackMan";



  #FIXME: TEST
  $self->info("*** calling PackMan with arguments $string");
  if ($allPackMan){
    $self->info("We are going to call all the packman");
    my $response =$self->{SOAP}->CallSOAP("IS", "getAllServices", "PackMan")
     or return;
    $response = $response->result;
    #    print Dumper($response);
    my @n=split (/###/, $response->{NAMES});
     $silent and $string.=" -s";
    foreach my $n (@n){
      $self->info("Checking $n");

      $self->f_packman("-name $n $string");
    }
    return 1;
  }



  my $direct = 0;
  $string =~ s{-?-silent\s+}{} and $silent=1;
  if ( $string =~ s{-?-n(ame)?\s+(\S+)}{} ){
    my $name=$2;
    $self->info( "Talking to the packman $name");

    my $done=$self->{CONFIG}->CheckServiceCache("PACKMAN", $name)
      or $self->info( "Error looking for the packman $name") 
	and return;
    $self->{SOAP}->Connect({address=>"http://$done->{HOST}:$done->{PORT}",
			    uri=>"AliEn/Service/PackMan",
			    name=>"PackMan_$name",
			    options=>[timeout=>5000]}) or return;
    $serviceName="PackMan_$name";
    @arg=split (" ", $string);
  }

  my $operation=shift @arg;
  $operation or 
    $self->info( $self->f_packman_HELP(),0,0) and return;
  my $soapCall;
  my $requiresPackage=0;
  if ($operation =~ /^l(ist)?$/){
    $soapCall="getListPackages";
    $operation="list";
    $serviceName eq "PackMan" and $direct=1;
    if (grep (/^-local$/i, @arg)){
      return $self->getListPackagesLocally(grep (! /^-local$/i, @arg));
    }
  } elsif  ($operation =~ /^listI(nstalled)?$/){
    $soapCall="getListInstalledPackages";
    $operation="listInstalled";
    $serviceName eq "PackMan" and $direct=1;
    if (grep (/^-local$/i, @arg)){
      return $self->getListInstalledPackagesLocally(grep (! /^-local$/i, @arg));
    }
  } elsif  ($operation =~ /^t(est)?$/){
    $requiresPackage=1;
    $soapCall="testPackage";
    $operation="test";
    if (grep (/^-local$/i, @arg)){
      return $self->testPackageLocally(grep (! /^-local$/i, @arg));
    }
  } elsif ($operation =~ /^i(nstall)?$/){
    $requiresPackage=1;
    $soapCall="installPackage";
    $operation="install";
    if (grep (/^-local$/i, @arg)){
      return $self->installPackageLocally(grep (! /^-local$/i, @arg));
    }
  } elsif ($operation =~ /^r(emove|m)?$/){
    $requiresPackage=1;
    $soapCall="removePackage";
    $operation="remove";
    if (grep (/^-local$/i, @arg)){
      return $self->removePackageLocally(grep (! /^-local$/i, @arg));
    }
  } elsif ($operation =~ /^d(efine)?$/){
    return $self->definePackage(@arg);
  } elsif ($operation =~ /^u(ndefine)?$/){
    return $self->undefinePackage(@arg);
  } elsif ($operation =~ /^dependencies$/){
    $soapCall="getDependencies";
    $requiresPackage=1;
  } elsif ($operation =~ /^installLog?$/){
    $soapCall="getInstallLog";
    $requiresPackage=1;
  } elsif ($operation =~ /^recompute?$/){
    $soapCall="recomputeListPackages";
  } else {
    $self->info( "I'm sorry, but I don't understand $operation");
    $self->info( $self->f_packman_HELP(),0,0);
    return;
  }

  if ($requiresPackage) {
    my $package=shift @arg;
    $package or 
      $self->info( "Error not enough arguments in 'packman $operation") 
	and $self->info( $self->f_packman_HELP(),0,0) 
	  and return;
    my $version="";
    my $user=$self->{CATALOGUE}->{CATALOG}->{ROLE};
    $package =~ s/::([^:]*)$// and $version=$1;
    $package =~ s/^([^\@]*)\@// and $user=$1;
    if  ($operation =~ /^r(emove|m)?$/){
      if ($user ne $self->{CATALOGUE}->{CATALOG}->{ROLE}) {
	$self->{CATALOGUE}->{CATALOG}->{ROLE} eq "admin" or 
	  $self->info( "You can't uninstall the package of someone else") and return;
      }
    }
    @arg=($user, $package, $version, @arg);
  }

  my (@result, $done);
#FIXME
  if ($direct){
    $self->info("Calling directly $soapCall (@_)");
    return $self->$soapCall(@_);
  }else{
    $silent or $self->info( "Let's do $operation (@arg)");
    my $result=$self->{SOAP}->CallSOAP($serviceName, $soapCall,@arg);

    if (!$result){
      my $print=0;
      my $error=$self->{LOGGER}->error_msg();
      $error =~ /Package is being installed/  or $print=1+$print;
      $error or $error="Error talking to the PackMan";

      ($soapCall eq "installPackage" ) and 
	$error=~ /Package is being installed/
	  and $error="Don't PANIC!! The previous message is not a real error. The package is being installed.\n\t\tYou can use installLog to check the status of the installation";
      #If the error message is that the package is being installed, do not print an error

      $self->info(  $error );
      
      return;
    }
    ($done, @result)=$self->{SOAP}->GetOutput($result);
    $done or $self->info( "Error asking for the packages")
      and return;
  }
  my $return=1;
  if ($operation =~ /^list(installed)?/i){
    my $message="The PackMan has the following packages";
    $1 and $message.=" $1";
    $silent or $self->info( join("\n\t", "$message:",@result));

    if ($returnhash) {
	my @hashresult;
	map { 
          my $newhash = {}; 
          my ($user, $package) = split '@', $_; 
          $newhash->{user} = $user; 
          $newhash->{package} = $package ; 
          push @hashresult, $newhash;
        } @result;
	return @hashresult;
    }

    $return=\@result;
  } elsif  ($operation =~ /^t(est)?$/){
    $silent or $self->info( "The package (version $done) has been installed properly\nThe package has the following metainformation\n". Dumper(shift @result));
    my $list=shift @result;
    $silent or $self->info("This is how the directory of the package looks like:\n $list");
    my $env=shift @result;
    $env and $self->info("The package will configure the environment to something similar to:\n$env");
  } elsif ($operation =~ /^r(emove|m)$/){
    $self->info("Package removed!!\n");
  } elsif ($operation =~ /^dependencies$/){
    my $info=shift @result;
    $info or $self->info("The package doesn't have any dependencies\n") and return;
    $self->info("The information of the package is:");
    foreach ( keys %$info){
      /entryId/ and next;
      $info->{$_} and $self->info("\t$_:\t\t$info->{$_}",0,0);
    }
  } elsif ($operation =~ /^installLog$/){
    $self->info("The installation log is\n
=========================================================
$done
=========================================================
\n");
  }

  return $return;
}

sub definePackage{
  my $self=shift;
  my $packageName=shift;
  my $version=shift;
  my $tar=shift;
  my $message="";


  $self->info( "Adding a new package");

  $packageName or $message.="missing Package Name";
  $version or $message.="missing version";
  $tar or $message.="missing tarfile";
  (-f $tar) or $message.="the file $tar doesn't exist";

  $message and $self->info( "Error: $message", 100) and return;

  my @args=();
  my $se="";
  my $lfnDir=lc($self->{CATALOGUE}->{CATALOG}->GetHomeDirectory()."/packages");
  my $sys1 = `uname -s`;
  chomp $sys1;
  my $sys2 = `uname -m`;
  chomp $sys2;
  my $platform="$sys1-$sys2";
  while (my $arg=shift){
    if ($arg=~ /^-?-se$/ ) {
      $se=shift;
      next;
    } 
    if ($arg=~ /^-vo$/) {
      $lfnDir=lc("/$self->{CONFIG}->{ORG_NAME}/packages");
      next;
    }
    if ($arg=~ /^-?-platform$/) {
     
      $platform=shift;
      next;
    }else {
      push @args, $arg;
    }
  }
  my $topDir=$lfnDir;
  $lfnDir.="/$packageName/$version";

  my $lfn="$lfnDir/$platform";
  $self->{CATALOGUE}->{CATALOG}->isFile($lfn) and
    $self->info( "The package $lfn already exists") and return;
  $self->{CATALOGUE}->execute("mkdir", "-p", $lfnDir)
    or $self->info( "Error creating the directory $lfnDir")
      and return;
  $self->{CATALOGUE}->execute("addTag", "$topDir/$packageName", "PackageDef")
    or $self->info( "Error creating the tag definition")
      and return;
  $self->{CATALOGUE}->execute("add", $lfn, $tar, $se) 
    or $self->info( "Error adding the file $lfn from $tar $se")
      and return;
  if (@args) {
    if (!$self->{CATALOGUE}->execute("addTagValue",$lfnDir, "PackageDef", @args)){
      $self->info( "Error defining the metainformation of the package");
      $self->{CATALOGUE}->execute("rm", "-rf", $lfn);
      return;
    }
  }
  $self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->do("update ACTIONS set todo=1 where action='PACKAGES'");
  $self->info( "Package $lfn added!!");
  return 1;
}
sub undefinePackage{
  my $self=shift;
  my $packageName=shift;
  my $version=shift;
  my $message="";
  $self->info( "Undefining a package");
  $packageName or $message.="missing Package Name";
  $version or $message.="missing version";

  $message and $self->info( "Error: $message", 100) and return;

  my $arguments=join (" ", @_);
  my $sys1 = `uname -s`;
  chomp $sys1;
  my $sys2 = `uname -m`;
  chomp $sys2;
  my $platform="$sys1-$sys2";
  if (($arguments=~ s{-?-platform\s+(\S+)}{})){
    $platform=$1;
    @_=split (" ",$arguments);
  }

  my $lfnDir=$self->{CATALOGUE}->{CATALOG}->GetHomeDirectory()."/packages/$packageName/$version";
  $arguments =~ s{-vo\s}{} and $lfnDir="/".lc($self->{CONFIG}->{ORG_NAME})."/packages/$packageName/$version";
  my $lfn="$lfnDir/$platform";

  $self->{CATALOGUE}->{CATALOG}->isFile($lfn) or
    $self->info( "The package $lfn doesn't exist") and return;
  $self->{CATALOGUE}->execute("rm", $lfn)
    or $self->info( "Error removing $lfn")
      and return;

  $self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->do("update ACTIONS set todo=1 where action='PACKAGES'");
  $self->info( "Package $lfn undefined!!");
  return 1;
}
#in
sub printPackages{
  my $self=shift;
  my $options=shift || {};
  my @packages=@_;
  my $silent=0;
  my $returnhash=0;
  if ($options->{input}){
    grep (/-s/, @{$options->{input}}) and $silent=1;
    grep (/-z/, @{$options->{input}}) and $returnhash=1;
  }
  #remove the status
  my $status=shift @packages;
  my $message="The PackMan has the following packages";
  $options->{text} and $message.="$options->{text}";
  $silent or $self->info( join("\n\t", "$message:",@packages));
  
  if ($returnhash) {
    my @hashresult;
	map { my $newhash = {}; my ($user, $package) = split '@', $_; $newhash->{user} = $user; $newhash->{package} = $package ; push @hashresult, $newhash;} @packages;
	return @hashresult;
    }

  return $status, @packages;
}

sub installPackageLocally{
  my $self=shift;
  my $package=shift;
  $package or
    $self->info( "Error not enough arguments in 'packman install -local")
      and $self->info( $self->f_packman_HELP(),0,0)
        and return;
  my $version="";
  my $user=$self->{CATALOGUE}->{CATALOG}->{ROLE};
  $package =~ s/::([^:]*)$// and $version=$1;
  $package =~ s/^([^\@]*)\@// and $user=$1;

  $self->info("Ready to install the package '$package' locally");
  my $p=AliEn::PackMan->new({PACKMAN_METHOD=>"Local"}) or
    $self->info("Error getting an instance of packman") and return;

  my ($ok, $source)=$p->installPackage($user, $package, $version, undef,
                                       {NO_FORK=>1});
  $self->info("Did it work??? $ok, and '$source'");
  ($ok eq '-1') and return;
  $self->info("Yes!!!!!");
  return ($ok, $source);

}


sub removePackageLocally{
  my $self=shift;
  my $package=shift;
  $package or 
    $self->info( "Error not enough arguments in 'packman remove -local") 
      and $self->info( $self->f_packman_HELP(),0,0) 
	and return;
  my $version="";
  my $user=$self->{CATALOGUE}->{CATALOG}->{ROLE};
  $package =~ s/::([^:]*)$// and $version=$1;
  $package =~ s/^([^\@]*)\@// and $user=$1;

  $self->info("Ready to remove the package '$package' locally");
  my $p=AliEn::PackMan->new({PACKMAN_METHOD=>"Local"}) or 
    $self->info("Error getting an instance of packman") and return;

  my ($ok)=$p->removePackage($user, $package, $version);

  $self->info("Did it work??? $ok");
  ($ok eq '-1') and return;
  $self->info("Yes!!!!!");
  return ($ok);

}



sub testPackageLocally{
  my $self=shift;
  my $package=shift;
  my $silent = shift;
  $package or
    $self->info( "Error not enough arguments in 'packman remove -local")
      and $self->info( $self->f_packman_HELP(),0,0)
        and return;
  my $version="";
  my $user=$self->{CATALOGUE}->{CATALOG}->{ROLE};
  $package =~ s/::([^:]*)$// and $version=$1;
  $package =~ s/^([^\@]*)\@// and $user=$1;

  $silent or $self->info("Ready to test the package '$package' locally");
  my $p=AliEn::PackMan->new({PACKMAN_METHOD=>"Local"}) or
    $self->info("Error getting an instance of packman") and return;

  my @result=$p->testPackage($user, $package, $version);

  @result or $self->info("the test failed") and return;
  
  my $vers = shift @result;
  $silent or $self->info( "The package (version $vers) has been installed properly\nThe package has the following metainformation\n". Dumper(shift @result));
  my $list=shift @result;
  $silent or $self->info("This is how the directory of the package looks like:\n $list");
  my $env=shift @result;
  $env and $self->info("The package will configure the environment to something similar to:\n$env");
  return;

}


sub getListInstalledPackagesLocally{
  my $self=shift;
  my $silent = shift;

  $silent or $self->info("Getting the list of packages locally installed");
  my $p=AliEn::PackMan->new({PACKMAN_METHOD=>"Local"}) or
    $self->info("Error getting an instance of packman") and return;

  my ($ok, @packages)=$p->getListInstalled_Internal();

  $ok or $self->info("Error getting the list of installed packages");

#  $silent or $self->info("The following packages are installed on the local cluster: @packages");

  $self->printPackages({input=>\@_, text=>" installed"}, ($ok, @packages));
  return @packages;

}

return 1;
