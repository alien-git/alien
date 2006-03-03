package AliEn::Service::Licensed;

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
use AliEn::Database::Licensed;
use AliEn::SE::Methods;
use vars qw(@ISA $DEBUG);

@ISA=qw(AliEn::Service);


$DEBUG=0;

use strict;
my $levels={""=>0,
	    "Package is being installed on the server"=>1,
	    "Checking if client can see server installation"=>2,
	    "Client should install software"=>3,
	    "License not ready"=>4,
	   };


# Use this a global reference.

my $self = {};

#  Possible return values
#  
#  die-> something wrong. The package can't be installed
#  -2, $msg, $action ->  The client should do $action before trying again ($msg explains why)
#  1, $msg, $action -> The client should do $action, and then it can go on



sub usePackage {
  shift;
  my $package=shift;
  my $version=shift || "";
  my $time   =shift || 3600;
  my $extraInfo=shift || {};
  $self->info("Trying to get the package $package and $version");
  my $packageInfo=$self->{DB}->getPackage($package, $version) or 
    die("The package $package with version $version does not exist");

  my $clientLevel=0;
  $extraInfo->{msg} and $clientLevel=$levels->{$extraInfo->{msg}};
  
  $self->info("The package is defined (client on level $clientLevel)");

  if (!$packageInfo->{installed}){
    $self->installPack($packageInfo) and  
      return (-2, "Package is being installed on the server", "sleep 60");
  }

  if ($clientLevel<2  and $packageInfo->{installed} eq 1) {
    #check if the client can see the instalation of the server
    $self->info("Asking if the client can see the server installation");
    return (-2, "Checking if client can see server installation", "[ -d $packageInfo->{installDir} ]");
  }
  my $packageReady=0;

  if ($clientLevel eq 2) {
    if ($extraInfo->{result} ne "0") {
      $self->info("The client can't see the installation on the server");
      die ("The client couldn't install the software\n");
    } else {
      $self->info("The client can see the installation on the server");
      $packageReady=1;
    }
  }

  if (($clientLevel <3) && ! $packageReady){
    if ($packageInfo->{installAction}) {
      $self->info("The client should install the software");
      return (-2, "Client should install software", "where dddddd");
    } else  {
      $self->info("The package is not installed and the client can't install it...");
      die ("The package cannot be installed on the client\n");
    }
  }

  if ($clientLevel eq "3") {
    if ($extraInfo->{result} ne "0") {
      $self->info("The client had to install the software, but it failed!!");
      die ("The client couldn't install the software\n");
    } else {
      $self->info("The client installed the software successfully");
    }
  }
  my $configuration;
  if ($packageInfo->{licensed} ) {
    $self->info("The package needs a license");
    
    my ($ok, $serverConf)=$self->getLicense($packageInfo, $time);
    $ok or return (-2, "License not ready", "sleep 60");
    $serverConf and $configuration=$serverConf;
  }

  if ($packageInfo->{configurationFile}){
    $self->info("The client should source a file");
    my $dir="$self->{INSTALLDIR}/$packageInfo->{packageName}/$packageInfo->{packageVersion}";

    $configuration.=" source $dir/$packageInfo->{configurationFile} $dir ;";
  }
  if ($packageInfo->{configurationCommand}){
    $self->info("The client should execute some commands");
    $configuration.="$packageInfo->{configurationCommand}";
  }
  $self->info("Ready to use the package $package (conf  $configuration)");


  return (1, "Package ready to be used!!", $configuration);
}

sub releaseToken {
  shift;
  my $token=shift;
  $self->info("Releasing the token $token");
  $self->{DB}->releaseLicenseToken($token);
  return 1;
}
#
##
# PRIVATE FUNCTIONS
#
#
sub getLicense {
  my $self=shift;
  my $packageInfo=shift;
  my $time=shift;


  $self->info("Trying to get a license for the package");
  my $servers=$self->{DB}->getLicenseServers($packageInfo) 
    or die("Error retrieving possible license servers");
  my $configuration="";
  my $server;
  foreach my $p (@$servers) {
    $self->info("Checking if any of the servers has free tokens");
    my $token=$self->{DB}->getLicenseToken($p, $time);
    print "Did we get a token?? $token\n";
    if ($token) {
      $self->info("This server can be used!");
      $server=$p;
      $configuration.="export ALIEN_PACKAGE_LICENSES=\${ALIEN_PACKAGE_LICENSES}$packageInfo->{packageName}:$token ;";
      last;
    }
  }

  if (!$server) {
    return (0, "");
  }
  $server->{configurationCommand} and 
    $configuration.=$server->{configurationCommand};
  $configuration =~ /;$/ or $configuration.=";";

  return (1,$configuration );
}

sub installPack {
  my $self=shift;
  my $packageInfo=shift;

  my $result=$self->{DB}->update("PACKAGES", {beingInstalled=>"TRUE"},"packageId=$packageInfo->{packageId}") or die("Error updating the database");
  if ($result=~ /^0E0$/) {
    $self->info("The package was already being installed");
    return 1;
  }
  my $pid=fork();
  $pid and return 1;
  $self->info("Let's install the package");
  my $installDir="$self->{INSTALLDIR}/$packageInfo->{packageName}/$packageInfo->{packageVersion}";
  
  if ( ! -d  $installDir){
    my $dir="";
    foreach ( split ( "/", $installDir ) ) {
      $dir .= "/$_";
      mkdir $dir, 0777;
    }
  }
  chdir $installDir;

  eval {
    $packageInfo->{installAction} or die("We don't know how to install the software");
    $self->_doAction($packageInfo->{installAction});
  };
  my $installed=1;
  if ($@) {
    $installed=-1;
    $self->info("Error: $@");
    system ("rm","-rf", $installDir);
  }
  $self->{DB}->update("PACKAGES",{installed=>$installed,
				  beingInstalled=>0, 
				  installDir=>$installDir},
		      "packageId=$packageInfo->{packageId}");
  $self->info("The package has been installed with $installed");
  exit -1;
}

sub _doAction {
  my $self=shift;
  my $action=shift;
  $self->info("Let's get $action and execute it");
  my $url=AliEn::SE::Methods->new($action) or die("Error parsing $action");
  
  my ($file)=$url->get() or die("Error getting the file");
  system("tar", "zxf", $file) and die("Error uncompressing");
  return 1;

}
sub initialize {
  $self = shift;
  my $options =(shift or {});

  $self->debug(1, "Creatting a PackMan" );

  $self->{PORT}="9992";
  $self->{HOST}= $self->{CONFIG}->{HOST};

  $self->{SERVICE}="Licensed";
  $self->{SERVICENAME}="Licensed\@$self->{HOST}";
  $self->{LISTEN}=1;
  $self->{PREFORK}=5;
  
  $self->{DB}=AliEn::Database::Licensed->new() or return;
  #Remove all the possible locks;
  $self->info( " Removing old lock files");

  $self->{DB}->update("PACKAGES", {beingInstalled=>0});

  $self->{INSTALLDIR}="/afs/cern.ch/user/p/psaiz/public/lic/packages";
  return $self;

}
return 1;


