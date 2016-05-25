package AliEn::ConfigService;

use strict;

#use IO::Socket;
#use AliEn::PackMan;
require AliEn::Logger::LogObject;
use Net::Domain;

use vars qw(@ISA $DEBUG);

push @ISA, "AliEn::Logger::LogObject";
my $self;
$DEBUG=0;

my $organisations = {};

sub new {
  my $proto = shift;

  defined $self and return $self;
  return ( Initialize( $proto, @_ ) );
}

sub DESTROY {

    undef $self;
}

sub Reload {
  my $t       = shift;
  my $options = shift;
  $DEBUG and $self->debug(1, "Reloading the configuration");
  $t->Initialize($options);
}

sub Initialize {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $temp  = shift || {};

  umask 0;
  my $organisation = "Alice";
  $ENV{ALIEN_ORGANISATION} and $organisation=$ENV{ALIEN_ORGANISATION};
  $temp->{organisation} and  $organisation=$temp->{organisation};

  my $organisationLowerCase = "\L$organisation\E";

  $temp->{DEBUG} or $temp->{DEBUG}=0;
  $temp->{SILENT} or $temp->{SILENT}=0;
  $DEBUG=$temp->{DEBUG};

  $temp->{silent} and $temp->{SILENT}=$temp->{silent};
  $temp->{debug} and $temp->{DEBUG}=$temp->{debug};

  (! $temp->{SILENT}) and ($temp->{DEBUG}>0) and
    print "DEBUG LEVEL 1. Checking config for $organisationLowerCase\n";

  if ($organisations->{$organisationLowerCase}) {
    $self=$organisations->{$organisationLowerCase};
    $DEBUG and $self->debug(1, " Configuration already exists!!");
    $temp->{force}  or return $self;
    $self->debug(1, "Forcing to reload the configuration");
  }

  bless( $temp, $class );

  $temp->SUPER::new({logfile=>$temp->{logfile}}) or return;
  $DEBUG and $temp->debug(1, "Getting config for $organisationLowerCase");


  (defined $ENV{ALIEN_DOMAIN})
    or $ENV{ALIEN_DOMAIN} = Net::Domain::hostdomain();
  $temp->{DOMAIN} = $ENV{ALIEN_DOMAIN};

  (defined $ENV{ALIEN_HOSTNAME})
    or $ENV{ALIEN_HOSTNAME} = Net::Domain::hostfqdn();
  $ENV{ALIEN_HOSTNAME} =~ s/\.$//;
  $temp->{HOST}=$ENV{ALIEN_HOSTNAME};

  undef $self;
  undef  $organisations->{$organisationLowerCase};
  $organisations->{$organisationLowerCase} = $self = $temp;

  #    print "IN CONFIG, we have $self->{HOST}\n";
  $self->{LOGGER} or $self->{LOGGER}=new AliEn::Logger;
  $self->{DEBUG} and $self->{LOGGER}->debugOn($self->{DEBUG});
  $self->{SILENT} and $self->{LOGGER}->silentOn();

  $self->{ORG_NAME} = "$organisation";

  if ( $ENV{ALIEN_CM_AS_LDAP_PROXY}  ) {
    $self=$self->GetConfigFromService("ClusterMonitor") or return;
  } elsif ( $ENV{ALIEN_CONFIG_FROM_SERVICE} ){
    $self=$self->GetConfigFromService() or return;
  } else {
    $self=$self->GetConfigurationFromLDAP() or return;
  }
  $self->{skip_localfiles}
    or $self->checkConfigFile(
    "$ENV{ALIEN_ROOT}/etc/alien/$self->{ORG_NAME}.conf",
    "/etc/alien/$self->{ORG_NAME}.conf",
    "$ENV{ALIEN_HOME}/\L$self->{ORG_NAME}\E.conf"
    );
  return $self;
}
sub GetConfigurationFromLDAP {
  my $self=shift;
  $self->debug(1, "Getting the configuration from the LDAP server");
  
  $self->{LOCAL_USER} = getpwuid($<);
  $ENV{ALIEN_USER} and $self->{LOCAL_USER}=$ENV{ALIEN_USER};
  
  $self->{ROLE} = $self->{LOCAL_USER};
  $self->{role} and $self->{ROLE} = $self->{role};
  
  $self->debug(1, "Config for user $self->{LOCAL_USER} ($self->{ROLE})");
  
  #    my $ldapConfig=$struct->{proxyport}[0];
  
  my $ldap=$self->GetLDAPDN();
  $ldap or return;
  
  $self->GetOrganisation($ldap) or return;
  
  #  $self->{PACKMAN} = AliEn::PackMan->new( { "CONFIG", $self } )
  #    or print STDERR "Error creating the package manager\n"
  #      and return;
  
  
  #ORGANISATION" =>$self->{ORG_NAME}, "PACKINSTALL"=>$self->{PACKINSTALL}, "SILENT" =>$self->{SILENT} , "DEBUG", $self->{DEBUG}}) 
  
  $self->GetSite($ldap) or return;
  
  foreach ("SE", "CE", "FTD", "CLC", "TCPROUTER") {
    $self->GetServices($ldap,  $_) or return;
  }
  $self->GetServices($ldap, "PACKMAN");
  
  $self->GetHostConfig($ldap) or return;
  
  if ( $self->{queue} ) {
    $self->setService( $ldap, $self->{queue}, "CE" ) or return;
  }
  $self->GetGridPartition($ldap) or return;
  $self->GetPackages($ldap) or return;
  
  $self->GetTopLevelServices($ldap) or return;

  $ldap->unbind;    # take down session
  return $self;
}

sub checkConfigFile {
  my $self=shift;
  my $config;
  $self->debug(1,"Reading the local configuration");

  foreach my $file (@_) {
    (-f $file) or next;
    $self->info("Reading the configuration file from $file");
    eval {
      require Config::ApacheFormat;
      $config or $config=Config::ApacheFormat->new();
      $config->read($file);
    };
    if ($@){
      $self->info("Error reading the config file $file: $@");
      return;
    }
  }
  #if there are no configuration files, just return;
  $config or return 1;

  #now, we should go through all the things defined in the config...
  foreach ($config->get()) {
    my $key=uc($_);
    $self->debug(1, "\t\tOverwritten the value '$key'");
    my $done=0;
    eval {
      my $subconfig=$config->block($key);
      foreach my $subkey ($subconfig->get()){
	my $name="${key}_\U$subkey\E";
	  #the value exists: just get the new value:

        $self->{$name}
          or $self->info("The local configuration defines '<$key> $subkey="
            . $subconfig->get($subkey)
            . "</$key>' (a variable that is not in the standard configuration)");
        $self->{$name} = $subconfig->get($subkey);
      }
      $done = 1;
    };
    $done and next;
    $self->{$key}
      or $self->info("The local configuration defines '$key="
        . $config->get($key)
        . "' (a variable that is not in the standard configuration)");
    $self->{$key} = $config->get($key);
  }
  return 1;
}

sub GetLDAPDN {
  my $self=shift;

  eval "require Net::LDAP" or print "Error requiring Net::LDAP:\n  $@\n" and return;

  if ($ENV{ALIEN_LDAP_DN}) {
    $self->debug(1, "Getting the Config from $ENV{ALIEN_LDAP_DN}");
    ($self->{LDAPHOST} , $self->{LDAPDN})=
      split ("/",  "$ENV{ALIEN_LDAP_DN}");
    $ENV{ALIEN_LDAP_DN}=~ /o=$self->{ORG_NAME},/i 
      or $self->info("We are supposed to get the configuration from ALIEN_LDAP_DN=$ENV{ALIEN_LDAP_DN}, but this doesn't look like our organisation $self->{ORG_NAME})") and return;
    my $ldap = Net::LDAP->new($self->{LDAPHOST} ) or die "Error connecting to$self->{LDAPHOST}\n $@";
    $ldap->bind;      # an anonymous bind
    return $ldap;
  }
  my $ldap = Net::LDAP->new('alien.cern.ch:8389') or die "$@";
  my $base = "o=alien,dc=cern,dc=ch";

  $DEBUG and $self->debug(1, "Getting the Config of $self->{ORG_NAME} from $base");

  $ldap->bind;    # an anonymous bind

  my $mesg = $ldap->search(    # perform a search
			   base   => "$base",
			   filter => "(ou=$self->{ORG_NAME})"
			  );

  $mesg->code && die $mesg->error;

  my $total = $mesg->count;

  if ( !$total ) {
    print STDERR "ERROR: There are no organisations called '$self->{ORG_NAME}'\n";
    return;
  }
  my $entry    = $mesg->entry(0);
  my $ldaphost = $entry->get_value('ldaphost');
  $ldaphost =~ s/\s+$//;

  $self->{LDAPHOST} = $ldaphost;
  $self->{LDAPDN}   = $entry->get_value('ldapdn');

  $ldap->unbind;    # take down session

  $ldap = Net::LDAP->new($ldaphost) or die "Error contacting LDAP in $ldaphost\n $@\n";

  $ldap->bind;      # an anonymous bind

  return $ldap;
}


sub GetGridPartition {
  my $self=shift;
  my $ldap=shift;

  $self->debug(1, "Getting the grid partition!");

  if (!$self->{CE_NAME}) {
      $self->debug(1, "The machine does not have a CE");
      return 1;
     }
  my $filter = "(&(objectClass=AliEnPartition)(CEname=$self->{CE_FULLNAME}))";
  my $base   = "ou=Partitions,$self->{LDAPDN}";


  my $mesg = $ldap->search(    # perform a search
			   base   => "$base",
			   filter => "$filter"
			  );

  my $total = $mesg->count;

  if ( !$total ) {
    $self->debug(1, "The machine $self->{CE_FULLNAME} does not belong to any Grid Partition");
    return 1;
  }

  my @list=();
  my $i=0;

  while ($i<$total){
    my $name=$mesg->entry($i)->get_value('name');
    $self->debug(1,"PARTITITON $name");
    @list= (@list, $name);
    $i++;
  }
  $self->{GRID_PARTITION}=$list[0];
  $self->{GRID_PARTITION_LIST}=\@list;
  return 1;
}
sub GetPackages {
    my $self = shift;
    my $ldap = shift;

    my @list = ( @{ $self->{PACKAGES_LIST} } );

    if ( $self->{CE_PACKAGES_LIST} ) {
        @list = ( @list, @{ $self->{CE_PACKAGES_LIST} } );
    }
    if ( $self->{HOST_PACKAGES_LIST} ) {
        @list = ( @list, @{ $self->{HOST_PACKAGES_LIST} } );
        my $noPack = grep ( /^none$/, @{ $self->{HOST_PACKAGES_LIST} } );
        $noPack and @list = ();
    }

    #Now, check if the packages have a version
    my @newlist=();
    
    foreach my $pack (@list){
      if  ($pack !~ /::/) {
	  
	#This package does not have a version. Get it
	my $version=$self->GetPackageVersion($ldap, $pack);
      grep(/^${pack}::$version$/, @newlist)
        or @newlist = (@newlist, "${pack}::$version");
      }
      grep(/^$pack$/, @newlist) or @newlist=(@newlist, $pack);
    }

#    my $package;
    if ($self->{PACKCONFIG}){
      foreach (@newlist) {
        $self->addPackage( $ldap, $_ ) or return;
      }
    }
    $self->{PACKAGES_LIST} = \@newlist;
    return 1;
}
sub GetPackageVersion {
  my $self=shift;
  my $ldap=shift;
  my $package=shift;

  my $filter = "(objectClass=AliEnPackage)";
  my $base   = "version=current,name=$package,ou=Packages,$self->{LDAPDN}";

#  print "looking for the version of $package";
  my $mesg = $ldap->search(    # perform a search
			   base   => "$base",
			   filter => "$filter"
			  );

  my $total = $mesg->count;
  
  if ( !$total ) {
    ( !$self->{SILENT} )
       and print STDERR "Warning: LDAP package '$package' does not exist in $self->{ORG_NAME}\n";
    return;
  }
  return $mesg->entry(0)->get_value('packVersion');
}

sub GetSite {
    my $self = shift;
    my $ldap = shift;

    my $domain =$ENV{ALIEN_CONFIG_DOMAIN} || $ENV{ALIEN_DOMAIN}; 

    $DEBUG and $self->debug(1,"Configuring the site $domain");

    my $mesg = $ldap->search(    # perform a search
        base   => "ou=Sites,$self->{LDAPDN}",
        filter => "(&(domain=$domain)(objectClass=AliEnSite))"
    );

    my $total = $mesg->count;

    if ( !$total ) {
        print STDERR "ERROR: There are no sites in $self->{ORG_NAME} with domain $domain\n";
        return;
    }
    if ($total > 1) {
      (!$self->{SILENT})
        and print STDERR "Warning: There are more than one site with domain $domain\n Taking the first one :"
        . $mesg->entry(0)->get_value('ou')
        . " (there is also "
        . $mesg->entry(1)->get_value('ou') . ")\n";
    }


    my $entry = $mesg->entry(0);

    $self->{LOG_DIR} = $entry->get_value('logdir');
    $self->{TMP_DIR} = $entry->get_value('tmpdir');

    $self->{SITE_LATITUDE} = $entry->get_value('latitude');
    $self->{SITE_LONGITUDE} = $entry->get_value('longitude');
    $self->{SITE_LOCATION}  = $entry->get_value('location');
    $self->{SITE_COUNTRY}   = "";
    $self->{PACKMAN_ADDRESS}= $entry->get_value('packmanAddress');
    # Setting the Cache directory. First, home directory of the user
    $self->ChangeCacheDir( $entry->get_value('cachedir') );
    $self->ChangeCacheDir( $ENV{ALIEN_CACHE} );

    $self->{CACHE_DIR}
      or $self->ChangeCacheDir("$ENV{ALIEN_HOME}/cache")
      or return;

    $self->{WORK_DIR} = ($entry->get_value('workdir') or "");
    $self->{DOMAIN}   = $entry->get_value('domain');
    $self->{SITE}     = $entry->get_value('ou');

    $self->{FULLLDAPDN} = "ou=$self->{SITE},ou=Sites,$self->{LDAPDN}";

#    $self->{SE_HOST} = "";
#    $self->{SE_PORT} = "";

    my $saveSE = ( $entry->get_value('SaveSE') or "none" );

    my @SaveSEs = ( $entry->get_value('SaveSE') );
    $self->setService( $ldap, $saveSE, "SaveSE", "SE" ) or return;
    $self->{SaveSEs_FULLNAME}=\@SaveSEs;
    #    my $package;
    my @list = $entry->get_value('packages');
    $self->{PACKAGES_LIST} = \@list;

    my @processPorts = $entry->get_value('processPorts');
    if (@processPorts) {
        $self->{PROCESS_PORT}      = $entry->get_value('processPorts');
        $self->{PROCESS_PORT_LIST} = \@processPorts;
    }
    $DEBUG and $self->debug(1,"$self->{SITE} configured!");

    return 1;
}

sub ChangeCacheDir {
    my $self     = shift;
    my $cachedir = shift;
    $cachedir or return;

     $DEBUG and $self->debug(2,"Using $cachedir as Cache from the LDAP");

    my $dbPath = "$cachedir/LCM.db";
    if ( !( -d $dbPath ) ) {
        my $dir = "";
        foreach ( split ( "/", $dbPath ) ) {
	  $self->debug(1, "Creating the directory $dir");
	  $dir .= "/$_";
	  mkdir ($dir, 0777);
        }
    }
    my $exists = -e "$dbPath/LOCALFILES";

    open( FILE, ">>$dbPath/LOCALFILES" )
      or print STDERR "Warning! not able to use  $cachedir as cache dir\n" and return;

    close(FILE);
    chmod 0777, "$dbPath/LOCALFILES";

    $exists or unlink "$dbPath/LOCALFILES";
    $self->{CACHE_DIR} = $cachedir;
    return 1;
}

sub GetServices {
    my $self    = shift;
    my $ldap    = shift;
    my $service = shift;

    $DEBUG and $self->debug(4,"Getting ${service}s of site $self->{SITE}");

    my $base = "ou=$service,ou=services,$self->{FULLLDAPDN}";
    my $mesg = $ldap->search(                                 # perform a search
        base   => "$base",
        filter => "(objectClass=AliEn$service)"
    );

    my $total = $mesg->count;

    if ( !$total ) {
      $self->debug(1, "Warning: no $service defined for your site");
      return 1;
    }
    my $entry   = $mesg->entry(0);
    my $name    = $entry->get_value('name');
    my $version = $entry->get_value('version');
    $self->{$service} = $name;

    $self->setService( $ldap, $name, $service ) or return;

    my @list  = ($name);
    my @types = ( $entry->get_value('type') );
    my $i     = 1;
    while ( $i < $total ) {
        @list  = ( @list,  $mesg->entry($i)->get_value('name') );
        @types = ( @types, $mesg->entry($i)->get_value('type') );
        $i++;
    }

    $self->{"${service}s"}      = \@list;
    $self->{"${service}s_TYPE"} = \@types;

    my @fullNames = @list;
    map { $_ = "$self->{ORG_NAME}::$self->{SITE}::$_" } @fullNames;
    $self->{"${service}s_FULLNAME"} = \@fullNames;

    $DEBUG and $self->debug(1, "ALL ${service}s are: "
      . @{ $self->{"${service}s"} }
      . "\n\t\tDefault $service '$self->{$service}'");
    return 1;
}

sub GetOrganisation {
    my $self = shift;
    my $ldap = shift;

    my $VERSION  = 1;
    my $REVISION = 1;

    ( !$self->{SILENT} )
      and ( $self->{DEBUG} > 7 )
      and print "DEBUG LEVEL 6 Setting the organisation from $self->{LDAPDN}\n";

    if ( -f "$ENV{ALIEN_ROOT}/scripts/VERSION" ) {
        open VERSION, "$ENV{ALIEN_ROOT}/scripts/VERSION";
        my @lines = <VERSION>;
        close VERSION;
        foreach my $line (@lines) {
            eval "\$$line";
        }
    }
 
    my $A_REVISION = $REVISION;


    if ( -f "$ENV{ALIEN_ROOT}/share/alien/Version" ) {
        open VERSION, "$ENV{ALIEN_ROOT}/share/alien/Version";
        my @lines = <VERSION>;
        close VERSION;
        foreach my $line (@lines) {
            eval "\$$line";
        }
    }

    my $B_REVISION = $REVISION;

    $self->{VERSION} = $VERSION . "." . $B_REVISION . "." . $A_REVISION;

    my $mesg = $ldap->search(    # perform a search
        base   => "$self->{LDAPDN}",
        filter => "&(ou=Config)(objectClass=AliEnVOConfig) "
    );

    #    $struct = $mesg->as_struct->{"ou=Config,$ldapdn"};
    my $entry = $mesg->entry(0);
    $entry
      or print STDERR
      "Error getting the configuration for the organisation from host=$self->{LDAPHOST} and dn=$self->{LDAPDN}\n"
      and return;
    my $attr;
    foreach $attr ( $entry->attributes ) {
        my $value = $attr;
        $value =~ s/([A-Z])/_$1/g;
        $value = uc($value);

        $self->{$value} = $entry->get_value($attr);
        my @list = $entry->get_value($attr);
        $self->{"${value}_LIST"} = \@list;

        ( !$self->{SILENT} )
          and ( $self->{DEBUG} > 7 )
          and print "DEBUG LEVEL 6 Setting $value as ($attr)  "
          . $entry->get_value($attr)
          . " (@list)\n";
    }

    ( !$self->{SILENT} )
      and ( $self->{DEBUG} > 7 )
      and print "DEBUG LEVEL 6 Organisation done!\n";

    return 1;
}

sub GetHostConfig {
  my $self = shift;
  my $ldap = shift;
  
  my $domain = $ENV{ALIEN_CONFIG_DOMAIN} || $ENV{ALIEN_DOMAIN};
  chomp  $domain;

  $self->{domain} and $domain = $self->{domain};

  my $host = $ENV{'ALIEN_HOSTNAME'};
  chomp $host;


  ( !$self->{SILENT} )
    and ( $self->{DEBUG} > 4 )
      and print "DEBUG LEVEL 1 Getting special configuration for $host\n";
  
  my $base = "ou=config,$self->{FULLLDAPDN}";
  my $mesg = $ldap->search(                     # perform a search
			   base   => "$base",
			   filter => "(host=$host)"
			  );
  
  my $total = $mesg->count;
  
  if ( !$total ) {
    $self->debug(1, "No local configuration found. Using the default");
    return 1;
  }
  my $entry = $mesg->entry(0);
  $self->{SITE_HOST} = $entry->get_value('host');
  
  $self->debug(1, "Using the configuration of $self->{SITE_HOST}");

  my @variables=("logdir", "tmpdir", "cachedir");
  my @variablesName=("LOG_DIR", "TMP_DIR", "CACHE_DIR");
  foreach (@variables){
    my $var=$entry->get_value($_);
    my $name=shift @variablesName;
    if ( $var) {
      $self->debug(1, "Using another variable $_ : $var");
      $self->{$name}=$var;
    }
  }


  my @services=("SE", "SaveSE", "CE", "FTD" , "CLC" , "TCPROUTER", "PACKMAN");
  my @serviceName=("SE", "SE","CE", "FTD" , "CLC" , "TCPROUTER", "PACKMAN");

  foreach (@services)      {
    my $name=shift @serviceName;

    my $service=$entry->get_value($_);
    if ( $service) {
      $self->debug(1, "Using another $_ ($name): $service");
      $self->setService( $ldap, $service, $_, $name ) or return;
    }
  }

  my @packages = $entry->get_value('Packages');

  if (@packages) {
    $self->{"HOST_PACKAGES_LIST"} = \@packages;
  }

  #Checking the close SE;

  my @se=$entry->get_value('CloseSE');
  if (@se) {
    $self->debug(1, "We should put @se as closeSE");
    my @seList=();
    my @fullNames=();
    foreach my $serviceName (@se) {
      my $se=$self->CheckService("SE", $serviceName, $ldap);
      $se
        or $self->{LOGGER}
        ->error("Config", "Error host  '$host' is supposed to be close to $serviceName, but that SE does not exist")
        and return;
      push @seList, $se->{NAME};
      push @fullNames, $se->{FULLNAME};
    }
    $self->{SEs}=\@seList;
    $self->{SEs_FULLNAME}=\@fullNames;

  }
  $self->debug(1, "$self->{SITE_HOST} configured!!");
  return 1;
}

sub setService {
    my $self        = shift;
    my $ldap        = shift;
    my $name        = shift;
    my $service     = shift;
    my $serviceName = ( shift or $service );

    $self->debug(1, "Setting $service as $name !");

    my @all = grep ( /^${service}_/, keys %{$self} );
    map { delete $self->{$_} } @all;

    if ( $name eq "none" ) {
        $self->{$service} = "";

        $self->{"${service}_FULLNAME"} = "";
        my @list = ();
        $self->{"${service}s_FULLNAME_LIST"} = @list;
	$self->debug(1, "Using no $service");
        return 1;
      }
    my $se=$self->CheckService($serviceName, $name, $ldap);

    $se or return;

    @all = keys %{$se};
    map { $self->{"${service}_$_"}=$se->{$_} } @all;
    $self->{$service}=$se->{NAME};

    $DEBUG and $self->debug(1, "Using $service $self->{$service}");

    return 1;
}

sub getValue {
    my $self  = shift;
    my $value = shift;

    return $self->{$value};
}

sub addPackage {
    my $self = shift;
    my $ldap = shift;
    my ( $name, $version ) = split ( "::", shift );
    $version or $version = "current";
    $DEBUG and self->debug( 1,"Adding package $name (version $version)");

    my $filter = "(&(objectClass=AliEnPackage)(version=$version))";
    my $base   = "name=$name,ou=Packages,$self->{LDAPDN}";

    my $mesg = $ldap->search(    # perform a search
        base   => "$base",
        filter => "$filter"
    );

    my $total = $mesg->count;

    if ( !$total ) {
        ( !$self->{SILENT} )
          and print STDERR "Warning: LDAP package '$name' (version '$version') does not exist in $self->{ORG_NAME}\n";
        return;
    }

    my $entry = $mesg->entry(0);

    my $package = { split "=", ( join "=", $entry->get_value('options') ) };
    $package->{version} = $entry->get_value('packVersion');
    $package->{path}    = $entry->get_value('path');
    $package->{name}    = $entry->get_value('package');

#    my @requirements = $entry->get_value('require');
#    $package->{require} = \@requirements;
#    $DEBUG and $self->debug(1,"Calling the package manager!!");
#    $self->{PACKMAN}->Add($package) or return;
#    $DEBUG and $self->debug(1,"Adding package $name done!!");
    return 1;
}

sub GetConfigFromService {
  my $self=shift;
  my $service=shift ||"Config";


  $DEBUG and $self->debug(1,"Getting the configuration from the $service");

  my ($host, $port);

  if ($service eq "ClusterMonitor") {
    ($host, $port )=split ":", $ENV{ALIEN_CM_AS_LDAP_PROXY};
    ($host and $port)
      or print STDERR
"ERROR: The environment variable ALIEN_CM_AS_LDAP_PROXY was set ($ENV{ALIEN_CM_AS_LDAP_PROXY}), but not with a host:port syntax!!\n"
      and return;
  } else {
    ($host, $port)=("pcegee02.cern.ch",8085);
  }
  $DEBUG and $self->debug(2, "Using the $service at $host:$port");


  my $config=SOAP::Lite
    -> uri( "AliEn/Service/$service" )
      -> proxy("http://$host:$port" )
	->GetConfiguration($self->{ORG_NAME}, $self->{DOMAIN}, $self->{HOST});

  $config or print STDERR "Error contacting the $service at $host:$port\n" and return;

  $config=$config->result;

  $config or print STDERR "Error the $service at $host:$port did not return anything\n" and return;

  (UNIVERSAL::isa($config, "HASH"))
    or print STDERR "Error the $service did not return a hash ($config)\n" and return;

  foreach my $key (sort keys %$config) {
    $DEBUG and $self->debug(6,"Setting $_ as ". defined $config->{$_} ? $config->{$_} : "(undef)");
    $self->{$key}=$config->{$key};
  }

  $DEBUG and $self->debug(1,"Getting the configuration done!");
  
  my $dom = Net::Domain::hostdomain();
  $dom =~ s/\.$//;
  $self->{DOMAIN}=  $ENV{ALIEN_DOMAIN} = $dom;
  
  my $fqdn = Net::Domain::hostfqdn();
  $fqdn =~ s/\.$//;
  $self->{HOST}= $ENV{ALIEN_HOSTNAME} = $fqdn;

  return $self;
}


sub CheckServiceCache {
  my $this=shift;
  my $service=shift;
  my $name=shift;
  $name or return;
  $self->{CACHE} or $self->{CACHE}={};
  $self->{CACHE}->{$service} or $self->{CACHE}->{$service}={};
  $self->{CACHE}->{$service}->{$name} and 
    $self->{CACHE}->{$service}->{$name}->{expires}> time() 
      and return $self->{CACHE}->{$service}->{$name}->{value};

  $self->{CACHE}->{$service}->{$name}->{value}=$self->CheckService($service, $name, @_)
    or return;
  $self->{CACHE}->{$service}->{$name}->{expires}=time()+600;
  return $self->{CACHE}->{$service}->{$name}->{value};
}
sub CheckService{

  my $this=shift;
  my $service=shift;
  my $name=shift;
  my $ldap=(shift or "");
  my $disconnect=0;
  if (! $ldap){
    eval "require Net::LDAP" or print "Error requiring LDAP  $@\n" and  return;

    $ldap = Net::LDAP->new( $self->{LDAPHOST}) 
      or print STDERR "Error contacting ldap: $@" and return;
    
    $ldap->bind or print STDERR "Error binding to LDAP" and return;
    $disconnect=1;
  }
  $service =~ /^((SE)|(CE)|(FTD)|(CLC)|(TCPROUTER)|(PACKMAN))$/
    or print STDERR "Error service type $service does not exist\n" and return;

  ($name) or print STDERR "Error not enough arguments in CheckService\n" and return;

  my $se={};
 

  my $filter = "(&(objectClass=AliEn$service)(name=$name))";
  my $base   = "ou=$service,ou=services,$self->{FULLLDAPDN}";
  my $site = $self->{SITE};
  
  if ( $name =~ /\:\:/ ) {
    ( !$self->{SILENT} ) and ( $self->{DEBUG} > 4 )
      and print "DEBUG LEVEL 1 WE ARE USING ANOTHER $service from another site\n";
    my $org;
    ( $org, $site, $name ) = split "::", $name;
        ( $org eq $self->{ORG_NAME} )
          or print STDERR
	    "ERROR: You are trying to use a resource from $org, while your organisation is $self->{ORG_NAME}\n"
	      and return;
    
    $base = "ou=$service,ou=services,ou=$site,ou=Sites,$self->{LDAPDN}";
    $filter = "(&(objectClass=AliEn$service)(name=$name))";
  }
  

  my $mesg = $ldap->search( base   => "$base",  filter => "$filter");

  my $total = $mesg->count;
  
  if ( !$total ) {
    ( !$self->{SILENT} )
      and print STDERR "Warning: no $service $name defined at $site\n";
    return;
  }
  my $entry = $mesg->entry(0);

#  print "GOT $total\n";
#  $se->{$service} = $entry->get_value('name');

  my $attr;
  
  foreach $attr ( $entry->attributes ) {
    my $value = $entry->get_value($attr);
    my @list  = $entry->get_value($attr);
    $attr =  uc($attr);
    ( !$self->{SILENT} ) and ( $self->{DEBUG} > 6 )
	and print  "DEBUG LEVEL 7 Putting $attr as $value\n\tAnd ${attr}_LIST=@list\n";
    $se->{"$attr"}        = $value;
    $se->{"${attr}_LIST"} = \@list;
  }

  $se->{"FULLNAME"} ="$self->{ORG_NAME}::${site}::$se->{NAME}";

  $disconnect and $ldap->unbind;

  
  return $se;
}
sub GetMaxJobs{

  my $this=shift;
  my $host=shift;
  my $ldap=(shift or "");
  my $disconnect=0;
  if (! $ldap){
    eval "require Net::LDAP" or print "Error requiring LDAP  $@\n" and  return;

    $ldap = Net::LDAP->new($self->{LDAPHOST})
      or print STDERR "Error contacting ldap: $@" and return;
    
    $ldap->bind or print STDERR "Error binding to LDAP" and return;
    $disconnect=1;
  }

  $self->info("Searching for $host in ldap");
 

  my $filter = "(&(objectClass=AliEnCE)(host=$host))";
  my $base   = "$self->{LDAPDN}";

  my $mesg = $ldap->search( base   => "$base",  filter => "$filter");

  my $total = $mesg->count;
  
  if ( !$total ) {
    $self->{LOGGER}->error("Config", "Warning: '$host' can't execute jobs");
    $disconnect and $ldap->unbind;
    return;
  }
  my $entry = $mesg->entry(0);

  my $jobs=($entry->get_value('maxjobs') or "");
  my $queuedJobs=($entry->get_value('maxqueuedjobs')or "");


  $disconnect and $ldap->unbind;

  $self->debug(1, "Returning $jobs, $queuedJobs");

  return $jobs, $queuedJobs;
}
sub getInfoDomain {
  my $self=shift;
  my $domain=shift;
  my $ldap=(shift or "");
  my $disconnect=0;
  $self->info("Getting info of $domain");

  if (! $ldap){
    eval "require Net::LDAP" or print "Error requiring LDAP  $@\n" and  return;

    $ldap = Net::LDAP->new($self->{LDAPHOST})
      or print STDERR "Error contacting ldap: $@" and return;
    $ldap->bind or print STDERR "Error binding to LDAP" and return;
    $disconnect=1;
  }
  my $base = "ou=Sites,$self->{LDAPDN}";
  my $filter = "(&(objectClass=AliEnSite)(domain=$domain))";

  my $mesg = $ldap->search( base   => "$base",  filter => "$filter");
  $mesg->code && die $mesg->error;
  my $total = $mesg->count;

  if ( !$total ) {
    $self->info("ERROR: There are no sites with domain '$domain'");
    $disconnect and $ldap->unbind;
    return;
  }
  $self->info("There are $total sites with domain '$domain'");
  my $entry    = $mesg->entry(0);
  my $attr;
  my $object={};
  foreach $attr ( $entry->attributes ) {

    my $value = $attr;
    $value =~ s/([A-Z])/_$1/g;
    $value = uc($value);
    $self->info("Putting $value as ".$entry->get_value($attr));
    $object->{$value}=  $entry->get_value($attr);  
    }
  $disconnect and $ldap->unbind;
  return $object;
}

sub getAttributes {
  my $self = shift;
  my $entry = shift;
  my $noList = shift;

  my $target = {};
  foreach my $attr ( $entry->attributes ) {
    my $value = $attr;
    $value =~ s/([A-Z])/_$1/g;
    $value = uc($value);

    $target->{$value} = $entry->get_value($attr);
    unless ($noList) {
      my @list = $entry->get_value($attr);
      $target->{"${value}_LIST"} = \@list;
    }
  }

  return $target;
}

sub GetTopLevelServices {
  my $self=shift;
  my $ldap=shift;

  $self->GetgContainer($ldap)
    or return;

  return 1;
}

sub GetgContainer {
  my $self=shift;
  my $ldap=shift;

  $self->debug(1, "Getting the gContainer Config");

  my $filter = "(&(objectClass=AliEngContainer)(ou=gContainer))";
  my $base   = "ou=Services,$self->{LDAPDN}";
  my $mesg = $ldap->search(    # perform a search
                          base   => "$base",
                          filter => "$filter"
                          );

  if ($mesg->count != 1) {
    $self->debug(1, "Could not find gContainer Configuration");
    return 1;
  }

  $self->{G_CONTAINER} = $self->getAttributes($mesg->entry(0));

  $filter = "(&(objectClass=AliEngContainerJudge))";
  $base   = "ou=Judges,ou=gContainer,ou=Services,$self->{LDAPDN}";
  $mesg = $ldap->search(    # perform a search
                          base   => "$base",
                          filter => "$filter"
                          );
  my $total = $mesg->count;
  $self->debug(1, "We have $total judges");
  $self->{G_CONTAINER}->{JUDGES_LIST} = [];
  for (my $i=0; $i<$total; ++$i) {
    push @{$self->{G_CONTAINER}->{JUDGES_LIST}}, $self->getAttributes($mesg->entry($i), 1);
  }

  $filter = "(&(objectClass=AliEngContainerService))";
  $base   = "ou=Services,ou=gContainer,ou=Services,$self->{LDAPDN}";
  $mesg = $ldap->search(    # perform a search
                          base   => "$base",
                          filter => "$filter"
                          );
  $total = $mesg->count;
  $self->debug(1, "We have $total services");
  $self->{G_CONTAINER}->{SERVICES_HASH} = {};
  for (my $i=0; $i<$total; ++$i) {
    my $service = $self->getAttributes($mesg->entry($i), 0);
    my $serviceName = $service->{NAME};

    my $judgeFilter = "(&(objectClass=AliEngContainerJudge))";
    my $judgeBase   = "ou=Judges,name=$serviceName,ou=Services,ou=gContainer,ou=Services,$self->{LDAPDN}";
    my $judgeMesg = $ldap->search(    # perform a search
                            base   => "$judgeBase",
                            filter => "$judgeFilter"
                            );

    if ($judgeMesg->count) {
      my $count = $judgeMesg->count;
      $self->debug(1, "$serviceName has $count own judges");

      $service->{JUDGES_LIST} = [];
      for (my $i=0; $i<$count; ++$i) {
        push @{$service->{JUDGES_LIST}}, $self->getAttributes($judgeMesg->entry($i), 1);
      }
    }

    if (grep(/AliEngContainerServiceGAS/, @{$service->{OBJECT_CLASS_LIST}})) {
      $self->debug(1, "Getting the GAS Modules");

      my $modulesFilter = "(&(objectClass=AliEnGASMODULE))";
      my $modulesBase   = "name=$serviceName,ou=Services,ou=gContainer,ou=Services,$self->{LDAPDN}";

      my $modulesMesg = $ldap->search(    # perform a search
                              base   => "$modulesBase",
                              filter => "$modulesFilter"
                              );

      my $modulesCount = $modulesMesg->count;
      $self->debug(1, "We have $modulesCount GAS modules");

      $service->{GAS_MODULES_HASH} = {};
      for (my $i=0; $i<$modulesCount; ++$i) {
        my $entry = $modulesMesg->entry($i);
        $service->{GAS_MODULES_HASH}->{$entry->get_value("alias")} = $self->getAttributes($entry, 1);
      }
    }

    $self->{G_CONTAINER}->{SERVICES_HASH}->{$serviceName} = $service;
  }

  return 1;
}

return 1;



