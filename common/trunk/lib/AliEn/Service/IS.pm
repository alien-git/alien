#
#  Information service daemon for Alien
#

package AliEn::Service::IS;

use AliEn::Database::IS;
use AliEn::Database::TaskQueue;
use AliEn::Database::Catalogue;


use AliEn::UI::Catalogue;
use AliEn::Service;
use strict;

use Classad;

use vars qw (@ISA);
@ISA=("AliEn::Service");

my $self = {};

my @services = ("SE", "XROOTD", "ClusterMonitor", "FTD");

###############################    Private Functions #########################
my $my_getActiveFTD = sub {
    my $db     = shift;
    my $domain = shift;
    my (@allFTD) = $db->getAllFromFTD($domain);

    return split "###", $allFTD[0];
};

#############################################################################
sub initialize {
  $self=shift;
  my $options={};
  

  my $role="admin";


  $ENV{ALIEN_DATABASE_SSL} and $role.="ssl";
  my $connect={DEBUG=>$self->{DEBUG}, 
	       ROLE=>$role};

  if ( (defined $ENV{ALIEN_NO_PROXY}) && ($ENV{ALIEN_NO_PROXY} eq "1") && (defined $ENV{ALIEN_DB_PASSWD}) ) {
    $connect->{USE_PROXY}=0;
    $connect->{PASSWD} = $ENV{ALIEN_DB_PASSWD};
  }

  $self->{DB} = new AliEn::Database::IS($connect) or
    print $self->info("In initialize creation of IS database instance failed" )
      and return;

  $self->{PORT}=$self->{CONFIG}->{'IS_PORT'};
  $self->{HOST}=$self->{CONFIG}->{'IS_HOST'};
  $self->{SERVICE}="IS";
  $self->{SERVICENAME}="IS";
  $self->{LISTEN}=1;
  $self->{PREFORK}=5;

  # connect to the catalogue
  $options->{role} = $role;
  $self->{CATALOGUE} = AliEn::UI::Catalogue->new($options);
  ($self->{CATALOGUE} )
    or $self->{LOGGER}->error( "IS", "In initialize error creating userinterface" )
      and return;


  my $queue_host = $self->{CONFIG}->{QUEUE_DB_HOST};
  my $db         = $self->{CONFIG}->{QUEUE_DATABASE};
  my $driver     = $self->{CONFIG}->{QUEUE_DRIVER};
  
  $self->{CPUQUEUE} = AliEn::Database::TaskQueue->new({
						       "DB"     => $db, 
						       "HOST"   => $queue_host,
						       "DRIVER" => $driver,
						       ROLE=>$role});

  $self->{CPUQUEUE}
  or print $self->{LOGGER}->error( "IS", "In initialize creation of TaskQueue instance failed" )
      and return;

    $self;

}


sub setAlive {
  my $s=shift;

  my $date=time;

  ($date<$self->{LASTALIVE})
     and return;
  if($self->{MONITOR}){
    # send the alive status also to ML
    $self->{MONITOR}->sendBgMonitoring();
    #$self->info("setAlive -> sent Bg Mon info to ML.");
  }
  $self->{LASTALIVE}=$date+100;
  return $self->markAlive("IS", "IS", $self->{HOST}, $self->{PORT},
			  {VERSION=>$self->{CONFIG}->{VERSION}, 
			   URI=>$self->{URI}});

}
sub markAlive {
  my $this    = shift;
  my $service = shift;
  my $name    = shift;
  my $host    = shift;
  my $port    = shift;
  my $data    = shift;

  my $version = ($data->{VERSION} or "");
  my $uri     = ($data->{URI} or "AliEn::Services::$service");
  my $protocols = ($data->{PROTOCOLS} or "");
  my $cert    =($data->{CERTIFICATE} or "");

  my $now     = time;

  my $table= "Services";
  if ($service =~ /^(SE)|(FTD)|(ClusterMonitor)|(XROOTD)|(PackMan)$/) {
    $table=$service;
  }

  $self->debug(1, "In markAlive setting data for service $name in table $service ($cert)");

  my $done = $self->{DB}->setService($table,$name,$host,$port,"ACTIVE",$now,$version,$uri,$protocols, $cert)
    or $self->{LOGGER}->error( "IS", "In markAlive error setting data for service $name in table $service")
      and return (-1, "Error inserting the entry in the database");

  $self->info("$service $name at $host $port is alive" );
  ($service eq "IS") or $self->setAlive();
  return $done;
}

sub getRoute {

    #This will probably be either a SOAP Call or checking a central database;
    my $this      = shift;
    my $source    = shift;
    my $finaldest = shift;
    my $size      = shift;
    my $finaldestPort = (shift or "8091");

    $self->info("Giving route from $source to $finaldest at  $finaldestPort" );
    my $result;

    my $host = $source;
    $host =~ s/\./\#/g;
    my @temp = split ( "#", $host );
    my $next = join ( ".", @temp );

    while (1) {
        my $finaldesthost = $finaldest;
        $finaldesthost =~ s/\./\#/g;
        my @tempdest = split ( "#", $finaldesthost );
        my $nextdest = join ( ".", @tempdest );
        my $testflag = 1;
        while ($testflag) {
            $self->debug(1, "In getRoute testing Source: $next Dest: $nextdest" );

			$result = $self->{DB}->getRouteByPath($next, $nextdest);

			defined $result
				or $self->{LOGGER}->error( "IS", "In getRoute error during execution of database query" )
				and return;

			if (@$result) {
				my $route = $result->[0];
				#my @route = split ( "###", $result[0] );
                if ( $route->{nextdest} eq "*" ) {
                    $route->{nextdest} = $finaldest;
                }
                if ( $route->{soaphost} eq "*" ) {
                    $route->{soaphost} = $finaldest;
                }

				$route->{soapport}
					or $route->{soapport} = $finaldestPort;

				my @tmpArr = ($route->{nextdest},$route->{method},$route->{soaphost},$route->{soapport});

				$self->info("Giving back route @tmpArr" );

				return @tmpArr;
            }

            if ( $nextdest eq "*" ) {

                #This was search for default route
                $testflag = 0;
            }
            shift (@tempdest);
            if (@tempdest) {
                $nextdest = "*." . join ( ".", @tempdest );
            }
            else {
                $nextdest = "*";
            }
        }

        shift (@temp);
        if (@temp) {
            $next = "*." . join ( ".", @temp );
        }
        else {
            $next = "*";
        }
    }
    $self->info("Giving back BBFTP $finaldest $finaldestPort" );
    return ( $finaldest, "BBFTP", $finaldest, $finaldestPort );

}

sub DESTROY {
    my $this = shift;
    $self->{DB} and $self->{DB}->destroy();
	$self->{CPUQUEUE} and $self->{CPUQUEUE}->destroy();
}

sub GetPhysicalFile {
    my $t          = shift;
    my $file       = shift;
    my $targetHost = shift;
    print STDERR "\n";
    $self->info("Sending file $file to $targetHost" );
    my $targetDomain = "";
    ( $targetHost =~ /\.([^\.]*\.[^\.]*)$/ ) and $targetDomain = $1;
    ($targetDomain)
      or $self->{LOGGER}->warning( "IS", "In getRoute impossible to get domain from $targetHost" )
      and return;

    my $srcDomain = "";

    ( $file =~ /\$site=\'([^\']*)\';/ ) and $srcDomain = $1;

    ($srcDomain)
      or $self->{LOGGER}->warning( "IS", "In getRoute impossible to get domain from $file" )
      and return;

    $self->debug(1, "In getRoute sending file from $srcDomain to $targetDomain" );
    ( $srcDomain eq $targetDomain )
      and $self->info("Same source and target" )
      and return 1;

    my ( $srcFTD, $srcPort ) = $my_getActiveFTD->( $self->{DB}, $srcDomain );

    ($srcFTD)
      or $self->info("There are no active FTD at $srcDomain" )
      and return;

    my ( $targetFTD, $targetPort ) =
      $my_getActiveFTD->( $self->{DB}, $targetDomain );

    ($targetFTD)
      or
      $self->info("There are no active FTD at $targetDomain" )
      and return;

    $self->debug(1, "In getRoute sending file from $srcFTD to $targetFTD" );

    my $FTDresponse =
      SOAP::Lite->uri("SE/FTD")->proxy("http://$srcFTD:$srcPort")
      ->sendFile( $file, $targetFTD, $targetPort );

    ($FTDresponse)
      or $self->{LOGGER}->warning( "IS", "In getRoute impossible to contact FTD at $srcFTD:$srcPort" )
      and return;

    $self->info("Done with " . $FTDresponse->result );
    return $FTDresponse->result;
}

sub getService {
  my $this=shift;
  my $name=shift;
  my $service=shift;
  
  $service 
    or $self->info("Service name is missing")
      and return (-1, "service name is missing");
  
  $self->info("Getting the $service of $name" );
  
  # Fetching of host, port, protocols field of a row, containing the value of $name in the name field and statous is active, from the table $service of INFORMATIONSERVICE database. 
  my $ftd = $self->{DB}->getActiveServices($service,"host,port,protocols,certificate,uri",$name);
  
  defined $ftd
    or $self->{LOGGER}->error( "IS", "In getService error during execution of database query" )
      and return (-1, "error during execution of database query" );
  
  @$ftd or
    $self->info("No ACTIVE ${service}s for $name" ) and
      return (-1, "no ACTIVE ${service}s for $name" );
  
  $self->info("Returning service $ftd->[0]->{host}:$ftd->[0]->{port}" );
  my $entry=shift @{$ftd};
  $entry->{PORT}=$entry->{port};
  $entry->{HOST}=$entry->{host};
  $entry->{PROTOCOLS}=$entry->{protocols};

  # Returning the value of host, port and protocol field
  return $entry;
}

sub getReverse {
  my ($this, $hostport) = @_;
  $self->info("Called getReverse for $hostport");

  my ($host, $port) = split ":", $hostport;
  for (@services) {
		my $result = $self->{DB}->getField($_,$host,$port,"name");
		$result and
			$self->info("Returning LHN $result SERVICE $_") and
			return {"LHN", $result, "SERVICE", $_};
  }
  
  $self->info("$hostport not found");
  return {-1, "Host $hostport not found"};
}

# get all active hosts for Service $service
sub getAllServices {
  my $this=shift;
  my $service=shift;

  $self->info("Getting all $service services" );
  my ($aservices) =
    $self->{DB}->getActiveServices($service,"host, port, name");
  
  defined $aservices
    or $self->{LOGGER}->error( "IS", "In getAllServices error during execution of database query" )
      and return;
  
  @$aservices or
    $self->info("No active ${service}s" ) and
      return (-1, "No ACTIVE ${service}s");
  
  my $hosts = "";
  my $ports = "";
  my $names  = "";
  for (@$aservices) {
    $hosts .= "$_->{host}:";
    $ports .= "$_->{port}:";
    $names .= "$_->{name}###";
  }
  chop $hosts;
  chop $ports;
  $names = substr ($names,0,-3);
  return { "HOSTS", $hosts, "PORTS", $ports,"NAMES",$names};
}



sub getTimestamp {
  my $this = shift;
  my $oldtime = (shift or "");
  my $newtime = time;
  $self->info("Get Timestamp $oldtime");
  if ($oldtime) {
    $oldtime .= "\n";
    return $oldtime;
  } else {
    $newtime .= "\n";
    return $newtime;
  }
}

sub getFTD {
    my $this = shift;
    my $SE   = shift;
    my $name = "";

    $SE =~ /^(.*)::[^:]*$/ and $name = $1;

    return  $self->getService($name, "FTD");

}

sub getFTDbyHost {
    my $this = shift;
    my $ftdhost = shift;
    
    $self->info("Called getFTDbyHost  with $ftdhost");
    # Fetching of name field of a particular host($ftdhost) from FTD table in INFORMATIONSERVICE database 
    my $ftdname = $self->{DB}->getServiceNameByHost("FTD",$ftdhost);
    (@$ftdname) or return;
    $ftdname=$ftdname->[0]->{name}; 
    $self->info(" getFTDbyHost returned  with $ftdname");
    # Fetching of row, contained $ftdname value in name field, from the FTD table of INFORMATIONSERVICE database. 
    return $self->getService($ftdname,"FTD");
}
    

sub getArgs {
  my $this = shift;
  my @args = @_;
  my $retargs = join '',@args;
  $retargs .= "\n";
  $self->info("Called getArgs  with @args and $retargs");
  return $retargs;
}

sub getPrint {
    my $this = shift;
    $self->info("Called getPrint" );    
    return 1000;
}
 
sub getSE {
    my $this = shift;
    my $SE   = shift;

    return  $self->getService($SE, "SE");
    
}


sub getXROOTD {
    my $this = shift;
    my $SE   = shift;
    $self->info("Called getXROOTD for $SE");
    return $self->getService($SE, "XROOTD");
}


sub getServiceUser {
    my $this    = shift;
    my $service = shift or return;
    my $name    = shift or return;

    my $serviceinfo = $self->getService($name,$service);
    
    my ($done) =SOAP::Lite->uri("AliEn/Service/$service")
      ->proxy("http://$serviceinfo->{'HOST'}:$serviceinfo->{'PORT'}")
	->owner();
    $done and $done=$done->result;
    
    return $done;
}

sub alive {
    my $self2 = shift;
    my $host    = shift;
    my $port    = shift;
    my $version = ( shift or "" );

    my $date = time;


    if ( (!$host) || (!$port) ) {
      my $servicestate = $self->getServiceState();
      return {"VERSION" => $self->{CONFIG}->{VERSION}, "Disk" => $servicestate->{'Disk'}, "Run" => $servicestate->{'Run'}, "Sleep" => $servicestate->{'Sleep'}, "Trace" => $servicestate->{'Trace'}, "Zombie" => $servicestate->{'Zombie'} };
    }

    $self->{LOGGER}
      ->info( "IS", "Host $host (version $version) is alive" );


    my ($error) =
      $self->{CPUQUEUE}->getFieldFromHosts($host, "hostId");

    if ( !$error ) {
		$self->InsertHost($host, $port)
			or print "DEVOLVEMOS ".$self->{LOGGER}->error_msg and
			return (-1, $self->{LOGGER}->error_msg);
    }

    $error = $self->{CPUQUEUE}->updateHost($host,{status=>'CONNECTED',
											connected=>1,
											hostPort=>$port,
											date=>$date,
											version=>$version}
      )
		or $self->{LOGGER}->error("IS","In alive error updating host $host")
		and return(-1,"error updating host $host");

    $self->debug(1, "Done" );
    return 1;
}


# if you want to put the CLC certificates, you need first to delete all existing
# certificates under /keys/peters/ using the normal catalog functionality
# after createCertifiate, the permissions of the .pem files have to be changed to
# chmod 0600 !

sub createCertificate {
    # create a new certificate pair ....
  my $this = shift;
  my $user = shift;
  my $name = shift;

  my $privkey="";
  my $certificate="";
  my $randomnumber=rand();
  my $certrandom  =rand();
  my $certfile = "/tmp/$randomnumber";


  my @args = $self->{CATALOGUE}->execute("ls","-la","/keys/$user/$name%");

    if (@args) {
      $self->info("$user has to delete the existing /keys/$user/$name file!");
      return;
    }

  open (INPUT,"openssl genrsa 1024 |");
  open (CERTIFICATE,">$certfile");
  chmod 0600, $certfile;

  while (<INPUT>) {
    $privkey = "$privkey$_";
  }
  close (INPUT);
  print CERTIFICATE $privkey;
  print $privkey,"\n";
  close (CERTIFICATE);
  print "Doing Open\n";
  my $openssl = `which openssl`;
  chomp $openssl;
#  system("echo \"CH\nGeneva\nGeneva\nCERN\nAliEn\nCLC-Certificate\ninfo\@alien.cern.ch\n\" | $openssl req -new -x509 -key $certfile -days 365 -out ");
  print "Doing Open\n";
  open (INPUT," echo \"CH\nGeneva\nGeneva\nCERN\nAliEn\nCLC-Certificate\ninfo\@alien.cern.ch\n\" | $openssl req -new -x509 -key $certfile -days 365 |");

  while (<INPUT>) {
    $certificate = "$certificate$_";
    print $certificate,"\n";
  }
  print "Done Open\n";
  close (INPUT);
  system("rm","$certfile");
  print $certificate,"\n";
  # now put them in place in the cataloge into /keys/<user>/ dir
  $self->putCertificate($user,$privkey,"$name-key","$name-key-$certrandom.pem");
  $self->putCertificate($user,$certificate,"$name-cert","$name-cert-$certrandom.pem");
  return $certificate;
}


sub putCertificate {
    my $this = shift;
    my $user = shift;
    my $certificate = shift;
    my $tag  = shift;
    my $name = shift;
    my @args = $self->{CATALOGUE}->execute("ls","-la","/keys/$user/$tag%");

    if (@args) {
      $self->info("User $user has to delete the existing /keys/$user/$name file!");
      return;
    }

    $self->{DB}->deleteCertificate($user,"$tag%")
		or $self->{LOGGER}->error( "IS", "In putCertificate error deleting old certificates for $user")
		and return;

    $self->{DB}->insertCertificate($user,$name,$certificate)
    	or $self->{LOGGER}->error( "IS", "In putCertificate error inserting certificate for $user")
		and return;

	@args = $self->{CATALOGUE}->execute("mkdir","/keys/$user/");
    @args    = $self->{CATALOGUE}->execute("chown","$user","/keys/$user/");
    @args = $self->{CATALOGUE}->execute("ls","-la","/keys/$user/");
    if (! @args) {
      $self->info("Cannot create certificate entry /keys/$user for $user");
      return;
    }
    @args = $self->{CATALOGUE}->execute("register","/keys/$user/$name","soapfunc://$self->{HOST}:$self->{PORT}/?URI=IS?CALL=getCertificate?ARGS=$user,$name");
    @args = $self->{CATALOGUE}->execute("ls","-la","/keys/$user/$name");
    if (! @args) {
      $self->info("Cannot create certificate entry /keys/$user/$name for $user");
      return;
    }
    @args    = $self->{CATALOGUE}->execute("chown","$user","/keys/$user/$name");
    @args    = $self->{CATALOGUE}->execute("chmod","666","/keys/$user/$name");
    return "/keys/$user/$name";
}

sub getCertificate {
    my $this = shift;
    my $user  = shift;
    my $name = shift;

    $self->info("Get Certificate for $user" );
    $self->info("List /keys/$user/$name" );
    my @args = $self->{CATALOGUE}->execute("ls","-la","/keys/$user/$name");

    if (! @args) {
      $self->info("No certificate for $user in the catalogue");
    }

    $self->info("@args");
    my @listing = split "###", $args[0];


    if ( ($listing[0] eq "-rw-------") && ($listing[1] eq "$user") ) {
		# the file has reasonable permissions, so get it and return via SOAP
		my $cert = $self->{DB}->getCertificate($user,$name);

		$cert
			and $self->info("Certificate successfully retrieved")
			and return $cert;
	}else {
		$self->info("Suspicious listing @listing");
    }

    $self->info("No certificate for $user in the catalogue");
    return;

}

sub InsertHost {
    my $this =shift;
    my $host =shift;
    my $port =shift;

    $self->info("Inserting new host $host" );
    
    my $domain;
    ( $host =~ /^[^\.]*\.(.*)$/ ) and $domain = $1;
    ($domain)
	or $self->{LOGGER}->error( "IS", "In InsertHost domain not known" )
	    and return;

    $self->info("Domain '$domain'" );
    my ($domainId) =$self->{CPUQUEUE}->getSitesByDomain($domain,"siteId");

	defined $domainId
		or $self->{LOGGER}->warning( "IS", "In InsertHost error during execution of database query" )
		and return;

	@$domainId
		or $self->{LOGGER}->error( "IS", "In InsertHost domain $domain not known" )
		and return;

    $self->{CPUQUEUE}->insertHostSiteId($host, $domainId)
		or $self->{LOGGER}->error( "IS", "In InsertHost error inserting host $host in domain $domainId" )
		and return;

    $self->info("Host inserted");
    return 1;
}

# Forward it to AliEn::Database::IS;
sub getCpuSI2k {
  my $this = shift;
  my $cpu_type = shift;
  my $cm_host = shift;

  my $result = $self->{DB}->getCpuSI2k($cpu_type, $cm_host);
  if(! $result){
    return (-1, "CPU not found in the cpu_si2k table for host='$cpu_type->{host}'");
  }else{
    return $result;
  }
}

sub getCloseSE{
  my $this=shift;
  my $site=shift || "";
  my $type=shift || "";
  my $excludeListRef=shift;
  my $closeListRef=shift;
  
  my @excludeList=();
  $excludeListRef and push @excludeList, @$excludeListRef;
  my @closeList=();
  $closeListRef and push @closeList, @$closeListRef;

  $self->info("Getting the close SE for the site $site");

  my $query="SELECT name from SE where status='ACTIVE'";
  $type and $query .=" and protocols='$type'";
  my $entries=$self->{DB}->queryColumn($query);
  use Data::Dumper;
  print Dumper $entries;
  my @se;
  #first, let's remove the ones from the excludeList
  foreach my $se (@$entries){
    grep (/^$se$/i, @excludeList) and 
      $self->info("The client doesn't want '$se'") 
	and next;
    push @se, $se;
  }
  if (@se){
    #Now, let's see if there is any in the 'closeList'
    foreach my $se (@closeList){
      grep (/^$se$/, @se) or next;
      $self->info("The se $se is in the close list");
      return $se;
    }
    #ok, let's see if there is anything in the site
    my @site=grep (/\:\:$site\:\:/i, @se);
    if (@site){
      $self->info("The se @site are in the same site. Returning $site[0]");
      return $site[0];
    }
    $self->info("Returning $se[0] (out of @se)");
    return $se[0];

  }

  $self->info("There are no close SE with policy '$type'");
  return (-1, "There are no close SE to $site with policy '$type'");
}


sub getSEListFromSiteSECache{
   my $this=shift;
   my $count=(shift || 0);
   my $type=(shift || "none");
   my $sitename=(shift || "none");
   my $excludeList=(shift || "");
   $self->info("The SERank Cache is accessed");
   $self->info("Parameters are, Type: $type, Count: $count, Site: $sitename, Exclud. Ses: @$excludeList");

   my $catalogueDB=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryColumn("select sename from SE");

$self->info("CATALOGUE_DB: We asked for the SE table in the catalogue, we got:");
foreach (@$catalogueDB) { $self->info("CATALOGUE_DB: one se element is: $_"); }

   $self->checkSiteSECache($sitename) or return 0;

   my $query="SELECT SE.seName FROM SERanks,SE WHERE "
      ." sitename = '$sitename' and SERanks.seNumber = SE.seNumber ";
   foreach(@$excludeList){   $query .= "and SE.seName <> '$_' ";   }  
   $type and $query .=" and SE.seQoS  LIKE '%$type%'"; 
   $query .= " ORDER BY rank ASC limit $count ;";

$self->info("query on DB will be: ||$query||");
 
   return $self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryColumn($query);
}


sub checkSiteSECache{
   my $this=shift;
   my $site=shift;

$self->info("Checking the SERank Cache for site: $site");

   my $query="SELECT sitename FROM SERanks WHERE sitename = '$site';";

$self->info("query on DB will be: ||$query||");

   my $reply = $self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryColumn($query);

$self->info("Reply was: @$reply .");  

   (scalar(@$reply) < 1) and $self->info("We need to update the SERank Cache. Adding not listed site: $site")
            and return $self->updateSiteSECacheForSite($site);
   return 1;
}



sub updateSiteSECacheForSite{
   my $this=shift;
   my $site=shift;
   $self->info("Starting the add SERank Cache entries for site: $site");

   my $query = "INSERT INTO SERanks (sitename,rank,seNumber,updated) "
           ."SELECT '$site' sitename, \@num := \@num + 1 rank , SE.seNumber, 0 updated FROM "
           ."(SELECT \@num := 0) rank, SE ;";

$self->info("query on DB will be: ||$query||");

   my $reply = $self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB}->queryColumn($query);
   
   return 1;
}



return 1;
__END__

=head1 NAME
  
AliEn::Service::IS

=head1 SYNOPSIS

 getFTDbyHost($ftdhost)

$self->getService($name,"FTD")

=head1 METHODS 

=item C<getFTDbyHost>

The INFORMATIONSERVICE database contains informations about services. It contains following tables:tables for services, Service table and certificate tables. Each service has it's own table. All tables for services have same set of attributes.The function getFTDbyHost calls the function getServiceNameByHost of AliEn::Database::IS module and the function getServiceNameByHost fetches the name field from the FTD table for a particular host($ftdhost). After that getFTDbyHost calls the function getService of this module.

=item C<getService>

getService function calls the function getActiveServices of AliEn::Database::IS module. The getActiveServices function fetches the value of port,host, protocol field for services with status ACTIVE and name $name from table $serviceName of INFORMATIONSERVICE database. 
