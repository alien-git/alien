use strict;
use AliEn::Config; 
use SOAP::Lite;
use Data::Dumper;
use Net::Domain;
use AliEn::SOAP;
my %serviceConfigMap = (
"Authen" => ["AUTH_HOST", "AUTH_PORT"],
"Logger" => ["LOG_HOST", "LOG_PORT"],
"Broker::Transfer" => ["TRANSFER_BROKER_ADDRESS", ""],
"Manager::Transfer" => ["TRANSFER_MANAGER_ADDRESS", ""],
"Optimizer::Transfer" => ["TRANSFER_OPTIMIZER_ADDRESS", ""],
"Optimizer::Job" => ["JOB_OPTIMIZER_ADDRESS", ""],
"Optimizer::Catalogue" => ["CATALOGUE_OPTIMIZER_ADDRESS", ""],
"Manager::Job" => ["JOB_MANAGER_ADDRESS", ""],
"Broker::Job" => ["JOB_BROKER_ADDRESS", ""],
"ClusterMonitor" => [undef, "CLUSTERMONITOR_PORT"],
# "PackManMaster" => ["PACKMANMASTER_ADDRESS", ""],
"MessagesMaster" => ["MESSAGESMASTER_ADDRESS", ""],
"Manager::SEMaster" =>["SEMASTER_MANAGER_ADDRESS", ""],
"Manager::JobInfo" =>["JOBINFO_MANAGER_ADDRESS", ""],
);

my $serviceName = shift;
my $logDir = shift;

$serviceName && $logDir
  or &syntax();

my $config = eval("new AliEn::Config();");
$config 
  or &error(-2, "Could not get Config. (Error $@)");
  
my $crtHost = $config->{HOST} || $config->{SITE_HOST} || Net::Domain::hostfqdn();

#print Dumper($config);

my $configHost = exists($serviceConfigMap{$serviceName}) ? $serviceConfigMap{$serviceName}->[0] : uc($serviceName) . "_HOST";
my $configPort = exists($serviceConfigMap{$serviceName}) ? $serviceConfigMap{$serviceName}->[1] : uc($serviceName) . "_PORT";


my $host = (defined($configHost) ? $config->{$configHost} : $crtHost);
#print $host;
my $HostHttps="";
if ($host) {
  $host =~ /^https/ and $HostHttps = $host ;
  $host =~ s/^http:\/\///;
  $host =~ s/^https:\/\///;
} 
my $port;
if ($host && $host =~ /^(.*):(\d+)$/){
  $host = $1;
  $port = $2;
}else{
  $port = (defined($configPort) ? $config->{$configPort} : "");
}

# commented out this part since the services can run in httpd, so checking the pid is wrong
#if($host && $port && ($host ne $crtHost)){
#  print "Skipping PID check. Service runs on a different machine ($host and we test from $crtHost)\n";
#}else{
#  print "Checking PID for $serviceName...\n";
#  check_pid($logDir, $serviceName);
#}


# This script cannot check the ProxyServer and MonaLisa because they do not inherit from AliEn::Service
#if ($serviceName =~ /^(ProxyServer)|(MonaLisa)|(CE.*)|(FTD)|(Optimizer.*)$/)
if ($serviceName =~ /^(ProxyServer)|(MonaLisa)|(CE.*)|(FTD)|(CMreport)$/)
{
  print "Doing PID-only check for $serviceName...\n";
  check_pid($logDir, $serviceName);
  exit 0;
}

if( $serviceName =~ /^(Optimizer.*)$/ )
{
	unless ( $HostHttps =~ /^https/)
	{
  		print "Doing PID-only check for $serviceName...\n";
  		check_pid($logDir, $serviceName);
  		exit 0;
  
	}
	
}
print "Pinging service $serviceName...\n";

$host
  or &error(-3, "Could not get host. Is it supposed to run here?");
$port
    or &error(-3, "Could not get service port. Is it supposed to run here?");

#my $uri = exists($servicesURIMap{$serviceName}) ? $servicesURIMap{$serviceName} : $serviceName;
my $uri = $serviceName;
$uri =~ s{::}{/};

print "The service is running at $host:$port, uri $uri\n";

my $errorNr;
my $errorMsg;
my $count = 0;

while ($count++ < 3)
{
  sleep 5 if ($count != 1);

  print "Attempt $count (" . scalar(localtime()) . ")\n";
 my $done ;
 
 
  if ( $serviceName =~ /^ClusterMonitor/ )
   {
 		if( $config->{CLUSTERMONITOR_ADDRESS} =~ /^https/ )
		{
			 my $soap= new AliEn::SOAP;
                  $soap->Connect({uri=>"AliEn/Service/$uri",address=>"https://$host:$port",name=>"$uri"});
                  $done = eval("SOAP::Lite->uri(\"AliEn/Service/$uri\")->proxy(\"https://$host:$port\", timeout => 10)->ping()"); 
		}else{					
			 $done = eval("SOAP::Lite->uri(\"AliEn/Service/$uri\")->proxy(\"http://$host:$port\", timeout => 10)->ping()");
   			  			         
		}
   }else {
  		if ($HostHttps =~ /^https/)
       {
       	  print $uri;
       	  print $host;
       	  print $port;
           my $soap= new AliEn::SOAP;
          $soap->Connect({uri=>"AliEn/Service/$uri",address=>"https://$host:$port",name=>"$uri"});
           $done = eval("SOAP::Lite->uri(\"AliEn/Service/$uri\")->proxy(\"https://$host:$port\", timeout => 10)->ping()"); 
         } else{  
         	$done = eval("SOAP::Lite->uri(\"AliEn/Service/$uri\")->proxy(\"http://$host:$port\", timeout => 10)->ping()"); 
         }   

}
  if (!$done)
  {
    $errorNr = -4;
    $errorMsg = "Could not contact service. (Error $@)";
    next;
  }

  if ($done->fault)
  {
    $errorNr = -5;
    $errorMsg = "Error in call. " . Dumper($done);
    next;
  }

  if (!$done->result)
  {
    $errorNr = -6;
    $errorMsg = "Nothing returned. ($done->{error})";
    next;
  }

  print "The service $serviceName is alive and running version " . $done->result->{VERSION} . "\n";
  exit 0;
}

&error($errorNr, $errorMsg);

sub syntax()
{
  print "Syntax: pingService.pl <serviceName> <logDir>\n";
  exit -1;
}

sub check_pid {
  my $logdir = shift;
  my $serviceName = shift;

  if(open(FILE, "$logdir/$serviceName.pid")){
    my @lines = <FILE>;
    close(FILE);
    my @pids=split(/\s+/, join(" ", @lines));
    my $subprocess = 0;
    for my $pid (@pids){
      if(! kill(0, $pid)){
        print "".($subprocess ? "SUB-PROCESS " : "") . "DEAD\n";
        exit(1);
      }
      $subprocess = 1;
    }
  }else{
    print "DEAD\n";
    exit(1);
  }
}

sub error()
{
  my $errorCode = shift;
  my $errorMsg = shift;

  print "ERROR: $errorMsg\n";
  exit $errorCode;
}
