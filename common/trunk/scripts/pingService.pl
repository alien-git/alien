use strict;
use AliEn::Config; 
use Data::Dumper;
use Net::Domain;
use AliEn::RPC;

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
"Manager::JobInfo" =>["JOBINFO_MANAGER_ADDRESS", ""],
);

my $serviceName = shift;
my $logDir = shift;

$serviceName && $logDir
  or &syntax();

print "HOLA $serviceName and $logDir\n";
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
my $HTTPS=0;
if ($host) {
  $host =~ s/^http:\/\/// and $HTTPS=1;
  $host =~ s/^https:\/\///;
} 
my $port;
if ($host && $host =~ /^(.*):(\d+)$/){
  $host = $1;
  $port = $2;
}else{
  $port = (defined($configPort) ? $config->{$configPort} : "");
}



# This script cannot check the ProxyServer and MonaLisa because they do not inherit from AliEn::Service
#if ($serviceName =~ /^(ProxyServer)|(MonaLisa)|(CE.*)|(FTD)|(Optimizer.*)$/)
if ($serviceName =~ /^(MonaLisa)|(CE.*)|(FTD)|(CMreport)|(Optimizer.*)$/)
{
  print "Doing PID-only check for $serviceName...\n";
  check_pid($logDir, $serviceName);
  exit 0;
}

print "Pinging service $serviceName...\n";

$host
  or &error(-3, "Could not get host. Is it supposed to run here?");
$port
    or &error(-3, "Could not get service port. Is it supposed to run here?");


my $uri = "http";
$HTTPS and $uri.="s";
$uri.="://$host:$port/alien/";

print "The service $serviceName is running at $uri\n";

my $errorNr;
my $errorMsg;
my $count = 0;

while ($count++ < 3)
{
  sleep 5 if ($count != 1);

  print "Attempt $count (" . scalar(localtime()) . ")\n";
  my $done ;
 
  print "We have to contact $serviceName at $uri";
 
  my $rpc=AliEn::RPC->new();
  $rpc->Connect($serviceName, $uri) or print "Error connecting to the service" and next;
  my ($version)=$rpc->CallRPC($serviceName, "status");

  if (!$version)
  {
    $errorNr = -4;
    $errorMsg = "Could not contact service. (Error $@)";
    next;
  }

  print "The service $serviceName is alive and running version $version\n";
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
