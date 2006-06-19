use strict;
use AliEn::Config;
use SOAP::Lite;
use Data::Dumper;
use Net::Domain;

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
);

my $serviceName = shift;

$serviceName
  or &syntax();

# This script cannot check the ProxyServer and MonaLisa because they do not inherit from AliEn::Service
if ($serviceName =~ /^(ProxyServer)|(MonaLisa)|(CE)$/)
{
  print "We cannot check $serviceName...\n";
  exit 0;
}

print "Pinging service $serviceName...\n";

my $config = eval("new AliEn::Config();");
$config
  or &error(-2, "Could not get Config. (Error $@)");

my $configHost = exists($serviceConfigMap{$serviceName}) ? $serviceConfigMap{$serviceName}->[0] : uc($serviceName) . "_HOST";
my $configPort = exists($serviceConfigMap{$serviceName}) ? $serviceConfigMap{$serviceName}->[1] : uc($serviceName) . "_PORT";

my $host = Net::Domain::hostfqdn();
$host = $config->{$configHost} if (defined $configHost);
$host
  or &error(-3, "Could not get host. Is it supposed to run here?");

my $port;
if ($host =~ /^(.*):(\d+)$/)
{
  $host = $1;
  $port = $2;
}
else
{
  $port = $config->{$configPort};
  $port
    or &error(-3, "Could not get service port. Is it supposed to run here?");
}

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

  my $done = eval("SOAP::Lite->uri(\"AliEn/Service/$uri\")->proxy(\"http://$host:$port\", timeout => 10)->ping()");
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
  print "Syntax: pingService.pl <serviceName>\n";
  exit -1;
}

sub error()
{
  my $errorCode = shift;
  my $errorMsg = shift;

  print "ERROR: $errorMsg\n";
  exit $errorCode;
}
