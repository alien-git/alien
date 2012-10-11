package AliEn::RPC;

use JSON::RPC::Client;
use AliEn::Logger::LogObject;
use AliEn::Config;
use AliEn::X509;
use Data::Dumper;
use vars qw (@ISA $DEBUG);
use strict;
push @ISA, 'AliEn::Logger::LogObject';

sub new {
  my $proto = shift;
  my $GUID  = (shift or "");
  my $self  = {};
  bless($self, (ref($proto) || $proto));
  $self->SUPER::new() or return;
  
  $self->{X509}=AliEn::X509->new();
  
  $self->{CLIENTS}={};
  $self->{CONFIG}=AliEn::Config->new();
  return $self;
   
}
sub Connect {
  my $self=shift;
  my $service=shift;
  my $address=shift || "";
  my $type=shift || $service;
  
  
  my @methods;
  
  if ($type =~ /^JobAgent$/){
    @methods=('dieGracefully', "getFile");
  }elsif($type =~ /^MessagesMaster$/){
    @methods ="getMessages";
  }elsif($type =~ /^Authen$/){
    @methods =("doOperation","insertCert",'doPackMan');
  }elsif($type eq "IS"){
    @methods= ("getCpuSI2k", "markAlive","getAllServices", "getService")    
  }elsif($type eq "Manager/Job"){
    @methods= ("alive", "enterCommand","changeStatusCommand", "getSpyUrl", "SetProcInfoBunch","SetProcInfoBunchFromDB")    
  }elsif($type eq "Manager/Transfer"){
    @methods= ("checkOngoingTransfers", "enterTransfer", "listTransfer", "checkOngoingTransfers", "changeStatusTransfer", "killTransfer", "FetchTransferMessages")    
  }elsif($type eq "ClusterMonitor"){
    @methods=("getStdout","getNumberJobs", "GetConfiguration","jobStarts", "putJobLog","putFILE", "SetProcInfo","getCpuSI2k", "packmanOperations");
  }elsif($type eq "Broker/Job"){
    @methods=("offerAgent","getJobAgent");
  }elsif($type eq "Broker/Transfer"){
    @methods=("requestTransferType");
  }elsif($type eq "Manager/JobInfo"){
    @methods=("getTrace");
  }
  push @methods, "status";
  $self->debug(1, "Checking if we are connected to $service");
  $self->{CLIENTS}->{$service} and return 1;
  
  
  ( $address) or $address=$self->findAddress($service);
  
  $address or $self->info("Error finding the address of $service") and return;
  
  ($address =~ /^http/ or $address="http://$address" );
  
  $self->info("Connecting to $service in $address");
  $self->{CLIENTS}->{$service} = new JSON::RPC::Client;
  if ($address=~ /^https:/) {
    my $proxy = ( $ENV{X509_USER_PROXY} || "/tmp/x509up_u$<" );
    
    $self->info("This is in fact a secure connection (using the proxy $proxy)");

    $self->{X509}->checkProxy();
    my $ua=$self->{CLIENTS}->{$service}->ua();
    $ua->ssl_opts('verify_hostname' => 0);
    $ua->ssl_opts( 'SSL_cert_file' => $proxy);
    $ua->ssl_opts( 'SSL_key_file' => $proxy);
  }

  return $self->{CLIENTS}->{$service}->prepare($address, \@methods);
  
}

sub findAddress {
  my $self=shift;
  my $service=shift;
  
  my $address;
  $self->info("Finding the address of '$service'");
  if ($service =~ /^Authen$/){
    $address="$self->{CONFIG}->{AUTH_HOST}:$self->{CONFIG}->{AUTH_PORT}";    
  } elsif ($service=~ /^IS$/){
    $address="$self->{CONFIG}->{IS_HOST}:$self->{CONFIG}->{IS_PORT}";  
  } elsif ($service eq "Manager/Job"){
    $address="$self->{CONFIG}->{JOB_MANAGER_ADDRESS}";
  } elsif ($service eq "Manager/Transfer"){
    $address="$self->{CONFIG}->{TRANSFER_MANAGER_ADDRESS}";
  } elsif ($service eq "Broker/Job"){
    $address="$self->{CONFIG}->{JOB_BROKER_ADDRESS}";
  } elsif ($service eq "Broker/Transfer"){
    $address="$self->{CONFIG}->{TRANSFER_BROKER_ADDRESS}";
  } elsif ($service eq "Manager/JobInfo"){
    $address="$self->{CONFIG}->{JOBINFO_MANAGER_ADDRESS}";
  } elsif($service eq "ClusterMonitor"){
    if ($ENV{ALIEN_CM_AS_LDAP_PROXY}){
      $address=$ENV{ALIEN_CM_AS_LDAP_PROXY};
    }else{
      $address="$self->{CONFIG}->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}";  
    }
  }
   
  return $address;
  
}

sub CallRPCAndDisplay{
  my $self=shift;
  my $silent = shift;
  my @signature = @_;
  my $service = shift;
  my $function = shift;
  my $result;

  my $maxTry = 2;
  my $tries;
  
  
  for ($tries = 0; $tries < $maxTry; $tries++) { # try five times
    ($result)=$self->CallRPC(@signature) and last;
    $self->info("Calling $service over RPC did not work, trying ".(4 - $tries)." more times till giving up.");
    sleep(($maxTry*$tries));
  }
  if ( $tries == $maxTry ){
    $self->error("We have asked the server $maxTry times, and it didn't work");
    return;
  }
  $result or $self->info("The call didn't return anything") and return;
  if (defined($result->{rcmessages})) {
   map {defined $_ or $_=""} @{$result->{rcmessages}};
    $silent
      or $self->raw(join ("", @{$result->{rcmessages}}),undef,0);
  }

  defined($result->{rcvalues}) and
    (ref $result->{rcvalues} eq "ARRAY") and  return @{$result->{rcvalues}};
  $self->info("There was no rcvalues");
  return $result;
  
  
}
sub CallRPC {
  my $self=shift;
  my $service=shift;
  my $op=shift;
  
  $self->{CLIENTS}->{$service} or $self->Connect($service);
  
  $self->{CLIENTS}->{$service} or $self->info("We are not connected to $service") and return;
  
  my $retry=0;
  
  
  if ($_[0] and $_[0] =~ /-retry/){
    shift;
    
    $retry=1;
  }
   # Easy access
  
  if ($self->{CLIENTS}->{$service}->ua()->ssl_opts('SSL_cert_file')){
     $self->info("This is in fact a secure call. Checking the proxy from the call");    
     $self->{X509}->checkProxy();
  }
  
  while(1){
    my @data;
    eval {
      $self->debug(1, "Ready to make an RPC call to $service $op()");
      @data=$self->{CLIENTS}->{$service}->$op(@_);
      $self->debug(1, "The RPC call finished correctly");
      $self->debug(2, Dumper(@data));  

    };
    if ($@){
          # 0 1 2
      my $i=0;
      my ($package, $filename, $line) = caller($i);
      while ($package eq "RPC") {
        $i++;
        ($package, $filename, $line) = caller($i);
        $package or last;        
      }
      $self->info("PROBLEMS with the RPC call '$op' to $service (from $package $filename:$line): $@");
    }
    @data and return @data;

    $retry or last;
    $self->info("We will retry the rpc call forever...");
    sleep(5);
  }
  return;

}
sub checkService{
  my $self=shift;
  my $service=shift;
  $self->CallRPCAndDisplay(0, $service, "status", @_); 
}

return 1;
