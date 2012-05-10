package AliEn::SOAP;
use strict;
use Carp qw(cluck);
use AliEn::Config;
use AliEn::XML;
my $GLOBALERROR = "";
use SOAP::Lite;
use vars qw(@ISA);
push @ISA, 'AliEn::Logger::LogObject';
my $self;
my $my_faultSub = sub {
  my $soap    = shift;
  my $message = shift;
  $message =~ /Can't connect/s          and $GLOBALERROR = $message;
  $message =~ /SSL negotiation failed/s and $GLOBALERROR = $message;
  $self->debug(1, "Got the error $message");
  return;
};

sub new {
  my $proto = shift;
  $self and return $self;
  my $class = ref($proto) || $proto;
  $self = (shift or {});
  bless($self, $class);
  $self->SUPER::new() or return;
  $self->{CONFIG} = new AliEn::Config;
  $self->{LOGGER} and $self->{CONFIG} or return;
  $self->{LOGGER}->debugOn() if ($self->{debug});
  $self->{XML} = new AliEn::XML;
  $self->{NOPROXY}
    and $self->debug(1, "Not using the SOAPProxy")
    and return $self;
  my $host = $self->{CONFIG}->{SOAPPROXY_ADDRESS};
  $host
    or $self->debug(1, "No proxy defined in LDAP")
    and return $self;
  $self->debug(1, "Using the SOAPProxy");
  $self->{SOAPPROXY} = SOAP::Lite->uri("AliEn/Service/SOAPProxy")->proxy("http://$host");
  $self->{SOAPPROXY} and $self->{SOAPPROXY}->ping and return $self;
  $self->debug(1, "The proxy did not answer");
  undef $self->{SOAPPROXY};
  return $self;
}

sub getPassphrase {
  my $self    = shift;
  my $oldCert = ($ENV{X509_USER_CERT} || "");
  my $oldKey  = ($ENV{X509_USER_KEY} || "");
  ($ENV{X509_USER_CERT}, $ENV{X509_USER_KEY}) = ($ENV{HTTPS_CERT_FILE}, $ENV{HTTPS_KEY_FILE});

  #First, let's try to get the proxy... maybe we don't need a password"
  my $d = open(FILE, "$ENV{GLOBUS_LOCATION}/bin/grid-proxy-init -pwstdin 2>/dev/null </dev/null|");
  ($ENV{X509_USER_CERT}, $ENV{X509_USER_KEY}) = ($oldCert, $oldKey);
  if (!$d) {
    $self->info("Error doing proxy-init: $!");
    return;
  }
  my @data = <FILE>;
  close FILE
    and $self->info("@data", undef, 0)
    and return "";

  #Ok, let's get the password;
  my ($line) = grep (/^Your identity/, @data);
  $self->info("${line}Enter GRID pass phrase for this identity:");
  system("stty -echo");
  my $passphrase = <>;
  chomp $passphrase;
  system("stty echo");
  ($ENV{X509_USER_CERT}, $ENV{X509_USER_KEY}) = ($ENV{HTTPS_CERT_FILE}, $ENV{HTTPS_KEY_FILE});
  $d =
    open(FILE, "$ENV{GLOBUS_LOCATION}/bin/grid-proxy-init -pwstdin <<EOF
$passphrase
EOF|"
    );
  ($ENV{X509_USER_CERT}, $ENV{X509_USER_KEY}) = ($oldCert, $oldKey);
  $d or print "Error doing proxy-init: $!\n" and return;
  my @d = <FILE>;
  close FILE or print STDERR "NOPE" and return;
  @d = grep (!/^Your identity/, @d);
  $self->info("@d", undef, 0);
  return $passphrase;
}

#sub splitProxy{
#  my $self=shift;
#  my $proxy=shift;
#  $ENV{HTTPS_CA_FILE}=$ENV{HTTPS_KEY_FILE}=$ENV{HTTPS_CERT_FILE}=$proxy;
#  return 1;
#  $proxy or $self->info( "No proxy specified in splitProxy") and return;
#  open (FILE, "<$proxy") or
#    $self->info( "Error reading the file $proxy") and return;
#  my $file=join("", <FILE>);
#  close FILE;
#  my $b="-----BEGIN";
#  my $c="CERTIFICATE-----";
#  my $k="RSA PRIVATE KEY-----";
#  my $e="-----END";
#  $file=~ /^.*($b $c.*$e $c).*($b $k.*$e $k).*($b $c.*$e $c)/s or
#    $self->info( "We couldn't find the certificate inside $proxy") and return;
#  my $files={CERT=>$1, KEY=>$2, CA=>$3};
#  foreach my $key (keys %$files){
#    open(FILE, ">$proxy.$key") or $self->info( "Error opening the file $proxy.$key") and return;
#    print FILE $files->{$key};
#    close FILE;
#    chmod 0600, "$proxy.$key";
#    $ENV{"HTTPS_${key}_FILE"}="$proxy.$key";
#  }
#  $self->{USING_PROXY}=$proxy;
#  return 1;
#}
sub exportSecureEnvironment {
  my $self = shift;
  $self->debug(1, "Trying to connect to a secure service");
  $self->{SECURE_ENVIRONMENT} and return 1;
  my $passphrase;
  $ENV{HTTPS_VERSION} = 3;
  $ENV{HTTPS_CA_DIR}  = $ENV{X509_CERT_DIR};

  #First, let's check if we have a proxy:
  if (!system("$ENV{GLOBUS_LOCATION}/bin/grid-proxy-info -exists -valid 0:5 >/dev/null 2>&1")) {
    my $proxy = ($ENV{X509_USER_PROXY} || "/tmp/x509up_u$<");
    $self->debug(1, "Using the proxy-certificate in $proxy");
    $ENV{HTTPS_CA_FILE} = $ENV{HTTPS_KEY_FILE} = $ENV{HTTPS_CERT_FILE} = $proxy;
  } else {
    my $dir = "$ENV{ALIEN_HOME}/globus";
    foreach my $type ("CERT", "KEY") {
      my $name = "HTTPS_${type}_FILE";
      $ENV{$name} = ($ENV{$name} || $ENV{"X509_USER_$type"} || "$dir/user\L$type\E.pem");
      (-f $ENV{$name})
        or $self->info("Warning! $ENV{$name} doesn't exit");
    }
    $self->debug(1, "Using the certificate in $ENV{HTTPS_CERT_FILE}");
    $passphrase = $self->getPassphrase();
    defined $passphrase or return;
  }

  #  require IO::Socket::SSL;
  #  $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_cert_file}=$ENV{HTTPS_CERT_FILE};
  #  $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_key_file}=$ENV{HTTPS_KEY_FILE};
  #  $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_use_cert}=1;
  #  $ENV{HTTPS_CA_FILE} and
  #    $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_ca_file}= $ENV{HTTPS_CA_FILE};
  #  $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_verify_mode}=3;
  #  $passphrase and $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_passwd_cb}=
  #    sub {return $passphrase};
  $self->{SECURE_ENVIRONMENT} = 1;
  return 1;
}

sub Connect {
  my $self    = shift;
  my $options = shift;
  my $message;
  my $address = $options->{address} or $message .= "No address";
  my $uri     = $options->{uri}     or $message .= "No uri of the service";
  my $name    = $options->{name}    or $message .= "No name for the service";
  $message and print "Error: $message \n" and return;
  $self->debug(1, "Connecting to $address as $uri");

  if ($address =~ /^https/) {
    $self->exportSecureEnvironment() or return;
  }
  eval {
    my @list = ($address);
    $options->{options} and push @list, @{$options->{options}};
    $self->{$name} = SOAP::Lite->uri($uri)->proxy(@list)->on_fault($my_faultSub);
  };
  if ($@) {
    $self->info("Error connecting to $address as $uri: $@");
    return;
  }
  return 1;
}

sub deleteCache {
  my $self = shift;
  $self->debug(1, "Deleting the cache of all the connections");
  foreach (keys %$self) {
    /^(XML)|(CONFIG)|(LOG_REF)|(LOGGER)/ and next;
    delete $self->{$_};
  }
  $self->{CONFIG} = $self->{CONFIG}->Reload({force => 1});
  return 1;
}

sub IsConnected {
  my $self    = shift;
  my $service = shift;
  $self->debug(1, "Checking if we are connected to $service");
  $self->{$service} or return;
  return 1;
}

sub GetOutput {
  my $self   = shift;
  my $return = shift;
  $return or return;
  return ($return->result, $return->paramsout);
}

sub CallAndGetOverSOAP {
  my $self      = shift;
  my $silent    = shift;
  my @signature = @_;
  my $service   = shift;
  my $function  = shift;
  my $callsoap;

  my $maxTry = 2;
  my $tries;
  for ($tries = 0 ; $tries < $maxTry ; $tries++) {    # try five times
    $callsoap = $self->CallSOAP(@signature) and last;
    $self->info("Calling $service over SOAP did not work, trying " . (4 - $tries) . " more times till giving up.");
    sleep(($maxTry * $tries));
  }
  if ($tries == $maxTry) {
    $self->error("We have asked the server $maxTry times, and it didn't work");
    return;
  }
  my $rcvals = $callsoap->result;

  if (defined($rcvals->{rcmessages})) {
    map { defined $_ or $_ = "" } @{$rcvals->{rcmessages}};
    $silent
      or $self->raw(join("", @{$rcvals->{rcmessages}}), undef, 0);
  }

  defined($rcvals->{rcvalues})
    and (defined(@{$rcvals->{rcvalues}})
    and (scalar(@{$rcvals->{rcvalues}}) gt 0))
    and return @{$rcvals->{rcvalues}};
  return;
}

sub CallSOAP {
  my $self     = shift;
  my $service  = shift;
  my $function = shift;
  my $retry;
  my @arguments;
  my $silent = "";
  undef $GLOBALERROR;
  if ((defined @_) && (@_)) {

    for (@_) {
      ($_ && /ALIEN_SOAP_RETRY/)  and $retry  = 1   and next;
      ($_ && /ALIEN_SOAP_SILENT/) and $silent = "s" and next;
      push @arguments, $_;
    }
  }
  my $result;
  my @definedArguments = grep (defined $_, @arguments);
  $self->debug(1, "Making the SOAP call $function(@definedArguments) to $service");
  my $sleep     = 1;
  my $max_sleep = 300;
  while (1) {
    eval {
      if ((defined $self->{SOAPPROXY}) && ($self->{SOAPPROXY})) {
        $self->debug(1, "Using the SOAPProxy");
        $result = $self->{SOAPPROXY}->Call($self->{CONFIG}->{ORG_NAME}, $service, $function, @arguments);
      } else {
        $self->debug(1, "Calling it directly");
        if (!$self->{$service}) {
          $self->debug(1, "Trying to connect to $service");
          $self->checkService($service)
            or $self->debug(1, "Service $service is not up");
        }
        ($self->{$service})
          and $result = $self->_CallSOAPInternal($service, $function, @arguments);
      }
    };
    if ($@) {
      print "The soap call died!!\n$@\n";
    }
    if (!$retry || $result) {
      $self->debug(1, "Got the answer " . ((defined $result) ? $result : ""));
      $self->checkSOAPreturn($result, $service, $silent) or return;
      return $result;
    }
    $retry or return;
    $sleep = $sleep * 2 + int(rand(2));
    $sleep > $max_sleep and $sleep = int(rand(4));
    $self->info("Sleeping for $sleep seconds and retrying");
    sleep($sleep);
  }
}

sub _CallSOAPInternal {
  my $self     = shift;
  my $service  = shift;
  my $function = shift;
  return $self->{$service}->$function(@_);
}

sub CallSOAPCheckResult {
  my $self   = shift;
  my $result = $self->CallSOAP(@_);
  $result or return;
  my @s = $self->GetOutput($result) or return;
  return @s;
}

sub CallSOAPXML {
  my $self = shift;
  my @s    = $self->CallSOAPCheckResult(@_);
  return $self->{XML}->parse2($s[0]);
}

#
#
#Arguments:
#     $done=return value of a soap call
#     $server= server that was called.
#     $options=
#               n -> Don't report any error (just put them as info)
#
#
sub checkSOAPreturn {
  my $self         = shift;
  my $done         = shift;
  my $server       = (shift or "server");
  my $options      = (shift or "");
  my $errorMessage = "";
  $GLOBALERROR
    and ($GLOBALERROR =~ /SSL negotiation failed/s)
    and $self->info("Error reason: SSL negotiation failed!");
  (defined $done)
    or $self->info("WARNING!!! SOAP ERROR, while contacting the $server")
    and return;
  $GLOBALERROR and $errorMessage = $GLOBALERROR;
  my $error = "";

  if ($done) {

    #    print "Got $done\n";
    $errorMessage = $done->faultstring();
    if (!$errorMessage) {
      eval { $error = $done->result; };
      if ($@) {
        $errorMessage = "Invalid result value";
      } else {
        unless (defined $error) {
          $errorMessage = "Error the $server did not return any value";
        }
        if (($error) and ($error eq -1)) {
          $done->paramsout and $errorMessage .= $done->paramsout;
          $errorMessage =~ /^The \S+ returned an error: /
            or $errorMessage = "The $server returned an error: $errorMessage";
        }
      }
    }
  }
  if ($errorMessage) {
    $self->{LOGGER}->set_error_msg($errorMessage);
    if ($options !~ /s/) {
      $options =~ /n/ or $self->{LOGGER}->error("Config", $errorMessage, 11);
      $options =~ /n/ and $self->info("Failed: $errorMessage", 11);
    }
    return;
  }
  return 1;
}

sub checkService {
  my $self  = shift;
  my $retry = 0;
  if (grep (/-retry/, @_)) {
    $retry = 1;
    @_ = grep (!/-retry/, @_);
  }
  my $service     = shift;
  my $configName  = (shift or uc($service));
  my $options     = shift;
  my $serviceName = "AliEn/Service/$service";
  $configName eq "AUTHEN"  and $configName  = "AUTH";
  $service    eq "PACKMAN" and $serviceName = "AliEn/Service/PackMan";

  #  ($service eq "Manager/Job" )and $configName="JOB_MANAGER";
  $service =~ /^(.*)\/(.*)$/ and $configName = uc("${2}_$1");
  if ($self->{SOAPPROXY}) {
    $self->info("Using SOAPProxy");
    my $result = $self->{SOAPPROXY}->Call($self->{CONFIG}->{ORG_NAME}, $service, "ping");
    $self->info("We have $result");
    $self->checkSOAPreturn($result)
      or return (-1, $self->{LOGGER}->error_msg);
    return 1;
  }
  $self->debug(1, "Checking connection to $service");
  my $done;
  if ($self->{$service}) {
    eval { $done = $self->_CallSOAPInternal($service, "ping")->result; };
  }
  ($done)
    and $self->debug(1, "Connection is up")
    and return 1;
  $self->debug(1, "We didn't have a connection");
  $self->debug(1, "Making connection to $service");
  my $host = ($self->{CONFIG}->{"${configName}_HOST"} or "");
  $ENV{ALIEN_CM_AS_LDAP_PROXY}
    and ($service eq "ClusterMonitor")
    and $host = $ENV{ALIEN_CM_AS_LDAP_PROXY};
  $host or $host = $self->{CONFIG}->{"\U${configName}_ADDRESS\E"};
  ($service eq "ClusterMonitor") and $host = $self->{CONFIG}->{HOST};
  my @sublist = ($host);
  $self->{CONFIG}->{"${configName}_HOST_LIST"}
    and @sublist = @{$self->{CONFIG}->{"${configName}_HOST_LIST"}};

  my @sublist2 = ();

  foreach $host (@sublist) {
    $self->{CONFIG}->{"${configName}_PORT"}
      and $host .= ":" . $self->{CONFIG}->{"${configName}_PORT"};
    $host =~ /^http/ or $host = "http://$host";
    $host =~ /^https/ and $self->exportSecureEnvironment();
    push(@sublist2, $host);
  }
  my $sleep = 1;
  foreach $host (@sublist2) {
    while (1) {
      my @list = ($host);
      $options and push @list, @$options;
      $self->debug(1, "(Re)making connection to $service: @list");
      $self->{$service} = SOAP::Lite->uri($serviceName)->proxy(@list)->on_fault($my_faultSub);
      if ($self->{$service}) {
        eval { $done = $self->_CallSOAPInternal($service, "ping")->result; };
      }
      ($done)
        and $self->debug(1, "Connection is up")
        and return 1;
      $retry or last;
      $self->info("Connnection to $service ($host) is down... let's sleep $sleep seconds");
      sleep($sleep);
      $sleep = $sleep * 2 + int(rand(3));
      $sleep > 3600 and $sleep = 1;
    }
  }
  $self->debug(1, "Connection is down");
  return;
}

#sub DESTROY {
#  my $self=shift;
#  $self or return;
#  $self->{USING_PROXY} or return;
#  print "Deleting the proxy\n";
#  system("rm -rf $self->{USING_PROXY}.*");
#}
sub resolveSEName {
  my $self   = shift;
  my $sename = shift;
  my $now    = time;
  $self->{SE_CACHE} or $self->{SE_CACHE} = {};
  ($self->{SE_CACHE}->{$sename} and $self->{SE_CACHE}->{$sename}->{expired} < $now)
    and delete $self->{SE_CACHE}->{$sename};
  my $response;
  if (!$self->{SE_CACHE}->{$sename}) {
    $self->debug(1, "Contacting the IS to ask $sename");
    my $response = $self->CallSOAP("IS", "getSE", $sename) or return;
    $response = $response->result;
    my $remoteSE = $response->{host};
    my $portSE   = $response->{port};
    $self->Connect(
      { address => "http://$remoteSE:$portSE",
        uri     => "AliEn/Service/SE",
        name    => "SE_$sename",
        options => [ timeout => 5000 ]
      }
    ) or return;
    $self->{SE_CACHE}->{$sename}->{certificate} = $response->{certificate};
    $self->{SE_CACHE}->{$sename}->{expired}     = time() + 300;
  }
  return ("SE_$sename", $self->{SE_CACHE}->{$sename}->{certificate});
}
return 1;
