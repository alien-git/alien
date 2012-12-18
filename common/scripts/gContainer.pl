=head1 NAME

gContainer - A secure WSRF Container

=head1 DESCRIPTION

The gContainer is a secure web service container following the WSRF standard. It is based upon WSRF::Lite and provides
load management, service management and discovery.

It consists of:
  - the container itself.
  - the stateful management service: gController.
  - the stateless factory service: gFactory.

Its configuration is done in LDAP. The gController communicates with other instances of gContainer in an
hierarchical way. During creation stage all possible locations for creating the service are taken
into account using a set of judge classes (see gController::Judge). Then the service is created at the best
suitable location.

The gFactory is able to rerieve the user's proxy from a myproxy server.

=head1 SEE ALSO

WSRF::Lite, gController, gFactory, gController::Judge

=cut

# uses parts of SContainer.pl of WSRF::Lite, copyright see below
use WSRF::SSLDaemon;
use WSRF::Lite;
use IO::Socket::SSL;
use Data::Dumper;
use AliEn::Config;
use AliEn::EGEE::WSRF;

use strict;

my $action = lc(shift);
my $debug = grep /-debug/, @ARGV;
my $silent = grep /-silent/, @ARGV;
my $port = 50000;
my $processName = "AliEn-gLite-gContainer";

open SAVEOUT1, ">&STDOUT";
open SAVEERR1, ">&STDERR";
if ($silent) {
  open(STDOUT, ">/dev/null");
  open(STDERR, ">/dev/null");
}

my $resultCode = 0;
if ($action eq "start") {
  print "Starting gContainer...\n";
  $resultCode = &start($port, $debug);
  if ($resultCode) {
    print "Startup FAILED.\n";
  } else {
    print "Startup complete.\n";
  }
} elsif ($action eq "stop") {
  $resultCode = &stop();
} elsif ($action eq "restart") {
  &stop();
  $resultCode = &start($port);
  if ($resultCode) {
    print "Startup FAILED.\n";
  } else {
    print "Startup complete.\n";
  }
} elsif ($action eq "status") {
  $resultCode = &status($port);
} else {
  print "Syntax: gContainer.pl start|stop|restart|status [--debug]\n";
}

if ($silent) {
  open STDOUT, ">&SAVEOUT1";
  open STDERR, ">&SAVEERR1";
}

exit $resultCode if (defined $resultCode);
exit 0;

# small hack to prevent annoying warning: name "main::SAVEERR" used only once: possible typo at ...
<SAVEOUT1>;
<SAVEERR1>;

sub start() {
  my $port = shift;
  my $debug = shift;

    # redirect output
  my $logDir = &getLogDir();
  my $logFile = $logDir . '/gContainer.log';
  print "LogFile: $logFile\n";

  open SAVEOUT2, ">&STDOUT";
  open SAVEERR2, ">&STDERR";

  open(STDOUT, ">>" . $logFile);
  open(STDERR, ">&STDOUT");

  select(STDERR);
  $| = 1;
  select(STDOUT);
  $| = 1;

  my $pid = $$;

  my $result;
  eval {
    $ENV{WSRF_MODULES} = $ENV{ALIEN_ROOT} . '/modules';

    system("rm -rf /tmp/wsrf/*");
    system("mkdir -p /tmp/wsrf/data");
    system("mkdir -p $logDir/modules_logs");
    system("rm -f $logDir/modules_logs/*");
    if ( ! -l  "$ENV{WSRF_MODULES}/logs") {
      system("rm -rf $ENV{WSRF_MODULES}/logs");
      system("ln -s $logDir/modules_logs $ENV{WSRF_MODULES}/logs");
    }
    $result = &startContainer($port, $debug);
    if ($result == 0) {
      $result = &startController($port, $debug);
    }
  };

  exit if ($pid != $$);

  $@ and $result = -1;

  open STDOUT, ">&SAVEOUT2";
  open STDERR, ">&SAVEERR2";

  return $result;

  # small hack to prevent annoying warning: name "main::SAVEERR" used only once: possible typo at ...
  <SAVEOUT2>;
  <SAVEERR2>;
}

# REAPER kills of stry children.
# this REAPER is designed to be used with Perl 5.8 though
# it should still work with Perl 5.6
sub REAPER {
  local $!;
  waitpid(-1,0);
  $SIG{CHLD} = \&REAPER;  # still loathe sysV
}

sub startContainer() {
  my $port = shift;
  $ENV{'TZ'} = "GMT";

  if ( ! -d $WSRF::Constants::SOCKETS_DIRECTORY )
  {
    die "Directory $WSRF::Constants::SOCKETS_DIRECTORY does not exist\n";
  }
  if ( ! -d $WSRF::Constants::Data )
  {
    die "Directory $WSRF::Constants::Data does not exist\n";
  }

  $SIG{CHLD} = \&REAPER;

  #Check that the path to the Grid Service Modules is set
  if ( !defined($ENV{'WSRF_MODULES'}) )
  {
    die "Enviromental Variable WSRF_MODULES not defined";
  }

  #Not sure if we need to set this!!
  $ENV{SSL}="TRUE";

  #loop to handle major errors - eval should
  #catch exceptions and this while should start things up
  #again.
  while (1)
  {
    #create the Service Container - just a Web Server
    #Certificate information is provided here - could
    #use a personal certificate
    $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_verify_callback}=\&callback;

    my $config = new AliEn::Config();
    my $CertDir="$ENV{ALIEN_HOME}/identities.".lc($config->{ORG_NAME})."/gContainer/$config->{HOST}";

    my $d = WSRF::SSLDaemon->new(
                LocalPort => $port,
                Listen => SOMAXCONN,
                Reuse => 1,
                SSL_key_file => "$CertDir/key.pem",
                SSL_cert_file => "$CertDir/cert.pem",
                SSL_ca_path => "$ENV{X509_CERT_DIR}/",
                SSL_ca_file => undef,
                SSL_verify_mode => 0x01 | 0x02 | 0x04,
#                DEBUG => 0
                    ) || die "ERROR $!\n";


    #Store the Container Address in the ENV variables - child
    #processes can then pick it up and use it
    $ENV{'URL'} = $d->url;

    print "\nContainer Contact Address: ", $d->url, "\n";

    my $pid = fork;
    if ($pid) {
      print "PID: $pid\n";
      my $pidfile = &getLogDir() . "gContainer.pid";
      open FILE, ">$pidfile";
      print FILE $pid;
      close FILE;

      return 0;
    }

    $0 = $processName;

    while ( 1 ) {   #wait for client to connect
      my $client = $d->accept;
      if ( defined $client )
      {
        print "Got Connection\n";
        if (my $pid = fork){  #fork a process to deal with request
          print "Parent $$ :: forked\n"; #parent should go back to accept now
          $client->close( SSL_no_shutdown => 1 );
          undef $client;
        }
        elsif (defined($pid) )  #child
        {
          print "Child created is $$\n";
          WSRF::Container::handle($client);
          print "d->close= ". $d->close( SSL_no_shutdown => 1 )." $?\n";
          undef ($d);
          $client->close;
          undef ($client);
          exit;
        }
        else
        {  #fork failed
          print "fork failed\n";
        }
      }
      else
      {
        next if $!{EINTR}; # just a child exiting, go back to sleep.
      }
    }
  }

  return -2;
}

sub startController {
  my $port = shift;
  my $debug = shift;

  &initSSL();

  my $options = {};
  $options->{debug} = 1 if ($debug);

  my ($wsAddress) = AliEn::EGEE::WSRFHelper::soapCall("https://localhost:$port", $AliEn::EGEE::WSRFHelper::staticConfiguration->{namespace},
                '/Session/gControllerFactory/', 'gControllerFactory', undef, 'createControllerResource', $options);

  print "gControllerFactory, result: " . Dumper($wsAddress);
  ref($wsAddress) eq "HASH"
    and exists($wsAddress->{'ReferenceProperties'}->{'ResourceID'})
    or return -3;

  return 0;
}

sub initSSL {
  AliEn::EGEE::WSRFHelper::initializeServerSSL();
}

my $logDir;
sub getLogDir {
  if (!$logDir) {
    my $config = new AliEn::Config();
    $config
      or die "Could not get config";
    $logDir = $config->{LOG_DIR} . '/gContainer/';
    system("mkdir -p $logDir");
  }
  return $logDir;
}

sub stop {
  my $pid = &getPid();
  $pid
    or print "gContainer is NOT up\n"
    and return -4;

  system("kill $pid");
  system("rm -f " . &getPidFile());

  system('kill `ps -ef | grep "' . $processName . '" | awk \'{print $2}\'`');

  return 0;
}

sub getPidFile {
  return &getLogDir() . "gContainer.pid";
}

sub getPid {
  open FILE, &getPidFile()
    or return;

  my $pid = <FILE>;
  close FILE;
  chomp($pid);

  return $pid;
}

sub status {
  my $port = shift;

  print "Checking status of gContainer...";

  my $pid = &getPid();
  $pid
    or print "is NOT up.\n"
    and return -5;

  kill(0, $pid)
    or print "is NOT up.\n"
    and return -6;

  print "is up.\n";

  print "Checking status of gController...";

  &initSSL();

  my $containerAddress = "https://localhost:$port";
  my ($errorCode, $errorString) = AliEn::EGEE::WSRFHelper::soapCall($containerAddress, $AliEn::EGEE::WSRFHelper::staticConfiguration->{namespace},
                '/WSRF/gController/', 'gController', AliEn::EGEE::WSRFHelper::getControllerID($containerAddress), 'ping');

  defined($errorCode) and ($errorCode == 0)
    or print "is NOT up\n"
    and return -7;

  print "is up.\n";

  return 0;
}

sub callback {
  #print "Callback called ($_[2])\n";
  my @tmp = @_;
  $AliEn::EGEE::WSRF::SSL_VERIFY_PARAMS = \@tmp;

  return 1;
}

__END__

### copyright

# glite Copyright:
###


# copyright for the parts taken from WSRF::Lite, which are mainly located in sub startContainer
# COPYRIGHT UNIVERSITY OF MANCHESTER, 2003
#
# Author: Mark Mc Keown
# mark.mckeown@man.ac.uk
#
# LICENCE TERMS
#
# WSRF::Lite is free software; you can redistribute it
# and/or modify it under the same terms as Perl itself.
#
#
# version 0.4
#
#
# Secure version of the Container script - uses SSL
#
