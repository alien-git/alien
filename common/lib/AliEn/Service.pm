package AliEn::Service;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use Carp qw(cluck);

use AliEn::Logger;

use vars qw($VERSION @ISA);

use AliEn::RPC;
use AliEn::Config;
use AliEn::Util;
use strict;
use POSIX ":sys_wait_h";
use Socket;

use LockFile::Simple;

use base qw(JSON::RPC::Legacy::Procedure);
push @ISA, 'AliEn::Logger::LogObject';

my $self;

sub status :  Public()  {
  return {rcmessages=>["AliEn Service $self->{SERVICENAME}: $self->{CONFIG}->{VERSION}"], 
      rcvalues=>[1]};
}  

sub new {
  my $proto = shift;
  my $this  = (shift or {});
  my $class = ref($proto) || $proto;
  $self = (shift or {});

  bless($self, $class);
  $self->SUPER::new({logfile => $this->{logfile}}) or return;

  # Initialize the logger
  $self->{USER} = $this->{user};

  $self->{LOGGER} or print STDERR "Error getting the logger\n" and return;

  $self->{RPC} = new AliEn::RPC;
  $self->{RPC} or return;
  my $inittxt = "Initializing Service";
  $self->{DEBUG} = $this->{debug};
  if ($self->{DEBUG}) {
    $self->{LOGGER}->debugOn($this->{debug});
    $self->debug(1, "Starting the module in debug mode");

    $inittxt .= " in debug mode";
  }

  $self->{LASTALIVE} = 0;
  $self->{CONFIG}    = new AliEn::Config();
  $self->{CONFIG} or return;

  $self->info($inittxt);
  $self->{ALIVE_COUNTS} = 0;

  $self->{VERIFY_MODE} = 0;

  $self->{SECURE}       = 0;
  $self->{PROTOCOL}     = "http";
  $self->{SLEEP_PERIOD} = 60;
  if ($this->{callback}) {
    $self->debug(1, "callback request: $this->{callback}");
    $self->{CALLBACK} = $this->{callback};
  }

  $self->initialize($this, @_) or return;

  my $certdir = "$ENV{ALIEN_HOME}/.alien/globus/";
  if ((-f "$certdir/usercert.pem") && (!$ENV{X509_USER_CERT})) {
    $self->info("Using the certificate in $certdir");
    $ENV{X509_USER_CERT} = "$certdir/usercert.pem";
    $ENV{X509_USER_KEY}  = "$certdir/userkey.pem";
  }

  my $message = "";
  $self->{PORT} or $message = "No port defined.";
  $self->{HOST}        or $message .= " No host defined.";
  $self->{SERVICE}     or $message .= " No service defined.";
  $self->{SERVICENAME} or $message .= " No servicename defined.";

  $message
    and $self->{LOGGER}->error("Service", $message)
    and return;

  $self->{URI} or $self->{URI} = "AliEn::Service::$self->{SERVICE}";

  $self->debug(1, "Setting URI $self->{URI}");

  AliEn::Util::setupApMon($self);
  AliEn::Util::setupApMonService($self);
  $self->dumpEnvironment();

  return $self;
}

=head1 NAME

AliEn::Service

=head1 SYNOPSIS

=item setAlive()

$self->setAlive()

=head1 DESCRIPTION

setAlive informs the IS that the service is up and running. 

Service::startListening() calls the setAlive() function in AliEn::Service module. 

From setAlive() function the markAlive() function is called in AliEn::Service::IS module through CallRPC() function.CallRPC function receives the arguments like service, servicename, version, name, host, port, uri, protocols from setAlive() function and calls markAlive() with those arguments.

=cut

sub setAlive {
  my $s = shift;

  my $date = time;

  my $markport;

  if (!defined $self->{PROTOCOLS}) {
    $self->{PROTOCOLS} = '';
  }

  ($date < $self->{LASTALIVE})
    and return;
  $self->{LASTALIVE} = $date + 400;

  #$self->info("setAlive was called.");
  if ($self->{MONITOR}) {

    # send the alive status also to ML
    if (not $self->{SKIP_BGMONITOR}) {
      $self->{MONITOR}->sendBgMonitoring();
    }

    #$self->info("setAlive -> sent Bg Monitoring to ML.");
  }

  if ($self->{REGISTER_IN_IS}) {
    foreach my $key (keys %{$self->{REGISTER_IN_IS}}) {
      my $elem = $self->{REGISTER_IN_IS}->{$key};
      my $name = "$self->{CONFIG}->{ORG_NAME}::$self->{CONFIG}->{SITE}::$key";
      my $host = $elem->{host} || $self->{HOST};
      $self->info("Registering the $name in the IS");
      my ($response) = $self->{RPC}->CallRPC(
        "IS",
        "markAlive",
        $self->{SERVICE},
        $name, $host,
        $elem->{PORT},
        { VERSION     => $self->{CONFIG}->{VERSION},
          URI         => $elem->{URI},
          PROTOCOLS   => $self->{PROTOCOLS},
          CERTIFICATE => $self->{CERTIFICATE}
        }
      );

      if ($self->{SERVICE} ne "Logger") {
        ($response)
          or $self->{LOGGER}->warning("Service", "IS is not up")
          and return;
      }
    }
  }

  $self->debug(1, "Registering the service in the IS");

  my ($response) = $self->{RPC}->CallRPC(
    "IS",
    "markAlive",
    $self->{SERVICE},
    $self->{SERVICENAME},
    $self->{HOST},
    $self->{PORT},
    { VERSION     => $self->{CONFIG}->{VERSION},
      URI         => $self->{URI},
      PROTOCOLS   => $self->{PROTOCOLS},
      CERTIFICATE => $self->{CERTIFICATE}
    }
  );

  foreach (grep (/^$self->{SERVICE}_VIRTUAL_/, keys %{$self->{CONFIG}})) {
    $self->info("Telling the IS that $_ is alive");
    $self->{RPC}->CallRPC(
      "IS",
      "markAlive",
      $self->{SERVICE},
      $self->{CONFIG}->{$_}->{FULLNAME},
      $self->{HOST},
      $self->{PORT},
      { VERSION     => $self->{CONFIG}->{VERSION},
        URI         => $self->{URI},
        PROTOCOLS   => $self->{CONFIG}->{$_}->{PROTOCOLS},
        CERTIFICATE => $self->{CERTIFICATE},
      }
    );

  }

  if ($self->{SERVICE} ne "Logger") {
    ($response)
      or $self->{LOGGER}->warning("Service", "IS is not up")
      and return;
  }
  $self->debug(1, "Service Registered");

  return 1;
}


sub dumpEnvironment {
  my $self = shift;
  my $file = "$self->{CONFIG}->{LOG_DIR}/$self->{SERVICE}.env";
  $self->info("Putting the environment in $file");
  system("env >$file") and $self->info("Error opening the file $file");
  return 1;
}

my $alien_verify_subject;


sub alien_verify {
  my ($ok, $x509_store_ctx) = @_;
  print "**** AliEn verify called ($ok)\n";
  my $x = Net::SSLeay::X509_STORE_CTX_get_current_cert($x509_store_ctx);
  $x or print "The client did not present a certificate!!!\n" and return;

  print "Certificate:\n";
  my $subject = Net::SSLeay::X509_NAME_oneline(Net::SSLeay::X509_get_subject_name($x));
  print "  Subject Name: $subject \n";
  if ($subject =~ /\/CN=proxy/) {
    print "This is a proxy certificate...\n";
  }
  print "  Issuer Name:  " . Net::SSLeay::X509_NAME_oneline(Net::SSLeay::X509_get_issuer_name($x)) . "\n";
  if ($alien_verify_subject) {
    print "Only $alien_verify_subject can talk to us\n";
    $subject =~ /^$alien_verify_subject(\/CN=proxy)*$/
      or print "This user cannot talk to us!!\n" and return 0;
  }

  return 1;
}

sub stopWholeService {
  shift;
  my $next = $$;
  my $ppid;

  $self->debug(1, "We are in stopWholeService of Service.pm (and $next)");

  while ($next != 1) {
    $ppid = $next;
    $next = $self->getParentProcessId($ppid);
    $next or return;
  }
  $self->debug(1, "We are in stopWholeService of Service.pm and we have $next and $ppid");

  $ppid or return;
  $self->stopService($ppid);
  $self->info("Let's kill also the rotate-log ( $ENV{ALIEN_PROCESSNAME}-RotateLog-$ppid)");
  open(FILE, "ps -ef |grep '$ENV{ALIEN_PROCESSNAME}-RotateLog-$ppid'|grep -v grep|");
  my @pid = <FILE>;
  close FILE;
  map { s/^\s*\S+\s*(\S+)\s.*$/$1/ } @pid;
  kill 9, @pid;

  exit();
}

sub getParentProcessId {
  shift;
  my $pid = shift;
  $pid or return;
  my $parent = `ps -eo "pid ppid" | grep -E '^ *$pid\ ' | awk '{print \$2}'`;
  chomp($parent);
  $self->debug(1, "Parent is $parent");
  return $parent;
}

sub stopService {
  my $s = shift;

  my $pid = shift;
  $pid or $self->info("Trying to stop the service without passing the pid...") and return;
  $pid eq "1" and $self->info("We are not going to kill everybody...") and return;
  $self->info("Stopping the service (pid $pid) (and I'm $$)");

  my @pids = ($pid, $self->findChildProcesses($pid));

  @pids = grep (!/^${$}$/, @pids);
  $self->info("Killing the monitoring Daemon (processes @pids)");

  kill(9, @pids);
  $self->info("Do we have to kill more??");
  return 1;
}

sub findChildProcesses {
  shift;
  my $pid = shift;

  # find the pids of child processes of $pid, which are not $pid
  my $d = `ps -A -o "pid ppid" | grep -E '(^|\ ) *$pid(\ |\$)' | awk '{print \$1}' | grep -vE '(^|\ ) *$pid(\ |\$)'`;
  my @pids = split(/\n/, $d);

  my @children = ();
  foreach my $pid (@pids) {
    push @children, $self->findChildProcesses($pid);
  }

  return (@pids, @children);
}

sub quit {
  my $s = shift;
  $self->debug(1, "Killing Service\n");

}

sub forkCheckProcess {
  my $this = shift;
  my $pid  = fork();
  (defined $pid) or print STDERR "Error forking the process\n" and return;
  if (!$pid) {
    if ($self->{SERVICE} !~ /^JobAgent$/) {
      my $logFile = $self->{LOGGER}->{logfile};
      if ($logFile) {
        $logFile =~ s/.log$/.wakesup.log/;
      } else {
        $logFile = "$self->{CONFIG}->{LOG_DIR}/$self->{SERVICE}.wakesup.log";
      }
      $self->info("************Redirecting the log of the checkWakesUp to $logFile");
      $self->{LOGGER}->redirect($logFile);
    }
    $self->info("PID $$ sleeps");
    #sleep (600);
    $self->startChecking();

    # We should never come here
    print STDERR "Checking has died!!!\n";
    
    exit(0);
  }
  $self->{CHILDPID} = $pid;
  $self->info("The father returns (child $pid)");
  return 1;
}

sub startChecking {
  my $s = shift;

  my $silent = 0;

  # This is the father process, so now loop forever and check transfers.
  #$this->{CHILDPID}=$pid ;
  my $count = 0;
  while (1) {
    $count++;
    $self->debug(1, "Checking transfers");
    if ($count == 24 * 60) {

      #Every day
      $self->info("I'm still alive and checking");
      $count = 0;
    }
    my $done=$self->checkWakesUp($silent);
    $done or $self->debug(1, "Going back to sleep")
      and sleep($self->{SLEEP_PERIOD});
    $done and $done eq "-1" and $self->info("$$ WE DON'T HAVE TO CHECK ANY MORE!!") and return 1;
    $self->setAlive();
    $silent++;
    ($silent == 60) and $silent = 0;

  }
}

sub checkWakesUp {
  my $t = shift;
  $self->{LOGGER}->error("Service", "Error: $self->{SERVICE} has not defined the subroutine checkWakesUp");
  exit(-2);
}

sub ping {

  $self->{ALIVE_COUNTS}++;

  $self->debug(1, "Service $self->{SERVICE} contacted");

  if (($self->{ALIVE_COUNTS} == 12)) {
    $self->info("Service $self->{SERVICE} contacted");
    $self->{ALIVE_COUNTS} = 0;
  }

  #  print "PING DONE $self->{ALIVE_COUNTS}\n";
  return {"VERSION" => $self->{CONFIG}->{VERSION}};
}

sub owner {
  my $this = shift;
  return {"USER" => $ENV{'USER'}};
}

sub reply {
  my $this = shift;
  return {"VERSION" => $self->{CONFIG}->{VERSION}, "OK" => 1};
}

sub replystatus {
  my $this         = shift;
  my $result       = $self->ping();
  my $servicestate = $self->getServiceState();
  return {
    "VERSION" => $self->{CONFIG}->{VERSION},
    "Disk"    => $servicestate->{'Disk'},
    "Run"     => $servicestate->{'Run'},
    "Sleep"   => $servicestate->{'Sleep'},
    "Trace"   => $servicestate->{'Trace'},
    "Zombie"  => $servicestate->{'Zombie'},
    "OK"      => 1
  };
}

sub alive {
  my $result       = $self->ping();
  my $servicestate = $self->getServiceState();
  return {
    "VERSION" => $result->{'VERSION'},
    "Disk"    => $servicestate->{'Disk'},
    "Run"     => $servicestate->{'Run'},
    "Sleep"   => $servicestate->{'Sleep'},
    "Trace"   => $servicestate->{'Trace'},
    "Zombie"  => $servicestate->{'Zombie'}
  };
}

# recursive function to get all childs from the process tress
# like in ProcessMonitor.pm

sub getChildProcs {
  my $self    = shift;
  my $pid     = shift;
  my $results = shift;
  my $sallps  = shift;

  my $first;
  my @allps;

  if ($sallps) {
    @allps = @{$sallps};

  }

  my @all;

  if ($#allps == -1) {
    open(A, "ps -eo \"pid ppid\"|");
    my @output = <A>;
    close(A);
    shift @output;    # remove header
    while (@output) {
      push @allps, $_;
    }
  }

  foreach (@allps) {
    my ($newpid, $newppid) = split " ", $_;

    if ($newpid) {
      chomp $newpid;
    }

    if ($newppid) {
      chomp $newppid;
    }

    if (($newpid == $pid) || ($newppid == $pid)) {
      if (($newpid != $self->{PROCESSID})) {
        push @all, $_;
      }
    }
  }

  foreach (@all) {
    my ($newpid, $newppid) = split " ", $_;
    chomp $newpid;
    if ($newpid != $pid) {
      $self->getChildProcs($newpid, $results, \@allps);
    } else {

      #           print $newpid,"\n";
      push @{$results}, $newpid;
    }
  }
}

sub getServiceState {
  my $self = shift;
  my $gpid = getppid();
  my $ppid = (shift or $gpid);
  my @allprocs;

  my $nD = 0;
  my $nR = 0;
  my $nS = 0;
  my $nT = 0;
  my $nZ = 0;

  $self->getChildProcs($ppid, \@allprocs);
  for (@allprocs) {
    my $npid = $_;
    chomp $npid;

    my @all = `ps --pid $npid -o "state" ;`;
    shift @all;    # remove header

    my $all = $all[0];

    if ((!defined $all) || (!$all)) {
      next;
    }

    if ($all =~ /D/) { $nD++; }
    if ($all =~ /R/) { $nR++; }
    if ($all =~ /S/) { $nS++; }
    if ($all =~ /T/) { $nT++; }
    if ($all =~ /Z/) { $nZ++; }

  }
  return {"Disk" => $nD, "Run" => $nR, "Sleep" => $nS, "Trace" => $nT, "Zombie" => $nZ};
}

sub createJDL {
  my $self        = shift;
  my $expressions = shift;

  my $ca = AlienClassad::AlienClassad->new("[]");

  foreach my $key (keys %{$expressions}) {
    $self->debug(1, "Setting expression $key to $expressions->{$key}");
    $ca->set_expression($key, $expressions->{$key})
      or $self->{LOGGER}->error("Transfer", "Error putting $key as $expressions->{$key}")
      and return;
  }

  if (!$ca->isOK()) {
    $self->{LOGGER}->error("Transfer", "classad not correct ???!!!");
    return;
  }
  return $ca->asJDL();

}


sub checkFileSize {
  my $this = shift;
  my $file = shift;

  $self->info("Getting the size of $file");

  (-f $file)
    or $self->{LOGGER}->warning("Service", "$file does not exist")
    and return (-1, "File doesn't exist");

  my $size = -s $file;
  $self->info("Size of $file is $size");
  return $size;
}

#
#
#
sub getVersion {
  shift;
  my $version = $self->{CONFIG}->{VERSION};
  $self->info("Returning the version of this service: $version");
  return $version;
}

#
#
sub forkCheckProcInfo {
  my $self = shift;
  my $dir  = "$self->{CONFIG}->{LOG_DIR}/$self->{SERVICE}";
  mkdir $dir;
  my $id = fork();

  #
  defined $id or $self->info("Error forking a process") and return;
  if ($id) {
    $self->info("The father has started the messages thread ($id)");
    return $id;
  }
  $self->info("Putting the output in $dir/ProcInfo.log");
  $self->{LOGGER}->redirect("$dir/ProcInfo.log");
  my $silent = 0;
  while (1) {
    $self->checkProcInfo($silent);
    $silent++;
    $silent or $self->info("Going to sleep...");
    $silent > 5 and $silent = 0;
    sleep(60);
  }
  exit(0);
}

sub checkProcInfo {
  my $self   = shift;
  my $silent = (shift || 0);
  my $method = "info";
  my @data;
  $silent and $method = "debug" and push @data, 1;

  $self->$method(@data, "Checking the jobs that are running");

  my $messages = $self->{LOCALJOBDB}->retrieveMessages();
  $messages or return 1;
  my @list = @$messages;

  #We shouldn't send more than 200 messages in one go

  while (@list) {
    my @temp = ();
    for (my $i = 0 ; $i < 200 ; $i++) {
      my $item = shift @list or last;
      push @temp, $item;
    }
    $self->info("Sending $#temp to the job manager");
    $self->{RPC}->CallRPC("Manager/Job", "SetProcInfoBunch", $self->{HOST}, \@temp)
      or $self->info("ERROR!!! we couldn't send the messages to the job manager");
  }
  $self->info("All messages have been sent!");
}

sub GetConfiguration {
  my $this = shift;

  $self->info("Someone asked us the configuration");
  my $cache = AliEn::Util::returnCacheValue($self, "Config");
  $cache and return $cache;
  $self->info("Let's reload the configuration");
  $self->{CONFIG} = $self->{CONFIG}->Reload({"force", 1});

  my $t = {};
  
  foreach my $key (sort keys %{$self->{CONFIG}}) {
    $key =~ /^LOGGER$/ and next;
    if ($key=~ s/_ORIG$// ){
      $self->info("Setting $key to its previous value");
      $t->{$key} = $self->{CONFIG}->{"${key}_ORIG"};      
    }else {
      $t->{$key} = $self->{CONFIG}->{"${key}"};      
    }
  }
  AliEn::Util::setCacheValue($self, "Config", $t);

  return $t;
}

# ***************************************************************
# get subject from SSL_CLIENT_S_DN
# ***************************************************************

sub checkAuthentication {
  my $self = shift;
  my $role = shift;
  $self->{HOST} =~ /^https/ or return;


  if ($ENV{SSL_CLIENT_S_DN}) {
    $self->info("SSL_CLIENT_S_DN is : $ENV{SSL_CLIENT_S_DN}");

    # $self->debug(1,"SSL_CLIENT_S_DN is : $ENV{SSL_CLIENT_S_DN}");

    my $subject = $ENV{SSL_CLIENT_S_DN};

    $subject =~ s/(\/CN=(((limited )?proxy)|(\d+)))+$//;
    $self->debug(1, "The DN from subject is : $subject");


    if ($self->{UI}) {
      my ($uid) = $self->{UI}->execute("verifySubjectRole", $role, $subject);
      if (!$uid) {
        $self->info("Failure in translating $subject into $uid");
        return;
      }

      $self->info("***THE uid for $subject is $uid");
      use Data::Dumper;
      $self->info(Dumper($uid));
      $self->info("About $role,information in database is $uid ,$subject with roles ");
      return 1; 

    } else {
      print STDERR "Error: we don't have an instance of the catalog.\n";
      return;
    }
  } else {
    print STDERR "There is no environment SSL_CLIENT_S_DN.\n";
    return;
  }
}

return 1;

