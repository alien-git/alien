#####################################################
#  Proof interactive Analysis Master Module         #
#  (C) Andreas-J. Peters @ CERN                     #
#  mailto: Andreas.Peters@cern.ch                   #
#####################################################

package AliEn::ProofMaster;
use strict;
use AliEn::Config;
use AliEn::Command::MASTERPROOFD;
use AliEn::Logger;
use Socket;
use Carp;

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = {};
  bless( $self, $class );
  
  return $self;
}

sub init {
  my $self = shift;
  $self->{CONFIG} = new AliEn::Config();
  $self->{CONFIG} or return;


  # get the master proofd package from LDAP
  $self->{ROOT}            = $self->{CONFIG}->{'PROOF_PACKAGE'};
  $self->{PROOFMASTERPORT} = $self->{CONFIG}->{'PROOF_MASTER_PORT'};

  # set the fake environment like in a queue job
  $self->{HOME} = $ENV{'HOME'};
  $ENV{"ALIEN_WORKDIR"}  = $self->{HOME};
  $ENV{"ALIEN_PACKAGES"} = "$self->{ROOT}";

  # reload the packages with config
  my $options = {};
  $options->{PACKCONFIG} = 1;
  $options->{force} =1;
  $self->{CONFIG} = $self->{CONFIG}->Reload($options);

  # create the .alien/proof directory
  if (! (-d "$self->{HOME}/.alien/proofd") ) {
    if (!(mkdir "$self->{HOME}/.alien/proofd", 0700)) {
      print "Cannot create $self->{HOME}/.alien/proofd directory!\n";
      return;
    }
  }

  # create the .alien/proof/config directory, which contains the 
  # proof master configuration files

  if (! (-d "$self->{HOME}/.alien/proofd/config")) {
    if (!(mkdir "$self->{HOME}/.alien/proofd/config", 0700)) {
      print "Cannot create $self->{HOME}/.alien/proofd/config directory!\n";
      return;
    }
  }

  # remove the running master
  $self->killmaster();
  $self->checkport() or return;
  print "PROOF Master will run on Port $self->{PROOFMASTERPORT}\n";
  return 1;
}

sub readpid {
  my $self = shift;
  my $proofmasterpid;
  if ( -e "$self->{HOME}/.alien/proofd/proofmaster.pid") {
    open (INPUT, "$self->{HOME}/.alien/proofd/proofmaster.pid");
    while (<INPUT>) {
      $proofmasterpid = $_;
    }
    close INPUT;
  }
  
  return $proofmasterpid;
}

sub writepid {
  my $self = shift;
  my $pid = shift;

  if ( -e "$self->{HOME}/.alien/proofd/proofmaster.pid") {
    unlink "$self->{HOME}/.alien/proofd/proofmaster.pid";
  }

  open (OUTPUT, ">$self->{HOME}/.alien/proofd/proofmaster.pid");
  print OUTPUT "$pid";
  close OUTPUT;
}

sub killmaster {
  my $self = shift;
  my $nkilled = 0;
  # kill old PROOF master

  my $proofmasterpid = $self->readpid();
  print "Master PID $proofmasterpid\n";
  if ($proofmasterpid) {
    my $allpids = `ps -eo \"pid ppid\" | grep $proofmasterpid | LD_LIBRARY_PATH= awk '{print \$1}'`;
    my @splitpids = split " ",$allpids;
    
    foreach (@splitpids) {
      if (((kill 9, $_)>0)) {
#	$self->{LOGGER}->info("Proof","Killed old PROOF master at pid $_ !\n");
	print "Killed old PROOF master at pid $_ !\n";
	$nkilled++;
      }
    }
  }

  return $nkilled;
}

sub checkport {
  my $self = shift;
  # try to bind the PROOF master port ...
  my $port;
  my $proto = getprotobyname('tcp');
  if (
    (    socket(Server, PF_INET, SOCK_STREAM, $proto)
      && (setsockopt(Server, SOL_SOCKET, SO_REUSEADDR, pack("l", 1)))
      && (bind(Server, sockaddr_in($self->{PROOFMASTERPORT}, INADDR_ANY)))
    )
    ) {

    #    $self->{LOGGER}->info("Proof","PROOF Master will run on Port $self->{PROOFMASTERPORT}");
    return 1;
  } else {

#    $self->{LOGGER}->info("Proof","PROOF Master Port $self->{PROOFMASTERPORT} is already busy! Can not start, check this by hand!");
    print "PROOF Master Port $self->{PROOFMASTERPORT} is busy!\n";
    return;
  }
}

sub execute {
  my $self = shift;
  my @args = shift;

  push @args, "-p $self->{PROOFMASTERPORT}";

  $self->{PROOFMASTERPID} = fork();
  
 
  if ($self->{PROOFMASTERPID}) {
     $self->writepid($self->{PROOFMASTERPID});
  } else {
    my $command;
    $command = new AliEn::Command::MASTERPROOFD();
    $command or
      print "ERROR creating an instance of $command" and  exit -1;
    
    $command->Initialize() or print STDERR "Error initializing $!\n" and exit -1;
    $command->Execute(@args);
  }

  my $looper;
  for $looper ( 1 .. 20 ) {
    if (!( $self->checkport() )) {
      # now proofd has binded the port
#      $self->{LOGGER}->info("Proof","PROOF Master Daemon successfully started!");
      print "PROOF Master Daemon successfully started!\n";
      return 1;
    }
    sleep 1;
  }
#  $self->{LOGGER}->info("Proof","PROOF Master Daemon did not start in 20 seconds!");
  print "PROOF Master Daemon did not start in 20 seconds! Proof won't start!\n";
  return 0;
}

return 1;
