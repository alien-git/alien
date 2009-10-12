package AliEn::Service::Optimizer;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Service;

use strict;

use Classad;

use vars qw (@ISA);
@ISA=("AliEn::Service");

my $self = {};

sub initialize {
  $self     = shift;
  my $options =(shift or {});
  
  $self->{SERVICE} or
    print STDERR "Optimizer: in initialize service name missing\n"
      and return;
  
  $self->{SERVICENAME}="$self->{SERVICE}Optimizer";
  
  $self->debug(1, "In initialize setting up a $self->{SERVICENAME}");
  
  my $name="\U$self->{SERVICE}\E";
  
  $self->debug(1, "In initialize getting information for $self->{SERVICENAME}" );
  
  my ($host, $driver, $db) =
    split ("/", $self->{CONFIG}->{"${name}_DATABASE"});
  
  ($self->{HOST}, $self->{PORT})=
    split (":", $self->{CONFIG}->{"${name}_OPTIMIZER_ADDRESS"});
  
  $self->{LISTEN}=10;
  $self->{PREFORK}=1;
  $self->{FORKCHECKPROCESS}=1;
  
  $self->{DB_MODULE} or
    print STDERR "Optimizer: in initialize database module not defined\n"
      and return;
  
  $self->{LOGGER} ->debug( "Optimizer", "in initialize creating $self->{DB_MODULE} instance" );

  if ( (defined $ENV{ALIEN_NO_PROXY}) && ($ENV{ALIEN_NO_PROXY} eq "1") && (defined $ENV{ALIEN_DB_PASSWD}) ) {
      $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin',"USE_PROXY" => 0, PASSWD=>"$ENV{ALIEN_DB_PASSWD}"});
  } else {
      $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin'});
  }
  
  $self->{DB}
    or $self->{LOGGER}->error( "Optimizer", "In initialize creating $self->{DB_MODULE} instance failed" )
      and return;
  
  $self->{SERVICE}="Optimizer::$self->{SERVICE}";
  return $self;
}

sub StartChildren{
  my $self=shift;
  my @optimizers=@_;

  $self->info( "Let's start all the optimizers");
  my $i=0;


  my $service=$self->{SERVICE};
  $service=~ s/Optimizer:://;

  my $dir="$ENV{ALIEN_HOME}/var/log/AliEn/$self->{CONFIG}->{ORG_NAME}/${service}Optimizer";

  mkdir $dir,  0755;
  foreach (@optimizers){
    my $name="AliEn::Service::$self->{SERVICE}::$_";

    $self->info( "Starting the $name");
    eval "require $name";
    if ($@) {
      $self->info( "Error requiring the optimizer $name $! $@");
      return;
    }
    my $d="CHILDPID$i";
    $self->{$d}=fork();
    $i++;
    defined $self->{$d} 
      or $self->info( "Error forking a process") and return;
    #the father goes on...
    $self->{$d} and next;

    $self->info( "Putting the output in $dir/$_.log");
    $self->{LOGGER}->redirect("$dir/$_.log");
    #The children should just initialize and start checking;
    #	@ISA=($name,@ISA);
    $self->info( "The optimizer $name starts");
    bless($self, $name);
    #let's give time to the father to prepair everything
    sleep(10);
    
    my $tmpServName = $self->{SERVICENAME};
    my $shortName = $_;
    $self->{SERVICENAME} = "${service}_".$shortName."Optimizer";
    AliEn::Util::setupApMon($self);
    if(($shortName eq "MonALISA") && (! $self->{MONITOR})){
      $self->{LOGGER}->error("MonALISA", "Error: Can not initialize ApMon");
      exit(-2);
    }
    $self->{SERVICENAME} = $tmpServName;
    
    $self->startChecking();
    $self->{LOGGER}->error("Job", "Error: the job optimizer $shortName died!!\n");
    exit(-2);

  }


  return $self;
}

sub startListening {
  my $this=shift;

  $self->info("In fact, this is not a service. We don't listen for anything.");
  while(1){
    sleep(90000000);
  }
  return 1;
}



return 1;

