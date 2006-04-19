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



return 1;

