package AliEn::Service::Manager;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;
use AliEn::Service;

use AliEn::UI::Catalogue::LCM::Computer;

use AliEn::JDL;

use vars qw (@ISA);
@ISA=("AliEn::Service");

my $self = {};


sub initialize {
  $self     = shift;
  my $options =(shift or {});
  
  $self->{SERVICE} or
    $self->info("Manager: In initialize service name missing\n")
      and return;
  
  $self->{SERVICENAME} = "$self->{SERVICE}Manager";
  
  $self->debug(1, "In initialize setting up a $self->{SERVICENAME}");
  
  my $name="\U$self->{SERVICE}\E";
  
  $self->debug(1, "In initialize getting information for $self->{SERVICENAME}" );
  
  my ($host, $driver, $db) =
    split ("/", $self->{CONFIG}->{"${name}_DATABASE"});
  
  ($self->{HOST}, $self->{PORT})=
    split (":", $self->{CONFIG}->{"${name}_MANAGER_ADDRESS"});
  
  $self->{LISTEN}=10;
  $self->{PREFORK}=5;
  
  $options->{role}="admin";
  $ENV{ALIEN_DATABASE_SSL} and delete $options->{role};

  $self->debug(1, "In initialize creating AliEn::UI::Catalogue instance" );
  
  $self->{CATALOGUE} = AliEn::UI::Catalogue::LCM::Computer->new($options)
    or $self->{LOGGER}->error( "Manager", "In initialize error creating AliEn::UI::Catalogue instance" )
      and return;

  $self->debug(1, "In initialize AliEn::UI::Catalogue instance created" );
  
  $self->{DB_MODULE} or
    $self->info("Manager: in initialize database module not defined")
      and return;

  $self->debug(1, "In initialize creating $self->{DB_MODULE} instance" );

  my $role="admin";
  $ENV{ALIEN_DATABASE_SSL} and $role.="ssl";
  if ( (defined $ENV{ALIEN_NO_PROXY}) && ($ENV{ALIEN_NO_PROXY} eq "1") && (defined $ENV{ALIEN_DB_PASSWD}) ) {
      $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>$role,"USE_PROXY" => 0, PASSWD=>"$ENV{ALIEN_DB_PASSWD}"});
  } else {
      $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>$role});
  }

  $self->{DB}
    or $self->{LOGGER}->error( "Manager", "In initialize creating $self->{DB_MODULE} instance failed" )
      and return;

  $self->{LOGGER}->info( "Manager", "In initialize creating $self->{DB}->{ROLE} instance" );

  $self->{SERVICE}="Manager::$self->{SERVICE}";

  return $self;
}


return 1;

