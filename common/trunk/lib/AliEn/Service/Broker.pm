package AliEn::Service::Broker;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Service;
use strict;
use AliEn::UI::Catalogue;
use AliEn::Util;

use Classad;

use vars qw (@ISA);
@ISA=("AliEn::Service");


sub initialize {
  my $self     = shift;
  my $options =(shift or {});
  
  $self->{SERVICE} or
    print STDERR "Manager: In initialize service name missing\n"
      and return;
  
  $self->{SERVICENAME}="$self->{SERVICE}Broker";
	
  $self->debug(1, "In initialize setting up a $self->{SERVICENAME}");

  my $name="\U$self->{SERVICE}\E";

  $self->debug(1, "In initialize getting information for $self->{SERVICENAME}" );

  my ($host, $driver, $db) =
    split ("/", $self->{CONFIG}->{"${name}_DATABASE"});
 
  my $broker_address = $self->{CONFIG}->{"${name}_BROKER_ADDRESS"};
  if($broker_address =~ /\S+:\/\/(\S+):(\S+)/){
    ($self->{HOST}, $self->{PORT}) = ($1, $2);
  }else{
    ($self->{HOST}, $self->{PORT})=
      split (":", $broker_address);
  }
  
  $self->{LISTEN}=1;
  $self->{PREFORK}=5;

  $self->debug(1, "In initialize creating AliEn::UI::Catalogue instance" );

  $options->{role} = "admin";
  $ENV{ALIEN_DATABASE_SSL} and $options->{role}="adminssl";
  $self->{CATALOGUE} = AliEn::UI::Catalogue->new($options)
    or $self->{LOGGER}->error( "Broker", "In initialize error creating AliEn::UI::Catalogue instance" )
      and return;

  $self->debug(1, "In initialize AliEn::UI::Catalogue instance created" );
  
  $self->{DB_MODULE} or
    print STDERR "Broker: in initialize database module not defined\n"
      and return;
  
  $self->{LOGGER} ->debug( "Broker", "in initialize creating $self->{DB_MODULE} instance" );
  
  if ( (defined $ENV{ALIEN_NO_PROXY}) && ($ENV{ALIEN_NO_PROXY} eq "1") && (defined $ENV{ALIEN_DB_PASSWD}) ) {
    $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>$options->{role},"USE_PROXY" => 0, PASSWD=>"$ENV{ALIEN_DB_PASSWD}"});
  } else {
    $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>$options->{role}});
  }
  
  $self->{DB}
	  or $self->{LOGGER}->error( "Broker", "In initialize creating $self->{DB_MODULE} instance failed" )
      and return;

	$self->{SERVICE}="Broker::$self->{SERVICE}";

	return $self;
}

##############################################################################
# Private functions
##############################################################################

sub extractCommand {

    my $self   = shift;
    my $job_ca = shift;

    $self->debug(1, "in extractCommand" );

    my ( $ok, $cmd ) = $job_ca->evaluateAttributeString("Executable");

    $self->debug(1, "Executable=$cmd" );

    ( $ok, my $args ) = $job_ca->evaluateAttributeString("Arguments");

    $self->debug(1, "Arguments=$args" );

    return "$cmd###$args";

}

sub match {
  my $self   = shift;
  my $type    = shift;
  my $site_ca     = shift;
  my $pendingElements = shift;
  my $arg1  = shift;
  my $arg2  = shift;
  my $function=shift;
  my $counter=shift || 1;
  my $findIdFunction=shift;
  $self->info( "Checking $type");

  my $text = $site_ca->asJDL();

  $self->debug(1, "in match site_ca=$text" );
  my @toReturn=();
  for (my $currentJob=0; $currentJob<=$#$pendingElements; $currentJob++) {
    my $element=$$pendingElements[$currentJob];
    my $id = $element->{"${type}Id"};

    $self->debug(1, "in match pending$type = $id" );

    my $job_ca = Classad::Classad->new($element->{jdl});
    $self->info("CHecking $element->{jdl}");
    if ( !$job_ca->isOK() ) {
      $self->{LOGGER}->error( "Broker", "Got an incorrect $type ca ($id)");
      $self->{DB}->updateStatus($id,"%", "INCORRECT");
      splice(@$pendingElements, $currentJob,1);
      next;
    }

    my ( $match, $rank ) = Classad::Match( $job_ca, $site_ca );

    ($match) or next;
    my @possibleIds=({id=>$id, classad=>$job_ca, });
    if($type eq "queue"){
      $possibleIds[0]->{jdl}=$element->{jdl};
      $possibleIds[0]->{jdl}=~ s/\s+/ /g;
    }
    $self->info("WE HAVE A MATCH WITH $id!!!");
    if ($findIdFunction){
      @possibleIds=$self->$findIdFunction($id);
    }
    $self->info("Checking all the possible Ids");
    while (@possibleIds){
      my $item=shift @possibleIds;
      if ($findIdFunction){
	$self->$findIdFunction($id, \@possibleIds);
      }
      my $ret1=$item->{classad};
      my $ret2=$item->{jdl};
      my $realId=$item->{id};
      if (!$ret1 ){
	$self->info("Creating the classad of $item->{jdl}");
	$ret1=$item->{classad}= Classad::Classad->new($item->{jdl});
	($ret1 and $ret1->isOK())
	  or $self->info("Error creating the jdl") and next;
      }
      $self->debug(1, "Got returning arguments for  $realId: $ret1");
      if ($function) {
	$self->debug(1, "Before returning, let's check if the extra function $function thinks everything is ok");
	my @return=$self->$function($ret1);
	if ($return[0] ne "1"){
	  $self->info("$function didn't return 1. We don't assign the task");
	  return @return;
	}
      }
      
      $self->debug(1, "Checking if the $type is still free");
      splice(@$pendingElements, $currentJob,1);
      if ( $self->{DB}->assignWaiting($realId,$arg1,$arg2,$text)){
	$self->debug(1, "$realId successfully assigned");
	push @toReturn, ($realId, $ret1, $ret2);
	$counter--;
	$counter>0 or return @toReturn;
	$self->info("We found one match, but we are still looking for other $counter");
	
      } else {
	$self->debug(1, "$type has already been given");
      }
    }
  }

  $self->info("Returning  $#toReturn +1 entries that match" );
  
  return @toReturn;
}

return 1;


