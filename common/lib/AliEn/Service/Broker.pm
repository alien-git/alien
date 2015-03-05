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

    $self->info( " $$ in match pending$type = $id" );

    my $job_ca = Classad::Classad->new($element->{jdl});
    $self->debug(1, "Checking $element->{jdl}");
    if ( !$job_ca->isOK() ) {
      $self->info("Got an incorrect $type ca ($id)");
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
#      $self->{DB}->lock("QUEUE write, QUEUEPROC");
      $self->info("$$ WE HAVE LOCKED THE TABLE");
#    } elsif ($type eq "agent") {
#      $self->{DB}->lock("QUEUE write, QUEUEPROC");
#      $self->info("$$ WE HAVE LOCKED THE TABLE (agent)");
    }
    $self->info("$$ WE HAVE A MATCH WITH $id!!! ");

    if ($findIdFunction){
      @possibleIds=$self->$findIdFunction($id);
    }
    $self->info("$$ Checking all the possible Ids ");
    while (@possibleIds){
      my $item=shift @possibleIds;
      my $realId=$item->{id};

      $self->info("$$ First, assign the job");
      if (not  $self->{DB}->assignWaiting($realId,$arg1,$arg2,$text)){
         $self->info("$$ the job has already been given");
         next;
      }

      my $ret1=$item->{classad};
      my $ret2=$item->{jdl};
      if (!$ret2) {
        $self->info("$$ Now we get the jdl");
        $ret2=$self->{DB}->queryValue("SELECT jdl from QUEUE where queueid=?", undef, {bind_values=>[$realId]});
      }
      if (!$ret1 ){
	$self->debug(1, "Creating the classad of $item->{jdl}");
	$ret1=$item->{classad}= Classad::Classad->new($item->{jdl});
	if ( not $ret1 or not  $ret1->isOK()){

	$self->info("$$ Error creating the jdl of '$item->{jdl}'.Puttting the job to ERROR_I");
        $self->{DB}->updateStatus($realId,"WAITING","ERROR_I");
        next;

        }
      }
      $self->debug(1, "Got returning arguments for  $realId: $ret1");
      if ($function) {
	$self->debug(1, "Before returning, let's check if the extra function $function thinks everything is ok");
	my @return=$self->$function($ret1);
	if ($return[0] ne "1"){
	  $self->info("$$ $function didn't return 1. We don't assign the task");
          $self->{DB}->do("update QUEUE set status='WAITING' where queueid=?",  {bind_values=>[$realId]});
          $self->info("And putting back the counter of jobagents for $id");
          $self->{DB}->do("update JOBAGENT set counter=counter+1 where entryid=?", {bind_values=>[$id]});
	  return @return;
	}
      }
      
      splice(@$pendingElements, $currentJob,1);
      $self->debug(1, "$realId successfully assigned");
	push @toReturn, ($realId, $ret1, $ret2);
	$counter--;
	if ( $counter<=0 ){
          $self->info("$$ FOUND ALL OF THEM, and unlocking");
          return @toReturn;
        }
	$self->info("$$ We found one match, but we are still looking for other $counter");
	
    }
  }

  $self->info("$$ Returning  $#toReturn +1 entries that match" );
  
  return @toReturn;
}

sub redirectOutput{
  my $self=shift;
  my $var=shift;

  my $fullPath=$self->{CONFIG}->{LOG_DIR} || $ENV{ALIEN_HOME};
  $fullPath.="/$var.log";
  $self->{CURRENT_LOG} eq $fullPath and return 1;
  
  my $dir=$fullPath;
  $dir=~ s{[^/]*$}{};
  (-d $dir) || mkdir ($dir);
  (-d $dir) or $self->info("Error creating '$dir'") and return;

  #$self->info("Putting the output in $dir");
  $self->{LOGGER}->redirect($fullPath);
  $self->{CURRENT_LOG} =$fullPath; 
  return 1;
}


return 1;


