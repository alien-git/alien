package AliEn::Service::MessagesMaster;


use strict;
use AliEn::Service;
use AliEn::Database::TaskQueue;
use AliEn::SOAP;
use Data::Dumper;

use vars qw (@ISA $DEBUG);
@ISA=("AliEn::Service");
$DEBUG=0;

my $self = {};


sub initialize {
  $self = shift;

  my $options = (shift or {});

  ($self->{HOST}, $self->{PORT})=
    split (":", $self->{CONFIG}->{"MESSAGESMASTER_ADDRESS"});
  $self->{SERVICE}="MessagesMaster";
  $self->{SERVICENAME}="MessagesMaster";

  # Information for central Message log, can go into Config/Config      #

  $self->{DB} = AliEn::Database::TaskQueue->new({ROLE=>'admin'})
    or $self->info("Not possible to get  the database")
      and return;

  #                                                                     #
  #######################################################################

  return $self;
}

sub getMessages {  
  my $this=shift;

  $self->info("Getting the last MESSAGES for @_");

  my $service=shift;
  my $host=shift;
  my $lastAck=shift;

  if ($lastAck){
    $self->info("Deleting the MESSAGES smaller than $lastAck");
    $self->{DB}->delete("MESSAGES", "TargetService=? and TargetHost=? and ID<=?", {bind_values=>[$service,$host,$lastAck]});
  }

  my $res  =
    $self->{DB}->query("SELECT ID,TargetHost,Message,MessageArgs from MESSAGES 
      WHERE TargetService = ? AND TargetHost like ? order by ID limit 100",
    undef, {bind_values=>[$service, $host]});

  defined $res
    or $self->{LOGGER}->error("MessagesMaster","Error fetching messages from database")
      and return;
   
  if (scalar(@$res)){
  	  my $del = "(ID,Message) in (";           
  	  my @bind=();
	  foreach my $data ( @$res ) {
	  	$del .= "(?,?),";
	  	push @bind, $data->{ID};
	  	push @bind, $data->{Message};
	  }
	  $del =~ s/,$//;
	  $del .= ")";
	  
	  $self->info("Deleting $del, ".Dumper(@bind));
	  
	  $self->{DB}->delete("MESSAGES", $del, {bind_values=> \@bind});
  }
      
  $self->info("Returning ".( $#$res +1 )." messages");
  return $res;
}


1;

