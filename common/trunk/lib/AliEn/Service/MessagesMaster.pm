package AliEn::Service::MessagesMaster;


use strict;
use AliEn::Service;
use AliEn::Database::TaskQueue;
use AliEn::SOAP;

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
    $self->{DB}->delete("MESSAGES", "TargetService=? and TargetHost=? and ID<=?", {bind_values=>[$service,$host, $lastAck]});
  }


  my $time = time;

  my $res  =
  #  $self->{DB}->query("SELECT ID,TargetHost,Message,MessageArgs from MESSAGES WHERE TargetService = ? AND  ? like TargetHost AND (Expires > ? or Expires = 0)order by ID limit 300", undef, {bind_values=>[$service, $host, $time]});
  $self->{DB}->getMessages($service, $host, $time);
  defined $res
    or $self->{LOGGER}->error("ClusterMonitor","Error fetching messages from database")
      and return;
  $self->info("Returning ".( $#$res +1 )." messages");
  return $res;
}


1;

