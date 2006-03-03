package AliEn::Service::Optimizer::Job::HeartBeat;

use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");


sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";

  my $now = time;
  ##################################################################################################################
  ### ClusterMonitor heartbeat check

  $self->{LOGGER}->$method("HeartBeat", "In checkWakesUp .... checking site connectivity ...");

  my $allsites = $self->{DB}->getFieldsFromSiteQueueEx("site,status,statustime","group by site");
  if (@$allsites) {
    foreach (@$allsites) {
      if ( ($now - $_->{statustime}) > 120 ) {
	my $set={};
	$set->{'status'}     = "down";
	$set->{'statustime'} = $now;
	$self->{DB}->updateSiteQueue($set,"site='$_->{'site'}'");
	$self->{LOGGER}->$method("HeartBeat", "In checkWakesUp .... site $_->{'site'} is down");
      }
    }
  } else {
    $self->{LOGGER}->error("HeartBeat", "In checkWakesUp .... cannot get the SiteQueue table entries!");
  }

  return;
}


1
