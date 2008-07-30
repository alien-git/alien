package AliEn::Service::Manager::SEMaster;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;

use AliEn::Database::SE;
use AliEn::Service::Manager;

use AliEn::Util;



use vars qw (@ISA $DEBUG);
@ISA=("AliEn::Service::Manager");

$DEBUG=0;

my $self = {};

sub initialize {
  $self     = shift;
  my $options =(shift or {});

  $DEBUG and $self->debug(1, "In initialize initializing service SEMasterManager" );
  $self->{SERVICE}="SEMaster";

  $self->{DB_MODULE}="AliEn::Database::SE";
  $self->SUPER::initialize($options) or return;

  return $self;
}


sub getVolumeInfo {
  my $this=shift;
  my $name=shift;
  $self->info("Getting the volumes of the se $name");
  
  my $info=$self->{DB}->query("SELECT * from SE_VOLUMES where SENAME=? and freespace>0",undef,
			      {bind_values=>[$name]});
  $self->info("Query done, and there are $#$info");
  return $info;
  
}
1;


