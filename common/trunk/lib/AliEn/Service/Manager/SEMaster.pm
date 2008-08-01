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

sub getFilesToDelete {
  my $this=shift;
  my $name=shift;
  my $index=shift;
  $self->info("Getting the list of files to delete that are newer than index $index");
  my $seNumber=$self->{DB}->queryValue("SELECT seNumber from SE where seName=?", undef, {bind_values=>[$name]});
  if (! $seNumber){
    $self->info("Error getting the senumber of $name");
    return (-1, "Error getting the senumber of $name");
  }
    
  if ($index){
    $self->{DB}->do("delete from TODELETE where entryId<=? and seNumber=?", {bind_values=>[$index, $seNumber]});
  }
  my $info=$self->{DB}->query("select entryId, pfn from TODELETE where seNumber=? order by 1 limit 100", undef, {bind_values=>[$seNumber]});
  $self->info("Returning $#$info to $name ($seNumber)");
  return $info;
}
1;


