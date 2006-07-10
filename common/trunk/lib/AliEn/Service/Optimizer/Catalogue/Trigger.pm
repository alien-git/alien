package AliEn::Service::Optimizer::Catalogue::Trigger;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  $self->{SLEEP_PERIOD}=10;
  my $method="info";
  $silent and $method="debug" and  @info=1;

  $self->$method(@info, "The trigger optimizer starts");

  my ($hosts) = $self->{DB}->getAllHosts();
  foreach my $tempHost (@$hosts) {
    my $db=$self->{DB}->reconnectToIndex( $tempHost->{hostIndex},"",$tempHost) or $self->info("Error doing $tempHost->{db}") and next;;
    $self->$method(@info, "Doing $tempHost->{db}");
    $self->checkTriggers($silent, $db);
  }

  return;

}
sub checkTriggers{
  my $self=shift;
  my $silent=shift;
  my $db=shift;

  my $data=$db->query("SELECT * from TRIGGERS")
    or $self->info("Error getting the triggers of $db->{DB}")
      and return;
  my $entryId=0;
  foreach my $entry (@$data){
    $entry->{entryId}>$entryId and $entryId=$entry->{entryId};
    my ($file)=$self->{CATALOGUE}->execute("get", $entry->{triggerName});
    if ($file){
      chmod 0755, $file;
      $self->info("Calling $file $entry->{lfn}");
      system($file, $entry->{lfn});
    }else{
      $self->info("Error getting the file $entry->{triggerName}");
    }
  }
  if ($entryId){
    $entryId++;
    $self->info("Deleting the entries smaller than $entryId");
    $db->delete("TRIGGERS", "entryId<$entryId");
  }

  return 1;
}


return 1;
