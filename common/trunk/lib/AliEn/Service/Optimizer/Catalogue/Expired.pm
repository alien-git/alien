package AliEn::Service::Optimizer::Catalogue::Expired;
 
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

  $self->$method(@info, "The expired optimizer starts");

  my ($hosts) = $self->{DB}->getAllHosts();
  foreach my $tempHost (@$hosts) {
    my $db=$self->{DB}->reconnectToIndex( $tempHost->{hostIndex},"",$tempHost) or $self->info("Error doing $tempHost->{db}") and next;;
    $self->$method(@info, "Doing $tempHost->{db}");
    my $tables=$db->query("select tableName, lfn from INDEXTABLE where hostIndex='$tempHost->{hostIndex}'");
    foreach my $table (@$tables){
      $self->$method(@info,"Doing the table $table->{tableName} and $table->{lfn}");
      
      $self->checkExpired($silent, $db, "D$table->{tableName}L", $table->{lfn});
    }
  }

  return;

}
sub checkExpired{
  my $self=shift;
  my $silent=shift;
  my $db=shift;
  my $table=shift;
  my $dir=shift;


  my $data=$db->query("SELECT * from $table where expiretime<now()")
    or $self->info("Error getting the triggers of $db->{DB}")
      and return;
  my $entryId=0;
  foreach my $entry (@$data){
    $self->info("We have to do something with the entry $entry");
    use Data::Dumper;
    print Dumper($entry);
    my $newSEList=$db->queryValue("select group_concat(seNumber) from SE 
   where '$entry->{seStringList}' like concat('\%,',seNumber,',\%') and seQoS not like 'replica'");
    $newSEList and $newSEList=",$newSEList,";
    my $lfn="$dir/$entry->{lfn}";
    $db->update($table, {seStringList=>$newSEList}, "lfn='$entry->{lfn}'");
    if (!$newSEList) {
      $self->info("The $lfn file is not in any custodial SE. Renaming it");
      $self->{CATALOGUE}->execute("mv", $lfn, "$lfn.expired");
      my ($owner, $gowner)=($entry->{owner}, $entry->{gowner});
      $self->{CATALOGUE}->execute("chown", "$owner.$gowner", "$lfn.expired");
    } else {
      $self->info("The file stays");
      $self->{CATALOGUE}->execute("setExpired", -1, $lfn);
    }
    
  }

  return 1;
}


return 1;
