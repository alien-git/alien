package AliEn::Service::Optimizer::Catalogue::Deleted;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");


sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  $self->updateReferences($silent);
  $self->deleteNonReferencedGUID($silent);
  return ;
}

sub deleteNonReferencedGUID {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  my $hosts=$self->{DB}->getAllHosts();

  foreach my $host (@$hosts){
    use Data::Dumper;
    print Dumper($host);
    $self->info("PREPARADO PARA RECONECTAR");
    my ($db, $t)=$self->{DB}->{GUID_DB}->reconnectToIndex($host->{hostIndex}) 
      or $self->info("Error reconnecting to $host->{hostIndex}") and next;

    $self->info("Deleting the entries that have not been used in $host->{hostIndex}");
    my $tables=$db->queryColumn("SELECT tableName from GUIDINDEX where hostIndex='$host->{hostIndex}'");
    foreach my $tableD (@$tables){
      my $table="G${tableD}L";
      $self->info("Checking the table $table");
      $db->lock("$table WRITE, ${table}_PFN WRITE, TODELETE WRITE, SE");
      $db->do("INSERT INTO TODELETE(guid, seNumber, pfn) select $table.guid, seNumber, pfn from $table, ${table}_PFN where ref=0 and $table.guidId=${table}_PFN.guidId");
      #We should also insert the entries that have the seAutoString
      $db->do("INSERT INTO TODELETE(guid,seNumber) select $table.guid, seNumber from $table, SE where ref=0 and seAutoStringList like concat('%,',seNumber,',%')");
      $db->do("DELETE from $table where ref=0 and ctime<TIMESTAMPADD(minute,-2,now())");
      $db->unlock();

    }

  }

  return ;
}

sub updateReferences{
  my $self=shift;
  my $silent=shift;

  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  $self->$method(@info, "Checking the entries from the local databases that do not exist anymore in the central catalogue");
  my $hosts=$self->{DB}->getAllHosts();

  foreach my $host (@$hosts){
    my $maxEntryId=0;
    $self->info("Selecting the entries that have changed in $host");
    my ($db, $path2)=$self->{DB}->{LFN_DB}->reconnectToIndex($host->{hostIndex}) or next;
    $db->do("delete from LFN_UPDATES where guid is null");
    my $entries=$db->query("SELECT action,entryId,binary2string(guid) as guid from LFN_UPDATES");
    foreach my $entry (@$entries){
      $entry->{guid} or $self->info("The entry $entry->{entryId} doesn't have a guid") and next;
      $self->info("The guid '$entry->{guid}' has '$entry->{action}'");
      my $todo;
      $entry->{action} eq 'delete' and $todo="-1";
      $entry->{action} eq 'insert' and $todo="+1";
      $todo or $self->info("I don't know what to do with '$entry->{action}'") 
	and next;
      my ($db2, $table)=$self->{DB}->{GUID_DB}->selectDatabaseFromGUID($entry->{guid})
	or next;
      $db2->do("UPDATE ${table} set ref=ref$todo where guid=string2binary('$entry->{guid}')");
      $entry->{entryId}>$maxEntryId and $maxEntryId=$entry->{entryId};
    }
    $maxEntryId and $db->do("DELETE FROM LFN_UPDATES where entryId<=$maxEntryId");
  }

  
  return 1;
}

return 1;
