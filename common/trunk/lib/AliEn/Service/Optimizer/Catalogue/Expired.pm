package AliEn::Service::Optimizer::Catalogue::Expired;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  $self->$method(@info, "The expired optimizer starts");
  $self->updateQoS($silent) or return;

  my ($hosts) = $self->{DB}->getAllHosts();
  foreach my $tempHost (@$hosts) {
    my $db=$self->{DB}->reconnectToIndex( $tempHost->{hostIndex},"",$tempHost) or $self->info("Error doing $tempHost->{db}") and next;;
    $self->$method(@info, "Doing $tempHost->{db}");
    my $tables=$db->query("select tableName, lfn from INDEXTABLE where hostIndex=?", undef, {bind_values=>[$tempHost->{hostIndex}]});
    foreach my $table (@$tables){
      $self->$method(@info,"Doing the table $table->{tableName} and $table->{lfn}");
      
      $self->checkExpired($silent, $db, "D$table->{tableName}L", $table->{lfn});
    }
  }

  return;

}

sub updateQoS{
  my $self=shift;
  my $silent=shift;

  my $method="info";
  my @debug=();
  $silent and $method="debug" and @debug=1;
  $self->$method(@debug, "Updating the QoS of the SE");
  my ($hosts) = $self->{DB}->getAllHosts();
  foreach my $tempHost (@$hosts) {  
    print "Comparing $tempHost->{address}  eq $self->{CONFIG}->{IS_DB_HOST}\n";
    if ($tempHost->{address}  eq $self->{CONFIG}->{IS_DB_HOST}){
      $self->info("This is easy, same database");
      $self->{DB}->do("update SE, $self->{CONFIG}->{IS_DATABASE}.SE set seQoS=protocols where seName=name ");
    }else{
      $self->info("This is tricky, different databaes");
      my $IS=AliEn::Database::IS->new({ROLE=>'admin'}) or return;

      my $seRef=$self->{DB}->queryColumn("select seName from SE");
      foreach my $se (@$seRef){
	$self->info("Looking for $se");
	my $QoS=$IS->queryValue("SELECT protocols from SE where name=?", undef, {bind_values=>[$se]});
	$QoS or next;
	$self->info("Setting the QoS of $se to $QoS");
	$self->{DB}->update("SE", {seQoS=>$QoS}, "seName=?", {bind_values=>[$se]});
      }
    }
  }
  $self->$method(@debug, "QoS of the SE updated");
  return 1
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
    my $query="select group_concat(seNumber) from SE where ? like concat('\%,',seNumber,',\%') and (seQoS not like 'replica' or seQoS is NULL)";
    $self->info("Doing $query");
    my $newSEList=$db->queryValue($query, undef, {bind_values=>[$entry->{seStringlist}]});
    $newSEList and $newSEList=",$newSEList,";
    my $lfn="$dir/$entry->{lfn}";
    $db->update($table, {seStringList=>$newSEList}, "lfn=?", {bind_values=>[$entry->{lfn}]});
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
