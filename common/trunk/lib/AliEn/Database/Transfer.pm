#/**************************************************************************
# * Copyright(c) 2001-2002, ALICE Experiment at CERN, All rights reserved. *
# *                                                                        *
# * Author: The ALICE Off-line Project / AliEn Team                        *
# * Contributors are mentioned in the code where appropriate.              *
# *                                                                        *
# * Permission to use, copy, modify and distribute this software and its   *
# * documentation strictly for non-commercial purposes is hereby granted   *
# * without fee, provided that the above copyright notice appears in all   *
# * copies and that both the copyright notice and this permission notice   *
# * appear in the supporting documentation. The authors make no claims     *
# * about the suitability of this software for any purpose. It is          *
# * provided "as is" without express or implied warranty.                  *
# **************************************************************************/
package AliEn::Database::Transfer;

use AliEn::Database;
use strict;

use vars qw(@ISA);

@ISA=("AliEn::Database");

sub preConnect {
  my $self=shift;
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;

  $self->info("Using the default $self->{CONFIG}->{TRANSFER_DATABASE}");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB})
    =split ( m{/}, $self->{CONFIG}->{TRANSFER_DATABASE});

  return 1;
}

sub initialize {
  my $self=shift;
  my $tables={ TRANSFERS=>{columns=>{
				     transferId=>" int(11) not null auto_increment primary key", 
				     lfn=>"varchar(250)",
				     pfn=>"varchar(250)",
				     priority=>"tinyint(4) default 0",
				     received=>"int(20) default 0",
				     sent=>"int(20) default 0",
				     started=>"int(20) default 0",
				     finished=>"int(20) default 0",
				     expires=> "int(10) default 0",
				     error=>"int(11)",
				     jdl=>"text",
				     transferGroup=>"int(11)",
				     user=>"varchar(30)",
				     destination=>"varchar(50)",
				     size=>"int(11)",
				     options=>"varchar(250)",
				     status=>"varchar(15)",
				     type=>"varchar(30)",
				     SE=>"varchar(50)",

				    },
			   id=>"transferId",
			   index=>"transferId"},
	       ACTIONS=>{columns=>{action=>"char(40) not null primary key",
				   todo=>"int(1) not null default 0"},
			 id=>"action",
			 }
	     };
  foreach my $table  (keys %$tables) {
    $self->checkTable($table, $tables->{$table}->{id}, $tables->{$table}->{columns}, $tables->{$table}->{index})
      or $self->{LOGGER}->error("TaskQueue", "Error checking the table $table") and return;
  }
  $self->setupApMon();

  return $self->do("INSERT IGNORE INTO ACTIONS(action) values  ('INSERTING')");
}
sub insertTransferLocked {
  my $self = shift;
  my ($date, $user, $lfn, $pfn,$destination,$type) = @_;

  $self->debug(1,"In insertTransferLocked locking table TRANSFERS");

  $self->lock("TRANSFERS");

  $self->debug(1,"In insertTransferLocked inserting data: ".($date or "").", ".($user or "").", ".($lfn or "").", ".($pfn or "").", ".($destination or "").", ".($type or "") );
  unless($self->insert("TRANSFERS",{received=>$date, user=>$user, status=> "INSERTING", lfn=>$lfn, pfn=>$pfn, destination=>$destination, type=>$type})){
    $self->{LOGGER}->error("Transfer","In insertTransferLocked error inserting data. Unlocking table TRANSFER.");
    $self->unlock();
    return;
  }
  
  $self->debug(1,"In insertTransferLocked fetching transferId");
  my $lastID = $self->getLastId();
  
  unless($lastID){
    $self->{LOGGER}->error("Transfer","In insertTransferLocked unable to fetch transferId. Unlocking table TRANSFER");
    $self->unlock();
    return;
  }

  $self->debug(1,"In insertTransferLocked transfer $lastID successfully inserted");

  $self->unlock();

  $self->sendTransferStatus($lastID, "INSERTING", {destination=>$destination, user=>$user, received=>$date});


  $self->updateActions({todo=>1}, "action='INSERTING'");
  $lastID;
}

sub assignWaiting{
  my $self = shift;
  my $elementId = shift;
  my $date=time;
  return $self->updateStatus($elementId, "WAITING' OR status='LOCAL COPY' OR status='CLEANING", "ASSIGNED", {sent=>$date}) ;
}

sub updateExpiredTransfers{
	my $self = shift;

	my $yesterday=time;
    $yesterday -= 86400; #24*60*60

	$self->debug(1,"In updateExpiredTransfers updating status of expired transfers");
	$self->update({status=>'EXPIRED'},"status = 'ASSIGNED' and sent<$yesterday");
}

sub updateLocalCopyTransfers{
  my $self = shift;
  
  $self->debug(1,"In updateLocalCopyTransfers updating SE of LOCAL_COPY transfers");
  $self->do("UPDATE TRANSFERS SET SE = destination WHERE status = 'LOCAL_COPY' AND SE IS NULL");
}
sub updateActions{
  shift->SUPER::update("ACTIONS", @_);
}
sub update{
  shift->SUPER::update("TRANSFERS",@_);
}

sub delete{
  shift->SUPER::delete("TRANSFERS",@_);
}

sub updateStatus{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Transfer","In updateStatus transfer id is missing")
      and return;
  my $oldstatus = shift
    or $self->{LOGGER}->error("Transfer","In updateStatus old status is missing")
      and return;
  my $status = shift;
  my $set = shift || {};
  
  $set->{status} = $status;
  
  $self->debug(1, "In updateStatus locking table TRANSFERS");
  $self->lock("TRANSFERS");
  $self->debug(1, "In updateStatus table TRANSFERS locked");
  
  my $query="SELECT count(*) from TRANSFERS where transferid=$id";
  
  ($oldstatus eq "%") or $query.=" and status='$oldstatus'";

  my $message="";

  my $done=1;

  $self->debug(1, "In updateStatus checking if transfer $id with status $oldstatus exists");
  if ($self->queryValue($query)) {
    $self->debug(1, "In updateStatus setting transfer's $id status to ". ($status or ""));
    if (!$self->update($set,"transferId = $id")){
      $message="In update status failed";
    } else {
      $self->sendTransferStatus($id, $status, $set);
    }
  }
  else {
    $message="The transfer $id was no longer there";
    ($oldstatus eq "%") or  $message="The transfer $id was not $oldstatus any more";
  }

  $self->unlock();
  $self->debug(1, "In updateStatus table TRANSFERS successfully unlocked");
	
  if ($message) {
    $self->{LOGGER}->set_error_msg($message);
    $self->{LOGGER}->info("Job", $message);
    undef $done;
  }

  return $done;
}

sub updateTransfer{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Transfer","In updateTransfer transfer id is missing")
      and return;
  my $set = shift;
  
  $self->debug(1,"In updateTransfer updating transfer $id");
  $self->sendTransferStatus($id, $set->{status}, $set);

  $self->update($set,"transferid = $id");
}

sub deleteTransfer{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In deleteTransfer transfer id is missing")
      and return;
  
  $self->debug(1,"In deleteTransfer deleting transfer $id");	
  $self->delete("queueId=$id");
}

sub setSize {
  my $self = shift;
  
  $self->debug(1,"In setSize updating transfer's size");
  $self->updateTransfer(shift, {size=>shift});
}

sub setJdl {
	my $self = shift;
	
	$self->debug(1,"In setJdl updating transfer's jdl");
	$self->updateTransfer(shift, {jdl=>shift});
}

sub setSE {
	my $self = shift;
	
	$self->debug(1,"In setSE updating tranfers's SE");
	$self->updateTransfer(shift, {SE=>shift});
}

sub getSize {
	shift->getField(shift,"size");
}

sub getJdl {
	shift->getField(shift,"jdl");
}

sub getSE {
	shift->getField(shift,"SE");
}

sub isScheduled{
  my $self = shift;
  my $lfn = shift
    or $self->{LOGGER}->error("Transfer","In isScheduled lfn is missing")
      and return;
  my $destination = shift
    or $self->{LOGGER}->error("Transfer","In isScheduled destination is missing")
      and return;

  $self->debug(1,"In isScheduled checking if transfer of file $lfn to destination $destination is scheduled");
  $self->queryValue("SELECT transferId FROM TRANSFERS WHERE lfn='$lfn' AND destination='$destination' AND (status<>'FAILED' AND status<>'DONE' AND status <>'KILLED') ");
}

sub isWaiting{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Transfer","In isWaiting transfer id is missing")
		and return;

	$self->debug(1,"In isWaiting checking if transfer $id is waiting");
	$self->queryValue("SELECT COUNT(*) FROM TRANSFERS WHERE (status='WAITING' OR status='LOCAL COPY' OR status='CLEANING') AND transferid=$id");
}

sub getFields{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Transfer","In getFields transfer id is missing")
		and return;
	my $attr = shift || "*";

	$self->debug(1,"In getFields fetching attributes $attr of transfer $id");
	$self->queryRow("SELECT $attr FROM TRANSFERS WHERE transferid=$id");
}

sub getField{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Transfer","In getField transfer id is missing")
		and return;
	my $attr = shift || "*";

	$self->debug(1,"In getField fetching attribute $attr of transfer $id");
	$self->queryValue("SELECT $attr FROM TRANSFERS WHERE transferid=$id");
}

sub getFieldsEx{
	my $self = shift;
	my $attr = shift || "*";
	my $where = shift || "";

	$self->debug(1,"In getFieldsEx fetching attributes $attr with condition $where");
	$self->query("SELECT $attr FROM TRANSFERS $where");
}

sub getFieldEx{
	my $self = shift;
	my $attr = shift || "*";
	my $where = shift || "";

	$self->debug(1,"In getFieldEx fetching attributes $attr with condition $where");
	$self->queryColumn("SELECT $attr FROM TRANSFERS $where");
}

sub getWaitingTransfersBySE{
	my $self = shift;
	my $SE = shift;
	my $order = shift || "";

	my $query = "SELECT transferId,jdl FROM TRANSFERS WHERE (status='WAITING' OR status='LOCAL COPY' OR status='CLEANING') AND jdl IS NOT NULL AND ($SE)";

	$order and $query .= " ORDER BY $order";

	$self->query($query);
}

sub getNewTransfers{
	my $self = shift;

	$self->debug(1,"In getNewTransfers fetching attributes transferid,lfn, pfn, destination of transfers in INSERTING state");
	$self->query("SELECT transferid,lfn, pfn, destination FROM TRANSFERS WHERE STATUS='INSERTING'");
}


# send a job's status to MonaLisa
sub sendTransferStatus {
  my $self = shift;
  my ($jobID, $newStatus, $info) = @_;

  if($self->{MONITOR}){
    my $statusID = AliEn::Util::transferStatusForML($newStatus);

    my @params = ("statusID", $statusID);
    foreach ('started', 'finished', 'size', 'destination', 'received', 'SE'){
      push(@params, $_, $info->{$_}) if $info->{$_};
    }

    $self->{MONITOR}->sendParameters("TransferQueue_Transfers_".$self->{CONFIG}->{ORG_NAME}, $jobID, @params);
  }
}


=head1 NAME

AliEn::Database::Transfer

=head1 DESCRIPTION

The AliEn::Database::Transfer module extends AliEn::Database module. Module
contains method specific for table Transfer.

=head1 SYNOPSIS

  use AliEn::Database::Transfer;

  my $dbh = AliEn::Database::Transfer->new($dbOptions);

  $res = $dbh->getField($transferId, $attr);
  $hashRef = $dbh->getFields($transferId, $attr);
  $arrRef = $dbh->getFieldEx($attr, $addSql);
  $arrRef = $dbh->getFieldsEx($attr, $addSql);
  
  $res = $dbh->getSE($transferId);
  $res = $dbh->getJdl($transferId);
  $res = $dbh->getSize($transferId);
  
  $arrRef = $dbh->getWaitingTransfersBySE($SE,$orderBy);
  $arrRef = $dbh->getNewTransfers;
  
  $res = $dbh->isScheduled($lfn,$destination);
  $res = $dbh->isWaiting($transferId);
  
  $res = $dbh->insertLocked($date, $user, $lfn, $pfn,$destination,$type);
  
  $res = $dbh->update($updateSet,$where);
  $res = $dbh->updateTransfer($transferId, $set);
  $res = $dbh->updateStatus($transferId, $oldStatus, $newStatus, $set);
  $res = $dbh->updateSE($updateSet,$where);
  $res = $dbh->assignWaiting($transferId);
    
  $res = $dbh->setSE($transferId,$SE);
  $res = $dbh->setJdl($transferId,$jdl);
  $res = $dbh->setSize($transferId,$size);
  
  $res = $dbh->updateExpiredTransfers();
  $res = $dbh->updateLocalCopyTransfers();
  
  $res = $dbh->delete($where);
  $res = $dbh->deleteTransfer($transferId);
  
=cut

=head1 METHODS

=over

=item C<new>

  $dbh = AliEn::Database::Transfer->new( $attr );

  $dbh = AliEn::Database::Transfer->new( $attr, $attrDBI );

Creates new AliEn::Database::Transfer instance. Arguments are passed to AliEn::Database
method new. For details about arguments see AliEn::Database method C<new>.

=item C<getField>

  $res = $dbh->getField($transferId, $attr);

Method fetches value of attribute $attr for tuple with transfer id $transferId.
If transfer id is not defined method will return undef and report error.
Method calls AliEn::Database metod queryValue.

=item C<getFields>

  $hashRef = $dbh->getFields($transferId, $attr);

Method fetches set of attributes $attr for tuple with transfer id $transferId.
Result is reference to hash. Keys in hash are identical to names of attriutes 
in $attr set.
If set of attributes is not defined method returns values of all attributes. If
transfer id is not defined method will return undef and report error.
Method calls AliEn::Database metod queryRow.

=item C<getFieldEx>

  $arrRef = $dbh->getFieldEx($attr, $addSql);

Method fetches value of attribute $attr for tuples with condition $addSql.
Argument $addSql contains anything that comes after FROM part of SELECT statement.
Method returns reference to array which contains values of attribute $attr.
If $addSql condition is not defined method will return all tuples.
Method calls AliEn::Database metod queryColumn.

=item C<getFieldsEx>

  $arrRef = $dbh->getFieldsEx($attr, $addSql);

Method fetches set of attributes $attr for tuples with with condition $addSql.
Argument $addSql contains anything that comes after FROM part of SELECT statement.
If set of attributes is not defined method returns values of all attributes.
If $addSql condition is not defined method will return all tuples.
Method calls AliEn::Database metod query.

=item C<get*>     
   
  $res = $dbh->getSE($transferId);
  
  $res = $dbh->getJdl($transferId);
  
  $res = $dbh->getSize($transferId);

Method fetches transfer's $transferId attribute. 
If argument $transferId is not defined method will return undef
and report error.  

=item C<getWaitingTransfersBySE>  

  $arrRef = $dbh->getWaitingTransfersBySE($SE,$orderBy);
  
Method returns values of attributes transferId and jdl for tuples with status
WAITING, LOCAL_COPY or CLEANING and defined storage elements(SE). Argument $SE
contains list of storage elements in SQL form:
	SE = value1 OR SE = value2 OR ... OR SE = valueN
Argument $orderBy is ORDER BY part of SELECT query. 
Method uses AliEn::Database method query. 

=item C<getNewTransfers>  

  $arrRef = $dbh->getNewTransfers;
  
Method returns values of attributes transferid,lfn, pfn, destination for transfers 
with status INSERTING. Method uses AliEn::Database method query. 

=item C<isScheduled>  

  $res = $dbh->isScheduled($lfn,$destination);
  
Method checks if transfer of file with logical file name $lfn to destination $destination
is scheduled. If one of arguments is not defined method will return undef and report error.

=item C<isWaiting>  

  $res = $dbh->isWaiting($transferId);
  
Method checks if transfer with transfer id $transferId is in status WAITING. 
If $transferId is not defined method will return undef and report error.
    
=item C<insertLocked>  

  $res = $dbh->insertLocked($date, $user, $lfn, $pfn,$destination,$type);
  
Method inserts new transfer with defined arguments. Before inserting method
locks table TRANSFER and after inserting unlocks table.   
  
=item C<update> 
    
  $res = $dbh->update($updateSet,$where);
  
Method just calls AliEn::Database method C<update>. Method defines table argument
and passes $updateSet and $where arguments to AliEn::Database C<update> method. 

=item C<updateTransfer>     

  $res = $dbh->updateTransfer($id, $set);
  
Method updates transfer with id $id with update set $set. Form of 
$set argument is defined in AliEn::Database C<update> method.  
If job id is not defined method will return undef and report error.
  
=item C<updateStatus> 
  
  $res = $dbh->updateStatus($transferId, $oldStatus, $newStatus);
  
  $res = $dbh->updateStatus($transferId, $oldStatus, $newStatus, $set);
  
Method checkes if transfer with id $transferId and status $oldStatus exists.
If transfer exists method updates transfer's status to $newStatus. If argument
$oldStatus is set to "%" method will ignore old status of transfer $transferId.
Method can update other attributes if argument $set is defined. Form of 
$set argument is defined in AliEn::Database C<update> method.
If arguments $transferId or $oldStatus are not defined method will return undef
and report error.


=item C<assignWaiting>   
  
  $res = $dbh->assignWaiting($transferId);

Method checkes if transfer $transferId is in WAITING state. If transfer is
in WAITING state method will set it's state to ASSIGNED and it's sent attribute
to current time. 
Before doing any operation method locks table TRANSFER and unlocks it at the end.

=item C<set*>     
   
  $res = $dbh->setSE($transferId,$SE);
  
  $res = $dbh->setJdl($transferId,$jdl);
  
  $res = $dbh->setSize($transferId,$size);

Method updates transfer's $transferId attribute. 
If arguments $transferId is not defined method will return undef
and report error.  

=item C<updateExpiredTransfers>   
  
  $res = $dbh->updateExpiredTransfers();

Method updates state of transfers which are older then one day and in state 
ASSIGNED to EXPIRED.  
      
=item C<updateLocalCopyTransfers>   
  
  $res = $dbh->updateLocalCopyTransfers();

Method updates 
Method updates trnsfer's $transferId storage element to $SE. 
If arguments $transferId or $SE are not defined method will return undef
and report error.  
  
=item C<delete> 
    
  $res = $dbh->delete($where);
  
Method just calls AliEn::Database method C<delete>. Method defines table argument
and passes $where argument to AliEn::Database C<update> method. 

=item C<deleteTransfer>     

  $res = $dbh->deleteTransfer($id);
  
Method deletes transfer with id $id.
If transfer id is not defined method will return undef and report error.

=back

=head1 SEE ALSO

AliEn::Database

=cut

1;
