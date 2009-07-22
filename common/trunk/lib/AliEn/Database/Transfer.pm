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

use AliEn::Util;


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

my $tables={ TRANSFERS_DIRECT=>{columns=>{
					  transferId=>" int(11) not null auto_increment primary key", 
					  lfn=>"varchar(250)",
					  priority=>"tinyint(4) default 0",
					  received=>"int(20) default 0",
					  sent=>"int(20) default 0",
					  started=>"int(20) default 0",
					  finished=>"int(20) default 0",
					  expires=> "int(10) default 0",
					  error=>"int(11)",
					  jdl=>"text",
					  #				   transferGroup=>"int(11)",
					  size=>"bigint(11)",
					  status=>"varchar(15)",
					  attempts=>"int(11)",
					  
					  ctime=>"timestamp DEFAULT CURRENT_TIMESTAMP  ON UPDATE CURRENT_TIMESTAMP",
					  protocols=>"varchar(250)",
					  pfn=>"varchar(255)",
					  transferGroup=>"int(11)",
					  user=>"varchar(30)",
					  destination=>"varchar(50) COLLATE latin1_general_ci",
					  options=>"varchar(250)",
					  type=>"varchar(30)",
					  agentid=>"int(11)",
					  reason=>'varchar(255)',
					  protocolid=>'varchar(255)',
				  },
			 id=>"transferId",
				index=>"transferId",
				extra_index=>["INDEX (agentid)", "INDEX(status)"]},

	     ACTIONS=>{columns=>{action=>"char(40) not null primary key",
				 todo=>"int(1) not null default 0",
				 extra=>"varchar(255) default ''",},
		       id=>"action",
		      },
	     PROTOCOLS=>{columns=>{sename=>"varchar(50) COLLATE latin1_general_ci ",
				   protocol=>"varchar(50)",
				   options=>'varchar(255)',
				   max_transfers=>'int(11) default 5',
				   current_transfers=>'int(11) default 0',
				   updated=>'tinyint default 1',
				   deleteprotocol=>'tinyint not null default 0', 
				  },
			 id=>"sename"},
	     AGENT_DIRECT=>{columns=>{entryId=>"int(11) not null auto_increment primary key",
				      requirements=>"text not null",
				      counter=>"int(11) not null default 0",
				      priority=>"tinyint(4) default 0",
				     },
			    id=>"entryId",},

	     TRANSFERMESSAGES=>{columns=> {entryId=>" int(11) not null  auto_increment primary key",
					   transferId =>"int", 
					   message=>"varchar(200)",
					   tag=>"varchar(40)", 
					   timestamp=>"int", },
				id=>"entryId",
			       },

	   };



sub initialize {
  my $self=shift;
  foreach my $table  (keys %$tables) {
    $self->checkTable($table, $tables->{$table}->{id}, $tables->{$table}->{columns}, $tables->{$table}->{index},  $tables->{$table}->{extra_index})
      or $self->{LOGGER}->error("TaskQueue", "Error checking the table $table") and return;
  }
  AliEn::Util::setupApMon($self);
  

  return $self->do("INSERT IGNORE INTO ACTIONS(action) values  ('INSERTING'),('MERGING'), ('FAILED_T')");
}

sub getArchiveTable {
  my $self=shift;
  my $date=gmtime();
  $date=~ /^\S+\s+(\S+)\s+\S+\s+\S+\s+(\S+)/ or print "error getting the year!!\n" and return;
  
  my $name=uc("TRANSFERSARCHIVE_${2}_$1");
  $self->checkTable($name, $tables->{TRANSFERS}->{id}, $tables->{TRANSFERS}->{columns}, $tables->{TRANSFERS}->{index}) or return;
  return $name;
}

sub insertTransferLocked {
  my $self = shift;
  my $info = shift;
  $info->{status} or $info->{status}="INSERTING";

  $self->lock("TRANSFERS_DIRECT");
  my $lastID=0;
  if ($self->insert("TRANSFERS_DIRECT",$info)){
    $self->debug(1,"In insertTransferLocked fetching transferId");
    $lastID = $self->getLastId();
    ($lastID) or  $self->{LOGGER}->error("Transfer","In insertTransferLocked unable to fetch transferId. Unlocking table TRANSFER");
  } else{
    $self->{LOGGER}->error("Transfer","In insertTransferLocked error inserting data. Unlocking table TRANSFER.");
  }
  $self->unlock();
  $lastID or return;
  $self->debug(1,"In insertTransferLocked transfer $lastID successfully inserted");


  $self->sendTransferStatus($lastID, $info->{status}, {destination=>$info->{destination}, user=>$info->{user}, received=>$info->{received}});

  $self->updateActions({todo=>1}, "action='$info->{status}'");

#  $self->updateTransfer($lastID, $info);

  return $lastID;
}

sub assignWaiting{
  my $self = shift;
  my $elementId = shift;
  my $date=time;
  my $done=$self->updateTransfer($elementId, {status=>"ASSIGNED",sent=>$date});


  #And now, let's reduce the number of agents
  $self->do("UPDATE AGENT_DIRECT, TRANSFERS_DIRECT set counter=counter-1 where agentid=entryId and transferid=?", {bind_values=>[$elementId]});
  $self->do("delete from AGENT_DIRECT where counter<1");
  return $done;
}

sub updateExpiredTransfers{
  my $self = shift;
  
  my $yesterday=time;
  $yesterday -= 86400; #24*60*60
  
  $self->debug(1,"In updateExpiredTransfers updating status of expired transfers");
  $self->update({status=>'EXPIRED'},"(status = 'ASSIGNED' or status ='TRANSFERING')  and sent<?", {bind_values=>[$yesterday]});
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
  shift->SUPER::update("TRANSFERS_DIRECT",@_);
}

sub delete{
  shift->SUPER::delete("TRANSFERS",@_);
}


sub updateTransfer{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Transfer","In updateTransfer transfer id is missing")
      and return;
  my $set = shift;

  $self->debug(1,"In updateTransfer updating transfer $id");
  $self->sendTransferStatus($id, $set->{status}, $set);
  my $ok=1;
  if ($set->{status} =~ /^WAITING$/  and $set->{jdl}){
    $self->info("We have to insert an agent!!");
    $set->{agentid}=$self->insertAgent($set->{jdl});
    if (! $set->{agentid}){
      $self->info("Error creating the agentid");
      $set->{status}="FAILED";
      $set->{error}="Error creating an agent";
      $ok=0;
    }
    $self->info("THE AGENT ID for $id is  $set->{agentid}");
  }elsif ($set->{status}=~ /^KILLED$/){
    $self->info("Transfer killed. Shall we reduce the agents??");
  }
  my $where="transferid = ?";
  my @bind=($id);
  if ($set->{oldstatus}){
    push @bind, $set->{oldstatus};
    $where .=" and status=?";
    delete $set->{oldstatus};
  }


  my $done=$self->update($set,$where, {bind_values=>\@bind});

  #If the update didn't change any row, return error
  ($done eq "0E0") and return;
  $ok and return $done;


  return;


}

sub deleteTransfer{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("TaskQueue","In deleteTransfer transfer id is missing")
      and return;

  $self->debug(1,"In deleteTransfer deleting transfer $id");	
  $self->delete("queueId=?", {bind_values=>[$id]});
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
  my $agentid=shift;
  my $se=shift;
  $self->debug(1,"In setSE updating transfers's SE");
  $self->SUPER::update("AGENT", {SE=>$se}, "entryId=?", {bind_values=>[$agentid]});
}

sub getWaitingAgents {
  my $self=shift;
  
  return $self->query("SELECT entryId as transferId,requirements as jdl FROM AGENT_DIRECT ORDER BY PRIORITY DESC");
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
  $self->queryValue("SELECT transferId FROM TRANSFERS_DIRECT WHERE lfn=? AND destination=? AND ".$self->_transferActiveReq(), undef, {bind_values=>[$lfn, $destination]});
}

#sub isWaiting{
#  my $self = shift;
#  my $id = shift
#    or $self->{LOGGER}->error("Transfer","In isWaiting transfer id is missing")
#      and return;
#  
#  $self->debug(1,"In isWaiting checking if transfer $id is waiting");
#  $self->queryValue("SELECT COUNT(*) FROM TRANSFERS WHERE (status='WAITING' OR status='LOCAL COPY' OR status='CLEANING') AND transferid=?", undef, {bind_values=>[$id]});
#}

sub getFields{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Transfer","In getFields transfer id is missing")
		and return;
  my $attr = shift || "*";
  
  $self->debug(1,"In getFields fetching attributes $attr of transfer $id");
  $self->queryRow("SELECT $attr FROM TRANSFERS_DIRECT WHERE transferid=?", undef, {bind_values=>[$id]});
}

sub getField{
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Transfer","In getField transfer id is missing")
      and return;
  my $attr = shift || "*";
  
  $self->debug(1,"In getField fetching attribute $attr of transfer $id");
  $self->queryValue("SELECT $attr FROM TRANSFERS_DIRECT WHERE transferid=?", undef, {bind_values=>[$id]});
}

sub getFieldsEx{
  my $self = shift;
  my $attr = shift || "*";
  my $where = shift || "";
  
  $self->debug(1,"In getFieldsEx fetching attributes $attr with condition $where");
  $self->query("SELECT $attr FROM TRANSFERS_DIRECT $where", undef, @_);
}

sub getFieldEx{
  my $self = shift;
  my $attr = shift || "*";
  my $where = shift || "";

  $self->debug(1,"In getFieldEx fetching attributes $attr with condition $where");
  $self->queryColumn("SELECT $attr FROM TRANSFERS_DIRECT $where", undef, @_);
}


sub getNewTransfers{
  my $self = shift;

  $self->debug(1,"In getNewTransfers fetching attributes transferid,lfn, pfn, destination of transfers in INSERTING state");
  $self->query("SELECT transferid,lfn, destination,options,user FROM TRANSFERS_DIRECT WHERE STATUS='INSERTING'");
}


# send a job's status to MonaLisa
sub sendTransferStatus {
  my $self = shift;
  my ($jobID, $newStatus, $info) = @_;
    
  if($self->{MONITOR}){
    my @params = $newStatus ? ("statusID", AliEn::Util::transferStatusForML($newStatus)) : ();
    foreach ('started', 'finished', 'size', 'destination', 'received', 'SE'){
      push(@params, $_, $info->{$_}) if $info->{$_};
    }

    $self->{MONITOR}->sendParameters("TransferQueue_Transfers_".$self->{CONFIG}->{ORG_NAME}, $jobID, @params);
  }
}

sub getTransfersToMerge{
  my $self=shift;
  $self->lock("ACTIONS");
  my $value=$self->queryValue("SELECT extra from ACTIONS where action='MERGING'");
  $self->SUPER::update("ACTIONS", {extra=>'', todo=>0}, "action='MERGING'");
  $self->unlock();
  $value or return;
  $self->info("We have to investigate transfers $value");
  my @list=split(/,/, $value);
  return \@list;
}

sub getActiveSubTransfers{
  my $self=shift;
  my $id=shift;
  my $info=$self->queryColumn("SELECT transferId from TRANSFERS where transfergroup=? and ". $self->_transferActiveReq(), undef, {bind_values=>[$id]});

  
}

sub _transferActiveReq{
  return "status<>'FAILED' AND status<>'DONE' AND status <>'KILLED' AND status <>'EXPIRED'";
}

sub insertAgent{
  my $self=shift;
  my $text=shift;
  $text=~ s/\s*$//s;
  $text=~ /(Requirements\s*=[^;]*;)/is or $self->info("Error getting the requirements from $text") and return;
  my $req="[ $1 Type = \"transfer\"; ]";
  $self->info( "Inserting a jobagent with '$req'");
  $self->lock("AGENT_DIRECT");
  my $id=$self->queryValue("SELECT entryId from AGENT_DIRECT where requirements=?", undef, {bind_values=>[$req]});
  if (!$id){
    if (!$self->insert("AGENT_DIRECT", {counter=>"1", requirements=>$req})){
      $self->info("Error inserting the new jobagent");
      $self->unlock();
      return;
    }
    $id=$self->getLastId();
  }else{
    $self->do("UPDATE AGENT_DIRECT set counter=counter+1 where entryId=?", {bind_values=>[$id]});
  }
  $self->unlock();

  return $id;
}

sub getVirtualTransfers{
  my $self=shift;
  $self->info("Getting all the transfers that have to be done from the 'no_se'");

  my $info=$self->query("select lfn,transferid,jdl,options from TRANSFERS t, AGENT a where agentid=entryid and a.SE='no_se'");

  return $info;
}


sub findCommonProtocols {
  my  $self=shift;
  my $source=shift;
  my $dest=shift;
  my $p=$self->query("select protocol, A.options sourceopt, B.options targetopt from PROTOCOLS A join PROTOCOLS B using (protocol) where A.sename=? and B.sename=? and A.deleteprotocol=0 and B.deleteprotocol=0", undef, {bind_values=>[$source,$dest]});
  $self->info("Common protocols between $source and $dest: @$p");
  return @$p;
}

sub insertProtocol{
  my $self=shift;

  return $self->insert("PROTOCOLS", @_);
}

sub insertTransferMessage {
  my $self=shift;
  my $jobId=shift;
  my $tag=shift; 
  my $message=shift;
  my $time=time;
  return $self->insert("TRANSFERMESSAGES", {transferId=>$jobId, message=>$message,
					    tag=>$tag,  timestamp=>$time});
}

sub retrieveTransferMessages{
  my $self=shift;
  return $self->query("SELECT transferid,tag, message, entryid from TRANSFERMESSAGES");
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
