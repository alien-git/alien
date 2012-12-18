#/**************************************************************************
# * Copyright(c) 2001-2003, ALICE Experiment at CERN, All rights reserved. *
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

package AliEn::Database::Proof;

use AliEn::Database;

use strict;

use vars qw(@ISA);
@ISA=("AliEn::Database");

###		sessions

sub createSessionsTable{
    my $self = shift;

	$self->debug(1,"In createSessionsTable creating table sessions");

	$self->createTable("sessions","(sessionId INT, muxPid INT, user varchar(255), muxPort INT, assigntime INT, validitytime INT, expired int)",1);
}

sub getLastSessionId{
	my $self = shift;

	$self->debug(1,"In getLastSessionId fetching get last session ID from sessions");

	$self->queryValue("SELECT MAX(sessionId) from sessions");
}

sub insertIntoSessions{
        my $self = shift;
	$self->insert("sessions",@_);
}

sub updateSessions{
        my $self = shift;
	$self->update("sessions",@_);
}

sub getFieldsFromSessionsEx{
	my $self = shift;
	my $attr = shift || "*";
	my $where = shift || "";

	$self->debug(1,"In getFieldsFromSessionsEx fetching attributes $attr with condition $where");

	$self->query("SELECT $attr FROM sessions $where", @_);
}

sub getFieldFromSessionsEx{
	my $self = shift;
	my $attr = shift || "*";
	my $where = shift || "";

	$self->debug(1,"In getFieldFromSessionsEx fetching attributes $attr with condition $where");

	$self->queryColumn("SELECT $attr FROM sessions $where", @_);
}


###		reserved

sub createReservedTable{
	my $self = shift;

	$self->debug(1,"In createReservedTable creating table reserved");

	$self->createTable("reserved","(sessionId INT, site varchar(255), nassigned INT,  assigntime INT, validitytime INT, expired int)",1);
}

sub insertIntoReserved{
	my $self = shift;
	$self->insert("reserved",@_);
}

sub updateReserved{
        my $self = shift;
	$self->update("reserved",@_);
}

sub getFieldsFromReservedEx{
	my $self = shift;
	my $attr = shift || "*";
	my $where = shift || "";

	$self->debug(1,"In getFieldsFromReservedEx fetching attributes $attr with condition $where");

	$self->query("SELECT $attr FROM reserved $where");
}

sub getFieldFromReservedEx{
	my $self = shift;
	my $attr = shift || "*";
	my $where = shift || "";

	$self->debug(1,"In getFieldFromReservedEx fetching attributes $attr with condition $where");

	$self->queryColumn("SELECT $attr FROM reserved $where");;
}

sub getNumberPrebookedProofs{
	my $self = shift;
	my $site = shift
		or $self->{LOGGER}->error("Proof","In getNumberPrebookedProofs site is missing")
		and return;

	$self->debug(1,"In getNumberPrebookedPfs fetching number of prebooked proofs for site $site");

	$self->queryValue("SELECT SUM(nassigned) FROM reserved WHERE site='$site' and expired='0' and nassigned>0");
}

###		p tables

sub createProofTable{
	my $self = shift;
	my $sessionId = shift
		or $self->{LOGGER}->error("Proof","In createProofTable proof table id is missing")
		and return;

	$self->debug(1,"In createProofTable creating table reserved");

	$self->createTable("P$sessionId","(site varchar(255), mss varchar(255), muxhost varchar(255), muxport varchar(255), nrequested INT, nassigned INT)",1);
}

sub getAllFromProof{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Proof","In getAllFromProof proof table id is missing")
		and return;
	my $attr = shift || "*";

	$self->debug(1,"In getAllFromProof fetching attributes $attr of all entries");

	$self->getFieldsFromProofEx($id, $attr);
}

sub getFieldsFromProofBySite{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Proof","In getFieldsFromProofBySite proof table id is missing")
		and return;
	my $site = shift
		or $self->{LOGGER}->error("Proof","In getFieldsFromProofBySite site is missing")
		and return;
	my $attr = shift || "*";

	$self->debug(1,"In getFieldsFromProofBySite fetching attributes $attr of entries by site $site");

	$self->query("SELECT $attr FROM P$id WHERE site = '$site'");
}

sub getFieldsFromProofByMss{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Proof","In getFieldsFromProofByMss proof table id is missing")
		and return;
	my $mss = shift
		or $self->{LOGGER}->error("Proof","In getFieldsFromProofByMss mss is missing")
		and return;
	my $attr = shift || "*";

	$self->debug(1,"In getFieldsFromProofByMss fetching attributes $attr of entries by mss $mss");

	$self->query("SELECT $attr FROM P$id WHERE mss = '$mss'");
}

sub getFieldsFromProofEx{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Proof","In getFieldsFromProofEx proof table id is missing")
		and return;
	my $attr = shift || "*";
	my $where = shift || "";

	$self->debug(1,"In getFieldsFromProofEx fetching attributes $attr with condition $where");

	$self->query("SELECT $attr FROM P$id $where");
}

sub getFieldFromProofEx{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Proof","In getFieldFromProofEx proof table id is missing")
		and return;
	my $attr = shift || "*";
	my $where = shift || "";

	$self->debug(1,"In getFieldFromProofEx fetching attributes $attr with condition $where");

	$self->queryColumn("SELECT $attr FROM P$id $where");
}

sub insertIntoProof{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Proof","In insertIntoProof proof table id is missing")
		and return;
	$self->insert("P$id",@_);
}

sub updateProof{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Proof","In updateProof proof table id is missing")
		and return;

	$self->update("P$id",@_);
}

sub checkProofTable{
	my $self = shift;
	my $id = shift
		or $self->{LOGGER}->error("Proof","In checkProofTable proof table id is missing")
		and return;

	$self->debug(1,"In checkProofTable checking if table P$id already exists");

	if ($self->existsTable("P$id")){
		$self->{LOGGER}->warning("Proof","Proof table P$id already exists. Cleaning table P$id...");
		$self->delete("P$id","1");
	}else{
		$self->debug(1,"In checkProofTable table P$id doesnt exist. Creating table P$id...");
		$self->createProofTable($id);
	}
}



=head1 NAME

AliEn::Database::Proof

=head1 DESCRIPTION

The AliEn::Database::Proof module extends AliEn::Database module. Module contains
methods specific for tables sessions, reserved and P tables.

=head1 SYNOPSIS

  use AliEn::Database::Proof;

  my $dbh = AliEn::Database::Proof->new($dbOptions);

  $arrRef = $dbh->getFieldFromSessionsEx($attr, $addSql);
  $arrRef = $dbh->getFieldFromReservedEx($attr, $addSql);
  $arrRef = $dbh->getFieldFromProofEx($id, $attr, $addSql);
  
  $arrRef = $dbh->getFieldsFromSessionsEx($attr, $addSql);
  $arrRef = $dbh->getFieldsFromReservedEx($attr, $addSql);
  $arrRef = $dbh->getFieldsFromProofEx($id, $attr, $addSql);
        
  $res = $dbh->getLastSessionId();
  $res = $dbh->getNumberPrebookedProofs($site);
  $arrRef = $dbh->getAllFromProof($id, $attr);
  $arrRef = $dbh->getFieldsFromProofBySite($id, $site, $attr);
  
  $res = $dbh->insertIntoSessions($insertSet);
  $res = $dbh->insertIntoReserved($insertSet);
  $res = $dbh->insertIntoProof($id, $insertSet);
  
  $res = $dbh->updateSessions($updateSet, $where);
  $res = $dbh->updateReserved($updateSet, $where);
  $res = $dbh->updateProof($id, $updateSet, $where);
  
  $res = $dbh->checkProofTable($id);
    
  $res = $dbh->createSessionsTable();
  $res = $dbh->createReservedTable();
  $res = $dbh->createProofTable($id);

=cut

=head1 METHODS

=over

=item C<new>

  $dbh = AliEn::Database::Proof->new( $attr );

  $dbh = AliEn::Database::Proof->new( $attr, $attrDBI );

Creates new AliEn::Database::IS instance. Arguments are passed to AliEn::Database
method new. For details about arguments see AliEn::Database method C<new>.

=item C<getFieldFrom*Ex>

  $arrRef = $dbh->getFieldFromSessionsEx($attr, $addSql);
  
  $arrRef = $dbh->getFieldFromReservedEx($attr, $addSql);
  
  $arrRef = $dbh->getFieldFromProofEx($id, $attr, $addSql);

Method fetches value of attribute $attr for tuples with condition $addSql.
Argument $addSql contains anything that comes after FROM part of SELECT statement.
Method returns reference to array which contains values of attribute $attr.
If $addSql condition is not defined method will return all tuples.
Method calls AliEn::Database metod queryColumn.

For C<getFieldFromProofEx>: if $id is not defined method will return undef and
report error.

=item C<getFieldsFrom*Ex>

  $arrRef = $dbh->getFieldsFromSessionsEx($attr, $addSql);
  
  $arrRef = $dbh->getFieldsFromReservedEx($attr, $addSql);
  
  $arrRef = $dbh->getFieldsFromProofEx($id, $attr, $addSql);

Method fetches set of attributes $attr for tuples with with condition $addSql.
Argument $addSql contains anything that comes after FROM part of SELECT statement.
If set of attributes is not defined method returns values of all attributes.
If $addSql condition is not defined method will return all tuples.
Method calls AliEn::Database metod query.

For C<getFieldsFromProofEx>: if $id is not defined method will return undef and
report error. 
  
=item C<getLastSessionId> 

  $res = $dbh->getLastSessionId();
  
Method retrieves last sessionId (largest value of attriute sessionId) from table
sessions.
  
=item C<getNumberPrebookedProofs> 

  $res = $dbh->getNumberPrebookedProofs($site);

Method retrieves sum of values of attribute nassigned for tuples from table
reserved with site $site and and which are not expired. 
If argument $site is not defined method will return undef and report error.
  
=item C<getAllFromProof> 

  $arrRef = $dbh->getAllFromProof($id, $attr);
  
Method retrieves set of attributes $attr for all tuples from table P$id. If argument
$attr is not defined method will return all attributes. If $id is not defined
method will return undef and report error.
  
=item C<getFieldsFromProofBySite> 

  $arrRef = $dbh->getFieldsFromProofBySite($id, $site, $attr);
  
Method retrieves set of attributes $attr for tuples with site $site from table P$id. 
If argument $attr is not defined method will return all attributes. 
If $id or $site are not defined method will return undef and report error.
  
=item C<insertInto*> 

  $res = $dbh->insertIntoSessions($insertSet);
  
  $res = $dbh->insertIntoReserved($insertSet);
  
  $res = $dbh->insertIntoProof($id, $insertSet);
  
Method just calls AliEn::Database method C<insert>. Method defines table argument
and passes $insertSet and $where arguments to AliEn::Database C<insert> method. 

For C<insertIntoProof>: if $id is not defined method will return undef and
report error. 

=item C<update*> 

  $res = $dbh->updateSessions($updateSet, $where);
  
  $res = $dbh->updateReserved($updateSet, $where);
  
  $res = $dbh->updateProof($id, $updateSet, $where);

Method just calls AliEn::Database method C<update>. Method defines table argument
and passes $updateSet and $where arguments to AliEn::Database C<update> method. 

For C<updateProof>: if $id is not defined method will return undef and
report error. 

=item C<checkProofTable> 

  $res = $dbh->checkProofTable($id);
  
Method checks if table P$id already exists. If table exists method will report
existence of table and delete all information in it. If not method will call method
C<createProofTable>.

=item C<createSessionsTable>, C<createReservedTable>, C<createProofTable> 

  $res = $dbh->createSessionsTable();
  
  $res = $dbh->createReservedTable();
  
  $res = $dbh->createProofTable($id);

Methods for creating tables sessions, reserved and 'P$id'.

For C<createProofTable>: if $id is not defined method will return undef and
report error. 
  
=back

=head1 SEE ALSO

AliEn::Database

=cut

1;
