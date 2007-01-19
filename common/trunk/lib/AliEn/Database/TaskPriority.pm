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

package AliEn::Database::TaskPriority;

use AliEn::Database;

use strict;

use vars qw(@ISA);
@ISA=("AliEn::Database");

sub preConnect {
  my $self=shift;
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;

  $self->info("Using the default $self->{CONFIG}->{JOB_DATABASE}");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB})
    =split ( m{/}, $self->{CONFIG}->{JOB_DATABASE});

  return 1;
}
sub initialize {
  my $self     = shift;

  $self->{PRIORITYTABLE}="PRIORITY";

  $self->SUPER::initialize() or return;

  $self->{SKIP_CHECK_TABLES} and return 1;

  $self->checkPriorityTable()
      or $self->{LOGGER}->error( "TaskPriority", "In initialize altering tables failed for PRIORITY" )
	  and return;

  return 1;
}


sub setPriorityTable{
  my $self = shift;
  $self->{PRIORITYTABLE} = (shift or "PRIORITY");
}

sub checkPriorityTable{
  my $self = shift;
  $self->{PRIORITYTABLE} = (shift or "PRIORITY");

  my %columns = (	
		 user=>"varchar(64) not null",
		 priority=>"float",
		 maxparallelJobs=>"int",
		 nominalparallelJobs=>"int",
		 computedpriority=>"float"
		);

  $self->checkTable($self->{PRIORITYTABLE}, "user", \%columns, 'user');
  
}

sub checkPriorityValue() {
  my $self = shift;
  my $user = shift or $self->{LOGGER}->error("TaskPriority","no username provided in checkPriorityValue");
  $self->debug(1,"Checking if the user $user exists");

  my $exists = $self->getFieldFromPriority("$user", "count(*)");
  if ($exists) {
    $self->debug(1, "$user entry for priority exists!" );
  } else {
    $self->debug(1, "$user entry for priority does not exist!" );
    my $set = {};
    $set->{'user'} = "$user";
    $set->{'priority'} = "1.0";
    $set->{'maxparallelJobs'} = 20;
    $set->{'nominalparallelJobs'} = 10;
    $set->{'computedpriority'} = 1;
    $self->insertPrioritySet($user,$set);
  }
}


sub insertPriority{
  my $self = shift;
  $self->insert("$self->{PRIORITYTABLE}",@_);
}

sub updatePriority{
    my $self = shift;
    $self->update("$self->{PRIORITYTABLE}",@_);
}

sub deletePriority{
    my $self = shift;
    $self->delete("$self->{PRIORITYTABLE}",@_);
}

sub insertPrioritySet{
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("TaskPriority","In insertPrioritySet user is missing")
      and return;
  my $set =shift;
  
  $self->debug(1,"In insertPrioritySet user is missing");
  $self->insert($self->{PRIORITYTABLE}, $set);
}

sub updatePrioritySet{
	my $self = shift;
	my $user = shift
		or $self->{LOGGER}->error("TaskPriority","In updatePrioritySet user is missing")
		and return;
	my $set =shift;
	
	$self->debug(1,"In updatePrioritySet user is missing");
	$self->updatePriority($set,"user='$user'");
}

sub deletePrioritySet{
	my $self = shift;
	my $user = shift
		or $self->{LOGGER}->error("TaskPriority","In deletePrioritySet user is missing")
		and return;
	
	$self->debug(1,"In deletePrioritySet deleting user $user");	
	$self->deleteFromPriority("user='$user'");
}

sub getFieldFromPriority {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("TaskPriority","In getFieldFromPriority user is missing")
      and return;
  my $attr = shift || "*";
  
  $self->debug(1,"In getFieldFromPriority fetching attribute $attr of user $user");
  $self->queryValue("SELECT $attr FROM $self->{PRIORITYTABLE} WHERE user='$user'");
}

sub getFieldsFromPriority {
	my $self = shift;
	my $user = shift
		or $self->{LOGGER}->error("TaskPriority","In getFieldsFromPriority user is missing")
		and return;
	my $attr = shift || "*";
	
	$self->debug(1,"In getFieldsFromPriority fetching attributes $attr of user $user");
	$self->queryRow("SELECT $attr FROM $self->{PRIORITYTABLE} WHERE user='$user'");
}

sub getFieldsFromPriorityEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$self->debug(1,"In getFieldsFromPriorityEx fetching attributes $attr with condition $addsql from table $self->{PRIORITYTABLE}");
	$self->query("SELECT $attr FROM $self->{PRIORITYTABLE} $addsql");
}

sub getFieldFromPriorityEx {
	my $self = shift;
	my $attr = shift || "*";
	my $addsql = shift || "";
	
	$self->debug(1,"In getFieldFromPriorityEx fetching attributes $attr with condition $addsql from table $self->{PRIORITYTABLE}");
	$self->queryColumn("SELECT $attr FROM $self->{PRIORITYTABLE} $addsql");
}
