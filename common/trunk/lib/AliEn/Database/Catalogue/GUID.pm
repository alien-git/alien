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
package AliEn::Database::Catalogue::GUID;

use AliEn::Database::Catalogue::Shared;
use strict;
use AliEn::SOAP;
use AliEn::GUID;

=head1 NAME

AliEn::Database::GUID - database wrapper for AliEn catalogue

=head1 DESCRIPTION

This module interacts with a database of the AliEn Catalogue. The AliEn Catalogue can be distributed among several databases, each one with a different layout. In this basic layout, there can be several tables containing the entries of the catalogue. 

=cut

use vars qw(@ISA $DEBUG);

#This array is going to contain all the connections of a given catalogue
push @ISA, qw(AliEn::Database::Catalogue::Shared);
$DEBUG=0;

=head1 SYNOPSIS

  use AliEn::Database::Catalogue;

  my $catalogue=AliEn::Database::Catalogue->new() or exit;


=head1 METHODS

=over

=cut


=item C<createCatalogueTables>

This methods creates the database schema in an empty database. The tables that this implemetation have are:
HOSTS, 

=cut


#
# Checking the consistency of the database structure
sub createCatalogueTables {
  my $self = shift;
  my $db   =shift;
  if ($db){
    $self->debug(1,"We are going to do the tables in another database!!!");
  } else {
    $db=$self;
  }

  $DEBUG and $self->debug(2,"In createCatalogueTables creating all tables...");

  foreach ("SE") {
    my $method="check".$_."Table";
    $db->$method() or 
      $self->{LOGGER}->error("Catalogue", "Error checking the $_ table") and
	return;
  }

  my %tables=(HOSTS=>["hostIndex", {hostIndex=>"serial primary key",
				    address=>"char(50)", 
				    db=>"char(40)",
				    driver=>"char(10)", 
				    organisation=>"char(11)",},"hostIndex"],
	      ACL=>["entryId", 
		    {entryId=>"int(11) NOT NULL auto_increment primary key", 
		     owner=>"char(10) NOT NULL",
		     perm=>"char(4) NOT NULL",
		     aclId=>"int(11) NOT NULL",}, 'entryId'],
	      GROUPS=>["Username", {Username=>"char(15) NOT NULL", 
				    Groupname=>"char (85)",
				    PrimaryGroup=>"int(1)",}, 'Username'],
	      GUIDINDEX=>["indexId", {indexId=>"int(11) NOT NULL auto_increment primary key",
				      guidTime=>"char(16)", 
				      hostIndex=>"int(11)",
				      tableName=>"int(11)",}, 
			  'indexId', ['UNIQUE INDEX (guidTime)']],
	      TODELETE=>["entryId",  {entryId=>"int(11) NOT NULL auto_increment primary key", 
				      pfn=>"varchar(255)",
				      seNumber=>"int(11) not null",
				      guid=>"binary(16)"}],
	      GL_STATS=>["tableNumber", {
				     tableNumber=>"int(11) NOT NULL",
				     seNumber=>"int(11) NOT NULL",
				     seNumFiles=> "bigint(20)", 
				     seUsedSpace=>"bigint(20)",
				    },undef,['UNIQUE INDEX(tableNumber,seNumber)']],
	      GL_ACTIONS=>["tableNumber", {tableNumber=>"int(11) NOT NULL",
					   action=>"char(40) not null", 
					   time=>"timestamp default current_timestamp",
					   extra=>"varchar(255)",}
			   , undef, ['UNIQUE INDEX(tableNumber,action)']],);

	     
  foreach my $table (keys %tables){
    $self->info("Checking table $table");
    $db->checkTable($table, @{$tables{$table}})
      or return;
  }

  $self->checkGUIDTable("0", $db) or return;

  $self->info("Let's create the functions");
  $db->createGUIDFunctions;

  $DEBUG and $self->debug(2,"In createCatalogueTables creation of tables finished.");
  

  1;
}
sub checkGUIDTable {
  my $self =shift;
  my $table =shift;
  defined $table or $self->info( "Error: we didn't get the table number to check") and return;
  my $db=shift || $self;
  
  $table =~ /^\d+$/ and $table="G${table}L";
  
  my %columns = (guidId=>"int(11) NOT NULL auto_increment primary key", 
		 ctime=>"timestamp" ,
		 expiretime=>"datetime",
		 size=>"bigint default 0 not null ",
		 seStringlist=>"varchar(255) default ',' not null ",
		 seAutoStringlist=>"varchar(255) default ',' not null ",
		 aclId=>"int(11)",
		 perm=>"char(3)",
		 guid=>"binary(16)",
		 md5=>"varchar(32)",
		 ref=>"int(11) default 0",
		 owner=>"varchar(20)",
		 gowner=>"varchar(20)",
		 type=>"char(1)",
		);

   $db->checkTable(${table}, "guidId", \%columns, 'guidId', ['UNIQUE INDEX (guid)', 'INDEX(seStringlist)', 'INDEX(ctime)'],) or return;
  
  %columns= (pfn=>'varchar(255)',
	     guidId=>"int(11) NOT NULL",
	     seNumber=>"int(11) NOT NULL",);
  $db->checkTable("${table}_PFN", "guidId", \%columns, undef, ['INDEX guid_ind (guidId)', "FOREIGN KEY (guidId) REFERENCES $table(guidId) ON DELETE CASCADE","FOREIGN KEY (seNumber) REFERENCES SE(seNumber) on DELETE CASCADE"],) or return;


  $db->checkTable("${table}_REF", "guidId", {guidId=>"int(11) NOT NULL",
					     lfnRef=>"varchar(20) NOT NULL"},
		  '', ['INDEX guidId(guidId)', 'INDEX lfnRef(lfnRef)', "FOREIGN KEY (guidId) REFERENCES $table(guidId) ON DELETE CASCADE"]) or return;

  $db->checkTable("${table}_QUOTA", "user", {user=>"varchar(64) NOT NULL", nbFiles=>"int(11) NOT NULL", totalSize=>"bigint(20) NOT NULL"}, undef, ['INDEX user_ind (user)'],) or return;

  $db->optimizeTable($table);
  $db->optimizeTable("${table}_PFN");

  my $index=$table;
  $index=~ s/^G(.*)L$/$1/;
  #$db->do("INSERT IGNORE INTO GL_ACTIONS(tableNumber,action)  values  (?,'SE')", {bind_values=>[$index, $index]}); 


  return 1;

}
##############################################################################
##############################################################################

=item C<getSEListFromGUID($guid)>

Retrieves the list of SE that have a copy of the lfn 

=cut


sub getSEList{
  my $self=shift;

  my $guid=shift;
  my $seStringlist=shift;
  if (not $seStringlist) {
    $seStringlist = $self->getAllInfoFromGUID( {retrieve=>"replace(concat(seStringlist,seAutoStringList),',,',',')",
						method=>"queryValue",},$guid
					     ) or return;
  }
  $DEBUG and $self->debug(1,"Getting the name of the se $seStringlist");
  return $self->queryColumn("SELECT seName from SE where '$seStringlist' like concat('%,',concat(seNumber,',%')) ");

}

sub _sortGUIDbyDB {
  my $self=shift;
  my $entries={};
  foreach my $entry (@_){
    my $guid=$entry->{guid};
    $guid or $self->info("Error missing guid in insertGUID") and return;
    $self->debug(2,"Inserting a new guid in the catalogue");
    my ($db, $table)=$self->selectDatabaseFromGUID($guid) or return;
    $entries->{$db} or $entries->{$db}={tables=>{}, db=>$db};
    my @list=$entry;
    $entries->{$db}->{tables}->{$table} and 
      push @list, @{$entries->{$db}->{tables}->{$table}};
    $entries->{$db}->{tables}->{$table}=\@list;
  }
  return $entries;
}

sub _prepareEntries {
  my $self=shift;
  my (@pfns, @new);

  foreach my $origEntry(@_){
    my $entry={};
    foreach  my $field (keys %$origEntry){
      $field =~ /^(lfn)|(pfn)|(se)|(pfns)$/ and next;
      $entry->{$field}="'$origEntry->{$field}'";
    }
    $entry->{guid}="string2binary($entry->{guid})";
    if ($origEntry->{se}){
      my @list=();
      $origEntry->{pfns} and push @list, @{$origEntry->{pfns}};
      $origEntry->{se} =~ s /^,//;
      $origEntry->{se} =~ s /,$//;
      $origEntry->{se} =~ s /,+/,/;
      my @ses=split(/,/, $origEntry->{se});
      foreach my $se (@ses) {
	push @list, {seName=>$se, pfn=>$origEntry->{pfn}};
      }
      $origEntry->{pfns}=\@list;
    } elsif  ($origEntry->{pfn}){
      my @list=({seName=>'no_se', pfn=>$origEntry->{pfn}});
      $origEntry->{pfns} and push @list, @{$origEntry->{pfns}};
      $origEntry->{pfns}=\@list;
    }
    if ($origEntry->{pfns}){
      my $defaultNumber=$self->getSENumber("no_se");
      foreach my $item (@{$origEntry->{pfns}}){
	my $seNumber=$defaultNumber;
	if ($item->{seName}) {
	  $seNumber=$self->getSENumber($item->{seName}) or 
	    $self->info( "Error getting the number of se '$item->{seName}'")
	      and return;
	}
	$self->debug(1, "Checking the guid consistency ( in $seNumber)");
	my $column="seAutoStringList";
	$item->{pfn} and $column="seStringList";
	if ($entry->{$column}) {
	  $entry->{$column}=~ s/^'(.*)'$/$1/;
	}else{
	  $entry->{$column}=',';
	}
	if ($item->{pfn}){
	  push @pfns, {pfn=>$item->{pfn}, se=>$seNumber, guid=>$origEntry->{guid}};
	}
	$entry->{$column}.="$seNumber,"; 
      }
      $entry->{seAutoStringList} and 
	$entry->{seAutoStringList}="'$entry->{seAutoStringList}'";
      $entry->{seStringList} and 
	$entry->{seStringList}="'$entry->{seStringList}'";
    }
    #And now, let's set the defaults
    $entry->{perm} or $entry->{perm}="755";
    $entry->{owner} or $entry->{owner}=$self->{VIRTUAL_ROLE};
    $entry->{gowner} or $entry->{gowner}=$self->{VIRTUAL_ROLE};
    push @new, $entry;
  }
  return \@pfns, \@new;
}
sub insertGUID {
  my $self=shift;
  my $options=shift;
  @_ or $self->info("Error not enough arguments in insertGUID") and return;

  #First let's split the entries according to where they are supposed to be
  my $entries=$self->_sortGUIDbyDB(@_) or return;
  my $error=0;
  #Ok, let's go and insert the things
  my @done;
  my $seNumbers={};
  $self->debug(1, "Ready to do the inserts");
  my $multiInsertOpt={noquotes=>1};
  $options=~ /i/ and $multiInsertOpt->{ignore}=1;
  foreach my $db (keys %$entries){
    my $obj=$entries->{$db}->{db};
    foreach my $table (keys %{$entries->{$db}->{tables}}){
      my @entries=@{$entries->{$db}->{tables}->{$table}};
      my ($pfnRef, $guidRef)=$self->_prepareEntries(@entries)
	or return;
      $self->debug(1, "Ready to insert the info");

      if (! $obj->multiinsert($table, $guidRef,$multiInsertOpt)){
	$error=1;
	$self->info("There was a problem with @entries");
	last;
      }else {
	push @done, {db=>$obj, table=>$table, entries=>\@entries};
      }
      if ($pfnRef and @$pfnRef){
	$self->debug(1, "And now insert the pfn");
	foreach (@$pfnRef) {
	  if (!$obj->do("insert into ${table}_PFN (seNumber,guidId,pfn) select ?, guidId, ? from $table where guid=string2binary(?)",
			{bind_values=>[$_->{se}, $_->{pfn}, $_->{guid}]} )){
	    $self->info("Error inserting the pfns $_!!");
	    $error=1;
	    last;
	  }
	}
	$error and last;
      }
    }
    $error and last;
  }
  if ($error){
    $self->info("Let's undo everything that we have done");
    foreach (@done){
      my ($db, $table)=($_->{db}, $_->{table});
      $self->info("WE SHOULD DELETE FROM $db $table");
    }
    return;
  }
  $self->debug(1,"All the guids have been registered");
  return 1;
}

sub getAllInfoFromGUID{
  my $self=shift;
  my $options=shift;
  my $guid=shift or
    $self->info("Error missing the guid in getAllInfoFromGUID") and return;

  $options->{retrieve} and $options->{retrieve} = $options->{retrieve}.',binary2string(guid) as guid'; 
  my $retrieve=$options->{retrieve} || '*,binary2string(guid) as guid';
  my $method=$options->{method} || "queryRow";
  
  my ($db, $table)=$self->selectDatabaseFromGUID($guid) or return;

  $self->debug(2,"Looking into the table  $table");

  my $info=$db->$method("select $retrieve from $table where guid=string2binary(?)",undef, {bind_values=>[$guid]});

  if ($options->{return} ){
    $info->{db}=$db; $info->{table}=$table;
  }


  $options->{pfn} or return $info;
  $DEBUG and $self->debug(1,"Let's get also the pfn");
  my $extraTable=", $table g where p.guidId=g.guidId and ";
  my $where="guid=string2binary(?)";
  my @bind=($guid, $guid);
  if ($info->{guidId}){
    $extraTable=" where ";
    $where=" guidId=?";
    @bind=($info->{guidId}, $info->{guidId});
  }
 my $fullQuery="select seName, pfn from ${table}_PFN p, SE$extraTable $where and p.seNumber=SE.seNumber union select seName, '' as pfn from $table g, SE where $where and seAutoStringlist like concat('%,', concat(seNumber , ',%'))";
  my $pfn=$db->query($fullQuery, undef ,{bind_values=>\@bind})
    or $self->info("Error doing the query '$fullQuery'") and return;
  $info->{pfn}=$pfn;

  return $info
}
  

sub selectDatabaseFromGUID {
  my $self=shift;
  my $guid=shift;
  my $info=$self->getIndexHostFromGUID($guid)
    or return;
  return $self->reconnectToIndex( $info->{hostIndex}, $info->{tableName})
}


sub getIndexHostFromGUID {
  my $self=shift;
  my $guid=shift || "";
  $self->debug(1, "Let's find the database that holds the guid '$guid'");
  my $query = "SELECT hostIndex, tableName from GUIDINDEX where guidTime<string2date(?) order by guidTime desc ";
  $query = $self->paginate($query,1,0);
  my $entry=$self->queryRow($query, undef, {bind_values=>[$guid]})
    or $self->info("Error doing the query  for the guid '$guid'") and return;
  $entry->{hostIndex} or $self->info("Error getting the index for the guid '$guid'") and return;;
  $self->debug(1, "It should be in $entry->{hostIndex} and $entry->{tableName}");
  defined $entry->{tableName} and $entry->{tableName}="G$entry->{tableName}L";
  return $entry
}


sub addSEtoGUID {
  my $self=shift;
  my $guid=shift;
  my $se=shift;
  my $options=shift || {};
  ($guid and $se)
    or $self->info("Error not enough arguments in addSEtoGUID") 
      and return;
  my $column="seAutoStringlist";
  $options->{pfn} and $column='seStringList';
  my $seNumber=$self->getSENumber($se,{existing=>1})
    or $self->info("Error getting the se number") and return;

  my $table=$options->{table};
  my $db=$self;
  if (! $table){
    ($db, $table)=$self->selectDatabaseFromGUID($guid) or return;
  }
  my $update="concat($column, '$seNumber,')";
  $options->{remove} and $update="replace($column, ',$seNumber,',',')";
  my $done=$db->do("UPDATE $table set $column=$update where guid=string2binary('$guid')") or return;
  if ($done eq "0E0"){
    $self->info("The GUID '$guid' doesn't exist in the catalogue");
    return;
  }

  return $seNumber;
}
sub insertMirrorToGUID {
  my $self=shift;
  my $guid=shift;
  my $se=shift;
  my $pfn=shift;
  my $md5=shift;

  my $info=$self->checkPermission("w", $guid, 'md5,db') or return;

  my $db=$info->{db};
  if ($md5){
    if ($info->{md5} ne $md5) {
      $self->info("The md5 of the file in the database ('$info->{md5}') does not match the one of the file ('$md5')");
      return;
    }
  }
  my $seNumber=$db->addSEtoGUID($guid, $se,{table=>$info->{table},pfn=>$pfn}) 
    or $self->info("Failed to add the SE/PFN ($se/$pfn) to the GUID ($guid) as a mirror.") and return;

  if ($pfn) {
    if (! $db->insert("$info->{table}_PFN",{guidId=>$info->{guidId},
					    pfn=>$pfn, seNumber=>$seNumber})){
      $db->removeSEfromGUID($guid, $se, {table=>$info->{table}});
      $self->info("It wasn't possible to add the SE/PFN ($se/$pfn) to the GUID ($guid) as a mirror.");
      return;
    }
  }
  return  1;
}


sub removeSEfromGUID{
  my $self=shift;
  my ($guid, $se, $options)=(shift, shift, shift ||{});
  $options->{remove}=1;
  return $self->addSEtoGUID($guid, $se, $options, @_);
}

sub getListPFN{
  my $self=shift;
  my $guid=shift;
  my ($db, $table)=$self->selectDatabaseFromGUID($guid) or return;

  my $guidId=$db->queryValue("SELECT guidId from $table where guid=string2binary(?)", undef, {bind_values=>[$guid]})
    or $self->info("Error getting the guidId from '$guid'") and return;
  my $done=$db->query("SELECT * from ${table}_PFN where guidId=?",undef, {bind_values=>[$guidId]}) or $self->info("Error doing the query") and return;

  return $done;


}

sub getAllReplicatedData{
  my $self=shift;

  my $guid = $self->query("SELECT * from GUIDINDEX");
  defined $guid
    or $self->info("Error: not possible to get all the guid indexes")
      and return;

  return {guidindex=>$guid};
}

sub setAllReplicatedData {
  my $self=shift;


  return 1;
}


sub increaseReferences {
  my $self=shift;
  my $options=shift;
  my $done=0;
  foreach my $entry (@_){
    my $guid=$entry->{guid};
    $self->debug(2, "Making sure that the guid $guid exists (and that the user has privileges"); 
    $self->checkPermission("r", $guid) or return;
    $done++;
  }
  return $done;
}


# Checks if the current user has permission to do an action on a specific guid
# The operation can be 'rwx'
#
# Possible options:
#   empty: In case the guid doesn't exist, return the table where is supposed to be
#

sub checkPermission{
  my $self=shift;
  my $op=shift;
  my $guid=shift;
  my $retrievemore=(shift || 0);
  my $empty=(shift || 0);

  my $retrieve='guidId,perm,owner,gowner,'.$self->reservedWord("size");
  my $returndb=0;
  $retrievemore =~ /db/ 
    and $returndb=1
    and $retrievemore =~ s/db//
    and $retrievemore =~ s/,,/,/;
  $retrievemore =~ s/^,//;
  $retrievemore =~ s/,$//;
  $retrievemore and $retrieve.=",".$retrievemore;
  my $info=0;
  if($returndb) {
    $info=$self->getAllInfoFromGUID({retrieve=>$retrieve,
				     return=>"db"}, $guid);
  } else {
    $info=$self->getAllInfoFromGUID({retrieve=>$retrieve}, $guid);
  }
  if (!($info and $info->{guidId})){
    $empty and return $info;
    $self->info("Error the guid '$guid' is not in the catalogue");
    return;
  }

  $self->debug(2, "Checking if the user $self->{VIRTUAL_ROLE} has $op rights to the guid");
  $self->{VIRTUAL_ROLE} =~ /^admin(ssl)?$/ and return $info;
  my $permInt=2;
  if  ($self->{VIRTUAL_ROLE} eq $info->{owner}){
    $permInt=0;
  } else {
    ($self->checkUserGroup($self->{VIRTUAL_ROLE}, $info->{gowner})) and $permInt=1;
  }

  my $subperm=substr( $info->{perm}, $permInt, 1 );

  $self->debug(3,"CHECKING $subperm");
  my $action="access";
  if ($op eq 'r'){
    $subperm>3 and return $info;
  }elsif ( $op eq 'w' ) {
    ($subperm%4)>1 and return $info;
    $action="modify";
  } elsif ( $op eq 'x' ) {
    ($subperm%2) and return $info;
    $action="execute";
  }

  $self->info("You ($self->{VIRTUAL_ROLE}) don't have permission to $action that guid");
  return;
}

sub updateOrInsertGUID{
  my $self=shift;
  my $guid=shift;
  my $update=shift;
  my $options=shift;
  
  my $newUp={};
  foreach (keys %$update){
    $newUp->{$_}=$update->{$_};
  }
  if ($newUp->{guid}){
    $self->info("We are changing to another guid...");
    $guid=$newUp->{guid};
  }

  my $info=$self->checkPermission('w', $guid, "db", 1) or return;
  $self->debug(1,"The checkpermission of the guid worked!!!");

  if (! $info->{guidId}){
    if ($newUp->{guid}){
      $self->info("We have to insert the guid");
      return $self->insertGUID({}, $newUp);
    }else{
      $self->info("The guid is not in the catalogue");
      return;
    }
  }
  if (defined $newUp->{se}){
    my $column="seStringlist";
    $options->{autose} and $column="seAutoStringlist";
    $self->debug(1, "Trying to update the SE to $update->{se}");

    $newUp->{$column}=",";
    if ($newUp->{se} ne "none"){
      my @ses=split (/,/, $update->{se});
      foreach (@ses) {
	my $newSE=$self->getSENumber($_)
	  or $self->info("Error getting the SeNumber of $_") and return;
	$newUp->{$column}="$newUp->{$column}$newSE,";
      }
    }
    delete $newUp->{se};
    $self->debug(1, "Settintg the senumber in $column to $newUp->{$column}");
  }

  delete $newUp->{guid};
  keys %$newUp or $self->debug(2, 'No fields to update') and return 1;
  $self->debug(1,"We should just update the info of the guid");
  return $info->{db}->update($info->{table}, $newUp, "guidId=$info->{guidId}");
}




=item C<moveGUIDs($guid>

This function moves all the bigger than a certain guid to a new table
A new table is always created.


=cut

sub moveGUIDs {
  my $self=shift;
  my $guid=shift;
  my $options=shift ||"";
  $DEBUG and $self->debug(1,"Starting  moveGUIDs, with $guid ");

  my ($db, $table)=$self->selectDatabaseFromGUID($guid) or return;

  #Create the new guid table
  my $tableName=$self->queryValue("SELECT max(tableName)+1 from GUIDINDEX");
  $tableName or $tableName=1;
  $self->checkGUIDTable($tableName) or return;


  my $info=$self->describeTable("G${tableName}L");
  my $columns="";
  foreach my $c (@$info){
    $columns.="$c->{Field},";
  }
  $columns =~ s/,$//;

  my $hostId=$self->{CURHOSTID};

  #insert it into the index
  if (!$self->insertInIndex($hostId, $tableName, $guid,{guid=>1})){
    $self->info("Error creating the index");
    return;
  }
  $self->debug(4, "INDEX READY!!!");

  #move the entries from the old table to the new one
  my $error=1;
  if ($self->{HOST} eq $db->{HOST} and $self->{DRIVER} eq $db->{DRIVER}){
    #at least is in the same host, and driver
    my @queries=("INSERT INTO $self->{DB}.G${tableName}L ($columns) select $columns from $table where  binary2date(guid)>string2date('$guid')",
		 "INSERT INTO $self->{DB}.G${tableName}L_PFN (guidid, pfn,seNumber) select p.guidid, p.pfn, p.seNumber from ${table}_PFN p, $self->{DB}.G${tableName}L g where p.guidId=g.guidId",
		"DELETE FROM $table where binary2date(guid)>string2date('$guid')",
		"delete from p using ${table}_PFN p left join $table g on p.guidId=g.guidId where g.guidId is null");

    my $counter=$#queries;
    foreach my $q (@queries){
      $db->do($q) or last;
      $counter--;
    }
    if ($options !~ /f/){
      $db->checkGUIDTable($table);
    }

    $self->info("From $#queries, $counter left");
    if ($counter<0) {
      $self->info("We have done all the queries");
      $error=0;
    }
    
  }else{
    #from a different database
  }

  #let's check the table again
#  if ($options !~ /f/){
    $self->checkGUIDTable($tableName, $db) or return;
#  } else {
#    $self->info("Skipping the index creation");
#  }
  if ($error){
    $self->info("WE SHOULD REMOVE THE INDEX\n");
    return;
  }
  #Finally, let's delete it from the old table
#  $db->do("DELETE FROM $table where guid>string2date(?)", undef, {bind_values=>[$guid]}) or return;
  $self->info("Now we have to grant privileges to the different users");
  my $users=$self->queryColumn("select Username from ADMIN.TOKENS")
    or $self->info("Error getting the users") and return;
  foreach my $info (@$users){
    my $user=$info;
    $self->debug(1,"User $user");
    $db->do("GRANT insert,update,delete on $self->{DB}.G${tableName}L to $user");
    $db->do("GRANT insert,update,delete on $self->{DB}.G${tableName}L_PFN to $user");
  } 
  $self->info("YUHUUU!!!");
  return 1;
}


sub deleteMirrorFromGUID{
  my $self=shift;
  my $guid=shift;
  my $lfn=shift;
  my $se=shift;
  my $pfn=shift;

  $self->debug(1,"Ready to delete the mirror of $lfn from $se");

  my $info=$self->checkPermission('w', $guid, "db") or return;
  my $seNumber=$self->getSENumber($se) or $self->info("Error getting the se number of '$se'") and return;
  $pfn or $pfn = $info->{db}->queryValue("select pfn from $info->{table}_PFN where guidId=? and seNumber=? limit 1",undef,
    {bind_values=>[$info->{guidId},$seNumber]});
  my $column="seAutoStringList";

  if ($pfn){
    $self->info("First, let's delete the pfn $pfn");
    $info->{db}->do("INSERT IGNORE INTO LFN_BOOKED(lfn, owner, expiretime, size, guid, gowner, user, pfn, se)
      select ?,g.owner,-1,g.size,string2binary(?),g.gowner,?,?,s.seName
      from $info->{table} g, $info->{table}_PFN g_p, SE s
      where g.guidId=g_p.guidId and g_p.guidId=? and g_p.seNumber=? and g_p.pfn=? and s.seNumber=g_p.seNumber",
      {bind_values=>[$lfn,$guid,$self->{VIRTUAL_ROLE},$pfn,$info->{guidId},$seNumber,$pfn]});
    $column="seStringList";
  }

  $self->debug(2,"Finally, let's update the column $column");

  return $info->{db}->do("update $info->{table} set $column=replace($column, '$seNumber,','') where guid=string2binary(?)", {bind_values=>[$guid]});
}

sub getNumberOfEntries {
  my $self=shift;
  my $entry=shift;

  my ($db, $path2)=$self->reconnectToIndex( $entry->{hostIndex}) or return -1;

  return $db->queryValue("SELECT COUNT(*) from G$entry->{tableName}L");
}



sub updateStatistics {
  my $self=shift;

  my $index=shift;
  my $table="G${index}L";
  $self->info("Checking if the table is up to date");
  $self->queryValue("select 1 from (select max(ctime) ctime, count(*) counter from $table) a left join  GL_ACTIONS on tablenumber=? and action='STATS' where extra is null or extra<>counter or time is null or time<ctime",undef, {bind_values=>[$index]}) 
    or return;

  $self->info("Updating the table");
  $self->do("delete from GL_ACTIONS where action='STATS' and tableNumber=?", {bind_values=>[$index]});

  $self->do("insert into GL_ACTIONS(tablenumber, time, action, extra) select ?,max(ctime),'STATS', count(*) from $table",
	    {bind_values=>[$index]});

  $self->do("delete from GL_STATS where tableNumber=?", {bind_values=>[$index]});
  $self->do("insert into GL_STATS(tableNumber, seNumber, seNumFiles, seUsedSpace) select ?, s.seNumber, count(*), sum(".$self->reservedWord("size").") from SE s, $table g ,${table}_PFN p where g.guidid=p.guidid and s.senumber=p.senumber group by s.senumber", {bind_values=>[$index]});
  return 1;
}

sub checkOrphanGUID {
  my $self=shift;
  my $number=shift;
  my $options=shift || "";

  my $table="G${number}L";
  $self->info("Checking the unused guids of $table");
  
  $self->do("delete from GL_ACTIONS where action='TODELETE' and tableNUmber=?", {bind_values=>[$number]});
  my $where="left join ${table}_REF r on g.guidid=r.guidid where ctime<now() -3600 and r.guidid is null";

  if ($options eq "force"){
    $self->info("remove the files regardless of time") and $where =~ s/\Qctime<now() -3600 and//;
	}
	else {
		(-f "$self->{CONFIG}->{TMP_DIR}/AliEn_TEST_SYSTEM") and
			$self->info("We are testing the file quotas. Let's remove the files immediately") and $where =~ s/-3600/-240/;
	}

  if (  $options eq "f"){
    $self->info("Locking the tables");
    $self->lock("$table as g write , ${table}_PFN as p write, ${table}_REF as r write, TODELETE");
  }

  $self->do("insert into TODELETE (pfn,seNumber, guid) select pfn,seNumber, guid from  ${table}_PFN p join  $table g on p.guidid=g.guidid $where");
  #$self->do("insert into TODELETE (pfn, seNumber, guid) select '',senumber,guid from $table g, SE $where and locate(concat(',',senumber,','), seautostringlist) ");
  my $replicas=$self->do("delete from p using ${table}_PFN p join $table g  on p.guidid=g.guidid $where");
  my $info=$self->do("delete from g using  $table g $where");

  if (  $options eq "f"){
    $self->info("Unlocking the table");
    $self->unlock();
  }
  $self->info("Done (removed $info entries and $replicas replicas)");
  return 1;
}


sub getLFNfromGUID {
  my $self=shift;
  my $options=shift;
  my $guid=shift;
  my @lfns;


  my ($db, $table)=$self->selectDatabaseFromGUID($guid) or return;

  $self->info("And now, let's check which lfn tables we are supposed to use ($table)");
  my $ref=$db->queryColumn("select lfnRef from 
  ${table}_REF join $table using (guidid)
 where guid=string2binary(?)", undef, {bind_values=>[$guid]});
  if ($options =~ /a/){
    $self->info("Looking in all possible tables");
    $ref=$self->queryColumn("select concat(hostIndex,'_',tableName) from INDEXTABLE");
  }
  foreach my $entry (@$ref){
    $self->info("We have to check $entry");
    my ($host, $lfnTable)=split(/_/, $entry);
#    $DEBUG and $self->debug(1, "Checking in the table $entry->{hostIndex}");
    my ($db2, $path2)=$self->reconnectToIndex( $host) or next;
    my $prefix=$db2->queryValue("SELECT lfn from INDEXTABLE where hostIndex=? and tableName=?", undef, {bind_values=>[$host, $lfnTable]});
    my $paths = $db2->queryColumn("SELECT concat('$prefix', lfn) FROM L${lfnTable}L WHERE guid=string2binary(?) ", undef, {bind_values=>[$guid]});
    $paths and push @lfns, @$paths;
  }
  if ($options !~ /a/ and not @lfns){
    $self->info("We didn't find any lfn in the normal tables. Doing a full search");
    return $self->getLFNfromGUID("a$options", $guid);
  }

  return @lfns;
} 


sub renumberGUIDtable {
  my $self=shift;
  my $guid=shift;
  my $table=shift;

  my $db=$self;
  if (! $table){
    ($db, $table)=$self->selectDatabaseFromGUID($guid) or return;
  }
  $self->debug(1,"We have to renumber the table $table");

  $db->renumberTable($table, "guidId", {lock=>"${table}_PFN write, ${table}_REF write, ",
				       update=>["${table}_PFN","${table}_REF"]});
  return 1;
}

=head1 SEE ALSO

AliEn::Database

=cut



sub getAllHostAndTable{
  my $self=shift;

  my $result = $self->query("SELECT distinct hostIndex, tableName from GUIDINDEX");
  defined $result
    or $self->info("Error: not possible to get all the pair of host and table")
      and return;

  return $result;
}



1;
