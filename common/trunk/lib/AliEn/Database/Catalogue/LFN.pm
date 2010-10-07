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
package AliEn::Database::Catalogue::LFN;

use AliEn::Database::Catalogue::Shared;
use strict;

use Data::Dumper;


=head1 NAME

AliEn::Database::Catalogue - database wrapper for AliEn catalogue

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




my $binary2string="insert(insert(insert(insert(hex(guid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')";

#

# Checking the consistency of the database structure
sub createCatalogueTables {
  my $self = shift;


  $DEBUG and $self->debug(2,"In createCatalogueTables creating all tables...");

  foreach ("Constants", "SE") {
    my $method="check".$_."Table";
    $self->$method() or 
      $self->{LOGGER}->error("Catalogue", "Error checking the $_ table") and
	return;
  }

  my %tables=(HOSTS=>["hostIndex", {hostIndex=>"serial primary key",
				    address=>"char(50)", 
				    db=>"char(40)",
				    driver=>"char(10)", 
				    organisation=>"char(11)",},"hostIndex"],
	      TRIGGERS=>["lfn", {lfn=>"varchar(255)", 
				 triggerName=>"varchar(255)",
				entryId=>"int auto_increment primary key"}],
	      TRIGGERS_FAILED=>["lfn", {lfn=>"varchar(255)", 
				 triggerName=>"varchar(255)",
				entryId=>"int auto_increment primary key"}],
	      LFN_UPDATES=>["guid", {guid=>"binary(16)", 
				     action=>"char(10)",
				     entryId=>"int auto_increment primary key"},'entryId',['INDEX (guid)']
			   ],
	      ACL=>["entryId", 
		    {entryId=>"int(11) NOT NULL auto_increment primary key", 
		     owner=>"char(10) NOT NULL",
		     perm=>"char(4) NOT NULL",
		     aclId=>"int(11) NOT NULL",}, 'entryId'],
	      TAG0=>["entryId", 
		     {entryId=>"int(11) NOT NULL auto_increment primary key", 
		      path=>"varchar (255)",
		      tagName=>"varchar (50)",
		      tableName=>"varchar(50)",
		      user=>'varchar(20)'}, 'entryId'],
	      GROUPS=>["Userid", {Userid=>"int not null auto_increment primary key",
				  Username=>"char(20) NOT NULL", 
				  Groupname=>"char (85)",
				  PrimaryGroup=>"int(1)",}, 'Userid'],
	      INDEXTABLE=>["indexId", {indexId=>"int(11) NOT NULL auto_increment primary key",
				       lfn=>"varchar(50)", 
				       hostIndex=>"int(11)",
				       tableName=>"int(11)",}, 
			   'indexId', ['UNIQUE INDEX (lfn)']],
	      ENVIRONMENT=>['userName', {userName=>"char(20) NOT NULL PRIMARY KEY", 
					env=>"char(255)"}],
	      ACTIONS=>['action', {action=>"char(40) not null primary key",
				   todo=>"int(1) not null default 0"},
		       'action'],
	      PACKAGES=>['fullPackageName',{'fullPackageName'=> 'varchar(255)',
					    packageName=>'varchar(255)',
					    username=>'varchar(20)', 
					    packageVersion=>'varchar(255)',
					    platform=>'varchar(255)',
					    lfn=>'varchar(255)',
					    size=>'bigint'}, 
			],
	      COLLECTIONS=>['collectionId', {'collectionId'=>"int not null auto_increment primary key",
					     'collGUID'=>'binary(16)'}],
	      COLLECTIONS_ELEM=>['collectionId', {'collectionId'=>'int not null',
						  origLFN=>'varchar(255)',
						  guid=>'binary(16)',
						  data=>"varchar(255)",
						 localName=>"varchar(255)"},
				 
				 "",['INDEX (collectionId)']],

	      "SE_VOLUMES"=>["volume", {volumeId=>"int(11) NOT NULL auto_increment PRIMARY KEY",
					seName=>"char(255) collate latin1_general_ci NOT NULL ",
					volume=>"char(255) NOT NULL",
					mountpoint=>"char(255)",
					usedspace=>"bigint",
					freespace=>"bigint",
					size=>"bigint",
					method=>"char(255)",}, 
			     "volumeId", ['UNIQUE INDEX (volume)', 'INDEX(seName)'],],
	      "LL_STATS" =>["tableNumber", {
					    tableNumber=>"int(11) NOT NULL",
					    min_time=>"char(16) NOT NULL",
					    max_time=> "char(16) NOT NULL", 
				    },undef,['UNIQUE INDEX(tableNumber)']],
	      LL_ACTIONS=>["tableNumber", {tableNumber=>"int(11) NOT NULL",
					   action=>"char(40) not null", 
					   time=>"timestamp default current_timestamp",
					   extra=>"varchar(255)"}, undef, ['UNIQUE INDEX(tableNumber,action)']],
             SERanks=>["sitename", {sitename=>"varchar(100) collate latin1_general_ci  not null",
                                    seNumber=>"integer not null",
                                    rank=>"smallint(7) not null",
                                    updated=>"smallint(1)"}, 
                                    undef, ['UNIQUE INDEX(sitename,seNumber), PRIMARY KEY(sitename,seNumber), INDEX(sitename), INDEX(seNumber)']],
        LFN_BOOKED=>["lfn",{lfn=>"varchar(255)",
            expiretime=>"int",
            guid=>"binary(16) ",
            size=>"bigint",
            md5sum=>"varchar(32)",
            owner=>"varchar(20)",
            gowner=>"varchar(20)",
            pfn=>"varchar(255)",
            se=>"varchar(100)",
            quotaCalculated=>"smallint",
            user=>"varchar(20)",
            existing=>"smallint(1)",
          },
            undef, ['PRIMARY KEY(lfn,pfn)','INDEX(pfn)','INDEX(lfn)', 'INDEX(guid)', 'INDEX(expiretime)']
            
        ]                                      
	         );
  foreach my $table (keys %tables){
    $self->info("Checking table $table");
    $self->checkTable($table, @{$tables{$table}}) or return;
  }

  $self->checkLFNTable("0") or return;
  $self->do("INSERT IGNORE INTO ACTIONS(action) values  ('PACKAGES')");
  $self->info("Let's create the functions");
  $self->do("create function string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))");
  $self->do("create function binary2string (my_uuid binary(16)) returns varchar(36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')");
  $self->do("create function binary2date (my_uuid binary(16))  returns char(16) deterministic sql security invoker
return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))");
  $DEBUG and $self->debug(2,"In createCatalogueTables creation of tables finished.");
  $self->do("alter table TAG0 drop key path");
  $self->do("alter table TAG0 add index path (path)");

  1;
}

#
#
# internal functions
sub checkConstantsTable {
  my $self=shift;
  my %columns=(name=> "varchar(100) NOT NULL",
	       value=> "int",
	      );
  $self->checkTable("CONSTANTS",  "name", \%columns, 'name') or return;
  my $exists=$self->queryValue("SELECT count(*) from CONSTANTS where name='MaxDir'");
  $exists and return 1;
  return $self->do("INSERT INTO CONSTANTS values ('MaxDir', 0)");
}

sub checkLFNTable {
  my $self =shift;
  my $table =shift;
  defined $table or $self->info( "Error: we didn't get the table number to check") and return;
  
  $table =~ /^\d+$/ and $table="L${table}L";

  my $number;
  $table=~ /^L(\d+)L$/ and $number=$1;

  my %columns = (entryId=>"bigint(11) NOT NULL auto_increment primary key", 
		 lfn=> "varchar(255) NOT NULL",
		 type=> "char(1) NOT NULL default 'f'",
		 ctime=>"timestamp",
		 expiretime=>"datetime",
		 size=>"bigint  not null default 0",
		 aclId=>"mediumint(11)",
		 perm=>"char(3) not null",
		 guid=>"binary(16)",
		 replicated=>"smallint(1) not null default 0",
		 dir=>"bigint(11)",
		 owner=>"varchar(20) not null",
		 gowner=>"varchar(20) not null",
		 md5=>"varchar(32)",
		 guidtime=>"varchar(8)",
		 broken=>'smallint(1) not null default 0',
		);

  $self->checkTable(${table}, "entryId", \%columns, 'entryId', 
		    ['UNIQUE INDEX (lfn)',"INDEX(dir)", "INDEX(guid)", "INDEX(type)", "INDEX(ctime)", "INDEX(guidtime)"]) or return;
  $self->checkTable("${table}_broken", "entryId", {entryId=>"bigint(11) NOT NULL  primary key"}) or return;
  $self->checkTable("${table}_QUOTA", "user", {user=>"varchar(64) NOT NULL", nbFiles=>"int(11) NOT NULL", totalSize=>"bigint(20) NOT NULL"}, undef, ['INDEX user_ind (user)'],) or return;
  
  $self->do("optimize table ${table}");
#  $self->do("optimize table ${table}_QUOTA");
  
  return 1;
}

##############################################################################
##############################################################################
sub setIndexTable {
  my $self=shift;
  my $table=shift;
  my $lfn=shift;
  defined $table or return;
  $table =~ /^\d*$/ and $table="L${table}L";

  $DEBUG and $self->debug(2, "Setting the indextable to $table ($lfn)");
  $self->{INDEX_TABLENAME}={name=>$table, lfn=>$lfn};
  return 1;
}
sub getIndexTable {
  my $self=shift;
  return $self->{INDEX_TABLENAME};
}

sub getAllInfoFromLFN{
  my $self=shift;
  my $options=shift;

  my $tableName=$self->{INDEX_TABLENAME}->{name};
  $options->{table} and $options->{table}->{tableName} and 
    $tableName=$options->{table}->{tableName};
  my $tablePath=$self->{INDEX_TABLENAME}->{lfn};
  $options->{table} and $options->{table}->{lfn} and 
    $tablePath=$options->{table}->{lfn};
  defined $tableName or $self->info( "Error: missing the tableName in getAllInfoFromLFN") and return;
#  @_ or $self->info( "Warning! missing arguments in internal function getAllInfoFromLFN") and return;
  $tableName=~ /^\d+$/ and $tableName="L${tableName}L";
  my @entries=grep (s{^$tablePath}{}, @_);
  my @list=@entries;

  $DEBUG and $self->debug(2, "Checking for @entries in $tableName");
  my $op='=';
  ($options->{like}) and  $op="$options->{like}";

  map {$_="lfn $op ?"} @entries;

  my $where="WHERE ". join(" or ", @entries);
  my $opt=($options->{options} or "");
  ( $opt =~ /h/ ) and  $where .= " and lfn not like '%/.%'";
  ( $opt =~ /d/ ) and $where .= " and type='d'";
  ( $opt =~ /f/ ) and $where .= " and type='f'";

  my $order=$options->{order};
  $options->{where} and $where.=" $options->{where}";
  $order and $where .= " order by $order";

  if( $options->{retrieve}){
     $options->{retrieve} =~ s{lfn}{concat('$tablePath',lfn) as lfn};
     $options->{retrieve} =~ s{guid}{$binary2string as guid};
   }
  my $retrieve=($options->{retrieve} or "*,concat('$tablePath',lfn) as lfn, $binary2string as guid,DATE_FORMAT(ctime, '%b %d %H:%i') as ctime");

  my $method=($options->{method} or "query");

  if ($options->{exists}) {
    $method="queryValue";
    $retrieve="count(*)";
  }

  $options->{bind_values} and push @list, @{$options->{bind_values}};

  my $DBoptions={bind_values=>\@list};

  return $self->$method("SELECT $retrieve FROM $tableName $where", undef, $DBoptions);
}

=item c<existsLFN($lfn)>

This function receives an lfn, and checks if it exists in the catalogue. It checks for lfns like '$lfn' and '$lfn/', and, in case the entry exists, it returns the name (the name has a '/' at the end if the entry is a directory)

=cut

sub existsLFN {
  my $self=shift;
  my $entry=shift;

  $entry=~ s{/?$}{};
  my $options={bind_values=>["$entry/"]};
  my $tableRef=$self->queryRow("SELECT tableName,lfn from INDEXTABLE where lfn= left( ?, length(lfn)) order by length(lfn) desc limit 1",undef, $options);
  defined $tableRef or return;
  my $dataFromLFN = $self->getAllInfoFromLFN({method=>"queryValue",retrieve=>"lfn", table=>$tableRef},$entry,"$entry/");
  $dataFromLFN and return $dataFromLFN;
  my $bookingPool = $self->queryValue("select 1 from LFN_BOOKED where lfn=?",undef,{bind_values=>[$entry]});
  defined $bookingPool or return;
  $bookingPool->{fromBookingPool} = 1 if(defined $bookingPool->{lfn});
  return $bookingPool;
}

=item C<getHostsForEntry($lfn)>

This function returns a list of all the possible hosts and tables that might contain entries of a directory

=cut

sub getHostsForEntry{
  my $self=shift;
  my $lfn=shift;

  $lfn =~ s{/?$}{};
  #First, let's take the entry that has the directory
  my $entry=$self->query("SELECT tableName,hostIndex,lfn from INDEXTABLE where lfn=left('$lfn/',length(lfn)) order by length(lfn) desc limit 1");
  $entry or return;
  #Now, let's get all the possibles expansions (but the expansions at least as
  #long as the first index
  my $length=length (${$entry}[0]->{lfn});
  my $expansions=$self->query("SELECT distinct tableName, hostIndex,lfn from INDEXTABLE where lfn like '$lfn/%' and length(lfn)>$length");
  my @all=(@$entry, @$expansions);
  return \@all;
}


=item C<createFile($hash)>

Adds a new file to the database. It receives a hash with the following information:



=cut

sub createFile {
  my $self=shift;
  my $options=shift || "";
  my $tableName= $self->{INDEX_TABLENAME}->{name};
  my $tableLFN= $self->{INDEX_TABLENAME}->{lfn};
  my $entryDir;
  my $seNumbers={};
  my @inserts=@_;
  foreach my $insert (@inserts) {
    $insert->{type} or $insert->{type}='f';
    $entryDir or $entryDir=$self->getParentDir($insert->{lfn});
    $insert->{dir}=$entryDir;
    $insert->{lfn}=~ s{^$tableLFN}{};
    foreach my $key (keys %$insert){
      $key=~ /^guid$/ and next;
      if ($key=~ /^(se)|(pfn)|(pfns)$/){
	delete $insert->{$key};
	next;
      }
      $insert->{$key}="'$insert->{$key}'";
    }
    if ( $insert->{guid}) {
      $insert->{guidtime}="string2date('$insert->{guid}')";
      $insert->{guid}="string2binary('$insert->{guid}')";
    }
  }
  $self->info("Inserting the lfn");
  my $done= $self->multiinsert($tableName, \@inserts, {noquotes=>1,silent=>1});
  if (!$done and $DBI::errstr=~ /Duplicate entry '(\S+)'/ ){
    $self->info("The entry '$tableLFN$1' already exists");
  }
 #Update quota
 $self->fquota_update(0,scalar(@inserts));
 return $done;
}

#
# This subroutine returns the guid of an LFN. Before calling it, 
# you have to make sure that you are in the right database 
# (with checkPersmissions for instance)
sub getGUIDFromLFN{
  my $self=shift;

  return $self->getAllInfoFromLFN({retrieve=>'guid', method=>'queryValue'}, @_);
}

sub getParentDir {
  my $self=shift;
  my $lfn=shift;
  $lfn=~ s{/[^/]*/?$}{/};

  my $tableName= $self->{INDEX_TABLENAME}->{name} or return;
  $lfn=~ s{$self->{INDEX_TABLENAME}->{lfn}}{};
  return $self->queryValue("select entryId from $tableName where lfn=?", undef, 
			   {bind_values=>[$lfn]});
}
sub updateLFN {
  my $self=shift;
  $self->debug(2,"In updateFile with @_");
  my $file=shift;
  my $update=shift;

  my $lfnUpdate={};

  my $options={noquotes=>1};
  
  $update->{size} and $lfnUpdate->{size}= $update->{size};
  $update->{guid} and $lfnUpdate->{guid}="string2binary(\"$update->{guid}\")";
  $update->{owner} and $lfnUpdate->{owner}= "\"$update->{owner}\"";
  $update->{gowner} and $lfnUpdate->{gowner}= "\"$update->{gowner}\"";

  #maybe all the information to update was only on the guid side
  (keys %$lfnUpdate) or return 1;
  my $tableName=$self->{INDEX_TABLENAME}->{name};
  my $lfn=$self->{INDEX_TABLENAME}->{lfn};

  $self->debug(1,"There is something to update!!");
  $file=~ s{^$lfn}{};
  $self->update($tableName, $lfnUpdate, "lfn='$file'", {noquotes=>1}) or 
    return;
  $file and return 1;

  $self->info("This is in fact an index!!!!");
  my $parentpath=$lfn;
  #If it is /, we don't have to do anything
  ($parentpath =~ s{[^/]*/?$}{}) or return 1;

  $self->info("HERE WE SHOULD UPDATE ALSO THE FATHER");

  my $db=$self->selectDatabase($parentpath);
  $lfn=~ s{^$db->{INDEX_TABLENAME}->{lfn}}{};
  return $db->update($db->{INDEX_TABLENAME}->{name}, $lfnUpdate, "lfn='$lfn'", {noquotes=>1});
}

sub deleteFile {
  my $self=shift;
  my $file=shift;

  my $tableName=$self->{INDEX_TABLENAME}->{name};
#  my $index=",$self->{CURHOSTID}_$tableName,";
  $file=~ s{^$self->{INDEX_TABLENAME}->{lfn}}{};


  return $self->delete($tableName, "lfn='$file'");
}
sub getLFNlike {
  my $self=shift;
  my $lfn=shift;

  my @result;
  $lfn=~ s/\*/%/g;
  $lfn=~ s/\?/_/g;

  $self->debug(1, "Trying to find paths like $lfn");
  my @todo=$lfn;

  while (@todo) {
    my $parent =shift @todo;

    if ( $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{}){
      $self->debug(1,"Looking in $parent for $1 (still to do $2)");
      my ($pattern, $todo)=($1,$2);

      my $db=$self->selectDatabase($parent) or 
	$self->info("Error selecting the database of $parent") and next;
      my $parentdir=$db->getAllInfoFromLFN({retrieve=>'entryId', method=>'queryValue'}, $parent);

      my $tableName=$db->{INDEX_TABLENAME}->{name};
      my $tablelfn=$db->{INDEX_TABLENAME}->{lfn};

      my $ppattern="$parent$pattern";

      my $entries=$db->queryColumn("SELECT concat('$tablelfn',lfn) from $tableName where dir=? and (lfn like ? or lfn like ?)",
				  undef, {bind_values=>[$parentdir, $ppattern, "$ppattern/"]})
	or $self->info("error doing the query") and next;
      foreach my $entry (@$entries) {
	if ($todo){
	  $entry =~ m{/$} and 
	    push @todo, "$entry$todo";
	}else {
	  push @result, $entry;
	}
      }
    } else{
      my $db=$self->selectDatabase($parent) or 
	$self->info("Error selecting the database of $parent") and next;
      my $parentdir=$db->getAllInfoFromLFN({retrieve=>'entryId', method=>'queryValue'}, $parent, "$parent/") or next;
      push @result, "$db->{INDEX_TABLENAME}->{lfn}$parent";
    }
  }

  return \@result;
}

##############################################################################
##############################################################################
#
# Lists a directory: WARNING: it doesn't return '..'
#

=item C<listDirectory($entry, $options)>

Returns all the entries of a directory. '$entry' can be either an lfn (in which case listDirectory will retrieve the rest of the info from the database), or a hash containing the info of that directory. 

Possible options:

=over

=item a

list also the current directory

=item f

Do not sort the output

=item F

put a '/' at the end of directories


=back


=cut

sub listDirectory {
  my $self=shift;
  my $info=shift;
  my $options=shift || "";
  my $selimit=shift;
  my $entry;

  if ( UNIVERSAL::isa( $info, "HASH" )) {
    $entry=$info;
  } else {
    $entry=$self->getAllInfoFromLFN({method=>"queryRow"}, $info); 
    $entry or $self->info( "Error getting the info of $info") and return;
  }
  my $sort="order by lfn";
  $options =~ /f/ and $sort="";
  my @all;

  if (!$selimit){
    my $content=$self->getAllInfoFromLFN({table=>$self->{INDEX_TABLENAME}, 
					  where=>"dir=? $sort",
					  bind_values=>[$entry->{entryId}]});
    
    $content and push @all, @$content;
  } else{
    $self->debug(1,"We only have to display from $selimit (table $self->{INDEX_TABLENAME}");
    my $GUIDList=$self->getPossibleGuidTables( $self->{INDEX_TABLENAME}->{name});

    my $content=$self->getAllInfoFromLFN({table=>$self->{INDEX_TABLENAME}, 
					  where=>"dir=? and right(lfn,1)='/' $sort",
					  bind_values=>[$entry->{entryId}]});
    
    $content and push @all, @$content;
    foreach my $elem (@$GUIDList) {
      $self->debug(1,"Checking table $elem and $elem->{tableName}");
      ($elem->{address} eq $self->{HOST})
	or $self->info("We can't check the se (the info is in another host") 
	  and return;
      
      my $content=$self->getAllInfoFromLFN({table=>{tableName=>
"$self->{INDEX_TABLENAME}->{name} l, $elem->{db}.G$elem->{tableName}L g, $elem->{db}.G$elem->{tableName}L_PFN p",
						    lfn=>$self->{INDEX_TABLENAME}->{lfn}}, 
					    where=>"dir=? and l.guid=g.guid and p.guidid=g.guidid and p.seNumber=? $sort",
					    bind_values=>[$entry->{entryId}, $selimit],
					   retrieve=>"distinct l.*,lfn, insert(insert(insert(insert(hex(l.Guid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-') as Guid,DATE_FORMAT(l.ctime, '%b %d %H:%i') as ctime"});
      
      $content and push @all, @$content;
    }
    @all=sort {$a->{lfn} cmp $b->{lfn}} @all;
  }
  if ($options=~ /a/) {
    $entry->{lfn}=".";
    @all=($entry,@all);
  }
  if ($options !~ /F/) {
    foreach my $entry2 (@all) {
      $entry2->{lfn}=~ s{/$}{};
    }
  }
  return @all;
}

#
# createDirectory ($lfn, [$perm, [$replicated, [$table]]])
#
sub createDirectory {
  my $self=shift;
  my $insert={lfn=>shift,
	      perm=>(shift or "755"),
	      replicated=>(shift or 0),
	      owner=>$self->{VIRTUAL_ROLE},
	      gowner=>$self->{VIRTUAL_ROLE},
	      type=>'d'};
  return $self->_createEntry($insert,@_);
}

sub createRemoteDirectory {
  my $self=shift;
  my ($hostIndex,$host, $DB, $driver,  $lfn)=@_;
  my $oldtable=$self->{INDEX_TABLENAME};

  my ( $oldHost, $oldDB, $oldDriver ) = 
    ( $self->{HOST},$self->{DB}, $self->{DRIVER});

  #Now, in the new database
  $self->info("Before reconnecttoindex\n");
  my ($db, $path2)=$self->reconnectToIndex($hostIndex) or $self->info("Error: the reconnect to index $hostIndex failed") and return;
  my $newTable=$db->getNewDirIndex() or $self->info("Error getting the new dirindex") and return;
  $newTable="L${newTable}L";
  #ok, let's insert the entry in $table
  $self->info("Before callting createDirectory");
  my $done=$db->createDirectory("$lfn/",$self->{UMASK},0,{name=>$newTable, lfn=>"$lfn/"} );

  $done or $self->info("Error creating the directory $lfn") and return;
  $self->info("Directory $lfn/ created");
  $self->info( "Now, let's try to do insert the entry in the INDEXTABLE");
  if (! $self->insertInIndex($hostIndex, $newTable, "$lfn/")){
    $db->delete($newTable, "lfn='$lfn/'");
    return;
  }
  $self->info( "Almost everything worked!!");
  $self->createDirectory("$lfn/",$self->{UMASK}, 1 ,$oldtable) or return;
  $self->info( "Everything worked!!");

  return $done;

}

#Delete file from Catalogue
sub removeFile {
  my $self = shift;
  my $lfn = shift;
  my $filehash = shift;
  my $user = $self->{CONFIG}->{ROLE};
  #Insert into LFN_BOOKED
  my $parent = "$lfn";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $db = $self->selectDatabase($parent) 
    or $self->{LOGGER}->error("Database::Catalogue::LFN","Error selecting the database of $parent")
    and return;
  my $tableName = "$db->{INDEX_TABLENAME}->{name}";
  my $tablelfn = "$db->{INDEX_TABLENAME}->{lfn}";
  my $lfnOnTable = "$lfn";
  $lfnOnTable =~ s/$tablelfn//;
  my $guid = $db->queryValue("SELECT binary2string(l.guid) as guid FROM $tableName l WHERE l.lfn=?", undef, {bind_values=>[$lfnOnTable]}) || 0;
  #Insert into LFN_BOOKED only when the GUID has to be deleted
  $db->do("INSERT INTO LFN_BOOKED(lfn, owner, expiretime, size, guid, gowner, user, pfn)
    SELECT ?, l.owner, -1, l.size, l.guid, l.gowner, ?,'*' FROM $tableName l WHERE l.lfn=? AND l.type<>'l'", {bind_values=>[$lfn,$user,$lfnOnTable]})
    or return ("ERROR: Could not add entry $lfn to LFN_BOOKED","[insertIntoDatabase]");
  
  #Delete from table
  $db->do("DELETE FROM $tableName WHERE lfn=?",{bind_values=>[$lfnOnTable]});

  #Update Quotas
  if($filehash->{type} eq "f"){
    $self->fquota_update(-1*$filehash->{size},-1) 
      or $self->{LOGGER}->error("Database::Catalogue::LFN","ERROR: Could not update quotas")
      and return;
  }
  
  return 1;
}

#Delete folder from Catalogue
sub removeDirectory {
  my $self=shift;
  my $path=shift;
  my $parentdir=shift;
  my $user = $self->{CONFIG}->{ROLE};
  #Insert into LFN_BOOKED and delete lfns
  my $entries=$self->getHostsForEntry($path) 
    or $self->{LOGGER}->error("Database::Catalogue::LFN","ERROR: Could not get hosts for $path")
    and return;
  my @index=();
  my $size = 0;
  my $count = 0;          
  foreach my $db (@$entries) {
    $self->info(1, "Deleting all the entries from $db->{hostIndex} (table $db->{tableName} and lfn=$db->{lfn})");
    my ($db2, $path2)=$self->reconnectToIndex($db->{hostIndex}, $path);
    $db2 
      or $self->{LOGGER}->error("Database::Catalogue::LFN","ERROR: Could not reconnect to host")
      and return;
    my $tmpPath="$path/";
    $tmpPath=~ s{^$db->{lfn}}{};
    $count += ($db2->queryValue("SELECT count(*) FROM L$db->{tableName}L l WHERE l.type='f' AND l.lfn LIKE concat(?,'%')",
        undef, {bind_values=>[$tmpPath]})||0);
    $size += ($db2->queryValue("SELECT SUM(l.size) FROM L$db->{tableName}L l WHERE l.lfn LIKE concat(?,'%') AND l.type='f'",
        undef, {bind_values=>[$tmpPath]})||0);
    $db2->do("INSERT INTO LFN_BOOKED(lfn, owner, expiretime, size, guid, gowner, user, pfn)
      SELECT l.lfn, l.owner, -1, l.size, l.guid, l.gowner, ?,'*' FROM L$db->{tableName}L l WHERE l.type='f' AND l.lfn LIKE concat(?,'%')",
      {bind_values=>[$user,$tmpPath]})
      or $self->{LOGGER}->error("Database::Catalogue::LFN","ERROR: Could not add entries $tmpPath to LFN_BOOKED")
      and return;
    $db2->delete("L$db->{tableName}L", "lfn like '$tmpPath%'");
    $db->{lfn} =~ /^$path/ and push @index, "$db->{lfn}\%";
  }
  #Clean up index
  if ($#index>-1) {
    $self->deleteFromIndex(@index);
    if (grep( m{^$path/?\%$}, @index)){
      my $entries=$self->getHostsForEntry($parentdir) 
        or $self->{LOGGER}->error( "Database::Catalogue::LFN","Error getting the hosts for '$path'")
        and return;
      my $db=${$entries}[0];
      my ($newdb, $path2)=$self->reconnectToIndex($db->{hostIndex}, $parentdir);
      $newdb 
        or $self->{LOGGER}->error("Database::Catalogue::LFN","Error reconecting to index")
        and return;
      my $tmpPath="$path/";
      $tmpPath=~ s{^$db->{lfn}}{};
      $newdb->delete("L$db->{tableName}L", "lfn='$tmpPath'");
    }
  }
  $self->fquota_update(-$size,-$count) 
    or $self->{LOGGER}->error("Database::Catalogue::LFN","ERROR: Could not update quotas")
    and return;
  return 1;
}

#Move file
sub moveFile {
  my $self = shift;
  my $source = shift;
  my $target = shift;
  my $user = $self->{CONFIG}->{ROLE};
  my $parent = "$source";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $dbSource = $self->selectDatabase($parent)
    or $self->{LOGGER}->error("Database::Catalogue::LFN","Error selecting the database of $parent")
    and return;
  my $tableName_source = "$dbSource->{INDEX_TABLENAME}->{name}";
  my $tablelfn_source = "$dbSource->{INDEX_TABLENAME}->{lfn}";
  $parent = "$target";
  my $dbTarget = $self->selectDatabase($parent)
    or $self->{LOGGER}->error("Database::Catalogue::LFN","Error selecting the database of $parent")
    and return;
  my $tableName_target = "$dbTarget->{INDEX_TABLENAME}->{name}";
  my $tablelfn_target = "$dbTarget->{INDEX_TABLENAME}->{lfn}";
  
  my $lfnOnTable_source = "$source";
  $lfnOnTable_source =~ s/$tablelfn_source//;
  my $lfnOnTable_target = "$target";
  $lfnOnTable_target =~ s/$tablelfn_target//;
  
  if($tablelfn_source eq $tablelfn_target) {
    #If source and target are in same L#L table then just edit the names
    $dbSource->do("UPDATE $tableName_source SET lfn=? WHERE lfn=?",{bind_values=>[$lfnOnTable_target,$lfnOnTable_source]})
      or $self->{LOGGER}->error( "Database::Catalogue::LFN","Error updating database")
      and return;
  }
  else {
    #If the source and target are in different L#L tables then add in new table and delete from old table
    my $schema = $dbSource->queryRow("SELECT h.db FROM HOSTS h, INDEXTABLE i WHERE i.hostIndex=h.hostIndex AND i.lfn=?",
      undef,{bind_values=>[$tablelfn_source]})
      or $self->{LOGGER}->error("Database::Catalogue::LFN","Error updating database")
      and return;
    my $db = $schema->{db};
    $dbTarget->do("INSERT INTO $tableName_target(owner, replicated, ctime, guidtime, aclId, lfn, broken, expiretime, size, dir, gowner, type, guid, md5, perm) 
      SELECT owner, replicated, ctime, guidtime, aclId, ?, broken, expiretime, size, dir, gowner, type, guid, md5, perm FROM $db.$tableName_source WHERE lfn=?",{bind_values=>[$lfnOnTable_target,$lfnOnTable_source]})
      or $self->{LOGGER}->error("Database::Catalogue::LFN","Error updating database")
      and return;
  }
  my $parentdir = "$lfnOnTable_target";
  $parentdir =~ s{[^/]*$}{}; $parentdir = s/$tablelfn_target//;
  my $entryId = $dbTarget->queryValue("SELECT entryId FROM $tableName_target WHERE lfn=?",undef,{bind_values=>[$parentdir]});
  $dbTarget->do("UPDATE $tableName_target SET dir=? WHERE lfn=?",{bind_values=>[$entryId,$lfnOnTable_target]})
    or $self->{LOGGER}->error("Database::Catalogue::LFN","Error updating database")
    and return;
  $dbSource->do("DELETE FROM $tableName_source WHERE lfn=?",{bind_values=>[$lfnOnTable_source]})
    or $self->{LOGGER}->error("Database::Catalogue::LFN","Error updating database")
    and return;  
  return 1;
}

#Create softlink between two LFNs
sub softLink {
  my $self = shift;
  my $source = shift;
  my $target = shift;
  my $parent = "$source";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $dbSource = $self->selectDatabase($parent)
    or $self->{LOGGER}->error("Database::Catalogue::LFN","Error selecting the database of $parent")
    and return;
  my $tableName_source = "$dbSource->{INDEX_TABLENAME}->{name}";
  my $tablelfn_source = "$dbSource->{INDEX_TABLENAME}->{lfn}";
  $parent = "$target";
  my $dbTarget = $self->selectDatabase($parent)
    or $self->{LOGGER}->error("Database::Catalogue::LFN","Error selecting the database of $parent")
    and return;
  my $tableName_target = "$dbTarget->{INDEX_TABLENAME}->{name}";
  my $tablelfn_target = "$dbTarget->{INDEX_TABLENAME}->{lfn}";
  my $lfnOnTable_source = "$source";
  $lfnOnTable_source =~ s/$tablelfn_source//;
  my $lfnOnTable_target = "$target";
  $lfnOnTable_target =~ s/$tablelfn_target//;
  
  if($tablelfn_source eq $tablelfn_target) {
    #If source and target are in same L#L table then just edit the names
    $dbTarget->do("INSERT INTO $tableName_target(owner, replicated, ctime, guidtime, aclId, lfn, broken, expiretime, size, dir, gowner, type, guid, md5, perm) 
      SELECT owner, replicated, ctime, guidtime, aclId, ?, broken, expiretime, size, dir, gowner, 'l', guid, md5, perm FROM $tableName_source WHERE lfn=?",{bind_values=>[$lfnOnTable_target,$lfnOnTable_source]})
      or $self->{LOGGER}->error("Database::Catalogue::LFN","Error updating database","[updateDatabse]")
      and return;
  }
  else {
    #If the source and target are in different L#L tables then add in new table and delete from old table
    my $schema = $dbSource->queryRow("SELECT h.db FROM HOSTS h, INDEXTABLE i WHERE i.hostIndex=h.hostIndex AND i.lfn=?",
      undef,{bind_values=>[$tablelfn_source]})
      or $self->{LOGGER}->error("Database::Catalogue::LFN","Error updating database")
      and return;
    my $db = $schema->{db};
    $dbTarget->do("INSERT INTO $tableName_target(owner, replicated, ctime, guidtime, aclId, lfn, broken, expiretime, size, dir, gowner, type, guid, md5, perm) 
      SELECT owner, replicated, ctime, guidtime, aclId, ?, broken, expiretime, size, dir, gowner, 'l', guid, md5, perm FROM $db.$tableName_source WHERE lfn=?",{bind_values=>[$lfnOnTable_target,$lfnOnTable_source]})
      or $self->{LOGGER}->error("Database::Catalogue::LFN","Error updating database")
      and return;
  }
  my $parentdir = "$lfnOnTable_target";
  $parentdir =~ s{[^/]*$}{};
  my $entryId = $dbTarget->queryValue("SELECT entryId FROM $tableName_target WHERE lfn=?",undef,{bind_values=>[$parentdir]});
  $self->info("$parentdir : $entryId");
  $dbTarget->do("UPDATE $tableName_target SET dir=? WHERE lfn=?",{bind_values=>[$entryId,$lfnOnTable_target]})
    or $self->{LOGGER}->error("Database::Catalogue::LFN","Error updating database")
    and return;
 return 1; 
}

sub getSENumber{
  my $self=shift;
  my $se=shift;
  my $options=shift || {};
  $DEBUG and $self->debug(2, "Checking the senumber");
  defined $se or return 0;
  $DEBUG and $self->debug(2, "Getting the numbe from the list");
  my $senumber=$self->queryValue("SELECT seNumber FROM SE where seName=?", undef, 
				 {bind_values=>[$se]});
  defined $senumber and return $senumber;
  $DEBUG and $self->debug(2, "The entry did not exist");
  $options->{existing} and return;
  $self->{SOAP} or $self->{SOAP}=new AliEn::SOAP 
    or return ;

  my $result=$self->{SOAP}->CallSOAP("Authen", "addSE", $se) or return;
  my $seNumber=$result->result;
  $DEBUG and $self->debug(1,"Got a new number $seNumber");
  return $seNumber;
  
  my $newnumber=1;


  my $max=$self->queryValue("SELECT max(seNumber) FROM SE");
  if ($max) {
    $newnumber=$max*2;
  }
  $self->insert("SE", {seName=>$se, seNumber=>$newnumber}) or return;
  return $newnumber;
}
sub tabCompletion {
  my $self=shift;
  my $entryName=shift;
  my $tableName=$self->{INDEX_TABLENAME}->{name};
  my $lfn=$self->{INDEX_TABLENAME}->{lfn};
  my $dirName=$entryName;
  $dirName=~ s{[^/]*$}{};
  $dirName =~ s{^$lfn}{};
  $entryName =~ s{^$lfn}{};
  my $dir=$self->queryValue("SELECT entryId from $tableName where lfn=?",undef,
			    {bind_values=>[$dirName]});
  $dir or return;
  my $rfiles = $self->queryColumn("SELECT concat('$lfn',lfn) from $tableName where dir=$dir and lfn rlike '^$entryName\[^/]*\/?\$'");
  return @$rfiles;

}
##############################################################################
##############################################################################
sub actionInIndex {
  my $self=shift;
  my $action=shift;

  #updating the D0 of all the databases
  my ($hosts) = $self->getAllHosts;

  defined $hosts
    or return;
  my ( $oldHost, $oldDB, $oldDriver ) = ($self->{HOST}, $self->{DB}, $self->{DRIVER});
  my $tempHost;
  foreach $tempHost (@$hosts) {
    #my ( $ind, $ho, $d, $driv ) = split "###", $tempHost;
    $self->info( "Updating the INDEX table of  $tempHost->{db}");
    my ($db, $Path2)=$self->reconnectToIndex( $tempHost->{hostIndex}, "", $tempHost );
    $db or next;
    $db->do($action) or print STDERR "Warning: Error doing $action";
  }
  $self->reconnect( $oldHost, $oldDB, $oldDriver ) or return;

  $DEBUG and $self->debug(2, "Everything is done!!");

  return 1;
}
sub insertInIndex {
  my $self=shift;
  my $hostIndex=shift;
  my $table=shift;
  my $lfn=shift;
  
  $table=~ s/^L(\d+)L$/$1/;
  my $action="INSERT INTO INDEXTABLE (hostIndex, tableName, lfn) values('$hostIndex', '$table', '$lfn')";
  return $self->actionInIndex($action);
}
sub deleteFromIndex {
  my $self=shift;
  my @entries=@_;
  map {$_="lfn like '$_'"} @entries;
  my $action="DELETE FROM INDEXTABLE WHERE ".join(" or ", @entries);
  return $self->actionInIndex($action);
  
}
sub getAllIndexes {
  my $self=shift;
  return $self->query("SELECT * FROM INDEXTABLE");
  
}

=item C<copyDirectory($source, $target)>

This subroutine copies a whole directory. It checks if part of the directory is in a different database

=cut

sub copyDirectory{
  my $self=shift;
  my $options=shift;
  my $source=shift;
  my $target=shift;
  $source and $target or
    $self->info( "Not enough arguments in copyDirectory",1111) and return;
  $source =~ s{/?$}{/};
  $target =~ s{/?$}{/};
  $DEBUG and $self->debug(1,"Copying a directory ($source to $target)");

  # Let's check where the source is:
  my $sourceHosts=$self->getHostsForEntry($source);

  my $sourceInfo=$self->getIndexHost($source);

  my $targetHost=$self->getIndexHost($target);
  my $targetIndex=$targetHost->{hostIndex};
  my $targetTable="L$targetHost->{tableName}L";
  my $targetLFN=$targetHost->{lfn};


  my $user=$options->{user} || $self->{VIRTUAL_ROLE};

  #Before doing this, we have to make sure that we are in the right database
  my ($targetDB, $Path2)=$self->reconnectToIndex( $targetIndex) or return;

  my $sourceLength=length($source)+1;

  my $targetName=$targetDB->existsLFN($target);
  if ($targetName)  {
    if ($targetName!~ m{/$} ) {
      $self->info("cp: cannot overwrite non-directory `$target' with directory `$source'", "222");
      return;
    }
    my $sourceParent=$source;
    $sourceParent=~ s {/([^/]+/?)$}{/};
    $self->info( "Copying into an existing directory (parent is $sourceParent)");
    $sourceLength=length($sourceParent)+1;
    $options->{k}  and $sourceLength=length($source)+1;
  }
  my $beginning=$target;
  $beginning=~ s/^$targetLFN//;
  
  my $select="insert into $targetTable(lfn,owner,gowner,size,type,guid,guidtime,perm,dir) select distinct concat('$beginning',substring(concat('";
  my $select2="', t1.lfn), $sourceLength)) as lfn, '$user', '$user',t1.size,t1.type,t1.guid,t1.guidtime,t1.perm,-1 ";
  my @values=();

  my $binary2string=$binary2string;
  $binary2string=~ s/guid/t1.guid/;
  foreach my $entry (@$sourceHosts){
    $DEBUG and $self->debug(1, "Copying from $entry to $targetIndex and $targetTable");
    my ($db, $Path2)=$self->reconnectToIndex( $entry->{hostIndex});

    my $tsource=$source;
    $tsource=~ s{^$entry->{lfn}}{};
    my $like="t1.replicated=0";

    my $table="L$entry->{tableName}L";
    my $join="$table t1 join $table t2 where t2.type='d' and (t1.dir=t2.entryId or t1.entryId=t2.entryId)  and t2.lfn like '$tsource%'";
    if ($targetIndex eq $entry->{hostIndex}){
      $options->{k} and $like.=" and t1.lfn!='$tsource'";
      $DEBUG and $self->debug(1, "This is easy: from the same database");
      # we want to copy the lf, which in fact would be something like
      # substring(concat('$entry->{lfn}', lfn), length('$sourceIndex'))
      $self->do("$select$entry->{lfn}$select2 from $join and $like");

    }else {
      $DEBUG and $self->debug(1, "This is complicated: from another database");
      my $query="SELECT concat('$beginning', substring(concat('$entry->{lfn}',t1.lfn), $sourceLength )) as lfn, t1.size,t1.type,$binary2string as guid ,t1.perm FROM $join and $like";
      $options->{k} and $query="select * from ($query) d where lfn!=concat('$beginning', substring('$entry->{lfn}$tsource', $sourceLength ))";
      my $entries = $db->query($query);
      foreach  my $files (@$entries) {
	my $guid="NULL";
	(defined $files->{guid}) and  $guid="$files->{guid}";

	$files->{lfn}=~ s{^}{};
	$files->{lfn}=~ s{^$targetLFN}{};
	push @values, " ( '$files->{lfn}',  '$user', '$user', '$files->{size}', '$files->{type}', string2binary('$guid'), string2date('$guid'),'$files->{perm}', -1)";

      }
    }

  }
  if ($#values>-1) {
    my $insert="INSERT into $targetTable(lfn,owner,gowner,size,type,guid,guidtime,perm,dir) values ";

    $insert .= join (",", @values);
    $targetDB->do($insert);
  }

  $target=~ s{^$targetLFN}{};
  my $targetParent=$target;
  $targetParent=~ s{/[^/]+/?$}{/} or $targetParent="";;  
  $DEBUG and $self->debug(1, "We have inserted the entries. Now we have to update the column dir");

  #and now, we should update the entryId of all the new entries
  #This query is divided in a subquery to profit from the index with the column dir
  my $entries=$targetDB->query("select * from (SELECT lfn, entryId from $targetTable where dir=-1 or lfn='$target' or lfn='$targetParent') dd where lfn like '$target\%/' or lfn='$target' or lfn='$targetParent'");
  foreach my $entry (@$entries) {
    $DEBUG and $self->debug(1, "Updating tbe entry $entry->{lfn}");
    my $update="update $targetTable set dir=$entry->{entryId} where dir=-1 and lfn rlike '^$entry->{lfn}\[^/]+/?\$'";
    $targetDB->do($update);
    
  }
#  $db2=$self->reconnectToIndex($sourceInfo->{hostIndex});
#  $self=$db;

  $DEBUG and $self->debug(1,"Directory copied!!");
  return 1;
}

#sub reconnectToIndex{
#  my $self=shift;
#  my $hostIndex=shift;
#  my $info=$self->queryRow("SELECT address, db,driver from HOSTS where hostIndex=$hostIndex") or $self->info( "Error getting the info of the host $hostIndex") and return;
#  my ($db, $host, $driver)=($info->{db}, $info->{address}, $info->{driver});#
#
#  ($db eq $self->{DB}) and ($host eq $self->{HOST}) and 
#    ($driver  eq $self->{DRIVER}) and return 1;
#  $DEBUG and $self->debug(1, "Reconecting to the database $db,$host, $driver");
#  return $self->reconnect($host, $db, $driver);
#
#}

=item C<moveLFNs($lfn, $toTable)>

This function moves all the entries under a directory to a new table
A new table is always created.

Before calling this function, you have to be already in the right database!!!
You can make sure that you are in the right database with a call to checkPermission

=cut

sub moveLFNs {
  my $self=shift;
  my $lfn=shift;
  my $options=shift || {};

  $DEBUG and $self->debug(1,"Starting  moveLFNs, with $lfn ");


  my $toTable;

  my $isIndex=$self->queryValue("SELECT 1 from INDEXTABLE where lfn=?", undef,
			       {bind_values=>[$lfn]});


  my $entry=$self->getIndexHost($lfn) or $self->info( "Error getting the info of $lfn") and return;
  my $sourceHostIndex=$entry->{hostIndex};
  my $fromTable=$entry->{tableName};
  my $fromLFN=$entry->{lfn};
  my $toLFN=$lfn;

  if ($options->{b}){
    $isIndex or $self->info("We are supposed to move back, but the entry is not in a different table...")
      and return;
    $self->info("We have to move back!!");

    my $parent=$lfn;
    $parent =~ s{/[^/]*/?$}{/};
    my $entryP=$self->getIndexHost($parent);
    print "And the father is\n";
    print Dumper($entryP);
    $toTable=$entryP->{tableName};
    ($entryP->{hostIndex} eq $entry->{hostIndex})
      or $self->info("We can only move back if the entries are in the same database...")
	and return;

    $toLFN=$entryP->{lfn};

  } else{
    $isIndex and $self->info("This is already in a different table...") and return;
    $toTable=$self->getNewDirIndex();
  }
  defined $toTable or $self->info( "Error getting the name of the new table") and return;


  $toTable =~ /^(\d+)*$/ and $toTable= "L${toTable}L";
  $fromTable =~ /^(\d+)*$/ and $fromTable= "L${fromTable}L";

  defined $sourceHostIndex or $self->info( "Error getting the hostindex of the table $toTable") and return;

  $self->lock("$toTable WRITE, $toTable as ${toTable}d READ,  $toTable as ${toTable}r READ, $fromTable as ${fromTable}d READ, $fromTable as ${fromTable}r READ, $fromTable");
  $self->renumberLFNtable($toTable, {'locked',1});
  my $min=$self->queryValue("select max(entryId)+1 from $toTable");
  $min or $min=1;

  $self->renumberLFNtable($fromTable, {'locked',1, 'min',$min});

  #ok, this is the easy case, we just copy into the new table
  my $columns="entryId,md5,owner,gowner,replicated,aclId,expiretime,size,dir,type,guid,perm";
  my $tempLfn=$lfn;
  $tempLfn=~ s{$fromLFN}{};

  #First, let's insert the entries in the new table
  if (!$self->do("INSERT into $toTable($columns,lfn) select $columns,substring(concat('$fromLFN', lfn), length('$toLFN')+1) from $fromTable where lfn like '${tempLfn}%' and lfn not like ''")){
    $self->unlock();
    return;
  }
  $self->unlock();

  ($isIndex) and  $self->deleteFromIndex($lfn);
  if ($options->{b}){
    my $newLfn=$lfn;
    $newLfn=~ s/^$toLFN//;

    my $oldDir=$self->queryValue("select entryId from $fromTable where lfn=''");
    my $newDir=$self->queryValue("select entryId from $toTable where lfn=?", undef, {bind_values=>[$newLfn]});
    $self->do("update $toTable set replicated=0 where replicated=1 and lfn=?", {bind_values=>[$newLfn]});
    $self->do("update $toTable set dir=? where dir=?", {bind_values=>[$newDir, $oldDir]});
    $self->do("drop table $fromTable");
  }else{
    if (!$self->insertInIndex($sourceHostIndex, $toTable, $lfn)){
      $self->delete($toTable,"lfn like '${tempLfn}%'");

      return;
    }
    if (!$isIndex ){
      #Finally, let's delete the old table;
      $self->delete($fromTable,"lfn like '${tempLfn}_%'");
      $self->update($fromTable,{replicated=>1}, "lfn='$tempLfn'");
    } else {
      $self->delete($fromTable,"lfn like '${tempLfn}%'");
    }
    my $user=$self->queryValue("select owner from $toTable where lfn=''");
    $self->info("And now, let's give access to $user to '$toTable");

    $self->do("GRANT ALL on $toTable to $user");
  }

  return 1;
}
##############################################################################
##############################################################################

sub grantBasicPrivilegesToUser {
  my $self = shift;
  my $db = shift
    or $self->{LOGGER}->error("Catalogue","In grantBasicPrivilegesToUser database name is missing")
      and return;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In grantBasicPrivilegesToUser user is missing")
      and return;
  my $passwd = shift;

  $self->grantPrivilegesToUser(["EXECUTE ON *"], $user, $passwd)
    or return;

  my $rprivileges = ["SELECT ON $db.*",
		     "INSERT, DELETE ON $db.TAG0", 
		    ];


  $DEBUG and $self->debug(2,"In grantBasicPrivilegesToUser granting privileges to user $user"); 
  $self->grantPrivilegesToUser($rprivileges, $user);
}

sub grantExtendedPrivilegesToUser {
  my $self = shift;
  my $db = shift
    or $self->{LOGGER}->error("Catalogue","In grantExtendedPrivilegesToUser database name is missing")
	and return;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In grantExtendedPrivilegesToUser user is missing")
	and return;
  my $passwd = shift;

  $self->grantPrivilegesToUser(["SELECT ON $db.*"], $user, $passwd)
  	or return;

  my $rprivileges = [
		     "INSERT, DELETE  ON $db.TAG0",
#		     "INSERT, DELETE ON $db.FILES",
		     "INSERT, DELETE ON $db.ENVIRONMENT", 
#		     "INSERT ON $db.SE"
		     "EXECUTE ON *",
		     "UPDATE ON $db.ACTIONS",
		     "INSERT, DELETE, UPDATE on $db.COLLECTIONS",
		     "INSERT, DELETE, UPDATE on $db.COLLECTIONS_ELEM",

];

  $DEBUG and $self->debug(2,"In grantExtendedPrivilegesToUser granting privileges to user $user"); 
  $self->grantPrivilegesToUser($rprivileges, $user);
}


sub getNewDirIndex {
  my $self=shift;

  $self->lock("CONSTANTS");

  my ($dir) = $self->queryValue("SELECT value from CONSTANTS where name='MaxDir'");
  $dir++;
  
  $self->update("CONSTANTS", {value => $dir}, "name='MaxDir'");
  $self->unlock();

  $self->info( "New table number: $dir");

  $self->checkLFNTable($dir) or 
    $self->info( "Error checking the tables $dir") and return;

  return $dir;
}


#
#Returns the name of the file of a path
#
sub _basename {
  my $self = shift;
  my ($arg) = @_;
  my $pos = rindex( $arg, "/" );

  ( $pos < 0 ) and    return ($arg);

  return ( substr( $arg, $pos + 1 ) );
}

sub deleteLink {
    my $self = shift;
    my $parent = shift;
    my $basename = shift;
    my $newpath = shift;

    $self->deleteDirEntry($parent, $basename);
    $self->deleteFromD0Like($newpath);
}

### Hosts functions

sub getFieldFromHosts{
  my $self = shift;
  my $host = shift
    or $self->{LOGGER}->error("Catalogue","In getFieldFromHosts host index is missing")
      and return;
  my $attr = shift || "*";
  
  $DEBUG and $self->debug(2,"In getFieldFromHosts fetching value of attribute $attr for host index $host");
  $self->queryValue("SELECT $attr FROM HOSTS WHERE hostIndex = ?", undef, 
		    {bind_values=>[$host]});
}

sub getFieldsFromHostsEx {
    my $self = shift;
	my $attr = shift || "*";
    my $where = shift || "";

	$self->query("SELECT $attr FROM HOSTS $where");
}

sub getFieldFromHostsEx {
    my $self = shift;
	my $attr = shift || "*";
    my $where = shift || "";

	$self->queryColumn("SELECT $attr FROM HOSTS $where");
}

sub getHostIndex {
    my $self = shift;
    my $host = shift;
    my $DB = shift;
    my $driver = shift;

    $driver
      and $driver = " AND driver='$driver'"
	or  $driver = "";

    $self->queryValue("SELECT hostIndex from HOSTS where address LIKE '$host%' AND db ='$DB'$driver");
}
sub getIndexHost {
  my $self=shift;
  my $lfn=shift;
  $lfn=~ s{/?$}{/};
  my $options={bind_values=>[$lfn]};
  return $self->queryRow("SELECT hostIndex, tableName,lfn FROM INDEXTABLE where lfn=left(?,length(lfn))  order by length(lfn) desc limit 1", undef, $options);
}

sub getMaxHostIndex {
  my $self = shift;

  $self->queryValue("SELECT MAX(hostIndex) from HOSTS");
}

sub insertHost {
  my $self = shift;
  my $ind = shift;
  my $ho = shift;
  my $d = shift;
  my $driv = shift;
  my $organisation=shift;

  my $data={hostIndex=>$ind, address=>$ho, db=>$d, driver=>$driv};
  $organisation and $data->{organisation}=$organisation;
	
  $DEBUG and $self->debug(2,"In insertHost inserting new data");
  $self->insert("HOSTS",$data)
}

sub updateHost {
  my $self = shift;
  my $ind = shift
    or $self->{LOGGER}->error("Catalogue","In updateHost host index is missing")
      and return;
  my $set = shift;
  
  $DEBUG and $self->debug(2,"In updateHost updating host $ind");
  $self->update("HOSTS",$set,"hostIndex='$ind'")
}

sub deleteHost {
  my $self = shift;
  my $ind = shift
    or $self->{LOGGER}->error("Catalogue","In deleteHost host index is missing")
      and return;

  $DEBUG and $self->debug(2,"In deleteHost deleting host $ind");
  $self->delete("HOSTS","hostIndex='$ind'")
}

### Groups functions

sub getUserGroups {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In getUserGroups user is missing")
      and return;
  my $prim = shift;
  defined $prim or $prim=1;

  $DEBUG and $self->debug(2,"In getUserGroups fetching groups for user $user");
  $self->queryColumn("SELECT groupname,userId from GROUPS where Username='$user' and PrimaryGroup = $prim");
}


sub getAllFromGroups {
	my $self=shift;
	my $attr = shift || "*";

	$DEBUG and $self->debug(2,"In getAllFromGroups fetching attributes $attr for all tuples from GROUPS table");
	$self->query("SELECT $attr FROM GROUPS");
}

sub insertIntoGroups {
  my $self = shift;
  my $user = shift;
  my $group = shift;
  my $var = shift;
  
  $DEBUG and $self->debug(2,"In insertIntoGroups inserting new data");
  $self->_do("INSERT IGNORE INTO GROUPS (Username, Groupname, PrimaryGroup) values ('$user','$group','$var')");
}

sub deleteUser {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In deleteUser user is missing")
      and return;
  
  $DEBUG and $self->debug(2,"In deleteUser deleting entries with user $user from GROUPS table");
  $self->delete("GROUPS","Username='$user'");
}



###	Environment functions

sub insertEnv {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In insertEnv user is missing")
      and return;
  my $curpath = shift
    or $self->{LOGGER}->error("Catalogue","In insertEnv current path is missing")
      and return;
  
  $DEBUG and $self->debug(2,"In insertEnv deleting old environment");
  $self->delete("ENVIRONMENT","userName='$user'") 
    or $self->{LOGGER}->error("Catalogue", "Cannot delete old environment")
      and return;
  
  $DEBUG and $self->debug(2,"In insertEnv inserting new environment");
  $self->insert("ENVIRONMENT",{userName=>$user,env=>"pwd $curpath"})
    or $self->{LOGGER}->error("Catalogue", "Cannot insert new environment")
      and return;

  1;
}

sub getEnv {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In getEnv user is missing")
      and return;

  $DEBUG and $self->debug(2,"In insertEnv fetching environment for user $user");
  $self->queryValue("SELECT env FROM ENVIRONMENT WHERE userName='$user'");
}

#	TAG functions

# quite complicated manoeuvers in Catalogue/Tag.pm - f_addTagValue
# difficult to merge with the others
#sub insertDirtagVarsFileValuesNew {
sub insertTagValue {
  my $self = shift;
  my $action= shift;
  my $path = shift;
  my $tag = shift;
  my $file = shift;
  my $rdata = shift;
  
  my $tableName = $self->getTagTableName($path, $tag, {parents=>1});
  $tableName or 
    $self->info("Error: we can't find the name of the table",1)
      and return;
  my $fileName = "$path$file";
  
#  $tableName =~ /T[0-9]+V$tag$/ or $fileName="$path$fileName";
  
  my $finished = 0;
  my $result;
  $self->debug(1,"Ready to insert in the table $tableName and $fileName");
  if ($action eq "update") {
    $self->info( "We want to update the latest entry (if it exists)");
    my $maxEntryId = $self->queryValue("SELECT MAX(entryId) FROM $tableName where file=?", 
				       undef, {bind_values=>[$fileName]});
    $DEBUG and $self->debug(2, "Got $maxEntryId");
    
    if ($maxEntryId) {
      $DEBUG and $self->debug(2, "WE ARE SUPPOSED TO ALTER THE ENTRY $maxEntryId");
      $result = $self->update($tableName, $rdata, "file='$fileName' and entryId=$maxEntryId");
      $finished = 1;
    }
  }

  if (!$finished) {
    $DEBUG and $self->debug(2, "Ok, we have to add the entry");


    $rdata->{file} = $fileName;
    my @keys=keys %{$rdata};
    $self->info( "We have $rdata and @keys");
    $result = $self->insert($tableName, $rdata);
    if ($result) {
      $self->info( "Let's make sure that we only have one entry");
      $self->delete($tableName, "file='$fileName' and entryId<".$self->getLastId());
    }
  }

  return $result;
}

sub getTags {
  my $self = shift;
  my $directory = shift;
  my $tag = shift;

  my $tableName = $self->getTagTableName($directory, $tag, {parents=>1})
    or $self->info("In getFieldsFromTagEx table name is missing")
      and return;

  my $columns = shift || "*";
  my $where = shift || "";
  my $options=shift || {};
  my $query="SELECT $columns from $tableName t where t.entryId=(select max(entryId) from $tableName t2 where t.file=t2.file) and $where";
  if ($options->{filename}){
#    $query.=" and entryId=(select max(entryId) from $tableName where file=?)";
    $query.=" and file=? ";
    my @list=();
    $options->{bind_values} and @list=@{$options->{bind_values}};
    push @list, $options->{filename};
    $options->{bind_values}=\@list;

  }
  return $self->query($query, undef, $options);
}

sub getTagNamesByPath {
  my $self = shift;
  my $path = shift;
  
  $self->queryColumn("SELECT tagName from TAG0 where path=?",undef, {bind_values=>[$path]});
}

sub getAllTagNamesByPath {
  my $self = shift;
  my $path = shift;
  my $options=shift || {};

  my $rec="";
  my $rec2="";
  my @bind=($path);
  if ($options->{r}){
    $rec=" or path like concat(?, '%') ";
    push @bind, $path;
  }
  if ($options->{user}){
    $self->debug(1, "Only for the user $options->{user}");
    $rec2=" and user=?";
    push @bind, $options->{user};
  }
      
  $self->query("SELECT tagName,path from TAG0 where (? like concat(path,'%') $rec) $rec2 group by tagName", undef, {bind_values=>\@bind});
}

sub getFieldsByTagName {
  my $self = shift;
  my $tagName = shift;
  my $fields = shift || "*";
  my $distinct = shift;
  my $directory = shift;
  my @bind=($tagName);
  my $sql = "SELECT ";
  $distinct and $sql .= "DISTINCT ";
  
  $sql.="  $fields FROM TAG0 WHERE tagName=?";
  if ($directory) {
     $sql.=" and path like concat(?, '%')";
     push @bind, $directory;
   }

  $self->query($sql, undef, {bind_values=>\@bind});
}


sub getTagTableName {
  my $self=shift;
  my $path=shift;
  my $tag=shift;
  my $options=shift ||{};
  my $query="path=?";
  $options->{parents} and $query="? like concat(path, '%') order by path desc limit 1";
  $self->queryValue("SELECT tableName from TAG0 where tagName=? and $query",undef, 
		    {bind_values=>[$tag, $path]});
}

sub deleteTagTable {
  my $self=shift;
  my $tag=shift;
  my $path=shift;
  $DEBUG and $self->debug(2, "In deleteTagTable");

  my $done;
  my $tagTableName=$self->getTagTableName($path, $tag);
  $tagTableName or $self->info( "Error trying to delete the tag table of $path and $tag") and return 1;
  my $user=$self->{USER};
  $DEBUG and $self->debug(2, "Deleting entries from T${user}V$tag");
  my $query="DELETE FROM $tagTableName WHERE file like '$path%' and file not like '$path/%/%'";
  
  $self->_do($query);

  $done = $self->delete("TAG0","path='$path' and tagName='$tag'");
  $done and $DEBUG and $self->debug(2, "Done with $done");
  return $done;
}

sub insertIntoTag0 {
	my $self = shift;
	my $directory = shift;
	my $tagName = shift;
	my $tableName=shift;
	my $user=shift || $self->{CONFIG}->{ROLE};

	$self->insert("TAG0", {path => $directory, tagName => $tagName, 
	     tableName => $tableName, user=>$user
	});
}

=item getDiskUsage($lfn)

Gets the disk usage of an entry (either file or directory)

=cut

sub getDiskUsage {
  my $self=shift;
  my $lfn=shift;
  my $options=shift;

  my $size=0;
  if ($lfn=~ m{/$}){
    $DEBUG and $self->debug(1, "Checking the diskusage of directory $lfn");
    my $hosts=$self->getHostsForEntry($lfn);
    my $sourceInfo=$self->getIndexHost($lfn);

    foreach my $entry (@$hosts){
      $DEBUG and $self->debug(1, "Checking in the table $entry->{hostIndex}");
      my ($db, $path2)=$self->reconnectToIndex( $entry->{hostIndex});
      $self=$db;
      my $pattern=$lfn;
      $pattern =~ s{^$entry->{lfn}}{};
      my $where="where lfn like '$pattern%'";
      $entry->{lfn}=~ m{^$lfn} and $where="where 1";
      $options =~ /f/ and $where.= " and type='f'" ;
      my $partialSize=$self->queryValue ("SELECT sum(size) from L$entry->{tableName}L $where");
      $DEBUG and $self->debug(1, "Got size $partialSize");
      $size+=$partialSize;
    }
    my ($db, $Path2)=$self->reconnectToIndex($sourceInfo->{hostIndex});
    $self=$db;
    
  } else {
    my $table="D$self->{INDEX_TABLENAME}->{name}L";
    $DEBUG and $self->debug(1, "Checking the diskusage of file $lfn");
    $size=$self->queryValue("SELECT size from $table where lfn='$lfn'");
  }
  
  return $size;
}

=item DropEmtpyDLTables

deletes the tables DL that are not being used

=cut

sub DropEmptyDLTables{
  my $self=shift;
  $self->info("Deleting the tables that are not being used");
  #updating the D0 of all the databases
  my ($hosts) = $self->getAllHosts("hostIndex");

  defined $hosts
    or return;
  my ( $oldHost, $oldDB, $oldDriver ) = ($self->{HOST}, $self->{DB}, $self->{DRIVER});
  my $tempHost;
  foreach $tempHost (@$hosts) {
    #my ( $ind, $ho, $d, $driv ) = split "###", $tempHost;
    my ($db, $path2)=$self->reconnectToIndex( $tempHost->{hostIndex} );
    $self=$db;

    my $tables=$self->queryColumn("show tables like 'D\%L'") or print STDERR "Warning: error connecting to $tempHost->{hostIndex}" and next;
    foreach my $t (@$tables) {
      
      $self->info("Checking $t");
      $t=~ /^D(\d+)L$/ or $self->info("skipping...") and next;
      my $number=$1;
      my $n=$self->queryValue("select count(*) from $t") and next;
      $self->info("We have to drop $t!! (there are $n in $t)");
      my $indexes=$self->queryColumn("SELECT lfn from INDEXTABLE where tableName=$number and hostIndex=$tempHost->{hostIndex}");
      if ($indexes) {
	foreach my $i (@$indexes) {
	  $self->info("Deleting index $i");
	  $self->deleteFromIndex($i);
	}
      }
      $self->do("DROP TABLE $t");

    }

  }

  $DEBUG and $self->debug(2, "Everything is done!!");

  return 1;

}

=item executeInAllDB ($method, @args)

This subroutine calls $method in all the databases that belong to the catalogue
If any of the calls fail, it returns udnef. Otherwise, it returns 1, and a list of the return of all the statements. 

At the end, it reconnects to the initial database

=cut

sub executeInAllDB{
  my $self=shift;
  my $method=shift;


  $DEBUG and $self->debug(1, "Executing $method (@_) in all the databases");
  my $hosts=$self->getAllHosts("hostIndex");
  my ( $oldHost, $oldDB, $oldDriver) = 
    ($self->{HOST}, $self->{DB}, $self->{DRIVER});

  my $error=0;
  my @return;
  foreach my $entry (@$hosts){
    $DEBUG and $self->debug(1, "Checking in the table $entry->{hostIndex}");
    my ($db, $path2)=$self->reconnectToIndex( $entry->{hostIndex});
    if (!$db){
      $error=1;
      last;
    }

    my $info=$db->$method(@_);
    if (!$info) {
      $error=1;
      last;
    }
    push @return, $info;
  }

  $error and return;
  $DEBUG and $self->debug(1, "Executing in all databases worked!! :) ");
  return 1, @return;

}


sub selectDatabase {
  my $self=shift;
  my $path=shift;

  #First, let's check the length of the lfn that matches
  my $entry=$self->getIndexHost($path);
  $entry or  $self->info("The path $path is not in the catalogue ") and return;

  my $index=$entry->{hostIndex};
  my $tableName="L$entry->{tableName}L";
  if ( !$index ) {
    $DEBUG and $self->debug(1, "Error no index!! SELECT hostIndex from D0 where path='$path'");
    return;
  }
  $DEBUG and $self->debug(1, "We want to contact $index  and we are  $self->{CURHOSTID}");
  
  my ($db, $path2)=$self->reconnectToIndex($index, $path) or return;

  $db->setIndexTable($tableName, $entry->{lfn});
  return $db;
}


sub getPathPrefix{
  my $self=shift;
  my $table=shift;
  my $host=shift;
  $table=~ s{^D(\d+)L}{$1};
  return $self->queryValue("SELECT lfn from INDEXTABLE where tableName='$table' and hostIndex='$host'");
}

sub findLFN {
  my $self=shift;
  my ($path, $file, $refNames, $refQueries,$refUnions, %options)=@_;

  #first, let's take a look at the host that we want

  my $rhosts=$self->getHostsForEntry($path) or 
    $self->info( "Error getting the hosts for '$path'") and return;

  my @result=();
  my @done=();
  foreach my $rhost (@$rhosts) {
    my $id="$rhost->{hostIndex}:$rhost->{tableName}";
    grep (/^$id$/, @done) and next;
    push @done, $id;
    my $localpath=$rhost->{lfn};

    $DEBUG and $self->debug(1, "Looking in database $id (path $path)");

    my ($db, $path2)=$self->reconnectToIndex( $rhost->{hostIndex}, "", );
    $db or $self->info( "Error connecting to $id ($path)") and next;
    $DEBUG and $self->debug(1, "Doing the query");

    push @result, $db->internalQuery($rhost,$path, $file, $refNames, $refQueries, $refUnions, \%options);
  }
  return \@result;
}

# This subroutine looks for files that satisfy certain criteria in the 
# current database.
# Input:
#    $path: directory where the 'find' started'
#    $name: name of files that we are looking for
#    $refNames: reference to a list of paths with the tags
#    $refQueries: reference to a list of queries of metadata"
#    $refunion: reference to a list of unions between the queries
#    $options: d->return also the directories
# Output:
#    list of file that satisfy all the criteria
sub internalQuery {
  my $self=shift;
  my $refTable=shift;
  my $path=shift;
  my $file=shift;

  my $refNames=shift;
  my $refQueries=shift;
  my $refUnions=shift;
  my $options=shift;
  my $selimit=shift || "";

  my $indexTable="L$refTable->{tableName}L";
  my $indexLFN=$refTable->{lfn};
  my @tagNames=@{$refNames};
  my @tagQueries=@{$refQueries};
  my @unions=("and", @{$refUnions});

  my @paths=();
  my @queries=();

  my @dirresults;

  my @joinQueries;

  foreach my $f (@$file){
    if ($f ne "\\" ) {
      my $searchP=$path;
      my $concat="concat('$refTable->{lfn}', lfn)";
      $searchP =~ s/^$refTable->{lfn}// and $concat="lfn";
      my $d = ("WHERE $concat LIKE '$searchP%$f%' and replicated=0");
      $options->{d} or $d.=" and right(lfn,1) != '/' and lfn!= \"\"";
      push @joinQueries, $d;
    } else {
      # query an exact file name
      push @joinQueries ,("WHERE concat('$refTable->{lfn}', lfn)='$path'");
    }
  }

  #First, let's construct the sql statements that will select all the files 
  # that we want. 
  $self->debug(1, "READY TO START LOOKING FOR THE TAGS");
  my $tagsDone={};
  foreach my $tagName (@tagNames) {
    my $union=shift @unions;
    my $query=shift @tagQueries;
    my @newQueries=();

    if ($tagsDone->{$tagName}){
      $self->info("The tag $tagName has already been selected. Just add the constraint");
      foreach my $oldQuery (@joinQueries){
	push @newQueries, "$oldQuery $union $query";
      }
    }
    else {
      $DEBUG and $self->debug(1, "Selecting directories with tag $tagName");
      #Checking which directories have that tag defined
      my $tables = $self->getFieldsByTagName($tagName, "tableName", 1, $refTable->{lfn});
      $tables and $#{$tables} != -1
	or $self->info( "Error: there are no directories with tag $tagName in $self->{DATABASE}->{DB}") 
	  and return;
      foreach  (@$tables) {
	my $table=$_->{tableName};
	$self->debug(1, "Doing the new table $table");
	foreach my $oldQuery (@joinQueries) {
	  #This is the query that will get all the results. We do a join between 
	  #the D0 table, and the one with the metadata. There will be two queries
	  #like these per table with that metadata. 
	  #The first query gets files under directories with that metadata. 
	  # It is slow, since it has to do string comperation
	  #The second query gets files with that metadata. 
	  # (this part is pretty fast)

	  if ($options->{'m'}){
	    $self->info("WE WANT EXACT FILES!!");
	    my $l=length($refTable->{lfn});
	    push @newQueries, " JOIN $table $oldQuery $union $table.$query and substring($table.file,$l+1)=l.lfn  and left($table.file,$l)='$refTable->{lfn}'";

	  } else{
	    push @newQueries, " JOIN $table $oldQuery $union $table.$query and $table.file like '%/' and concat('$refTable->{lfn}', l.lfn) like concat( $table.file,'%') ";
	    
	    my $length=length($refTable->{lfn})+1;
	    push @newQueries, " JOIN $table $oldQuery $union $table.$query and l.lfn=substring($table.file, $length) and left($table.file, $length-1)='$refTable->{lfn}'";
	  }
	}
      }
    }
    @joinQueries=@newQueries;
    $tagsDone->{$tagName}=1;
  }
  my $order=" ORDER BY lfn";
  my $limit="";
  $options->{'s'} and $order="";
  $options->{'y'} and $order="";
  $options->{l} and $limit = "limit $options->{l}";
  $options->{o} and $limit .= " offset $options->{o}";

  my $b="$binary2string";
  $b=~ s/guid/l.guid/;

  map {s/^(.*)$/SELECT *,concat('$refTable->{lfn}', lfn) as lfn,
$b as guid from $indexTable l $1 $order $limit/} @joinQueries;

  $self->debug(1,"We have to do $#joinQueries +1 to find out all the entries");
  if ($options->{selimit}) {
    $self->debug(1, "Displaying only the files in a particular se");
    my $GUIDList=$self->getPossibleGuidTables( $self->{INDEX_TABLENAME}->{name});
    my @newQueries;
    foreach my $entry (@$GUIDList){
      foreach my $query (@joinQueries){
	my $q=$query;
	$q =~ s/ from / from $entry->{db}.G$entry->{tableName}L g,$entry->{db}.G$entry->{tableName}L_PFN p, /;
	$q =~ s/ where / where g.guid=l.guid and p.guidid=g.guidid and senumber='$options->{selimit}' and /i;
	push @newQueries, $q;
      }
    }
    @joinQueries=@newQueries;
  }
  #Finally, let's do all the queries:
  my @result;
  foreach my $q (@joinQueries) {
    if ($options->{'y'}){
      my $t="";
      $q=~ /JOIN (\S+VCDB) /m and $t=$1;     
      if ($t){
        $self->info("WE ARE RETRIEVING ONLY THE BIGGEST METADADATA from $t");          
	      $q =~ s/select .*? from /select substr(max(version_path),10) lfn from (SELECT version_path,dir_number from /si;
	      $q.=")d  group by dir_number";
      }
    }
    $DEBUG and $self->debug(1, "Doing the query $q");
   
#    print "SKIPPING THE QUERIES '$q'\n";
    my $query=$self->query($q);
    push @result, @$query;
  }
  return @result;
  
}

sub setExpire{
  my $self=shift;
  my $lfn=shift;
  my $seconds=shift;
  defined $seconds or $seconds="";
  my $table=$self->{INDEX_TABLENAME}->{name};
  $lfn=~ s{^$self->{INDEX_TABLENAME}->{lfn}}{};

  my $expire="now()+$seconds";
  if ($seconds =~ /^-1$/){
    $expire="null";
  }else {
    $seconds=~ /^\d+$/ or 
      $self->info("The number of seconds ('$seconds') is not a number") 
	and return;
  }
  return $self->update($table, {expiretime=>$expire}, "lfn='$lfn'", {noquotes=>1});
}


sub getAllReplicatedData{
  my $self=shift;

  my $rusers = $self->getAllFromGroups("Username,Groupname,PrimaryGroup");
  defined $rusers
    or $self->info("Error: not possible to get all users")
      and return;

  my $rindexes =$self->getAllIndexes()  
    or $self->info( "Error: not possible to get mount points")
      and return;

  my $rses=$self->query("SELECT * from SE") or return;

  my $rhosts = $self->getAllHosts();
  defined $rhosts
    or $self->info("Error: not possible to get all hosts")
      and return;

  return {hosts=> $rhosts, users=>$rusers, indexes=>$rindexes, se=>$rses};
}

sub setAllReplicatedData {
  my $self=shift;
  my $info=shift;
  
  $info->{hosts} or $self->info("Error missing the hosts") and return;
  $info->{indexes} or $self->info("Error missing the hosts") and return;
  $info->{users} or $self->info("Error missing the hosts") and return;
  $info->{se} or $self->info("Error missing the hosts") and return;
  #First, all the hosts in HOSTS
  foreach my $rtempHost (@{$info->{hosts}}) {
      $self->insertHost($rtempHost->{hostIndex}, $rtempHost->{address}, $rtempHost->{db}, $rtempHost->{driver});
    }
  #Now, we should enter the data of D0
  foreach my $rdir (@{$info->{indexes}}) {
    $self->debug(1, "Inserting an entry in INDEXES");
    $self->do("INSERT INTO INDEXTABLE (hostIndex, tableName, lfn) values('$rdir->{hostIndex}', '$rdir->{tableName}', '$rdir->{lfn}')");
  }
    
    #Also, GROUPS table;
  foreach my $ruser (@{$info->{users}}) {
    $self->debug(1, "Adding a new user");
    $self->insertIntoGroups($ruser->{Username}, $ruser->{Groupname}, $ruser->{PrimaryGroup});
  }

    #and finally, the SE
  foreach my $se (@{$info->{se}}) {
    $self->debug(1, "Adding a new user");
    $self->insert("SE", $se);
  }
  return 1;
}


sub createCollection {
  my $self=shift;
  my $insert=shift;
  $insert->{type}='c';
  
  $self->_createEntry($insert,@_) or return;

  if (! $self->insert("COLLECTIONS", {collGUID=>$insert->{guid}}, {functions=>{collGUID=>'string2binary'}} )){
    $self->debug(2,"Here we have to remove the entry");
    my $tableRef=shift || {};
    my $tableName=$tableRef->{name} || $self->{INDEX_TABLENAME}->{name};
    my $tableLFN=$tableRef->{lfn} || $self->{INDEX_TABLENAME}->{lfn};
    $insert->{lfn} =~ s{^$tableLFN}{};
    $self->delete($tableName, "lfn='$insert->{lfn}'");
    return;
  }
  return 1;
}




sub _createEntry{
  my $self=shift;
  my $insert=shift;
  my $tableRef=shift || {};

  my $tableName=$tableRef->{name} || $self->{INDEX_TABLENAME}->{name};
  my $tableLFN=$tableRef->{lfn} || $self->{INDEX_TABLENAME}->{lfn};
  #  delete $insert->{table};
  
  $tableName =~ /^\d*$/ and $tableName="L${tableName}L";
  
  $insert->{dir}=$self->getParentDir($insert->{lfn});
  $insert->{lfn} =~ s{^$tableLFN}{};

  return $self->insert($tableName, $insert, {functions=>{guid=>"string2binary"}});
}


sub addFileToCollection {
  my $self=shift;
  my $filePerm=shift;
  my $collPerm=shift;
  my $info=shift || {};
  my $collId=$self->queryValue("SELECT collectionId from COLLECTIONS where collGUID=string2binary(?)", undef, {bind_values=>[$collPerm->{guid}]}) or
    $self->info("Error getting the collection id of $collPerm->{lfn}") and 
      return;

  $info->{collectionId}=$collId;
  $info->{origLFN}=$filePerm->{lfn};
  $info->{guid}=$filePerm->{guid};

  my $done=$self->insert("COLLECTIONS_ELEM", $info,
			 {functions=>{guid=>"string2binary"},silent=>1});

  if (!$done){
    if ( $DBI::errstr=~ /Duplicate entry '(\S+)'/ ){
      $self->info("The file '$filePerm->{guid}' is already in the collection $collPerm->{lfn}");
    }else {
      $self->info("Error doing the insert: $DBI::errstr");
    }

    return;
  }
  return 1;
}


sub  getInfoFromCollection {
  my $self=shift;
  my $collGUID=shift;
  $self->debug(1,"Getting all the info of collection '$collGUID'");
  return $self->query("SELECT origLFN, $binary2string as guid,data, localName from COLLECTIONS c, COLLECTIONS_ELEM e where c.collectionId=e.collectionId and collGUID=string2binary(?)", undef, {bind_values=>[$collGUID]});
}

sub removeFileFromCollection{
  my $self=shift;
  my $permFile=shift;
  my $permColl=shift;
  $self->debug(1, "Ready to delete the entry from $permColl->{lfn}");
  my $collId=$self->queryValue("SELECT collectionId from COLLECTIONS where collGUID=string2binary(?)", undef, {bind_values=>[$permColl->{guid}]}) or
    $self->info("Error getting the collection id of $permColl->{lfn}") and 
      return;

  my $deleted=$self->delete("COLLECTIONS_ELEM","collectionId=? and guid=string2binary(?)", {bind_values=>[$collId, $permFile->{guid}]});
  if ($deleted =~ /^0E0$/){
    $self->info("The file '$permFile->{guid}' is not in that collection");
    return;
  }
  return $deleted;
}


sub renumberLFNtable {
  my $self=shift;
  my $table=shift || $self->{INDEX_TABLENAME}->{name};
  my $options=shift || {};
  $self->info("How do we renumber '$table'??");

  
  my $info=$self->query("select ${table}d.entryId as t from $table ${table}d left join $table ${table}r on ${table}d.entryId-1=${table}r.entryId where ${table}r.entryId is null order by t asc");

#  print Dumper($info);
  #Let's do this part before dropping the index
  my @newlist;
  my $reduce=0;

  print Dumper(@newlist);
  while (@$info){
    my $entry=shift @$info;
    my $r=$self->queryValue("select max(entryId) from $table where entryId<?", undef, {bind_values=>[$entry->{t}]});
    if (!$r){
      #If this is the first value of the table
      $entry->{t}<2 and next;
      $r=0;
    }
    $r=$entry->{t}-$r-1;
    $reduce+=$r;
    my $max=undef;
    $info and ${$info}[0] and $max=${$info}[0]->{t};
    push @newlist, {min=>$entry->{t}, reduce=>$reduce, max=>$max};
  }
  if ($options->{n}){
    $self->info("Just informing what we would do...");
    foreach my $entry (@newlist){
      my $message="For entries bigger than $entry->{min}, we should reduce by $entry->{reduce}";
      $entry->{max} and $message.=" (up to $entry->{max}";
      $self->info($message);
    }
    return 1;
  }
  #  print Dumper(@newlist);
  defined $options->{locked} or  $self->lock($table);
  $self->info("There are $#newlist +1 entries that need to be fixed");
#  $self->do("alter table $table modify entryId bigint(11),drop primary key");
#  $self->do("alter table $table drop primary key");
  my $changes=0;
  foreach my $entry( @newlist){
    my $message="For entries bigger than $entry->{min}, we should reduce by $entry->{reduce}";
    $entry->{max} and $message.=" (up to $entry->{max}";
    $self->info($message);
    my $max1="";
    my $max2="";
    my $bind=[$entry->{reduce}, $entry->{min}];
    if ($entry->{max}){
      $max1= "and dir<?";
      $max2="and entryId<?";
      $bind=[$entry->{reduce}, $entry->{min}, $entry->{max}];
    }
    my $done=$self->do("update $table set dir=dir-? where dir>=? $max1", {bind_values=>$bind});
    my $done2=$self->do("update $table set entryId=entryId-? where entryId>=? $max2 order by entryId", {bind_values=>$bind});
    ($done and $done2) or 
      $self->info("ERROR !!") and last;
    $changes=1;
  }
  if ($options->{min} and $options->{min}>1){
    $self->info("And now, updating the minimun (to $options->{min}");
    $self->do("update $table set entryId=entryId+$options->{min}-1, dir=dir+$options->{min}-1 order by entryId");
    $changes=1;
  }
#  $self->do("alter table $table modify entryId bigint(11) auto_increment primary key");
  if ($changes){
    $self->do("alter table $table auto_increment=1");
    $self->do("optimize table $table");
  }
  defined $options->{locked} or $self->unlock($table);

  return 1;
}




sub cleanupTagValue{
  my $self=shift;
  my $directory=shift;
  my $tag=shift;

  my $tags=$self->getFieldsByTagName($tag, "tableName", 1, $directory  )
    or $self->info("Error getting the directories for $tag and $directory")
      and return;

  my $dirs=$self->getHostsForEntry($directory)
    or $self->info("Error getting the hosts of $directory") and return;

  foreach my $tag (@$tags){
    $self->info("First, let's delete duplicate entries");
    $self->lock($tag->{tableName});
    $self->do("create temporary table $tag->{tableName}temp select file as f, max(entryId) as e from $tag->{tableName} group by file");
    $self->do("delete from  $tag->{tableName} using  $tag->{tableName},  $tag->{tableName}temp where file=f and entryId<e");
    $self->do("drop temporary table  $tag->{tableName}temp");
    $self->unlock($tag->{tableName});
    foreach my $host (@$dirs){
      $self->info("Deleting the entries from $tag->{tableName} that are not in $host->{tableName} (like $host->{lfn})");
      my @bind=($host->{lfn},$host->{lfn});
      my $where=" and file like concat(?,'%') ";
      foreach my $entry (@$dirs){
	$entry->{lfn} =~ /^$host->{lfn}./ or next;
	$self->info("$entry->{lfn} is a subdirectory!!");
	$where.=" and file not like concat(?,'%') ";
	push @bind, $entry->{lfn};
      }
      $self->do("delete from $tag->{tableName} using $tag->{tableName} left join L$host->{tableName}L on file=concat(?, lfn) where lfn is null $where", {bind_values=>\@bind});
    }

  }

  return 1;
}

sub getNumberOfEntries {
  my $self=shift;
  my $entry=shift;
  my $options=shift;
  my ($db, $path2)=$self->reconnectToIndex( $entry->{hostIndex}) or return -1;
  my $query="SELECT COUNT(*) from L$entry->{tableName}L";
  $options =~ /f/ and $query.=" where right(lfn,1) != '/'";
  return $db->queryValue($query);
}

sub updateStats {
  my $self=shift;
  my $table=shift;
  $self->info("Let's update the statistics of the table $table");

  $table =~ /^L/ or $table="L${table}L";
  my $number=$table;
  $number=~ s/L//g;
  $self->do("delete from LL_ACTIONS where action='STATS' and tableNumber=?", {bind_values=>[$number]});

  $self->do("insert into LL_ACTIONS(tablenumber, time, action, extra) select ?,max(ctime),'STATS', count(*) from L${number}L",
	    {bind_values=>[$number]});

  my $oldGUIDList=$self->getPossibleGuidTables($number);
  $self->do("delete from LL_STATS where tableNumber=?", {bind_values=>[$number]});
  $self->do("insert into LL_STATS (tableNumber, max_time, min_time) select  ?, concat(conv(conv(max(guidtime),16,10)+1,10,16),'00000000') max, concat(min(guidtime),'00000000')  min from $table", {bind_values=>[$number]});
  my $newGUIDList=$self->getPossibleGuidTables($number);
  
  my $done={};
  my @bind=();
  my $values="";
  my $total= $#$oldGUIDList +  $#$newGUIDList +2 ;
  $self->info("In total, there are $total guid tables affected");
  my $lfnRef="$self->{CURHOSTID}_$number";
  foreach my $elem (@$oldGUIDList, @$newGUIDList){
    $done->{$elem->{indexId}} and next;
    $done->{$elem->{indexId}}=1;
    $values.=" (?, 'TODELETE'), ";
    push @bind, $elem->{tableName};
    $self->info("Doing $elem->{tableName}");
    my $gtable="$elem->{db}.G$elem->{tableName}L";

    if ($elem->{address} eq $self->{HOST}){
      $self->debug(1, "This is the same host. It is easy");

      my $maxGuidTime=$self->queryValue("select left(min(guidTime),8) from GUIDINDEX where guidTime> (select guidTime from GUIDINDEX where tableName=?  and hostindex=?)", undef, {bind_values=>[ $elem->{tableName}, $elem->{hostIndex}]});
      my $query="insert into ${gtable}_REF(guidid,lfnRef) select g.guidid, ? from $gtable g join $table l using (guid) left join ${gtable}_REF r on g.guidid=r.guidid and lfnref=? where r.guidid is null and l.guidtime>=(select left(guidtime,8) from GUIDINDEX where tablename=? and hostIndex=? )";
      my $bind=[$lfnRef, $lfnRef, $elem->{tableName}, $elem->{hostIndex}];
      if ($maxGuidTime){
	$self->info("The next guid is $maxGuidTime");
	$query.=" and l.guidTime<?";
	push @$bind, $maxGuidTime;
      }
      $self->do("delete from ${gtable}_REF using ${gtable}_REF left join $gtable using (guidid) left join $table l using (guid) where l.guid is null and lfnRef=?", {bind_values=>[$lfnRef]});
      $self->do($query, {bind_values=>$bind});
    }else {
      $self->info("This is in another host. We can't do it easily :( 'orphan guids won't be detected'");
      my ($db, $path2)=$self->reconnectToIndex( $elem->{hostIndex}) or next;
      $db->do("update  $gtable g, $table l set lfnRef=concat(lfnRef,  ?, ',') where g.guid=l.guid and g.lfnRef not like concat(',',?,',')", {bind_values=>[$number,$number]});
    }
  }
  if ($values){
    $self->info("And now, let's put the guid tables in the list of tables that have to be checked");
    $values=~ s/, $//;
    $self->do("insert ignore into GL_ACTIONS(tableNumber, action) values $values", {bind_values=>[@bind]});
  }
  return 1;

}

sub getPossibleGuidTables{
  my $self=shift;
  my $number=shift;
  $number =~ s/L//g;

  return $self->query("select * from (select * from GUIDINDEX where guidTime< (select max_time from  LL_STATS where tableNumber=?)  and  guidTime>(select min_time from LL_STATS where tableNumber=?)  union select * from GUIDINDEX where guidTime= (select max(guidTime) from GUIDINDEX where guidTime< (select min_time from LL_STATS where tableNumber=?))) g, HOSTS h where g.hostIndex=h.hostIndex", undef, {bind_values=>[$number, $number, $number]});

}


=head1 SEE ALSO

AliEn::Database

=cut

sub getAllHostAndTable{
  my $self=shift;

  my $result = $self->query("SELECT distinct hostIndex, tableName from INDEXTABLE");
  defined $result
    or $self->info("Error: not possible to get all the pair of host and table")
      and return;

  return $result;
}

sub fquota_update {
  my $self = shift;
  my $size = shift;
  my $count = shift;

  my $user=$self->{CONFIG}->{ROLE};

  (defined $size) and (defined $count) or $self->info("Update fquota : not enough parameters") and return;

  $self->{PRIORITY_DB} or $self->{PRIORITY_DB}=AliEn::Database::TaskPriority->new({ROLE=>'admin',SKIP_CHECK_TABLES=> 1});
  $self->{PRIORITY_DB} or return;
  $self->{PRIORITY_DB}->do("UPDATE PRIORITY SET nbFiles=nbFiles+tmpIncreasedNbFiles+?, totalSize=totalSize+tmpIncreasedTotalSize+?, tmpIncreasedNbFiles=0, tmpIncreasedTotalSize=0 WHERE user=?", {bind_values=>[$count,$size,$user]}) or return;

  return 1;
}

1;

