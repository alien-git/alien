#/**************************************************************************
# * Copyright(c) 2001-2002, ALICE Experiment at CERN, All rights reserved. *
# *                                                                        *
# * Author: The ALICE Off-line Project / AliEn Team                        *
# * Contributors are mentioned in the code where appropriate.              *
# *                                                                        *
# * Permission to use, copy, modify and difstribute this software and its   *
# * documentation strictly for non-commercial purposes is hereby granted   *
# * without fee, provided that the above copyright notice appears in all   *
# * copies and that both the copyright notice and this permission notice   *
# * appear in the supporting documentation. The authors make no claims     *
# * about the suitability of this software for any purpose. It is          *
# * provided "as is" without express or implied warranty.                  *
# **************************************************************************/
package AliEn::Database::mysql;

use strict;
use AliEn::Database;

#use DBI;
use AliEn::Config;

use AliEn::Logger::LogObject;
use vars qw($DEBUG @ISA );

$DEBUG = 0;

=head1 NAME

AliEn::Database::mysql - database interface for mysql driver for AliEn system 

=head1 DESCRIPTION

This module implements the database wrapper in case of using the driver mysql. Sytanx and structure are different for each engine. This affects the code. The rest of the modules should finally abstract from SQL code. Therefore, instead we would ideally use calls to functions implemented in this module - case of mysql.

=cut

=item C<reservedWord>

  $res = $dbh->reservedWord($word);

=cut

sub reservedWord {
  my $self = shift;
  my $word = shift;
  return $word;
}

=item C<preprocessFields>

$res = $dbh->preprocessFields($keys);

=cut

sub preprocessFields {
  my $self  = shift;
  my $new_keys = shift;
 return $new_keys;
}

=item C<createTable>

  $res = $dbh->createTable($word);

=cut
sub createTable {
  my $self = shift;
  my $table = shift;
  my $definition = shift;
  my $checkExists = shift || "";

  $DEBUG and $self->debug(1,"Database: In createTable creating table $table with definition $definition.");

  my $out;

  $checkExists
    and $checkExists = "IF NOT EXISTS";
 
  $self->_do("CREATE TABLE $checkExists $table $definition DEFAULT CHARACTER SET latin1 COLLATE latin1_general_cs")
    or $self->{LOGGER}->error("Database","In createTable unable to create table $table with definition $definition")
      and return;

  1;
}


sub checkTable {
  my $self=shift;
  my $table=shift;
  my $desc=shift;
  my $columnsDef=shift;
  my $primaryKey=shift;
  my $index=shift;
  my $options=shift;
  

  my %columns=%$columnsDef;
  my $engine="";
  $options->{engine} and $engine = " engine=$options->{engine} ";
  $desc = "$desc $columns{$desc}";
  $self->createTable( $table, "($desc) $engine", 1 ) or return;

  my $alter=$self->getNewColumns($table, $columnsDef);

  if ($alter) {
  $self->lock($table);

  #let's get again the description
  $alter = $self->getNewColumns( $table, $columnsDef );
  my $done = 1;
  if ($alter) {

  #  chop($alter);
  $self->info("Updating columns of table $table");
  $done = $self->alterTable( $table, $alter );
  }
  $self->unlock($table);
  $done or return;
  }

  #Ok, now let's take a look at the primary key
  #$primaryKey or return 1;

  $self->setPrimaryKey( $table, $desc, $primaryKey, $index );

#  $desc =~ /not null/i or $self->{LOGGER}->error("Database", "Error: the table $table is supposed to have a primary key, but the index can be null!") and return;
}
sub gestTypes{return 1;}
=item C<getNewColumns>

  $res = $dbh->getNewColumns($table,$columnsDef);

=cut

sub getNewColumns {
  my $self       = shift;
  my $table      = shift;
  my $columnsDef = shift;
  my %columns    = %$columnsDef;

  my $queue = $self->describeTable($table);

  defined $queue
    or return;

  foreach (@$queue) {
    delete $columns{ $_->{Field} };
    delete $columns{ lc( $_->{Field} ) };
    delete $columns{ uc( $_->{Field} ) };
  }

  my $alter = "";

  foreach ( keys %columns ) {
    $alter .= "ADD $_ $columns{$_} ,";
  }
  chop($alter);

  return ( 1, $alter );
}

=item C<getIndexes>

  $res = $dbh->getIndexes($table,);

Returns the keys of the table $table

=cut

sub getIndexes {
  my $self  = shift;
  my $table = shift;
  return $self->query("SHOW KEYS FROM $table");
}

=item C<dropIndex>

  $res = $dbh->dropIndex($index,$table,);

Drop the index $index from the database.

=cut

sub dropIndex {
  my $self  = shift;
  my $index = shift;
  my $table = shift;
  $self->do("drop index $index on $table");
}

=item C<createIndex>

  $res = $dbh->createIndex($index,$table,);

Create the index for the table. 

=cut

sub createIndex {
  my $self  = shift;
  my $index = shift;
  my $table = shift;
  $self->alterTable( $table, "ADD $index" );
}



=item C<getLastId>

  $res = $dbh->getLastId($table,);

get the last id of the latest row inserted

=cut

sub getLastId {
  my $self = shift;
  my $id   = $self->{DBH}->{'mysql_insertid'};
  return $id;
}

=item C<getConnectionChain>

  $res = $dbh->getConnectionChain($table,);

get the combination for connecting through DBI

=cut

sub getConnectionChain {
  my $self   = shift;
  my $driver = shift || $self->{DRIVER};
  my $db     = shift || $self->{DB};
  my $host   = shift || $self->{HOST};
  my $dsn    = "DBI:$driver:$db;host=$host";
  if ( $self->{SSL} ) {

    my $cert = $ENV{X509_USER_CERT}
      || "$ENV{ALIEN_HOME}/globus/usercert.pem";
    my $key = $ENV{X509_USER_KEY} || "$ENV{ALIEN_HOME}/globus/userkey.pem";
    $DEBUG
      and $self->debug( 1,
      "Authenticating with the certificate in $cert and $key" );
    $dsn .=
      ";mysql_ssl=1;mysql_ssl_client_key=$key;mysql_ssl_client_cert=$cert";
  }
  return $dsn;
}

sub existsTable {
  my $self = shift;
  my $table = shift;
  return $self->queryValue("SHOW TABLES like '$table'");
}





sub describeTable {
  my $self = shift;
  my $table = shift;

  $self->_queryDB("DESCRIBE $table");
}
sub grantAllPrivilegesToUser {
  my $self = shift;
  my $user = shift;
  my $db = shift;
  my $table = shift;

  $self->grantPrivilegesToUser(["ALL PRIVILEGES ON $db.$table"],$user);
}

sub grantPrivilegesToUser {
  my $self = shift;
  my $rprivs = shift;
  my $user = shift;
  my $pass = shift;
  my $origpass=$pass;
  $DEBUG and $self->debug(1, "In grantPrivilegesToUser");
  $pass
  	and $pass = "$user IDENTIFIED BY '$pass'"
	or $pass = $user;

  my $success = 1;
  for (@$rprivs) {
    $DEBUG and $self->debug (0, "Adding privileges $_ to $user");
    $self->_do("GRANT $_ TO $pass")
      or $DEBUG and $self->debug (0, "Error adding privileges $_ to $user")
      and $success = 0;
  }
  return $success;
}

sub revokeAllPrivilegesFromUser {
  my $self = shift;
  my $user = shift;
  my $db = shift;
  my $table = shift;
  $self->revokePrivilegesFromUser(["ALL PRIVILEGES ON $db.$table"], $user);
}

sub revokePrivilegesFromUser {
  my $self = shift;
  my $rprivs = shift;
  my $user = shift;

  my $success = 1;
  for (@$rprivs) {
    $DEBUG and $self->debug(1, "Revoking privileges $_ of $user");
    $self->_do("REVOKE $_ FROM $user")
      or $DEBUG and $self->debug (0, "Error revoking privileges $_ of $user")
      and $success = 0;
  }
  return $success;
}

sub renameField {
  my $self  = shift;
  my $table = shift;
  my $old   = shift;
  my $new   = shift;
  my $desc  = shift;
  $self->do("ALTER TABLE $table CHANGE $old $new $desc");
}

sub binary2string {
  my $self = shift;
  my $column = shift || "guid";
  return
"insert(insert(insert(insert(hex($column),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')";
}

sub createLFNfunctions {
  my $self = shift;
  $self->do("create function string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))");
  $self->do("create function binary2string (my_uuid binary(16)) returns varchar(36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')");
  $self->do("create function binary2date (my_uuid binary(16))  returns char(16) deterministic sql security invoker
return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))");
  $DEBUG and $self->debug(2,"In createCatalogueTables creation of tables finished.");
  $self->do("alter table TAG0 drop key path");
  $self->do("alter table TAG0 add index path (path)");

}

sub createGUIDFunctions {
  my $self = shift;
  $self->do(
"create function string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))"
  );
  $self->do(
"create function binary2string (my_uuid binary(16)) returns varchar(36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')"
  );

  $self->do(
"create function string2date (my_uuid varchar(36)) returns char(16) deterministic sql security invoker return upper(concat(right(left(my_uuid,18),4), right(left(my_uuid,13),4),left(my_uuid,8)))"
  );

  $self->do(
"create function binary2date (my_uuid binary(16))  returns char(16) deterministic sql security invoker
return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))"
  );
}

sub lock {
  my $self  = shift;
  my $table = shift;

  $DEBUG and $self->debug( 1, "Database: In lock locking table $table." );

  $self->_do("LOCK TABLE $table WRITE");
}

sub unlock {
  my $self  = shift;
  my $table = shift;

  $DEBUG and $self->debug( 1, "Database: In lock unlocking tables." );

  $table and $table = " $table"
    or $table = "S";

  $self->do("UNLOCK TABLES");
}

sub optimizeTable {
  my $self  = shift;
  my $table = shift;
  $self->info("Ready to optimize the table $table (from $self->{DB})");
  if (
    $self->queryValue(
"SELECT count(*) FROM information_schema.TABLES where table_schema=? and table_name=? 
                          and (data_free > 100000000 or data_free/data_length>0.1)",
      undef, { bind_values => [ $self->{DB}, $table ] }
    )
    )
  {
    $self->info("We have to optimize the table");
    $self->do("optimize table $table");
  }
  return 1;
}

sub dbGetAllTagNamesByPath {
  my $self    = shift;
  my $path    = shift;
  my $options = shift || {};

  my $rec  = "";
  my $rec2 = "";
  my @bind = ($path);
  if ( $options->{r} ) {
    $rec = " or path like concat(?, '%') ";
    push @bind, $path;
  }
  if ( $options->{user} ) {
    $self->debug( 1, "Only for the user $options->{user}" );
    $rec2 = " and user=?";
    push @bind, $options->{user};
  }
  $self->query(
"SELECT tagName,path from TAG0 where (? like concat(path,'%') $rec) $rec2 group by tagName",
    undef,
    { bind_values => \@bind }
  );
}

sub paginate {
  my $self   = shift;
  my $sql    = shift;
  my $limit  = shift;
  my $offset = shift;
  $limit  and $sql .= " limit $limit";
  $offset and $sql .= " offset $offset";
  return $sql;
}

sub _timeUnits {
  my $self = shift;
  my $s    = shift;
  return $s;
}


sub dateFormat {
  my $self = shift;
  my $col  = shift;
  return "DATE_FORMAT($col, '%b %d %H:%i') as $col";
}

sub regexp {
  my $self    = shift;
  my $col     = shift;
  my $pattern = shift;
  return "$col rlike '$pattern'";
}



sub schema {
  my $self = shift;
  $DEBUG and $self->debug( 2, "getting schema in mysql module" );
  return $self->{DB};
}

sub quote_query {
  return;
}

sub process_bind {
  return;
}

sub addTimeToToken {
  my $self  = shift;
  my $user  = shift;
  my $hours = shift;
  return $self->do(
"update TOKENS set Expires=(DATE_ADD(now() ,INTERVAL $hours HOUR)) where Username='$user'"
  );

}

sub preprocess_where_delete {
  my $self  = shift;
  my $where = shift;
  return $where;
}

sub _getAllObjects {
  my $self   = shift;
  my $schema = shift;
  return "$schema\.\*";
}
sub grant {
  my $self=shift;
  my $grant=shift;
  
  return $self->do("grant $grant");
}

sub grantPrivilegesToObject {
  my $self        = shift;
  my $privs       = shift;
  my $schema_from = shift;
  my $object =
    shift;    # if object == * that means all the objects in the schema.
  my $user_to = shift;
  my $pass    = shift;

  $DEBUG and $self->debug( 1, "In grantPrivilegesToObject" );
  $pass and $pass = "$user_to IDENTIFIED BY '$pass'"
    or $pass = $user_to;
  my $success = 1;

  $object = "$schema_from\.$object";
  $DEBUG and $self->debug( 0, "Adding privileges $privs to $user_to" );

#print "inside granting privileges doing: GRANT $privs ON $object TO $pass and this is the database handler: " ;
#$self->_connect and;
  $self->do("GRANT $privs ON $object TO $pass")
    or $DEBUG
    and $self->debug( 0, "Error adding privileges $privs to $user_to" )
    and $success = 0;
  return $success;
}

sub renumberTable {
  my $self    = shift;
  my $table   = shift;
  my $index   = shift;
  my $options = shift || {};

  my $lock = "$table";
  $options->{lock} and $lock = "$options->{lock} $lock";
  my $info = $self->queryValue("select max($index)-count(1) from $table");
  $info or $info = 0;
  if ( $info < 100000 ) {
    $self->debug( 1, "Only $info. We don't need to renumber" );
    return 1;
  }

  $self->info("Let's renumber the table $table");

  $self->lock($lock);
  my $ok = 1;
  $self->do(
"alter table $table modify $index int(11), drop primary key,  auto_increment=1, add new_index int(11) auto_increment primary key, add unique index (guidid)"
  ) or $ok = 0;
  if ($ok) {
    foreach my $t ( @{ $options->{update} } ) {
      $self->debug( 1, "Updating $t" );
      $self->do(
"update $t set $index= (select new_index from $table where $index=$t.$index)"
      ) and next;
      $self->info("Error updating the table!!");
      $ok = 0;
      last;
    }
  }
  if ($ok) {
    $self->info("All the renumbering  worked! :)");
    $self->do(
"alter table $table drop column $index, change new_index $index int(11) auto_increment"
    );
  }
  else {
    $self->info("The update didn't work. Rolling back");
    $self->do(
"alter table $table drop new_index, modify $index int(11) auto_increment primary key"
    );
  }

  $self->unlock($table);

  return 1;

}

sub _deleteFromTODELETE {
  my $self = shift;
  $self->do(
"delete  from TODELETE  using TODELETE join SE s on TODELETE.senumber=s.senumber where sename='no_se' and pfn like 'guid://%'"
  );

}

sub getTransfersForOptimizer {
  my $self = shift;
  return $self->query(
"SELECT transferid FROM TRANSFERS_DIRECT where (status='ASSIGNED' and  ctime<SUBTIME(now(), SEC_TO_TIME(1800))) or (status='TRANSFERRING' and from_unixtime(started)<SUBTIME(now(), SEC_TO_TIME(14400)))"
  );
}

sub getToStage {
  my $self = shift;
  return $self->query(
"select s.queueid, jdl from STAGING s, QUEUE q where s.queueid=q.queueid and timestampadd(MINUTE, 5, staging_time)<now()"
  );

}

sub unfinishedJobs24PerUser {
  my $self = shift;
  return $self->do(
"update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, count(1) as unfinishedJobsLast24h from QUEUE q where (status='INSERTING' or status='WAITING' or status='STARTED' or status='RUNNING' or status='SAVING' or status='OVER_WAITING') and (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user set pr.unfinishedJobsLast24h=IFNULL(C.unfinishedJobsLast24h, 0)"
  );
}


sub cpuCost24PerUser {
  my $self = shift;
  return $self->do(
"update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, sum(p.cost) as totalCpuCostLast24h , sum(p.runtimes) as totalRunningTimeLast24h  from QUEUE q join QUEUEPROC p using(queueId) where (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user set pr.totalCpuCostLast24h=IFNULL(C.totalCpuCostLast24h, 0)"
  );
}

sub changeOWtoW {
  my $self = shift; 
  return $self->do(
"update QUEUE q join PRIORITY pr on pr.user=SUBSTRING( q.submitHost, 1, POSITION('\@' in q.submitHost)-1 ) set q.status='WAITING' where (pr.totalRunningTimeLast24h<pr.maxTotalRunningTime and pr.totalCpuCostLast24h<pr.maxTotalCpuCost) and q.status='OVER_WAITING'"
  );
}

sub changeWtoOW {
  my $self = shift;
  return $self->do(
"update QUEUE q join PRIORITY pr on pr.user=SUBSTRING( q.submitHost, 1, POSITION('\@' in q.submitHost)-1 ) set q.status='OVER_WAITING' where (pr.totalRunningTimeLast24h>=pr.maxTotalRunningTime or pr.totalCpuCostLast24h>=pr.maxTotalCpuCost) and q.status='WAITING'"
  );
}

sub updateFinalPrice {
  my $self     = shift;
  my $t        = shift;
  my $nominalP = shift;
  my $now      = shift;
  my $done     = shift;
  my $failed   = shift;
  my $update =
" UPDATE $t q, QUEUEPROC p SET finalPrice = round(p.si2k * $nominalP * price),chargeStatus=\'$now\'";
  my $where =
" WHERE (status='DONE' AND p.si2k>0 AND chargeStatus!=\'$done\' AND chargeStatus!=\'$failed\') and p.queueid=q.queueid";
  my $updateStmt = $update . $where;
  return $self->do($updateStmt);

}

sub optimizerJobExpired {
  return
"( ( (status='DONE') || (status='FAILED') || (status='EXPIRED') || (status like 'ERROR%')  ) && ( received < (? - 7*86540) ) )";
}

sub optimizerJobPriority {
  my $self = shift;
  my $userColumn = shift;
  return $self->do(
"INSERT IGNORE INTO PRIORITY(user, priority, maxparallelJobs, nominalparallelJobs) SELECT distinct $userColumn, 1,200, 100 from QUEUE"
  );
}

sub userColumn {
  return
"SUBSTR( submitHost, 1, POSITION('\@' in submitHost)-1  )";
}

sub getMessages {
  my $self    = shift;
  my $service = shift;
  my $host    = shift;
  my $time    = shift;
  return $self->query(
"SELECT ID,TargetHost,Message,MessageArgs from MESSAGES WHERE TargetService = ? AND  ? like TargetHost AND (Expires > ? or Expires = 0) order by ID limit 300",
    undef,
    { bind_values => [ $service, $host, $time ] }
  );
}

sub createUser {
  my $self = shift;
  my $user = shift;
  my $pwd  = shift;
  return $self->do("create user \'$user\' IDENTIFIED BY \'$pwd\'");
}

sub execHost {
  return "substr(exechost,POSITION('\\\@' in exechost)+1)";
}

sub currentDate {
  return " now() ";
}

sub resetAutoincrement{
 my $self = shift;
 my $table = shift;
 return $self->do("alter table $table auto_increment=1");
 
}

###########################
#Functions specific for AliEn/Catalogue/Admin
##########################
sub refreshSERank {
  my $self   = shift;
  my $site   = shift;
  my $rank   = shift;
  my $seName = shift;
  $self->do(
    "insert into SERanks (sitename,seNumber,rank,updated)
  select ?, seNumber,  ?, 0  from SE where seName LIKE ?",
    { bind_values => [ $site, $rank, $seName ] }
  );

}
#####
###Specific for Database/Catalogue/GUID
###

sub insertLFNBookedDeleteMirrorFromGUID{
  my $self = shift; 
  my $table = shift;
  my $lfn = shift; 
  my $guid = shift;
  my $role = shift;
  my $pfn = shift;
  my $guidId= shift;
  my $seNumber = shift;
  return $self->do("INSERT IGNORE INTO LFN_BOOKED(lfn, owner, expiretime, size, guid, gowner, user, pfn, se)
      select ?,g.owner,-1,g.size,string2binary(?),g.gowner,?,?,s.seName
      from ".$table." g, ".$table."_PFN g_p, SE s
      where g.guidId=g_p.guidId and g_p.guidId=? and g_p.seNumber=? and g_p.pfn=? and s.seNumber=g_p.seNumber",
      {bind_values=>[$lfn,$guid,$role,$pfn,$guidId,$seNumber,$pfn]});
    
}
####
# Specific for Database/IS
##
sub insertLFNBookedRemoveDirectory{
  my $self=shift;
  my $lfn = shift;
  my $tableName = shift,
  my $user = shift;
  my $tmpPath = shift;

 return $self->do("INSERT IGNORE INTO LFN_BOOKED(lfn, owner, expiretime, size, guid, gowner, user, pfn)
     SELECT concat('$lfn' , l.lfn), l.owner, -1, l.size, l.guid, l.gowner, ?,'*' FROM $tableName l WHERE l.type='f' AND l.lfn LIKE concat (?,'%')",    {bind_values=>[$user,$tmpPath]}) ;

}

###
##Specific for Catalogue/Authorize
###
sub insertLFNBookedAndOptionalExistingFlagTrigger{
  my $self=shift;
  my $lfn = shift;#$envelope->{lfn};
  my $user = shift;#$user;
  my $quota = shift;#, "1" 
  my $md5sum=shift;#,$envelope->{md5}
  my $expiretime = shift; #$lifetime,
  my $size = shift;#$envelope->{size},
  my $pfn = shift;#$envelope->{turl},
  my $se = shift;#$envelope->{se},$user,
  my $guid = shift;# $envelope->{guid},
  my $existing = shift; #$trigger,
  my $jobid = shift; #$jobid;

 
  return $self->do(
    "REPLACE INTO LFN_BOOKED (lfn, owner, quotaCalculated, md5sum, expiretime, size, pfn, se, gowner, guid, existing, jobid) VALUES (?,?,?,?,?,?,?,?,?,string2binary(?),?,?);"
    ,{bind_values=>[$lfn,$user, $quota ,$md5sum,$expiretime,$size,$pfn,$se,$user,$guid,$existing,$jobid]});
}

sub dbGetSEListFromSiteSECacheForWriteAccess{
 
   my $self=shift;
   my $user=shift ;
   my $fileSize=shift;
   my $type=shift;
   my $count=shift ;
   my $sitename=shift ;
   my $excludeList=(shift || "");
   
   my $query="SELECT DISTINCT SE.seName FROM SERanks,SE WHERE "
       ." sitename=? and SERanks.seNumber = SE.seNumber ";

   my @queryValues = ();
   push @queryValues, $sitename;

   foreach(@$excludeList){   $query .= "and SE.seName<>? "; push @queryValues, $_;  }
   
   $query .=" and SE.seMinSize <= ? and SE.seQoS  LIKE concat('%,' , ? , ',%' ) "
    ." and (SE.seExclusiveWrite is NULL or SE.seExclusiveWrite = '' or SE.seExclusiveWrite  LIKE concat ('%,' , ? , ',%') )"
    ." ORDER BY rank ASC limit ? ;";
 
   push @queryValues, $fileSize;
   push @queryValues, $type;
   push @queryValues, $user;
   push @queryValues, $count;

   return $self->queryColumn($query, undef, {bind_values=>\@queryValues});
   
}



##############
###optimizer Catalogue /SeSize
#############
sub updateVolumesInSESize{
  my $self = shift;

   $self->do("update SE, SE_VOLUMES set usedspace=seusedspace/1024, freespace=size-usedspace where  SE.sename=SE_VOLUMES.sename and size!= -1");
  return;
}
sub showLDLTables{
  my $self = shift;
  return $self->queryColum("show tables like 'L\%L'");
}
sub updateSESize{
 my $self = shift;
 return $self->do("update SE, SE_VOLUMES set usedspace=seusedspace/1024, freespace=size-usedspace where  SE.sename=SE_VOLUMES.sename and size!= -1"); 
 
}


#######
## optimizer Job/priority
#####
sub getPriorityUpdate{
my $self = shift;
my $userColumn = shift;
return "update PRIORITY p left join 
(select SUBSTRING( submitHost, 1, POSITION('@' in submitHost)-1 ) user ,count(*) w from QUEUE where status='WAITING' group by SUBSTRING( 
submitHost, 1, POSITION('@' in submitHost)-1 ) )  b using (user)
 left join (select SUBSTRING( submitHost, 1, POSITION('@' in submitHost)-1 ) user,count(*) r from QUEUE where (status='RUNNING' or status='STARTED' 
or status='SAVING') group by SUBSTRING( submitHost, 1, POSITION('@' in submitHost)-1 ) ) b2 using (user) 
 set waiting=coalesce(w,0), running=COALESCe(r,0) , 
userload=(running/maxparallelJobs), 
computedpriority=(if(running<maxparallelJobs, if((2-userload)*priority>0,50.0*(2-userload)*priority,1),1))" ;
 }
sub getJobAgentUpdate{
my $self = shift;
my $userColumn = shift;
return "UPDATE JOBAGENT j set j.priority=(SELECT computedPriority-(min(queueid)/(SELECT ifnull(max(queueid),1) from QUEUE)) from PRIORITY p, QUEUE q where j.entryId=q.agentId and status='WAITING' and $userColumn=p.user group by agentId)";
}

########
## optimizer Job/Expired
####

#sub getJobOptimizerExpiredQ1{
#  my $self = shift;
# return "where  (status in ('DONE','FAILED','EXPIRED') or status like 'ERROR%'  ) and ( mtime < addtime(now(), '-10 00:00:00')  and split=0) )";
#}

sub getJobOptimizerExpiredQ2{
  my $self = shift;
 return " left join QUEUE q2 on q.split=q2.queueid where q.split!=0 and q2.queueid is null and q.mtime<addtime(now(), '-10 00:00:00')";
}
sub getJobOptimizerExpiredQ3{
  my $self = shift;
 return "where mtime < addtime(now(), '-10 00:00:00') and split=0";
}

########
### optimizer Job/Zombies
####

sub getJobOptimizerZombies{
  my $self = shift;
  my $status = shift; 
 return "q, QUEUEPROC p where $status and p.queueId=q.queueId and DATE_ADD(now(),INTERVAL -3600 SECOND)>lastupdate";
}

########
### optimizer Job/Charge
####

sub getJobOptimizerCharge{
  my $self = shift;
  my $queueTable = shift;
  my $nominalPrice = shift; 
  my $chargingNow=shift;
  my $chargingDone=shift;
  my $chargingFailed = shift;
  my $update = " UPDATE $queueTable q, QUEUEPROC p SET finalPrice = round(p.si2k * $nominalPrice * price),chargeStatus=\'$chargingNow\'";
  my $where  = " WHERE (status='DONE' AND p.si2k>0 AND chargeStatus!=\'$chargingDone\' AND chargeStatus!=\'$chargingFailed\') and p.queueid=q.queueid";
  return $update.$where;
}
1;

