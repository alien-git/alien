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


sub resetAutoincrement {
  my $self  = shift;
  my $table = shift;
  $self->do("ALTER TABLE $table auto_increment=1");
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
    $self->info("We have to optimize the table - Disabled!");
    #$self->do("optimize table $table");
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


sub getMessages {
  my $self    = shift;
  my $service = shift;
  my $host    = shift;
  my $time    = shift;
  return $self->query(
"SELECT ID,TargetHost,Message,MessageArgs from MESSAGES WHERE TargetService = ? AND  ? like TargetHost AND (Expires > ? or Expires = 0) order by ID",
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
1;

