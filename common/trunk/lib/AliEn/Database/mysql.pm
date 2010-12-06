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
#push @ISA='AliEn::Database';

#sub new {
#  my $proto = shift;
#  my $self  = (shift or AliEn::Database::new() );
#  $self->{HOST}="localhost";
#  $self->{DB}="SQL";
#  $self->{DRIVER}="mysql";
#  $self->{TABLES}={};

 # $self->{CONFIG}=new AliEn::Config();
 # $self->{LOGGER}=new AliEn::Logger();
#  $self->{DIRECTORY}=$self->{CONFIG}->{TMP_DIR};
#  $self->debug(1, "Using the SQLite database in $self->{DIRECTORY}");
#return (bless($proto,$self));
 # return AliEn::Database::new($proto, $self, @_);
#}

=item C<reservedWord>

  $res = $dbh->reservedWord($word);

=cut

sub reservedWord {
	my $self = shift;
	my $word = shift;
	return $word;
}

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
=item C<preprocess_fields>

  $res = $dbh->preprocess_fields($keys);

=cut

sub preprocess_fields {
	my $self     = shift;
	my $new_keys = shift;
	return $new_keys;
}



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
#sub collateCS{
#return " collate latin1_general_cs";
#}

#sub collateCI{
#return " collate latin1_general_ci";
#}
sub getTypes {
	my $self = shift;

	$self->{TYPES} = {
		'serial'    => 'serial',
		'char'      => 'char',
		'binary'    => 'binary',
		'number'    => 'int',
		'tinyint'   => 'tinyint',
		'text'      => 'text',
		'bigint'    => 'bigint',
		'mediumint' => 'mediumint',
		'smallint'  => 'smallint',
		'date'      => 'datetime',
		'text'      => 'text'
	};
	return 1;
}

#sub setAutoincrement{
#return " auto_increment ";
#}
sub resetAutoincrement {
	my $self  = shift;
	my $table = shift;
	$self->do("ALTER TABLE $table auto_increment=1");
}

sub _queryDB{
  my ($self,$stmt, $options) = @_;
  $options or $options={};
  my $oldAlarmValue = $SIG{ALRM};
  local $SIG{ALRM} = \&_timeout;

  local $SIG{PIPE} =sub {
    print STDERR "Warning!! The connection to the AliEnProxy got lost\n";
    $self->reconnect();
  };

  $self->_pingReconnect or return;

  my $arrRef;
  my $execute;
  my @bind;
  $options->{bind_values} and push @bind, @{$options->{bind_values}};
  $DEBUG and $self->debug(2,"In _queryDB executing $stmt in database (@bind).");

  while (1) {
    my $sqlError="";
    eval {
      alarm(600);
      my $sth = $self->{DBH}->prepare_cached($stmt);
      #      my $sth = $self->{DBH}->prepare($stmt);
      $DBI::errstr and $sqlError.="In prepare: $DBI::errstr\n";
      if ($sth){
	$execute=$sth->execute(@bind);
	$DBI::errstr and $sqlError.="In execute: $DBI::errstr\n";
	$arrRef = $sth->fetchall_arrayref({});
	$DBI::errstr and $sqlError.="In fetch: $DBI::errstr\n";
	
#	$sth->finish;
#	$DBI::errstr and $sqlError.="In finish: $DBI::errstr\n";
      }
    };
    $@ and $sqlError="The command died: $@";
    alarm(0);

    if ($sqlError) {
      my $found=0;
      $sqlError =~ /(Unexpected EOF)|(Lost connection)|(Constructor didn't return a handle)|(No such object)|(Connection reset by peer)|(MySQL server has gone away at)|(_set_fbav\(.*\): not an array ref at)|(Constructor didn't return a handle)/ and $found=1;

      if ($sqlError =~ /Died at .*AliEn\/UI\/Catalogue\.pm line \d+/) {
	die("We got a ctrl+c... :( ");
      }
      if ($sqlError =~ /Maximum message size of \d+ exceeded/) {
	$self->info("ESTAMOS AQUI\n");
      }
      $found or $self->info("There was an SQL error: $sqlError",1001) and return;
    }
    #If the statment got executed, we can exit the loop
    $execute and last;

    $self->reconnect or $self->info( "The reconnection did not work") and return;
  }

  $oldAlarmValue
    and $SIG{ALRM} = $oldAlarmValue
      or delete $SIG{ALRM};


  $DEBUG and $self->debug(1,"Query $stmt successfully executed. ($#{$arrRef}+1 entries)");
  return $arrRef;
}

sub _do{
  my $self = shift;
  my $stmt = shift;
  my $options=(shift or {});

  my $oldAlarmValue = $SIG{ALRM};
  local $SIG{ALRM} = \&_timeout;

  local $SIG{PIPE} =sub {
    print STDERR "Warning!! The connection to the AliEnProxy got lost while doing an insert\n";
    $self->reconnect();
  };

  $DEBUG and $self->debug(2,"In _do checking is database connection still valid");

  $self->_pingReconnect or return;
  my @bind_values;
  $options->{bind_values} and push @bind_values, @{$options->{bind_values}} and $options->{prepare}=1;
  my $result;

  while (1) {
    my $sqlError="";

    $result = eval {
      alarm(600);
      my $tmp;
      if ($options->{prepare}) {
	$DEBUG and $self->debug(2,"In _do doing $stmt @bind_values");
	my $sth = $self->{DBH}->prepare_cached($stmt);
	$tmp = $sth->execute(@bind_values);
      }else {
	$DEBUG and $self->debug(1,"In _do doing $stmt @bind_values");
	$tmp=$self->{DBH}->do($stmt);
      }
      $DBI::errstr and $sqlError.="In do: $DBI::errstr\n";
      $tmp;
    };
    my $error=$@;
    alarm(0);
    if ($error) {
      $sqlError.="There is an error: $@\n";
      $options->{silent} or $self->info("There was an SQL error  ($stmt): $sqlError",1001);
      return;
    }
    defined($result) and last;

    if ($sqlError) {
      my $found=0;
      $sqlError=~ /(Unexpected EOF)|(Lost connection)|(MySQL server has gone away at)|(Connection reset by peer)/ and $found=1;
      if (!$found) {
	$oldAlarmValue
	  and $SIG{ALRM} = $oldAlarmValue
	    or delete $SIG{ALRM};
	chomp $sqlError;
	$options->{silent} or 
	  $self->info("There was an SQL error  ($stmt): $sqlError",1001);
	return;
      }
    }

    $self->reconnect() or return;
  }

  $oldAlarmValue
    and $SIG{ALRM} = $oldAlarmValue
      or delete $SIG{ALRM};
  
  $DEBUG and $self->debug(1, "Query $stmt successfully executed with result: $result");

  $result;
}
sub grantAllPrivilegesToUser {
	my $self  = shift;
	my $user  = shift;
	my $db    = shift;
	my $table = shift;
	$db =~ s/(.*):(.*)/$2/i;
	$self->grantPrivilegesToUser( ["ALL PRIVILEGES ON $db.$table"], $user );
}
##only works for mysql!!
sub grantPrivilegesToUser {
	my $self     = shift;
	my $rprivs   = shift;
	my $user     = shift;
	my $pass     = shift;
	my $origpass = $pass;
	$DEBUG and $self->debug( 1, "In grantPrivilegesToUser" );
	$pass and $pass = "$user IDENTIFIED BY '$pass'"
	  or $pass = $user;

	my $success = 1;
	for (@$rprivs) {
		$DEBUG and $self->debug( 0, "Adding privileges $_ to $user" );
		$self->_do("GRANT $_ TO $pass")
		  or $DEBUG
		  and $self->debug( 0, "Error adding privileges $_ to $user" )
		  and $success = 0;
	}
	return $success;
}

sub revokeAllPrivilegesFromUser {
	my $self  = shift;
	my $user  = shift;
	my $db    = shift;
	my $table = shift;
	$self->revokePrivilegesFromUser( ["ALL PRIVILEGES ON $db.$table"], $user );
}

sub revokePrivilegesFromUser {
	my $self   = shift;
	my $rprivs = shift;
	my $user   = shift;

	my $success = 1;
	for (@$rprivs) {
		$DEBUG and $self->debug( 1, "Revoking privileges $_ of $user" );
		$self->_do("REVOKE $_ FROM $user")
		  or $DEBUG
		  and $self->debug( 0, "Error revoking privileges $_ of $user" )
		  and $success = 0;
	}
	return $success;
}
sub describeTable {
  my $self = shift;
  my $table = shift;

  $self->_queryDB("DESCRIBE $table");
}
sub createLFNtables {
	my $self = shift;

}

sub createGUIDtables {
	my $self = shift;
	my $db = shift || $self;

	my %tables = (
		HOSTS => [
			"hostIndex",
			{
				hostIndex    => "serial primary key",
				address      => "char(50)",
				db           => "char(40)",
				driver       => "char(10)",
				organisation => "char(11)",
			},
			"hostIndex"
		],
		ACL => [
			"entryId",
			{
				entryId => "int(11) NOT NULL auto_increment primary key",
				owner   => "char(10) NOT NULL",
				perm    => "char(4) NOT NULL",
				aclId   => "int(11) NOT NULL",
			},
			'entryId'
		],
		GROUPS => [
			"Username",
			{
				Username     => "char(15) NOT NULL",
				Groupname    => "char (85)",
				PrimaryGroup => "int(1)",
			},
			'Username'
		],
		GUIDINDEX => [
			"indexId",
			{
				indexId   => "int(11) NOT NULL auto_increment primary key",
				guidTime  => "char(16)",
				hostIndex => "int(11)",
				tableName => "int(11)",
			},
			'indexId',
			['UNIQUE INDEX (guidTime)']
		],
		TODELETE => [
			"entryId",
			{
				entryId  => "int(11) NOT NULL auto_increment primary key",
				pfn      => "varchar(255)",
				seNumber => "int(11) not null",
				guid     => "binary(16)"
			}
		],
		GL_STATS => [
			"tableNumber",
			{
				tableNumber => "int(11) NOT NULL",
				seNumber    => "int(11) NOT NULL",
				seNumFiles  => "bigint(20)",
				seUsedSpace => "bigint(20)",
			},
			undef,
			['UNIQUE INDEX(tableNumber,seNumber)']
		],
		GL_ACTIONS => [
			"tableNumber",
			{
				tableNumber => "int(11) NOT NULL",
				action      => "char(40) not null",
				time        => "timestamp default current_timestamp",
				extra       => "varchar(255)",
			},
			undef,
			['UNIQUE INDEX(tableNumber,action)']
		],
	);

	foreach my $table ( keys %tables ) {
		$self->info("Checking table $table");
		$self->checkTable( $table, @{ $tables{$table} } )
		  or return;
	}

}

sub checkGUIDTable {
	my $self  = shift;
	my $table = shift;
	defined $table
	  or $self->info("Error: we didn't get the table number to check")
	  and return;
	my $db = shift || $self;

	$table =~ /^\d+$/ and $table = "G${table}L";

	my %columns = (
		guidId           => "int(11) NOT NULL auto_increment primary key",
		ctime            => "timestamp",
		expiretime       => "datetime",
		size             => "bigint not null default 0",
		seStringlist     => "varchar(255) not null default ','",
		seAutoStringlist => "varchar(255) not null default ','",
		aclId            => "int(11)",
		perm             => "char(3)",
		guid             => "binary(16)",
		md5              => "varchar(32)",
		ref              => "int(11) default 0",
		owner            => "varchar(20)",
		gowner           => "varchar(20)",
		type             => "char(1)",
	);

	$db->checkTable( ${table}, "guidId", \%columns, 'guidId',
		[ 'UNIQUE INDEX (guid)', 'INDEX(seStringlist)', 'INDEX(ctime)' ],
	) or return;

	%columns = (
		pfn      => 'varchar(255)',
		guidId   => "int(11) NOT NULL",
		seNumber => "int(11) NOT NULL",
	);
	$db->checkTable(
		"${table}_PFN",
		"guidId",
		\%columns,
		undef,
		[
			'INDEX guid_ind (guidId)',
			"FOREIGN KEY (guidId) REFERENCES $table(guidId) ON DELETE CASCADE",
			"FOREIGN KEY (seNumber) REFERENCES SE(seNumber) on DELETE CASCADE"
		],
	) or return;

	$db->checkTable(
		"${table}_REF",
		"guidId",
		{
			guidId => "int(11) NOT NULL",
			lfnRef => "varchar(20) NOT NULL"
		},
		'',
		[
			'INDEX guidId(guidId)',
			'INDEX lfnRef(lfnRef)',
			"FOREIGN KEY (guidId) REFERENCES $table(guidId) ON DELETE CASCADE"
		]
	) or return;

	$db->checkTable(
		"${table}_QUOTA",
		"user",
		{
			user      => "varchar(64) NOT NULL",
			nbFiles   => "int(11) NOT NULL",
			totalSize => "bigint(20) NOT NULL"
		},
		undef,
		['INDEX user_ind (user)'],
	) or return;

	$db->optimizeTable($table);
	$db->optimizeTable("${table}_PFN");

	my $index = $table;
	$index =~ s/^G(.*)L$/$1/;

#$db->do("INSERT IGNORE INTO GL_ACTIONS(tableNumber,action)  values  (?,'SE')", {bind_values=>[$index, $index]});

	return 1;

}

sub checkSETable {
	my $self = shift;

	my %columns = (
		seName =>
		  "varchar(60) character set latin1 collate latin1_general_ci NOT NULL",
		seNumber         => "int(11) NOT NULL auto_increment primary key",
		seQoS            => "varchar(200)",
		seioDaemons      => "varchar(255)",
		seStoragePath    => "varchar(255)",
		seNumFiles       => "bigint",
		seUsedSpace      => "bigint",
		seType           => "varchar(60)",
		seMinSize        => "int default 0",
		seExclusiveWrite => "varchar(300)",
		seExclusiveRead  => "varchar(300)",
		seVersion        => "varchar(300)",
	);

	return $self->checkTable( "SE", "seNumber", \%columns, 'seNumber',
		['UNIQUE INDEX (seName)'], { engine => "innodb" } );    #or return;

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
	$self->do(
"create function string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))"
	);
	$self->do(
"create function binary2string (my_uuid binary(16)) returns $self->{TYPES}->{varchar} (36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')"
	);
	$self->do(
"create function binary2date (my_uuid binary(16))  returns char(16) deterministic sql security invoker
return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))"
	);
}

sub createGUIDfunctions {
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
	$offset and $sql .= "offset $offset";
	return $sql;
}

sub _timeUnits {
	my $self = shift;
	my $s    = shift;
	return $s;
}

#sub defineAutoincrement{
#return 1;}

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

#sub getIgnore{
#return " IGNORE ";
#}
#sub characterSetCI{
#return "DEFAULT CHARACTER SET latin1 collate latin1_general_ci";
#}
#sub characterSetCS{
#	return " DEFAULT CHARACTER SET latin1 COLLATE latin1_general_cs";
#}

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
"update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, count(1) as unfinishedJobsLast24h from QUEUE q where (status='INSERTING' or status='WAITING' or status='STARTED' or status='RUNNING' or status='SAVING' or status='OVER_WAITING') and (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user collate latin1_general_cs set pr.unfinishedJobsLast24h=IFNULL(C.unfinishedJobsLast24h, 0)"
	);
}

sub totalRunninTimeJobs24PerUser {
	my $self = shift;
	return $self->do(
"update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, sum(p.runtimes) as totalRunningTimeLast24h from QUEUE q join QUEUEPROC p using(queueId) where (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user collate latin1_general_cs set pr.totalRunningTimeLast24h=IFNULL(C.totalRunningTimeLast24h, 0)"
	);
}

sub cpuCost24PerUser {
	my $self = shift;
	return $self->do(
"update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, sum(p.cost) as totalCpuCostLast24h from QUEUE q join QUEUEPROC p using(queueId) where (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user collate latin1_general_cs set pr.totalCpuCostLast24h=IFNULL(C.totalCpuCostLast24h, 0)"
	);
}

sub changeOWtoW {
	my $self = shift;
	return $self->do(
"update QUEUE q join PRIORITY pr on pr.user=SUBSTRING( q.submitHost, 1, POSITION('\@' in q.submitHost)-1 ) collate latin1_general_cs set q.status='WAITING' where (pr.totalRunningTimeLast24h<pr.maxTotalRunningTime and pr.totalCpuCostLast24h<pr.maxTotalCpuCost) and q.status='OVER_WAITING'"
	);
}

sub changeWtoOW {
	my $self = shift;
	return $self->do(
"update QUEUE q join PRIORITY pr on pr.user=SUBSTRING( q.submitHost, 1, POSITION('\@' in q.submitHost)-1 ) collate latin1_general_cs set q.status='OVER_WAITING' where (pr.totalRunningTimeLast24h>=pr.maxTotalRunningTime or pr.totalCpuCostLast24h>=pr.maxTotalCpuCost) and q.status='WAITING'"
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
	my $userColumn =
	  "SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 )";
	return $self->do(
"INSERT IGNORE INTO PRIORITY(user, priority, maxparallelJobs, nominalparallelJobs) SELECT distinct $userColumn, 1,200, 100 from QUEUE"
	);
}

sub userColumn {
	return
"SUBSTR( submitHost, 1, POSITION('\@' in submitHost)-1  )   collate latin1_general_cs";
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

sub setUpdateDefault {
	my $self  = shift;
	my $table = shift;
	my $col   = shift;
	my $val   = shift;
	my $desc  = shift;
	return $self->do(
		"ALTER TABLE $table MODIFY $col $desc ON UPDATE  " . $val );
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

