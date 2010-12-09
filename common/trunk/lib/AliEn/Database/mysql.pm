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

sub createLFNTables{
  my $self = shift;


  $DEBUG and $self->debug(2,"In createCatalogueTables creating all tables...");


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
            undef, ['PRIMARY KEY(lfn,pfn,guid)','INDEX(pfn)','INDEX(lfn)', 'INDEX(guid)','INDEX(expiretime)']],
        PFN_TODELETE=>[ "pfn", {pfn=>"varchar(255)", retry=>"integer not null"}, undef, ['UNIQUE INDEX(pfn)']]
           );
  foreach my $table (keys %tables){
    $self->info("Checking table $table");
    $self->checkTable($table, @{$tables{$table}}) or return;
  }

  $self->checkLFNTable("0") or return;
  $self->do("INSERT IGNORE INTO ACTIONS(action) values  ('PACKAGES')");
 
  1;
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


sub createGUIDTables {
  my $self = shift;


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
  
  my %columns = (seName=>"varchar(60) character set latin1 collate latin1_general_ci NOT NULL", 
		 seNumber=>"int(11) NOT NULL auto_increment primary key",
		 seQoS=>"varchar(200)",
		 seioDaemons=>"varchar(255)",
		 seStoragePath=>"varchar(255)",
		 seNumFiles=>"bigint",
		 seUsedSpace=>"bigint",
		 seType=>"varchar(60)",
		 seMinSize=>"int default 0",
                 seExclusiveWrite=>"varchar(300)",
                 seExclusiveRead=>"varchar(300)",
                 seVersion=>"varchar(300)",
		);

  return $self->checkTable("SE", "seNumber", \%columns, 'seNumber', ['UNIQUE INDEX (seName)'], {engine=>"innodb"} ); #or return;

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

sub createAdminTables{
  my $self = shift;
  $self->checkTable("USERS_LDAP", "user",{user=>"varchar(15) not null",
					  dn=>"varchar(255)",
					  up=>"smallint"}) or return;
  
  
  $self->checkTable("USERS_LDAP_ROLE", "user",{user=>"varchar(15) not null",
					       role=>"varchar(15)",
					       up=>"smallint"}) or return;
  $self->checkTable("TOKENS", "ID", {ID=>"int(11) not null auto_increment primary key",
				     "Username","varchar(16)",
				     "Expires","datetime",
				     "Token"=>"varchar(32)",
				     "password"=>"varchar(16)",
				     "SSHKey"=>"text",
				     "dn"=>"varchar(255)",
				    }) or return;
  $self->checkTable("DBKEYS", "Name", {"Name"=> "varchar(20) NOT NULL DEFAULT ''",
				       "DBKey"=>"blob",
				       "LastChanges"=>"datetime NOT NULL DEFAULT '0000-00-00 00:00:00'"
				      }) or return;
  $self->checkTable("jobToken", "jobId", { "jobId"=>"int(11) NOT NULL DEFAULT '0' PRIMARY KEY",
					   "userName"=>"char(20) DEFAULT NULL",
					   "jobToken"=>"char(255) DEFAULT NULL",
					 }) or return;
  return 1;
}

sub createTaskQueueTables{
  my $self = shift;
  my $queueColumns={columns=>{queueId=>"int(11) not null auto_increment primary key",
			      execHost=>"varchar(64)",
			      submitHost=>"varchar(64)",
			      priority =>"tinyint(4)",
			      status  =>"varchar(12)",
			      command =>"varchar(255)",
			      commandArg =>"varchar(255)",
			      name =>"varchar(255)",
			      path =>"varchar(255)",
			      current =>"varchar(255)",
			      received =>"int(20)",
			      started =>"int(20)",
			      finished =>"int(20)",
			      expires =>"int(10)",
			      error =>"int(11)",
			      validate =>"int(1)",
			      sent =>"int(20)",
			      jdl =>"text collate latin1_general_ci",
			      site=> "varchar(40)",
			      node=>"varchar(64)",
			      spyurl=>"varchar(64)",
			      split=>"int",
			      splitting=>"int",
			      merging=>"varchar(64)",
			      masterjob=>"int(1) default 0",
			      price=>"float",
			      chargeStatus=>"varchar(20)",
			      optimized=>"int(1) default 0",
			      finalPrice=>"float",
			      notify=>"varchar(255)",
			      agentid=>'int(11)',
			      mtime=>'timestamp',
            },
		    id=>"queueId",
		    index=>"queueId",
		    extra_index=>["INDEX (split)", "INDEX (status)", "INDEX(agentid)", "UNIQUE INDEX (submitHost,queueId)","INDEX(priority)",
				  "INDEX (site,status)",
				  "INDEX (sent)",
				  "INDEX (status,submitHost)",
				  "INDEX (status,agentid)",
				  "UNIQUE INDEX (status,queueId)"
				 ]
		   };
  my $queueColumnsProc={columns=>{queueId=>"int(11) not null auto_increment primary key",
				  runtime =>"varchar(20)",
				  runtimes =>"int",
				  cpu =>"float",
				  mem =>"float",
				  cputime =>"int",
				  rsize =>"int",
				  vsize =>"int",
				  ncpu =>"int",
				  cpufamily =>"int",
				  cpuspeed =>"int",
				  cost =>"float",
				  maxrsize =>"float",
				  maxvsize =>"float",
				  procinfotime =>"int(20)",
				  si2k=>"float",
				  lastupdate=>"timestamp",
				  batchid=>"varchar(255)",
				 },
			id=>"queueId",
			index=>"queueId"};
  my $tables={ QUEUE=>$queueColumns,
	       QUEUEPROC=>$queueColumnsProc,
	       QUEUEEXPIRED=>$queueColumns,
	       QUEUEEXPIREDPROC=>$queueColumnsProc,

	       $self->{QUEUEARCHIVE}=>$queueColumns,
	       $self->{QUEUEARCHIVEPROC}=>$queueColumnsProc,

	       JOBAGENT=>{columns=>{entryId=>"int(11) not null auto_increment primary key",
				    requirements=>"text not null",
				    counter=>"int(11) not null default 0",
				    afterTime=>"time",
				    beforeTime=>"time",
				    priority=>"int(11)",
				   },
			  id=>"entryId",
			  index=>"entryId",
			  extra_index=>["INDEX(priority)"],
			 },
	       SITES=>{columns=>{siteName=>"char(255)",
				 siteId =>"int(11) not null auto_increment primary key",
				 masterHostId=>"int(11)",
				 adminName=>"char(100)",
				 location=>"char(255)",
				 domain=>"char(30)",
				 longitude=>"float",
				 latitude=>"float",
				 record=>"char(255)",
				 url=>"char(255)",},
		       id=>"siteId",
		       index=>"siteId",
		       },
	       HOSTS=>{columns=>{commandName=>"char(255)",
				 hostName=>"char(255)",
				 hostPort=>"int(11) not null ",
				 hostId =>"int(11) not null auto_increment primary key",
				 siteId =>"int(11) not null",
				 adminName=>"char(100) not null",
				 maxJobs=>"int(11) not null",
				 status=>"char(10) not null",
				 date=>"int(11)",
				 rating=>"float not null",
				 Version=>"char(10)",
				 queues=>"char(50)",
				 connected=>"int(1)",
				 maxqueued=>"int(11)",
				},
		       id=>"hostId",
		       index=>"hostId"
		      },
	       MESSAGES=>{columns=>{ ID            =>" int(11) not null  auto_increment primary key",
				     TargetHost    =>" varchar(100)",
				     TargetService =>" varchar(100)",
				     Message       =>" varchar(100)",
				     MessageArgs   =>" varchar(100)",
				     Expires       =>" int(11)",
				     Ack=>         =>'varchar(255)'},
			  id=>"ID",
			  index=>"ID",},
	       JOBMESSAGES=>{columns=> {entryId=>" int(11) not null  auto_increment primary key",
					jobId =>"int", 
					procinfo=>"varchar(200)",
					tag=>"varchar(40)", 
					timestamp=>"int", },
			     id=>"entryId", 
			    },

	       JOBSTOMERGE=>{columns=>{masterId=>"int(11) not null primary key"},
			     id=>"masterId"},
	       STAGING=>{columns=>{queueid=>"int(11) not null primary key",
				  staging_time=>"timestamp"},
			 id=>"queueid"},
			
	     };
  foreach my $table  (keys %$tables) {
    $self->checkTable($table, $tables->{$table}->{id}, $tables->{$table}->{columns}, $tables->{$table}->{index}, $tables->{$table}->{extra_index})
      or $self->{LOGGER}->error("TaskQueue", "Error checking the table $table") and return;
  }

}
sub checkSiteQueueTable{
  my $self = shift;
  $self->{SITEQUEUETABLE} = (shift or "SITEQUEUES");

  my %columns = (		
		 site=> "varchar(40) not null",
		 cost=>"float",
		 status=>"varchar(20)",
		 statustime=>"int(20)",
		 blocked =>"varchar(10)",
		 maxqueued=>"int",
		 maxrunning=>"int",
		 queueload=>"float",
		 runload=>"float",
		 jdl => "text",
                 jdlAgent => 'text',
		 timeblocked=>"datetime", 
		);

  foreach (@{AliEn::Util::JobStatus()}) {
    $columns{$_}="int";
  }
  $self->checkTable($self->{SITEQUEUETABLE}, "site", \%columns, "site");
}
sub checkActionTable {
  my $self=shift;

  my %columns= (action=>"char(40) not null primary key",
		todo=>"int(1) not null default 0");
  $self->checkTable("ACTIONS", "action", \%columns, "action") or return;
  return $self->do("INSERT IGNORE INTO ACTIONS(action) values  ('INSERTING'), ('MERGING'), ('KILLED'), ('SPLITTING'), ('STAGING')");
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

