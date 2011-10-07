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
package AliEn::Database;

use DBI;

#use Cache::File;

use strict;

require AliEn::Config;

require AliEn::SOAP;
require AliEn::Logger::LogObject;

use vars qw($DEBUG @ISA $INDEX);

$DEBUG = 0;
push @ISA, "AliEn::Logger::LogObject";

=head1 NAME

AliEn::Database - database wrapper for AliEn system with caching capabilities

=head1 DESCRIPTION

The AliEn::Database module is designed to provide layer between DBI layer and application
layer. Beside wrapping, AliEn::Database module provide possibility of data caching.
Module stores query results on filesystem so it's possible for different processes
to use same query results.

AliEn::Database module is specially designed for AliEn system and uses modules for
validation of user. User can be validated by providing his password or token.

There are several classes that inherit from the basic AliEn::Database, specific to each
of the possible database schemas in AliEn: Catalogue, IS, TaskQueue, Transfer

=head1 SYNOPSIS

  use AliEn::Database;

  my $dbh = AliEn::Database->new({
    USER=>$user,
    PASSWD=>$pass,
    HOST=>$host,
    DB=>$db,
    DRIVER=>$driver});

  my $dbh = AliEn::Database->new({
    USER=>$user,
    PASSWD=>$pass,
    HOST=>$host,
    DB=>$db,
    DRIVER=>$driver,
    USE_CACHE=>1,
    CACHE_DIR=>/tmp/DBCache
    CACHE_SIZE=>10000} );

  $arrRef = $dbh->query($statement);
  $hashRef = $dbh->queryRow($statement);
  $arrRef = $dbh->queryColumn($statement);
  $res = $dbh->queryValue($statement);

  $res = $dbh->do($statement);

  $res = $dbh->insert($table,$fieldsRef);
  $res = $dbh->update($table,$fieldsRef,$where);
  $res = $dbh->delete($table,$where);

  $res = $dbh->createTable($table,$spec,$checkExists);
  $res = $dbh->alterTable($table,$colSpec);
  $res = $dbh->dropTable($table);
  $arrRef = $dbh->describeTable($table);
  $res = $dbh->existsTable($table);

  $res = $dbh->checkColumn($table,$colName,$colSpec);

  $dbh->lock($tableName);
  $dbh->unlock($tableName);

  $res = $dbh->reconnect($newHost,$newDB,$newDriver);

  $res = $dbh->changeUser($newUser,$newPassword);
  $res = $dbh->changeRole($newRole,$newPassword);

  $dbh->destroy;

=cut

####################################################################
####################################################################

###### public methods

####################################################################
####################################################################

=head1 METHODS

=over

=item C<new>

  $dbh = AliEn::Database->new( $attr );

  $dbh = AliEn::Database->new( $attr, $attrDBI );

Creates new AliEn::Database instance. Arguments are AliEn::Database attributes which will be
explained separately and attributes for DBI module. Attributes for DBI are optional. Default
attribute for DBI is C<PrintError=>>C<0>.

By calling C<new> actual connection to database is not created. In case when cache is
used connection will be created first time when cache miss happens. If cache is
not used connection will be created on first C<query> or C<_do> call.
C<new> will also validate user and report error on STDERR if user is not allowed to connect
to database.

=cut

sub new {
  my $proto = shift;

  my $class = ref($proto) || $proto;
  my $self = (shift or {});

  #  my $attr    = shift;
  my $attrDBI = shift;
  $self = bless($self, $class);

  $self->SUPER::new() or print "NOPE\n" and return;

  $self->{CONFIG} or $self->{CONFIG} = new AliEn::Config();

  $self->{CONFIG}
    or $self->{LOGGER}->error("Database", "Initial configuration not found.")
    and return;
  bless($self, $class);

  $self->preConnect() or return;

  ($self->{DB} and $self->{HOST} and $self->{DRIVER})
    or $self->{LOGGER}->error("Database", "No database specified.")
    and return;
  ##we need to map the types for each database.

  my $driver = $self->{DRIVER};
  my $name   = "AliEn::Database::";
  if ($driver =~ /^Oracle$/i or $driver =~ /^mysql$/i or $driver =~ /^SQLite$/i) {
    $name .= $driver;
    eval "require $name" or print "Could not requiere $name\n" and exit(-2);
    @ISA = ($name, @ISA);
    if ($driver =~ /Oracle/) {
      $self->SUPER::getTypes;
      $INDEX = 0;

      defined $self->{ORACLE_USER} or $self->{ORACLE_USER} = "ALIEN";
    }
  }

  my %defaults = (
    USE_PROXY          => 1,
    USER               => $self->{CONFIG}->{LOCAL_USER},
    ROLE               => $self->{CONFIG}->{LOCAL_USER},
    SILENT             => 0,
    USE_CACHE          => 0,
    DEBUG              => 0,
    DEFAULT_TTL        => "1 hour",
    CACHE_SIZE         => "4000000",
    CACHE_DIR          => "$self->{CONFIG}->{TMP_DIR}/DBCache",
    PASSWD             => ($self->{CONFIG}->{PASSWD} || ""),
    TOKEN              => ($self->{CONFIG}->{TOKEN} || ""),
    FORCED_AUTH_METHOD => ($self->{CONFIG}->{ForcedMethod} || ""),
    MAX_WAIT_TIME      => 40,
    RECONNECT_NUMBER   => 100,
  );
  foreach my $key (keys %defaults) {
    defined $self->{$key} or $self->{$key} = $defaults{$key};
  }
  $self->{PROXY_HOST} = $self->{PROXY_PORT} = "";
  if ($self->{USE_PROXY}) {
    $self->{PROXY_HOST} = $self->{CONFIG}->{PROXY_HOST};
    $self->{PROXY_PORT} = $self->{CONFIG}->{PROXY_PORT};
  }

  if ($self->{DEBUG}) {
    $self->{LOGGER}->debugOn($self->{DEBUG});
  }

  $ENV{ALIEN_USE_CACHE} and $self->{USE_CACHE} = $ENV{ALIEN_USE_CACHE};
  $ENV{ALIEN_DATABASE_SSL} and $self->{SSL} = 1 and $self->{ROLE} = $ENV{ALIEN_DATABASE_SSL};

  if ($ENV{ALIEN_DATABASE_PASSWORD}) {
    $self->debug(1, "CONNECTING TO THE DATABASE DIRECTLY!");
    $self->{USE_PROXY} = 0;
    $self->{PASSWD}    = $ENV{ALIEN_DATABASE_PASSWORD};
    $ENV{ALIEN_DATABASE_ROLE} and $self->{ROLE} = $ENV{ALIEN_DATABASE_ROLE};
  }
  if ($self->{USE_CACHE}) {
    $self->{CACHE_ROOT} = undef;
  }

  $self->{DBI_OPTIONS} = $attrDBI || {PrintError => 0};
  $self->{DBH} = undef;

  my $proxy = "";
  $self->{PROXY_HOST} and $proxy = "\nProxy host: $self->{PROXY_HOST}; Proxy port: $self->{PROXY_PORT}";

  $self->{USE_CACHE} and $self->_createCacheRoot;

  ($self->{USE_CACHE})
    and $DEBUG
    and $self->debug(1, "Using cache: $self->{CACHE_ROOT}; Cache size: $self->{CACHE_SIZE}");

  $self->_validate or return;

  $DEBUG and $self->debug(1, "Instance of Database successfully created.");

  $self->initialize or return;

  $self;
}

sub preConnect {
  return 1;
}

sub initialize {
  return 1;
}

=item C<query>

  $arrRef = $dbh->query( $statement );

  $arrRef = $dbh->query( $statement, $ttl );

Method executes query and returns reference on array of hashes that contain result.
Elements of array are tuples and tuples are represented by hash. Value of certain
attribute is fetched by attribute name. 
Warning: keys in hash are case sensitive, so when fetching values name from SQL query must be used.
If SQL query fails method returns undef. If result is empty set method returns reference
to empty array.

When cache is used module will first try to find result in cache.
If result is not in cache module will fetch data from database and store it in cache.
Arguments are query statement and optional statement time to live (ttl).
Time to live is used only in case when caching is used. In that case query result will be
valid in cache for stated period of time.

=cut

sub query {
  my $self    = shift;
  my $stmt    = shift;
  my $ttl     = shift;
  my $options = shift;
  my $result;

  $DEBUG and $self->debug(2, "In query executing $stmt");

  if (!$self->{USE_CACHE} || (defined $ttl && $ttl == 0)) {
    $result = $self->_queryDB($stmt, $options);
  } else {

    # finding out table name
    if ($stmt =~ /from(\s+)(\w+)(\s*)(where|)/i) {
      my $key = lc $2;

      #       $DEBUG and $self->debug(1,"Database: In query creating instance of cache $self->{CACHE_ROOT}/$key");
      #
      #       my $cache = Cache::File->new(
      #               default_expires=>$self->{DEFAULT_TTL},
      #               size_limit=>$self->{CACHE_SIZE},
      #               cache_root=>"$self->{CACHE_ROOT}/$key",
      #               lock_level=>Cache::File::LOCK_LOCAL
      #               );
      #
      #       $cache
      #         or print STDERR "Database: Cannot create instance of cache $self->{CACHE_ROOT}/$key\n"
      #         and return $self->_queryDB($stmt);
      #
      #
      #       my $entry = $cache->entry($stmt);
      #       if ($entry->exists())
      #       {
      #         $DEBUG and $self->debug(1,"Database: In query fetching result of $stmt from cache");
      #
      #         $result = $entry->thaw;
      #       }
      #       else
      #       {
      #         $result = $self->_queryDB($stmt) or return;
      #
      #         $DEBUG and $self->debug(1,"Database: In query storing result of $stmt from cache");
      #
      #         $entry->freeze($result,$ttl);
      #       }
      #temporary solution ... remove later!
      $result = $self->_queryDB($stmt);
    }
  }

  $result and $DEBUG and $self->debug(2, "In query returning result of $stmt");

  $result;
}

=item C<queryColumn>

  $arrRef = $dbh->queryColumn( $statement );

  $arrRef = $dbh->queryColumn( $statement, $ttl );

Method is used when result contains just one attribute, for example query like:
  SELECT name FROM customers

Method passes arguments to C<query> method and returns result in form of reference to hash.
If SQL query fails method returns undef. If result is empty set method returns reference
to empty array.

=cut

sub queryColumn {
  my $self = shift;

  my $result = $self->query(@_);

  defined $result
    or return;

  @$result
    or return [];

  my @column;
  for (@$result) {
    push @column, values %{$_};
  }

  return \@column;
}

=item C<queryRow>

  $arrRef = $dbh->queryRow( $statement );

  $arrRef = $dbh->queryRow( $statement, $ttl );

Method is used when result contains just one tuple, for example query like:
  SELECT name,age FROM custumers WHERE customerID = 1 (where customerID is PRIMARY KEY)

Method passes arguments to C<query> method and returns result in form of reference to hash.
If SQL query fails method returns undef. If result is empty set method returns reference
to empty hash.

=cut

sub queryRow {
  my $self = shift;

  my $result = $self->query(@_);

  defined $result
    or return;

  @$result
    and $#{$result} > -1
    and return $result->[0];

  return {};
}

=item C<queryValue>

  $arrRef = $dbh->queryValue( $statement );

  $arrRef = $dbh->queryValue( $statement, $ttl );

Method is used when result is single value, for example query like:
  SELECT COUNT(*) FROM customers

Method passes arguments to C<query> method and returns result as a scalar value.
If SQL query fails method returns undef. If result is empty set method undef.

=cut

sub queryValue {
  my $self = shift;
  my ($stmt, $ttl, $options) = (shift, shift, shift);
  $options or $options = {};
  my $result = $self->query($stmt, $ttl, $options, @_);
  my @tempStorage;

  $result and $#{$result} != -1 and @tempStorage = each(%{$result->[0]}) and return $tempStorage[1];

  undef;
}

sub do {
  my $self = shift;

  $self->_do(@_);
}

=item C<update>

  $res = $dbh->update($table,$fieldsRef,$where);

Method updates tuples from table $table that satisfy condition $where. New values are defined
with 2. argument in form of hash: {columnName => newValue}. Method puts ' around values so
values have to be scalar values. It's not possible to use values like column names or
complex expressions for newValue.

When cache is used all query results stored in cache from table that are modified are removed.

=cut

#
# Oracle note: we have column names that are reserved words for Oracle. In order to conserve the name
# This method is adapted for Oracle quoting the names of the columns so that we could still use reserved words as names for the columns.. For example:
# UPDATE table SET "col_reserved_word" = x WHERE col=y;
# we have to check if it is Oracle, since quoting column names will
# not work for the rest.
#

sub update {
  my $self = shift;
  $self->{DRIVER} =~ /Oracle/ and return $self->SUPER::update(@_);
  my $table   = shift;
  my $rfields = shift;
  my $where   = shift || "";
  my $options = shift || {};

  $self->{USE_CACHE} and $self->_clearCache($table);

  my $query = "UPDATE $table SET ";
  my $quote = "'";
  $options->{noquotes} and $quote = "";
  my @bind = ();
  foreach (keys %$rfields) {
    $query .= $self->reservedWord($_) . "=";
    if (defined $rfields->{$_}) {
      if ($quote) {
        $query .= "?,";
      } else {
        $rfields->{$_} =~ s/^([^'"]*)['"](.*)['"]([^'"]*)$/$2/;
        my $function    = "";
        my $functionend = "";
        if ($1 && $3) {
          $function = $1 and $functionend = $3;
        }
        $query .= " $function ? $functionend,";
      }
      push @bind, $rfields->{$_};
    } else {
      $query .= " NULL,";
    }
  }
  chop($query);

  $where and $query .= " WHERE $where";
  push(@bind, @{$options->{bind_values}}) if ($options->{bind_values});
  $self->_do($query, {bind_values => \@bind});
}

=item C<insert>

  $res = $dbh->insert($table,$fieldsRef);

Method insert new tuple into table $table. Values of attributes are defined
with 2. argument in form of hash: {columnName => value}. Method puts ' around values so
values have to be scalar values. It's not possible to use values like column names or
complex expressions for value.

When cache is used all query results stored in cache from table that are modified are removed.

=cut

sub insert {
  my $self    = shift;
  my $table   = shift;
  my $rfields = shift;
  my $options = shift || {};
  ### statement checking is a temporary solution ... remove later!!!
  if ($table =~ /\s/) { return $self->do($table); }

  $self->{USE_CACHE} and $self->_clearCache($table);
  my @fields = keys %$rfields;

  my $new_fields = $self->preprocessFields(\@fields);    #we quote them, because they are now case sensitive for Oracle

  # my $query = "INSERT INTO $table (" . join(", ", keys %$rfields) . ") VALUES (";
  my $query = "INSERT INTO $table (" . join(", ", @$new_fields) . ") VALUES (";

  #my @arr = values %$rfields;
  my @bind_values;
  foreach (keys %$rfields) {
    my $value = $rfields->{$_};
    if (defined $value) {
      if ($options->{functions} and $options->{functions}->{$_}) {
        $query .= "$options->{functions}->{$_}(?),";
      } else {
        $query .= "?,";
      }
      push @bind_values, $value;
    } else {
      $query .= "NULL,";
    }
  }

  chop($query);

  $query .= ")";

  $self->_do($query, {bind_values => \@bind_values, silent => $options->{silent}, zero_length => 0});
}

sub multiinsert {
  my $self = shift;
  if ($self->{DRIVER} =~ /Oracle/) { return $self->SUPER::multiinsert(@_); }
  my $table   = shift;
  my $rarray  = shift;
  my $options = shift;
  my $rloop;

  ###     statement checking is a temporary solution ... remove later!!!
  if ($table =~ /\s/) { return $self->do($table); }

  my $rfields = @$rarray[0];

  my $query = "INSERT";
  $options->{ignore} and $query .= " IGNORE";
  $query .= " INTO $table (" . join(", ", keys %$rfields) . ") VALUES ";
  my $quote = "'";
  $options->{noquotes} and $quote = "";

  #my @arr = values %$rfields;
  my @bind = ();

  foreach $rloop (@$rarray) {
    $query .= "(";
    foreach (keys %$rfields) {
      if (defined $rloop->{$_}) {

        if ($quote) {
          $query .= "?,";
        } else {
          $rloop->{$_} =~ s/^([^'"]*)['"](.*)['"]([^'"]*)$/$2/;
          my $function    = "";
          my $functionend = "";
          if ($1 && $3) {
            $function = $1 and $functionend = $3;
          }
          $query .= " $function ? $functionend,";
        }
        push @bind, $rloop->{$_};
      } else {
        $query .= "NULL,";
      }
    }

    chop($query);

    $query .= "),";
  }

  chop($query);
  my $doOptions = {bind_values => \@bind};
  $options->{silent} and $doOptions->{silent} = 1;
  $self->_do($query, $doOptions);
}

=item C<delete>

$res = $dbh->delete($table,$where);

Method deletes tuples from table $table that satisfy condition $where.

When cache is used all query results stored in cache from table that are modified are removed.

=cut

sub delete {
  my $self  = shift;
  my $table = shift;
  my $where = shift;

  $where
    or $self->{LOGGER}->error("Database", "In delete: No WHERE statement. Deleting whole table not permitted.")
    and return;

  $self->{USE_CACHE} and $self->_clearCache($table);

  my $query = "DELETE FROM $table WHERE $where";

  $self->_do($query, @_);
}

sub setPrimaryKey {
  my $self     = shift;
  my $table    = shift;
  my $desc     = shift;
  my $key      = shift;
  my $indexRef = shift;
  my $indexes  = $self->getIndexes($table);

  my @indexes = ();
  $indexRef and @indexes = @$indexRef;
  my $primary = 0;
  if (defined $indexes) {
    $DEBUG and $self->debug(1, "There are some keys for $table");
    foreach my $ind (@$indexes) {
      my $key_name = $ind->{Key_name}    || $ind->{key_name};
      my $col_name = $ind->{Column_name} || $ind->{column_name};
      if ($key_name eq "PRIMARY") {
        $key and ($col_name =~ /$key/i) and $primary = 1;
        next;
      }
      my @list = grep (/\W$col_name\W/i, @indexes);
      if ($list[0]) {
        $DEBUG and $self->debug(1, "Checking the column $list[0]");
        my $unique = grep (/unique/i, $list[0]);

        if ($unique eq $ind->{Non_unique}) {
          $self->info("The uniqueness of $list[0] in $table is not well defined");

          #  $self->alterTable($table, "drop index $ind->{Key_name}");
          $self->dropIndex($key_name, $table);
        } else {
          @indexes = grep (!/\W$col_name\W/i, @indexes);
        }
      }
    }
  }
  if ((!$primary) && $key) {
    $self->alterTable($table, "drop primary key");
    $self->alterTable($table, "ADD PRIMARY KEY ($key)");
    $self->info("Altering the primary key of $table");
  }
  foreach (@indexes) {
    $self->info("Creating the index $_ on the table $table");

    #  $self->alterTable($table, "ADD $_");
    $self->createIndex($_, $table, $INDEX);

  }
  return 1;
}

sub checkColumn {
  my $self    = shift;
  my $table   = shift;
  my $colName = shift;
  my $colDesc = shift;

  $DEBUG
    and $self->debug(1, "In checkColumn checking if table $table has column $colName");

  my $columns = $self->describeTable($table);

  foreach (@$columns) {
    my $field = $_->{Field} || $_->{field};
##We consider if the column was a reserved word that was quoted
    $field
      and (($field eq $colName) or ("\"$field\"" eq $colName))
      and $DEBUG
      and $self->debug(1, "In checkColumn table $table has column $colName")
      and return 1;
  }

  $DEBUG and $self->debug(1, "In checkColumn altering table $table adding column $colName ($colDesc)");

  $self->alterTable($table, "ADD  ($colName $colDesc)")
    or return;

  2;
}

sub describeTable {
  my $self = shift;
  return $self->SUPER::describeTable(@_);
}

sub dropTable {
  my $self  = shift;
  my $table = shift;

  $self->{USE_CACHE} and $self->_destroyCache($table);

  $DEBUG and $self->debug(1, "In dropTable dropping table $table");

  $self->_do("DROP TABLE $table");
}

sub alterTable {
  my $self  = shift;
  my $table = shift;
  my $spec  = shift;

  $DEBUG
    and $self->debug(1, "In alterTable altering table $table with definition $spec");

  $self->{USE_CACHE} and $self->_destroyCache($table);

  $self->_do("ALTER TABLE $table $spec");
}

sub reconnect {
  my $self    = shift;
  my $host    = shift || $self->{HOST};
  my $db      = shift || $self->{DB};
  my $driver  = shift || $self->{DRIVER};
  my $attrDBI = shift || $self->{DBI_OPTIONS};

  $DEBUG and $self->debug(1, "Database: In reconnect connecting to database $db on host $host using driver $driver.");

  AliEn::Database::destroy($self);

  unless ($host eq $self->{HOST}
    and $db     eq $self->{DB}
    and $driver eq $self->{DRIVER}
    and $attrDBI == $self->{DBI_OPTIONS}) {

    $self->{DB}          = $db;
    $self->{HOST}        = $host;
    $self->{DRIVER}      = $driver;
    $self->{DBI_OPTIONS} = $attrDBI;

    $self->{USE_CACHE} and $self->_createCacheRoot;
  }
  $self->_validate;
}

sub changeUser {
  my $self    = shift;
  my $newUser = shift;
  my $passwd  = shift || "";

  $newUser or print STDERR "Database: In changeUser new user is not stated.\n" and return;

  $DEBUG and $self->debug(1, "Database: In changeRole changing user to $newUser.");

  AliEn::Database::destroy($self);    #  $self->destroy;

  $self->{USER}   = $newUser;
  $self->{ROLE}   = $newUser;
  $self->{PASSWD} = $passwd;

  $self->{USE_CACHE} and $self->_createCacheRoot;

  $self->_validate;
}

sub changeRole {
  my $self    = shift;
  my $newRole = shift;
  my $passwd  = shift || "";

  $newRole or print STDERR "Database: In changeRole new role is not stated.\n" and return;

  $DEBUG and $self->debug(1, "Database: In changeRole changing role to $newRole.");

  AliEn::Database::destroy($self);

  $self->{ROLE}   = $newRole;
  $self->{PASSWD} = $passwd;

  $self->{USE_CACHE} and $self->_createCacheRoot;

  $self->_validate;
}

sub setForcedMethod() {
  my $self = shift;
  (my $FM = shift) or return;
  $self->{FORCED_AUTH_METHOD} = $FM;
  $DEBUG and $self->debug(1, "Database: Forcing $self->{FORCED_AUTH_METHOD} authentication");
  $self->_validate;
  return 1;
}

sub getForcedMethod() {
  my $self = shift;
  $self->{FORCED_AUTH_METHOD};
}

sub getUser() {
  my $self = shift;
  $self->{USER};
}

sub destroy {
  my $self = shift;
  local $SIG{PIPE} = 'IGNORE';

  local $SIG{ALRM} = sub {
    print "$$ timeout in the disconnect (and $self->{PID}\n";
    die("timeout in disconnect");
  };
  alarm(5);
        $self
    and $self->{LOGGER}
    and $DEBUG
    and $self->debug(1, "Disconneting");
  if ($self and $self->{PID} and $self->{DBH}) {
    if ($self->{PID} eq $$) {
      if (!$self->{DBH}->disconnect) {
        $self->{DBH}->errstr and warn $self->{DBH}->errstr;
      }
    }
    undef $self->{DBH};
  }

  alarm(0);
        $self
    and $self->{LOGGER}
    and $DEBUG
    and $self->debug(1, "disconnected");
}

sub DESTROY {
  my $self = shift;
  ($self) and $self->destroy;
}

sub disconnect {
  my $self = shift;
  $self->destroy;
}

sub close {
  my $self = shift;
  $self->destroy;
}
####################################################################
####################################################################

###### internal methods

####################################################################
####################################################################
sub getDatabaseDSN {
  my $self = shift;
  my $dsn  = "";

  if ($self->{USE_PROXY} and not $self->{SSL}) {
    $self->{CONFIG} or $self->{CONFIG} = new AliEn::Config();
    ($self->{CONFIG})
      or $self->{LOGGER}->error("Database", "Initial configuration not found")
      and return;
    $self->{PROXY_HOST} = $self->{CONFIG}->{PROXY_HOST};
    $self->{PROXY_PORT} = $self->{CONFIG}->{PROXY_PORT};
    if ($self->{CONFIG}->{PROXY_ADDRESS_LIST}) {
      my $number = int(rand($#{$self->{CONFIG}->{PROXY_ADDRESS_LIST}} + 1));
      $self->{PROXY_HOST} = ${$self->{CONFIG}->{PROXY_ADDRESS_LIST}}[$number];
      $self->{PROXY_HOST} =~ s/:(\d+)// and $self->{PROXY_PORT} = $1;
      $self->debug(1, "There are several proxies (using $self->{PROXY_HOST} $number)");

    }
    if ($self->{PROXY_HOST}) {

      $dsn =
"DBI:AliEnProxy:hostname=$self->{PROXY_HOST};port=$self->{PROXY_PORT};local_user=$self->{USER};forced_method=$self->{FORCED_AUTH_METHOD};";

      ($self->{PASSWD}) and $dsn .= "PASSWD=$self->{PASSWD};";

      $dsn .= ";dsn=";
    }
  }
  $dsn .= $self->getConnectionChain(@_);
  ##For oracle, the format in the LDAP should be:
  #hostname:oracle_port/Oracle/name_of_database:name_of_schema
  #Where the name of the schema maps to the name of the database in mysql
  return $dsn;

}

#
#  THIS METHOD IS GOING TO RETRY
#
#
sub _connect {
  my $self = shift;
  my $pass;
  local $SIG{PIPE} = sub {
    print STDERR "Warning!! The connection to the AliEnProxy got lost (in connect)\n";
    $self->reconnect();
  };

  ($self->{FORCED_AUTH_METHOD} eq "TOKEN") and $pass = $self->{TOKEN}
    or $pass = $self->{PASSWD};

  $DEBUG
    and
    $self->debug(1, "Database: In _connect user is trying to connect to $self->{DRIVER} $self->{DB} in $self->{HOST}.");

  my $sleep     = 1;
  my $max_sleep = 60000;
  $self->{SCHEMA} = $self->{ROLE};
  while (1) {
    my $dsn = $self->getDatabaseDSN();

    $self->{DRIVER} =~ /Oracle/i and $self->{DBI_OPTIONS}->{FetchHashKeyName} = "NAME_lc" and $self->{DBI_OPTIONS}->{LongReadLen}= 1024*1024;

    $self->{DBH} = DBI->connect($dsn, $self->{SCHEMA}, $pass, $self->{DBI_OPTIONS});
    $self->{DBH} and last;
    my $errStr = $DBI::errstr;
    if ($errStr) {
      if ($errStr =~ /please connect later/) {
        $self->info("The database is down...");
        $sleep = $sleep * 2 + int(rand(2));
        $sleep > $max_sleep and $sleep = int(rand(4));
        $self->info("The connection to the database is not active... let's sleep for $sleep seconds");
        sleep($sleep);
        next;
      }
      $self->info("Could not connect to database: $DBI::errstr ($DBI::err)", -1);
      if ($DBI::errstr =~ /Died at .*AliEn\/UI\/Catalogue\.pm line \d+/) {
        die("We got a ctrl+c... :( ");
      }
      return;
    }

    ($DBI::err and $DBI::err == 1)
      and $self->info("Database does not authenticate the user $self->{USER} as $self->{ROLE}")
      and return;

    $sleep = $sleep * 2 + int(rand(2));
    $sleep > $max_sleep and $sleep = int(rand(4));
    $self->info("There was an unknown error: " . $DBI::err . " (sleeping $sleep)");
    sleep($sleep);

  }
  $DEBUG and $self->debug(1, "User $self->{ROLE} connected to database!");

  $self->{PID} = $$;

  $self->{DRIVER} =~ /Oracle/ and $self->_connectSchema();
  return $self->{ROLE};
}

sub _timeout {
  my $tmp = new AliEn::Logger();

  alarm(0);

  $tmp->error("Database", "SQL Statement timed out");

  die;
}

sub _queryDB {
  my $self = shift;
  if ($self->{DRIVER} =~ /Oracle/) { return $self->SUPER::_queryDB(@_); }
  my $stmt    = shift;
  my $options = shift;
  $options or $options = {};
  my $oldAlarmValue = $SIG{ALRM};
  local $SIG{ALRM} = \&_timeout;

  local $SIG{PIPE} = sub {
    print STDERR "Warning!! The connection to the AliEnProxy got lost\n";
    $self->reconnect();
  };

  $self->_pingReconnect or return;

  my $arrRef;
  my $execute;
  my @bind;
  $options->{bind_values} and push @bind, @{$options->{bind_values}};
  $DEBUG and $self->debug(2, "In _queryDB executing $stmt in database (@bind).");

  while (1) {
    my $sqlError = "";
    eval {
      alarm(600);
      my $sth = $self->{DBH}->prepare_cached($stmt);

      #      my $sth = $self->{DBH}->prepare($stmt);
      $DBI::errstr and $sqlError .= "In prepare: $DBI::errstr\n";
      if ($sth) {
        $execute = $sth->execute(@bind);
        $DBI::errstr and $sqlError .= "In execute: $DBI::errstr\n";
        $arrRef = $sth->fetchall_arrayref({});
        $DBI::errstr and $sqlError .= "In fetch: $DBI::errstr\n";

        #  $sth->finish;
        #  $DBI::errstr and $sqlError.="In finish: $DBI::errstr\n";
      }
    };
    $@ and $sqlError = "The command died: $@";
    alarm(0);

    if ($sqlError) {
      my $found = 0;
      $sqlError =~
/(Unexpected EOF)|(Lost connection)|(Constructor didn't return a handle)|(No such object)|(Connection reset by peer)|(MySQL server has gone away)|(_set_fbav\(.*\): not an array ref at)|(Constructor didn't return a handle)/
        and $found = 1;

      if ($sqlError =~ /Died at .*AliEn\/UI\/Catalogue\.pm line \d+/) {
        die("We got a ctrl+c... :( ");
      }
      if ($sqlError =~ /Maximum message size of \d+ exceeded/) {
        $self->info("ESTAMOS AQUI\n");
      }
      $found or $self->info("There was an SQL error: $sqlError", 1001) and return;
    }

    #If the statment got executed, we can exit the loop
    $execute and last;

    $self->reconnect or $self->info("The reconnection did not work") and return;
  }

  $oldAlarmValue and $SIG{ALRM} = $oldAlarmValue
    or delete $SIG{ALRM};

  $DEBUG and $self->debug(1, "Query $stmt successfully executed. ($#{$arrRef}+1 entries)");
  return $arrRef;
}

sub _do {
  my $self = shift;
  if ($self->{DRIVER} =~ /Oracle/i) { return $self->SUPER::_do(@_); }
  my $stmt = shift;
  my $options = (shift or {});

  my $oldAlarmValue = $SIG{ALRM};
  local $SIG{ALRM} = \&_timeout;

  local $SIG{PIPE} = sub {
    print STDERR "Warning!! The connection to the AliEnProxy got lost while doing an insert\n";
    $self->reconnect();
  };

  $DEBUG and $self->debug(2, "In _do checking is database connection still valid");

  $self->_pingReconnect or return;
  my @bind_values;
  $options->{bind_values} and push @bind_values, @{$options->{bind_values}} and $options->{prepare} = 1;
  my $result;

  while (1) {
    my $sqlError = "";

    $result = eval {
      alarm(600);
      my $tmp;
      if ($options->{prepare}) {
        $DEBUG and $self->debug(2, "In _do doing $stmt @bind_values");
        my $sth = $self->{DBH}->prepare_cached($stmt);
        $tmp = $sth->execute(@bind_values);
      } else {
        $DEBUG and $self->debug(1, "In _do doing $stmt @bind_values");
        $tmp = $self->{DBH}->do($stmt);
      }
      $DBI::errstr and $sqlError .= "In do: $DBI::errstr\n";
      $tmp;
    };
    my $error = $@;
    alarm(0);

    defined($result) and last;

    $error and $sqlError .= "There is an error: $@\n";

    if ($sqlError) {
      my $found = 0;
      $sqlError =~ /(Unexpected EOF)|(Lost connection)|(MySQL server has gone away)|(Connection reset by peer)/
        and $found = 1;
      if (!$found) {
        $oldAlarmValue and $SIG{ALRM} = $oldAlarmValue
          or delete $SIG{ALRM};
        chomp $sqlError;
        $options->{silent}
          or $self->info("There was an SQL error  ($stmt): $sqlError", 1001);
        return;
      }
    }

    $self->reconnect() or return;
  }

  $oldAlarmValue and $SIG{ALRM} = $oldAlarmValue
    or delete $SIG{ALRM};

  $DEBUG and $self->debug(1, "Query $stmt successfully executed with result: $result");

  $result;
}

=item C<preprocessFields>

$res = $dbh->preprocessFields($keys);

=cut

sub preprocessFields {
  my $self   = shift;
  my $fields = shift;
  if ($self->{DRIVER} =~ /Oracle/i) {
    return $self->SUPER::preprocessFields($fields);
  } else {
    return $fields;
  }
}

sub _pingReconnect {
  my $self = shift;

  $DEBUG
    and
    $self->debug(2, "In _pingReconnect checking database connection DBH-PID: " . ($self->{PID} or "") . "; PID: $$.");

  $self->{PID}
    and ($self->{PID} == $$)
    and $self->{DBH}

    # and $self->{DBH}->ping
    and return 1;

  $self->reconnect;
}

sub _validate {
  my $self = shift;

  my $status;
  if ($ENV{ALIEN_PROC_ID} and $ENV{ALIEN_JOB_TOKEN}) {

    $DEBUG and $self->debug(1, "In _validate validating job $ENV{ALIEN_PROC_ID} token: $ENV{ALIEN_JOB_TOKEN}.");

    $self->{ROLE}               = $ENV{ALIEN_PROC_ID};
    $self->{PASSWD}             = $self->{TOKEN} = $ENV{ALIEN_JOB_TOKEN};
    $self->{FORCED_AUTH_METHOD} = 'JOBTOKEN';
    $self->debug(1, "READY TO AUTHENTICATE WITH THE JOB TOKEN");
    my $username = $self->_connect();
    if (!$username) {
      $self->info("Authentication with the jobid $ENV{ALIEN_PROC_ID} (from the environment ALIEN_PROC_ID) failed");
      return;
    }
    $self->{ROLE} = $ENV{ALIEN_JOBTOKEN_USER};

  } else {
    $DEBUG
      and $self->debug(1, "Database: In _validate validating user $self->{USER} (as $self->{ROLE}) using password.");

    $status = $self->_validateUser or return;
  }

  $DEBUG
    and $self->debug(1, "User $self->{USER} (as $self->{ROLE}) successfully validated.");

  1;
}

sub _validateUser {
  my $self = shift;

  $DEBUG and $self->debug(1, "Database: In _validateUser validating $self->{USER} (as $self->{ROLE}).");

  if ($self->{USE_CACHE} && (-d $self->{CACHE_ROOT})) {
    $DEBUG
      and $self->debug(1, "Database: In _validateUser $self->{USER} validated.");
    return 1;
  }

  AliEn::Database::destroy($self);

  if (!$self->_connect) {
    $self->info("Database: In _validateUser validation of user $self->{USER} (as $self->{ROLE}) failed.", 125, 0);
    return;
  }
  $DEBUG
    and $self->debug(1, "Database: In _validateUser $self->{USER} validated.");

  1;
}

sub _createCacheRoot {
  my $self = shift;
  my $key  = "$self->{DRIVER}#$self->{HOST}#$self->{DB}#$self->{USER}#$self->{ROLE}";

  my $attrDBI = $self->{DBI_OPTIONS};

  if ($self->{DBI_OPTIONS}) {
    $key .= "#";
    foreach my $i (keys %$attrDBI) { $key .= $i . $attrDBI->{$i}; }
  }

  $self->{CACHE_ROOT} = "$self->{CACHE_DIR}/DBCache#$key";

  $DEBUG
    and $self->debug(1, "Database: New cache root created: $self->{CACHE_ROOT}");
}

sub _clearCache {
  my $self = shift;
  my $key  = shift;

  #  if (-d "$self->{CACHE_ROOT}/$key"){
  #  my $cache = Cache::File->new(
  #  default_expires=>$self->{DEFAULT_TTL},
  #  size_limit=>$self->{CACHE_SIZE},
  #  cache_root=>"$self->{CACHE_ROOT}/$key",
  #  lock_level=>Cache::File::LOCK_LOCAL
  #  );
  #
  #  $DEBUG and $self->debug(1,"In _clearCache removing all objects from cache $self->{CACHE_ROOT}/$key");
  #
  #  $cache->clear;
  #  }

}

sub _destroyCache {
  my $self = shift;
  my $key  = shift;

  system("rm", "-r", "$self->{CACHE_ROOT}/$key");
}

=item C<createTable>

$res = $dbh->createTable($table,$spec,$checkExists);

Method creates table $table with create table specification $spec. For details about
create table specification see SQL Syntax. If argument $checkExists is set method
will check if table $table already exists before creating it.
Method returns result from DBI method C<do>.

=item C<alterTable>

$res = $dbh->alterTable($table,$colSpec);

Method creates table $table with alter specification $spec. For details about
alter specification see SQL Syntax.
Method returns result from DBI method C<do>.

When cache is used all query results stored in cache from table that are modified are removed.

=item C<dropTable>

$res = $dbh->dropTable($table);

Method drops table $table and returns result from DBI method do C<do>.

When cache is used whole cache related to table $table will be removed.

=item C<describeTable>

$arrRef = $dbh->describeTable($table);

Method is used for fetching informations about table $table.

=item C<existsTable>

$res = $dbh->existsTable($table);

Method returns true if table $table exists in current database, otherwise false.

=item C<lock>

  $arrRef = $dbh->lock( $tableName );

Method puts write lock on stated table. It returns result from DBI method C<do>.

=item C<unlock>

  $res = $dbh->unlock;

  $res = $dbh->unlock( $tableName );

Method unlocks stated table or if table is not stated it will unlock all tables locked
previously. It returns result from DBI method C<do>.

=item C<reconnect>

  $res = $dbh->reconnect( $newHost, $newDB, $newDriver, $newAttrDBI );

Method reconnects to database with given parameters. If any of arguments is undefined old
value will be used. After reconnection validation will be automatically preformed. If none
of arguments is stated validation will be skipped.
Method returns 1 if successful and undef otherwise.

=item C<changeUser>

  $res = $dbh->changeUser($newUser,$newToken,$newPassword);

Method changes user. Role is set to new user. After changing user and role validation
of new user is performed.

=item C<changeRole>

  $res = $dbh->changeRole($newRole,$newToken,$newPassword);

Method changes role of current user. After changing role validation is performed.

=item C<grantAllPrivilegesToUser>

  $res = $dbh->grantAllPrivilegesToUser($user,$db,$table);

Method grants all priviliges to user $user on table $table in database $database.

=item C<grantPrivilegesToUser>

  $res = $dbh->grantPrivilegesToUser($rPrivs,$user,$pass);

Method grants priviliges $rPrivs to user $user defined by password $pass. Privileges
are defined in a form of reference to array, which contain grant specifications.
For details about grant specification see SQL syntax. Basic form of revoke spec is:
SQL_operation ON db_name.table_name

=item C<revokeAllPrivilegesFromUser>

  $res = $dbh->revokeAllPrivilegesFromUser($user,$db,$table);

Method revokes all priviliges from user $user on table $table in database $database.

=item C<revokePrivilegesFromUser>

  $res = $dbh->revokePrivilegesFromUser($rPrivs,$user,$pass);

Method revokes priviliges $rPrivs from user $user. Privileges
are defined in a form of reference to array, which contain revoke specifications.
For details about grant specification see SQL syntax. Basic form of revoke spec is:
SQL_operation ON db_name.table_name

=back

=head1 ATTRIBUTES

=over

=item C<CACHE_DIR>

Attribute defines in which directory will cache be stored. Not needed when cache is not used.
Default value is /tmp/cache.

=item C<CACHE_SIZE>

Attribute defines size of the cache in kilobytes. Not needed when cache is not used.

=item C<DB>

Database name. Object cannot be created without this attribute.

=item C<DEFAULT_TTL>

Default time to live for all query results that this instance stores in cache. If two instances
use same cache and define different default time to live,

=item C<DRIVER>

Driver name. Driver name is defined . Object cannot be created without this attribute.

=item C<FORCED_AUTH_METHOD>

Attribute which defines method of authentication of user. Possible values are: TOKEN, SSH_KEY, GSS.
TOKEN means that user will be authenticated by his token. In two other cases user will be
authenticated by his password. When authentication method is not defined module will read
value from configuration.

=item C<HOST>

Host where database is. Object cannot be created without this attribute.

=item C<MAX_WAIT_TIME>

Maximum period  of in seconds which has to past before module tries to reestablish connection to
database. Default value is 40 seconds.

=item C<RECONNECT_NUMBER>

Number of attempts to reestablish connection to database. In case when cache is used
Default value is 100.

=item C<ROLE>

Role of user in system. If not stated role is identical to user name.

=item C<TOKEN>

Users token. When not defined module will read value from configuration.

=item C<USE_CACHE>

Value of attribute is 1 if cache is used, otherwise 0. Default value is 0.

=item C<USE_PROXY>

If USE_PROXY attribute is defined proxy will be used. Default value is 1.

=item C<USER>

User name. When user is not defined module will read value from configuration.

=back

=head1 ENVIRONMENT VARIABLES

=over

=item C<ALIEN_USE_CACHE>

Value of attribute is 1 if cache is used, otherwise 0. Default value is 0.

=back

=head1 SEE ALSO

DBI, Cache

=cut

1;

