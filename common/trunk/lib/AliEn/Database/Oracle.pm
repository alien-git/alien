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
package AliEn::Database::Oracle;

use strict;
use DBI;
use AliEn::Database;

use Tie::CPHash;

use AliEn::SOAP;
use AliEn::Logger::LogObject;
use vars qw($DEBUG @ISA);

$DEBUG = 0;

=head1 NAME

AliEn::Database::Oracle - database interface for Oracle driver for AliEn system 

=head1 DESCRIPTION

This module implements the database wrapper in case of using the driver Oracle. Sytanx and structure are different for each engine. This affects the code. The rest of the modules should finally abstract from SQL code. Therefore, instead we would ideally use calls to functions implemented in this module - case of Oracle. 

=cut

=item C<reservedWord>

  $res = $dbh->reservedWord($word);

=cut

sub reservedWord {
  my $self = shift;
  my $word = shift;
  return "\"" . uc $word . "\"";
}

=item C<preprocessFields>

  $res = $dbh->preprocessFields($keys);

=cut

sub preprocessFields {
  my $self     = shift;
  my $new_keys = shift;
  map { $_ = "\"" . uc $_ . "\"" } @$new_keys;
  return $new_keys;
}
sub checkTable {
  my $self=shift;
  my $table=shift;
  my $desc=shift;
  my $columnsDef=shift;
  my $primaryKey=shift;
  my $index=shift;
  my $options=shift;
  if ($index) {

    foreach (@$index) {
      $_ =~
s/(\s|\()(size|user|time|current|validate|date|file)(\s|\))/$1."\"".uc($2)."\"".$3/ieg;
    }
  }
  my %autoincrements = ();
  my %update         = ();
  my %columns        = %$columnsDef;
  my $desc2          = $desc;
  $desc2 =~
s/^size$|^user$|^time$|^current$|^validate$|^date$|^file$/"\"".uc($desc2)."\""/ie;
  $columns{$desc} =~
s/(DEFAULT )?CHARACTER SET latin1|COLLATE latin1_general_cs|COLLATE latin1_general_ci//ig;
  $columns{$desc} =~ s/\'0000-00-00\s00:00:00\'/sysdate/;

  if ($columns{$desc} =~ s/ON\s+UPDATE\s+CURRENT_TIMESTAMP//xi) {
    $update{$table} = $desc2;
  }
  if ($columns{$desc} =~ /serial/) { $autoincrements{$table} = $desc2; }
  $columns{$desc} =~
    s/(\s*)([a-zA-Z]+)(\(|\s+|$)(.*)/$self->{TYPES}->{$2}$3$4/i;
  $columns{$desc} =~ s/int\(/number\(/;
  if ($columns{$desc} =~ s/auto_increment//) {
    $autoincrements{$table} = $desc2;
  }

  $desc = "$desc2 $columns{$desc}";
  $self->createTable($table, "($desc) ", 1) or return;
  if (%autoincrements) {
    foreach my $t (keys %autoincrements) {
      $self->defineAutoincrement($t, $autoincrements{$t}) or return;
    }
  }

  my $alter = $self->getNewColumns($table, $columnsDef);

  if ($alter) {
    $self->lock($table);

    #let's get again the description
    my $update;
    my $a;
    ($a, $alter, $update) = $self->getNewColumns($table, $columnsDef);
    my $done = 1;
    if ($alter) {

      #  chop($alter);
      $self->info("Updating columns of table $table");
      $done = $self->alterTable($table, $alter);
    }
    %update = %$update;
    if (%update) {
      foreach my $u (keys %update) {
        $self->setUpdateDefaultCurrentTimestamp($u, $update{$u}) or return;
      }
    }
    $self->unlock($table);
    $done or return;
  }

  $self->setPrimaryKey($table, $desc, $primaryKey, $index);

  #Ok, now let's take a look at the primary key
  #$primaryKey or return 1;
  

#  $desc =~ /not null/i or $self->{LOGGER}->error("Database", "Error: the table $table is supposed to have a primary key, but the index can be null!") and return;
}
=item C<createTable>

  $res = $dbh->createTable($table,$definition);

=cut

sub createTable {
  my $self        = shift;
  my $table       = shift;
  my $definition  = shift;
  my $checkExists = shift || "";
  my $check       = shift;
  defined($check) or $check = 0;
  if ($checkExists) {
    if ($self->existsTable($table) != 0) { return 1; }
  }
  $DEBUG and $self->debug(1,
"Database: In createTable creating table $table with definition $definition."
  );
  my %autoincrements = ();
  my %indexes        = ();
  my $cont           = 0;

  #  my $desc2=$desc;
  if ($check) {
    $definition =~
      s/(size|user|time|current|validate|date|file)/"\"".uc($1)."\""/ieg;
    $definition =~
s/(DEFAULT)? CHARACTER SET latin1|COLLATE latin1_general_cs|COLLATE latin1_general_ci//ig;

    $definition =~
s/(\"\w+\"|\w+)(\s+)(\w+)(\(|\s+|\,|$)(.*)?/$1.$2.$self->{TYPES}->{$3}.$4.$5/ieg;
    $definition =~ s/int\(/number\(/;
    $definition =~ s/\'0000-00-00\s00:00:00\'/sysdate/;
    while ($definition =~ s/(,?)\s*index \((\"?[a-zA-Z]*\"?)\)//i) {
      $indexes{$cont} = $1;
      $cont++;
    }
    $cont = 0;
    $self->debug(1, "The definition before PK $definition");
    $definition =~
s/primary key \((\w+)\)/"constraint ".$table."_pk primary key (".$1.")" /ie;
    if ($definition =~ s/(\w+)(\s+)(\w+)\s+AUTO_INCREMENT/$1$2$3/ig) {
      $autoincrements{$table} = $1;
      $self->debug(1, "Creating autoincrement $definition");
    }

#if ($columns{$desc} =~ s/ON UPDATE CURRENT_TIMESTAMP// ){}#$self->setUpdateDefault($table,$desc2,"CURRENT_TIMESTAMP");}
# $alter .= " $desc2 $columns{$desc} ,";
    $DEBUG and $self->debug(1,
"Database: In createTable creating table $table with definition $definition."
    );
    $definition =~ s/\'/\'\'/g;
    $self->_do(
      "begin    exec_stmt(\'CREATE TABLE  $table  $definition \') ;    end;"
      )
      or $self->info("In checkQueueTable creating table $table failed", 3)
      and return;
    if (%autoincrements) {
      foreach my $t (keys %autoincrements) {
        $self->defineAutoincrement($t, $autoincrements{$t}) or return;
      }
      if (%indexes) {
        foreach my $t (values %autoincrements) {
          $self->do(
            "begin exec_stmt(\'CREATE INDEX " . $table . "_INDEX" . $t. " ON $table ($t) \') ; end;", { zero_lengt => 0 }
          );
        }
      }
      $cont = 0;

    }
  } else {
    $DEBUG and $self->debug(1,
"Database: In createTable creating table $table with definition $definition."
    );
    $definition =~ s/\'/\'\'/g;
    $self->_do(
      "begin   exec_stmt(\'CREATE TABLE  $table  $definition\');end;  ")
      or $self->info("In checkQueueTable creating table $table failed", 3)
      and return;
  }
### lets give access to this newtable
  my $grantedUser = $self->{CONFIG}->{ROLE};
  if ($grantedUser !~ /admin(ssl)/i) { $grantedUser = $self->{ORACLE_USER}; }
  else                               { return 1; }
  $self->do(
    "BEGIN EXEC_STMT(\'GRANT SELECT ON $table TO $grantedUser\');END;;");
  if ($table =~ m/T$self->{CONFIG}->{ROLE}V/i) {
    $self->debug(1, "GRANT INSERT,UPDATE,DELETE ON $table to $grantedUser");
    $self->do(
"BEGIN EXEC_STMT(\'GRANT INSERT, UPDATE,DELETE ON $table to $grantedUser\');END;;"
    );
    my $f = uc($self->{SCHEMA});
    $self->do("BEGIN GRANTSTAR('SELECT','$f', '$grantedUser');END;;");
  }

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

  %columns = map { uc $_ => $columns{$_} } keys %columns;
  my $queue = $self->describeTable($table);
  defined $queue
    or return;

  foreach (@$queue) {

    #we need to consider reserved words (quoted)
    delete $columns{ $_->{Field} };
    delete $columns{"\"$_->{Field}\""};
    delete $columns{ lc( $_->{Field} ) };
    delete $columns{"\"lc($_->{Field})\""};
    delete $columns{ uc( $_->{Field} ) };
    delete $columns{"\"uc($_->{Field})\""};
  }

  my $alter = "ADD ( ";

  my %autoincrements = ();
  my %update         = ();
  foreach my $desc (keys %columns) {
    my $desc2 = $desc;
    $desc2 =~
s/^size$|^user$|^time$|^current$|^validate$|^date$|^file$/"\"".uc($desc2)."\""/ie;
    $columns{$desc} =~
s/(DEFAULT)? CHARACTER SET latin1|COLLATE latin1_general_cs|COLLATE latin1_general_ci//ig;
    if ($columns{$desc} =~ /serial/) { $autoincrements{$table} = $desc2; }
    $columns{$desc} =~
      s/(\s*)([a-zA-Z]*)(\(|\s+|$)(.*)/$self->{TYPES}->{$2}$3$4/i;
    $columns{$desc} =~ s/int\(/number\(/;
    $columns{$desc} =~ s/\'0000-00-00\s00:00:00\'/sysdate/;
    if ($columns{$desc} =~ s/auto_increment//) {
      $autoincrements{$table} = $desc2;
    }

    if ($columns{$desc} =~ s/ON\s+UPDATE\s+CURRENT_TIMESTAMP//xi) {
      $update{$table} = $desc2;
    }

    $alter .= " $desc2 $columns{$desc} ,";
  }

  if (%autoincrements) {
    foreach my $t (keys %autoincrements) {
      $self->defineAutoincrement($t, $autoincrements{$t}) or return;
    }
  }
  if (chop($alter) =~ /^,/i) {
    $alter .= ")";

    return (1, $alter, \%update);
  }
  return;

}

=item C<getIndexes>

  $res = $dbh->getIndexes($table,);

Returns the keys of the table $table

=cut

sub getIndexes {

  my $self  = shift;

  my $table = uc shift;
return $self->query(

"SELECT DISTINCT MOD (INSTR(a1.uniqueness,'UNIQUE')+1, 2) AS \"Non_unique\" ,    a2.column_name as \"Column_name\", \'PRIMARY\' as \"Key_name\" FROM all_indexes A1 , all_ind_columns A2 , all_constraints  A3 where a1.index_name = a2.index_name and a1.table_name=a2.table_name and a3.constraint_name = a1.index_name   and A1.table_name LIKE ? AND a3.constraint_type='P'  union  SELECT DISTINCT MOD (INSTR(a1.uniqueness,'UNIQUE')+1, 2) AS \"Non_unique\" ,   a2.column_name as \"Column_name\", a1.index_name as \"Key_name\" FROM all_indexes A1 , all_ind_columns A2   where a1.index_name = a2.index_name and a1.table_name=a2.table_name   and A1.table_name LIKE ? minus  SELECT DISTINCT MOD (INSTR(a1.uniqueness,'UNIQUE')+1, 2) AS \"Non_unique\" ,    a2.column_name as \"Column_name\", a1.index_name as \"Key_name\" FROM all_indexes A1 , all_ind_columns A2 , all_constraints  A3 where a1.index_name = a2.index_name and a1.table_name=a2.table_name and a3.constraint_name = a1.index_name   and A1.table_name LIKE ? AND a3.constraint_type=\'P\'  ",undef, {bind_values=>[$table, $table, $table] , zero_length=>0});

}

=item C<dropIndex>

  $res = $dbh->dropIndex($index,$table,);

Drop the index $index from the database.

=cut

sub dropIndex {
  my $self  = shift;
  my $index = shift;
  my $table = shift;
  $self->do(    "begin exec_stmt(\'drop index $index\');end;"
  );
}

=item C<createIndex>

  $res = $dbh->createIndex($index,$table,);

Create the index for the table. This index cannot be named automatically, like it is the case for mysql.

=cut

sub createIndex {
  my $self     = shift;
  my $index    = shift;
  my $table    = shift;
  my $i        = shift ||  0;
  my $sqlError = "";
  if ($index =~ /^FOREIGN KEY/i) {
    $i = $index; 
    $i =~ s/(.*)REFERENCES (.*)\((.*)\)/$3/;
    $i =~ s/([a-zA-Z][a-zA-Z][a-zA-Z])(.)*(,)?/$1/g; 
    $self->do(
      "begin exec_stmt(\'ALTER TABLE $table ADD CONSTRAINT FK_" . $table . "_$i $index\');   end;"
    );
  } elsif ($index =~ /^PRIMARY KEY/i) {
    $self->do(
      "begin exec_stmt(\'ALTER TABLE $table ADD CONSTRAINT  " . $table . "_pk $index\'); end ; "
    );
  } elsif ($index =~ /^(.*)\((.*)\)/i) {
    my $name   = $1;
    my $index  = "";
    my $fields = $2;
    if (!($name =~ /(\w*)\s* INDEX\s+ (\w+)/xi)) {
      $index = $1;   
      $i=$fields;$i =~ s/([a-zA-Z])(.)*(,)?/$1/g; 
      $name  = "I_" . $table . "_$i";  
    }
    $fields =~
s/^size$|^user$|^time$|^current$|^validate$|^date$/"\"".uc($fields)."\""/ie;
    $self->do(
      "begin exec_stmt(\'CREATE $index " . $name . " ON " . $table . "  ( $fields )\');  end ;", {zero_length=>0}
    );
  }
  $DBI::errstr and $sqlError .= "In fetch: $DBI::errstr\n";
}

=item C<describeTable>

  $res = $dbh->describeTable($table,);

Describe the table exactly in the same way as mysql, since the output is processed in the interface.

=cut

sub describeTable {
  my $self  = shift;
  my $table = uc shift;
  return $self->_queryDB(
    "SELECT column_name \"Field\", 
REGEXP_REPLACE(REGEXP_REPLACE(nullable,\'\\Y\$\',\'YES\'),\'\\N\$\',\'NO\')  \"Null\", concat(concat(concat(data_type,\'(\'),data_length),\')\') \"Type\"  FROM ALL_tab_columns WHERE upper(owner) = upper(\'$self->{SCHEMA}\' ) AND upper(table_name)=upper(\'$table\')"
  );
}

=item C<getLastId>

  $res = $dbh->getLastId($table,);

get the last id of the latest row inserted

=cut

sub getLastId {
  my $self = shift;

  #here we take the table
  my $table = shift;

  #my $pk = shift;
  my $seq = $table . "_SEQ";
  my $id  = $self->queryColumn("select $seq.currval from dual");

  #my $id = $self->queryColumn("select max($pk) from $table");
  return $$id[0];
}

=item C<getionChain>

  $res = $dbh->getConnectionChain($table,);

get the combination for connecting through DBI

=cut

sub getConnectionChain {
  my $self = shift;

# defined $ENV{'ORACLE_SID'} or $self->info( "The Oracle SID is not defined in your system. Normally it should be. We take 'alien' as ORACLE_SID by convention" );
# my $db = $ENV{ORACLE_SID} || "db-alice-test";

  (my $db, $self->{SCHEMA}) = split(":", $self->{DB});
  if (uc($self->{SCHEMA}) ne uc($self->{ROLE})) {
    if ($self->{ROLE} !~ /^admin(ssl)?/i and $self->{SCHEMA} =~ /admin/i) {
      print STDERR "Only the administrator can access the admin schema\n";
      return;
    } elsif ($self->{ROLE} !~ /^admin(ssl)?/i and $self->{SCHEMA} !~ /admin/i) {
      $self->{SCHEMA} = $self->{ORACLE_USER};
    }
  }

  # ($db, $schema) = split(":" ,$self->{DB});
  if ($self->{HOST} =~ /^no_host/i) {
    return "DBI:Oracle:$db";
  } else {
    my ($host, $port) = split(":", $self->{HOST});
    return "DBI:Oracle:sid=$db;host=$host;port=$port";
  }
}

sub update {
  my $self    = shift;
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
      chop($query);
      $query .= "= NULL,";
    }
  }
  chop($query);
  $where =~ s/\=\s*\'\'/ IS NULL/g;
  $where and $query .= " WHERE $where";
  push(@bind, @{ $options->{bind_values} }) if ($options->{bind_values});
  $self->_do($query, { bind_values => \@bind, zero_length => 0 });
}

sub _queryDB {
  my ($self, $stmt, $options, $already_tried) = @_;
  $options or $options = {};
  my $oldAlarmValue = $SIG{ALRM};
  local $SIG{ALRM} = \&_timeout;
  $stmt =~ s/;$//;
  local $SIG{PIPE} = sub {
    print STDERR "Warning!! The connection to the AliEnProxy got lost\n";
    $self->reconnect();
  };

  $self->_pingReconnect or return;
  $stmt =~
s/(\,)(size|user|time|current|validate|date|file)(\,)/$1."\"".uc($2)."\"".$3/ieg;
  $stmt =~
s/(\()(size|user|time|current|validate|date|file)(\))/$1."\"".uc($2)."\"".$3/ieg;
  $stmt =~
s/\W(\s+)(size|user|time|current|validate|date|file)(\s+)/$1."\"".uc($2)."\"".$3/ieg;
  my $arrRef;
  my $execute;
  my @bind;
  my $b;
  $options->{bind_values} and push @bind, @{ $options->{bind_values} };
  $DEBUG
    and $self->debug(2, "In _queryDB executing $stmt in database (@bind).");

  ($stmt, $b) = $self->process_zero_length($stmt, \@bind);

  @bind = @{$b};

  if (!@bind) { undef @bind; }

  # $self->process_zero_length( $stmt, \@bind );
  while (1) {
    my $sqlError = "";
    eval {
      alarm(600);
      ###my $sth = $self->{DBH}->prepare_cached($stmt);

      my $sth = $self->{DBH}->prepare($stmt);
      $DBI::errstr and $sqlError .= "In prepare: $DBI::errstr\n";
      if ($sth) {

        $execute = $sth->execute(@bind);
        $DBI::errstr and $sqlError .= "In execute: $DBI::errstr\n";
        $arrRef = $sth->fetchall_arrayref({});
        $DBI::errstr and $sqlError .= "In fetch: $DBI::errstr\n";
        foreach (@$arrRef) {
          my %h;
          tie %h, 'Tie::CPHash';
          %h = %$_;
          $_ = \%h;
        }

        ###  $sth->finish;
        ### $DBI::errstr and $sqlError.="In finish: $DBI::errstr\n";
      }
    };
    $@ and $sqlError = "The command died: $@";
    alarm(0);

    if ($sqlError) {
      my $found = 0;
      $sqlError =~
/(Unexpected EOF)|(Lost connection)|(Constructor didn't return a handle)|(No such object)|(Connection reset by peer)|(MySQL server has gone away at)|(_set_fbav\(.*\): not an array ref at)|(Constructor didn't return a handle)/
        and $found = 1;

      if ($sqlError =~ /Died at .*AliEn\/UI\/Catalogue\.pm line \d+/) {
        die("We got a ctrl+c... :( ");
      }
      if ($sqlError =~ /Maximum message size of \d+ exceeded/) {

      }
      if ($sqlError =~ /ORA-/ and !$already_tried) {

#it could be because we are using a reserved word to select a field. We can quote all the fields in the selection.
        $stmt = $self->quote_query($stmt);

        #retry
        $already_tried = 1;
        $stmt
          and $self->_queryDB($stmt, $options, $already_tried)
          and $found = 1;

      }
      $found
        or $self->info("There was an SQL error: $sqlError", 1001)
        and return;
    }

    #If the statment got executed, we can exit the loop
    $execute and last;

    $self->reconnect
      or $self->info("The reconnection did not work")
      and return;
  }

  $oldAlarmValue and $SIG{ALRM} = $oldAlarmValue
    or delete $SIG{ALRM};

  $DEBUG
    and $self->debug(1,
    "Query $stmt successfully executed. ($#{$arrRef}+1 entries)");
  return $arrRef;
}

sub _rebuildIndexes {
  my $self    = shift;
  my $table   = shift;
  if  ($table){
    my $indexes = $self->query(
    "select index_name from all_indexes where upper(table_name) like upper(\'?\')",
    { bind_values=>[$table], zero_length=>0 }
   );
    if ($indexes) {
      foreach (@$indexes) {
        $self->do("ALTER INDEX $_->{index_name} REBUILD",
          {zero_length=>0 });
      }
    }
  }
  return;
}

sub _do {
  my $self    = shift;
  my $stmt    = shift;
  my $options = (shift or {});
  ($stmt !~ /^begin/) and $stmt =~ s/\;$//;
  if ($stmt =~ m/optimize table (.*)/) {
    $self->_rebuildIndexes($1);
    return;
  }

  # if($stmt =~ m/^insert/i ){ $options->{zero_length}=0;}
  $stmt =~
s/(\,)(size|user|time|current|validate|date|file)(\,)/$1."\"".uc($2)."\"".$3/ieg;
  $stmt =~
s/(\()(size|user|time|current|validate|date|file)(\))/$1."\"".uc($2)."\"".$3/ieg;
  $stmt =~
s/\W(\s)(size|user|time|current|validate|date|file)(\s)/$1."\"".uc($2)."\"".$3/ieg;
  my $oldAlarmValue = $SIG{ALRM};
  local $SIG{ALRM} = \&_timeout;
  my $check = $options->{zero_length};
  defined($check) or $check = 1;

  local $SIG{PIPE} = sub {
    print STDERR
"Warning!! The connection to the AliEnProxy got lost while doing an insert\n";
    $self->reconnect();
  };

  $DEBUG
    and $self->debug(2, "In _do checking is database connection still valid");

  $self->_pingReconnect or return;
  my @bind_values;
  $options->{bind_values}
    and push @bind_values, @{ $options->{bind_values} }
    and $options->{prepare} = 1;
  my $result;
  if ($check) {
    my $b = \@bind_values;
    ($stmt, $b) = $self->process_zero_length($stmt, $b);
   
    @bind_values = @{$b};
    if (scalar @bind_values == 0) { $options->{prepare} = 0; }
  }

  while (1) {
    my $sqlError = "";

    $result = eval {
      alarm(600);
      my $tmp;
      if ($options->{prepare}) {
        $DEBUG and $self->debug(2, "In _do doing $stmt @bind_values");

        my $sth = $self->{DBH}->prepare($stmt);
        $self->debug(2, "After  preparing the cached $stmt @bind_values");
        $tmp = $sth->execute(@bind_values);    # $tmp and $sth->finish;
      } else {
        $DEBUG and $self->debug(1, "In _do doing $stmt @bind_values");
        $tmp = $self->{DBH}->do($stmt);

      }

      $DBI::errstr and $sqlError .= "In do: $DBI::errstr\n";
      $tmp;
    };
    my $error = $@;
    alarm(0);
    if ($sqlError =~ m/ORA-01003/) {
      $self->reconnect();
      $self->_do($stmt, $options);
    } elsif ($sqlError =~ m/ORA-01502/) {

      $sqlError =~ m/ORA-01502\: index \'(.*)\' /i;
      my $wrong_index = $1;
      $self->reconnect();
      $self->_do("ALTER INDEX $wrong_index REBUILD");
      $self->reconnect();
      $self->_do($stmt, $options);
    }
    if ($error) {
      $sqlError .= "There is an error: $@\n";
      $options->{silent}
        or $self->info("There was an SQL error  ($stmt): $sqlError", 1001);
      return;
    }
    defined($result) and last;

    if ( $sqlError =~ /ORA-00955:/i
      or $sqlError =~ /already exists/i
      or $sqlError =~ /mit diesem Namen/i)
    {
      return 1;
    } else {
      my $found = 0;
      $sqlError =~
/(Unexpected EOF)|(Lost connection)|(MySQL server has gone away at)|(Connection reset by peer)/
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

  $DEBUG
    and
    $self->debug(1, "Query $stmt successfully executed with result: $result");

  $result;
}

sub getTypes {
  my $self = shift;

  $self->{TYPES} = {
    'serial'    => 'number(19) ',
    'SERIAL'    => 'number(19) ',
    'text'      => 'varchar2(4000)',
    'TEXT'      => 'varchar2(4000)',
    'char'      => 'varchar2',
    'CHAR'      => 'varchar2',
    'binary'    => 'raw',
    'BINARY'    => 'raw',
    'int'       => 'int',
    'INT'       => 'int',
    'number'    => 'number',
    'NUMBER'    => 'number',
    'tinyint'   => 'number',
    'TINYINT'   => 'number',
    'bigint'    => 'number',
    'BIGINT'    => 'number',
    'smallint'  => 'number',
    'SMALLINT'  => 'number',
    'mediumint' => 'number',
    'MEDIUMINT' => 'number',
    'float'     => 'float',
    'FLOAT'     => 'float',
    'datetime'  => 'date',
    'DATETIME'  => 'date',
    'varchar'   => 'varchar2',
    'VARCHAR2'  => 'varchar2',
    'timestamp' => 'timestamp',
    'TIMESTAMP' => 'timestamp',
    'time'      => 'timestamp',
    'TIME'      => 'timestamp',
    'integer'   => 'integer',
    'INTEGER'   => 'integer',
    'blob'      => 'blob',
    'BLOB'      => 'blob',
    'KEY'       => 'KEY',
    'KEY'       => 'KEY',
  };
  return 1;
}

sub binary2string {
  my $self = shift;
  my $column = shift || "guid";
  return " binary2string($column) ";

#return "insrt(insrt(insrt(insrt(rawtohex($column),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')";
}

sub grant {
  my $self = shift;
  $self->info("In oracle, we don't grant anything");
  return 1;
}

sub grantAllPrivilegesToUser {
  my $self  = shift;
  my $user  = shift;
  my $db    = shift;
  my $table = shift;

  $self->grantPrivilegesToUser(["ALL PRIVILEGES ON $db.$table"], $user);
}

sub grantPrivilegesToUser {
  my $self     = shift;
  my $rprivs   = shift;
  my $user     = shift;
  my $pass     = shift;
  my $origpass = $pass;
  if ($user !~
/alien_admin|alien_alien_system|alien_transfers|alien_processes|alien_informationservice/i
    )
  {
    $user = $self->{ORACLE_USER};
  }
  $DEBUG and $self->debug(1, "In grantPrivilegesToUser");
## if we get a pass then we want to create this user to the database
  $pass = $user;

  #and $pass = "$user IDENTIFIED BY '$pass'"
  #or $pass = $user;

  my $success = 1;
  for (@$rprivs) {
    $DEBUG and $self->debug(0, "Adding privileges $_ to $user");
    if ($_ =~ m/\*/i) {    ##INSERT, DELETE, UPDATE on $db.*
      $DEBUG and $self->debug(0, "Adding privileges with *");
      my $user_from = $_;
      $user_from =~ s/\.(.)*//;
      $user_from =~ s/(.)*\://;
      $user_from =~ s/(.)* ON//i;
      if (!$user_from || ($user_from =~ /\*/)) { $user_from = $self->{SCHEMA}; }

      #        $user_from=~s/(.)*:(.)*/$2/i;print "\nWe have a star $user_from";
      if ($_ =~ /INSERT/i) {
        $self->do(
          "begin grantstar (\'INSERT\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/UPDATE/i) {
        $self->do(
          "begin grantstar (\'UPDATE\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/DELETE/i) {
        $self->do(
          "begin grantstar (\'DELETE\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/SELECT/i) {
        print "\nWE have select FROM $user_from TO $user ****";
        $self->do(
          "begin grantstar (\'SELECT\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/EXECUTE/i) {
        $self->do(
          "begin grantstar (\'EXECUTE\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/ALL PRIVILEGES/i) {
        $self->do(
          "begin grantstar (\'ALL PRIVILEGES\',\'$user_from\',\'$user\'); end; "
        );
      }
    } else {

      $self->_do("GRANT $_ TO $pass")
        or $DEBUG
        and $self->debug(0, "Error adding privileges $_ to $user")
        and $success = 0;
    }
  }
  return $success;
}

sub revokeAllPrivilegesFromUser {
  my $self  = shift;
  my $user  = shift;
  my $db    = shift;
  my $table = shift;
  $self->revokePrivilegesFromUser(["ALL PRIVILEGES ON $db.$table"], $user);
}

sub revokePrivilegesFromUser {
  my $self   = shift;
  my $rprivs = shift;
  my $user   = shift;

  my $success = 1;
  for (@$rprivs) {
    $DEBUG and $self->debug(0, "Adding privileges $_ to $user");
    if ($_ =~ m/\*/i) {    ##INSERT, DELETE, UPDATE on $db.*
      $DEBUG and $self->debug(0, "Adding privileges with *");
      print "We have a star";
      my $user_from = $1 || $self->{SCHEMA};
      $user_from =~ s/(.)*:(.)*/$2/i;
      if ($_ =~ /INSERT/i) {
        $self->do(
          "begin revokestar (\'INSERT\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/UPDATE/i) {
        $self->do(
          "begin revokestar (\'UPDATE\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/DELETE/i) {
        $self->do(
          "begin revokestar (\'DELETE\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/SELECT/i) {
        $self->do(
          "begin revokestar (\'SELECT\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/EXECUTE/i) {
        $self->do(
          "begin revokestar (\'EXECUTE\',\'$user_from\',\'$user\'); end; ");
      } elsif ($_ =~ m/ALL PRIVILEGES/i) {
        $self->do(
"begin revokestar (\'ALL PRIVILEGES\',\'$user_from\',\'$user\'); end; "
        );
      }
    } else {
      $self->_do("REVOKE $_ FROM  $user")
        or $DEBUG
        and $self->debug(0, "Error adding privileges $_ to $user")
        and $success = 0;
    }
  }
  return $success;
}

sub multiinsert {
  my $self    = shift;
  my $table   = shift;
  my $rarray  = shift;
  my $options = shift;
  my $rloop;

  ###     statement checking is a temporary solution ... remove later!!!
  if ($table =~ /\s/) { return $self->do($table); }

  my $rfields = @$rarray[0];

  my $query = "INSERT";

  my @fields     = keys %$rfields;
  my $new_fields = $self->preprocessFields(\@fields);    #for the reserved words
  my @new_f      = @$new_fields;
  $query .= " INTO $table (" . join(", ", @new_f) . ") VALUES ";
  my $quote = "'";
  $options->{noquotes} and $quote = "";

  #my @arr = values %$rfields;
  my @bind = ();

  foreach $rloop (@$rarray) {
    my $query2 = "(";
    @bind = ();
    foreach (keys %$rfields) {
      if (defined $rloop->{$_}) {

        if ($quote) {
          $query2 .= "?,";
        } else {
          $rloop->{$_} =~ s/^([^'"]*)['"](.*)['"]([^'"]*)$/$2/;
          my $function    = "";
          my $functionend = "";
          if ($1 && $3) {
            $function = $1 and $functionend = $3;
          }
          $query2 .= " $function ? $functionend,";
        }
        push @bind, $rloop->{$_};
      } else {
        $query2 .= "NULL,";
      }
    }
    chop($query2);

    $query2 .= ")";
    my $doOptions = { bind_values => \@bind };

    # $doOptions->{zero_length}=0;
    $options->{silent} and $doOptions->{silent} = 1;
    $self->info("Estamos en multiinsert oracle con @bind");
    $self->_do($query . $query2, $doOptions);
    if ($options->{ignore} && $DBI::errstr =~ /ORA-00001: unique constraint/) {
      my $delete = "delete from $table where ";
      my $i;
      for $i (0 .. $#new_f) {
        $delete .= $new_f[$i] . " = $bind[$i]  AND ";
      }
      $delete =~ s/(.*)AND $/$1/;

      $self->_do($delete, { zero_length => 0 });
      $self->_do($query . $query2, $doOptions);
    }
  }
  return 1;

  # chop($query);
  # my $doOptions={bind_values=>\@bind};
  #$options->{silent} and $doOptions->{silent}=1;
  #use Data::Dumper;
  #print "\n\n\n_do  $query and ".Dumper($doOptions);
  # $self->_do($query, $doOptions);
}

sub createLFNfunctions {
  my $self = shift;

  $self->do(
    "create or replace function conv
(N varchar2 , from_base number, to_base number)
return varchar2
is
resul VARCHAR2(255);
begin
select TO_CHAR(sum(position_value)) INTO resul from

(
  select power(from_base,position-1) * case when digit between '0' and '9' then to_number(digit)
       else to_base + ascii(digit) - ascii('A')
      end
    as position_value
    from (
    select substr(input_string,length(input_string)+1-level,1) digit, level position
  from (select N input_string from dual)
  connect by level <= length(input_string)
   )
);
if(N like 'NULL') then return null;
else
return resul;
end if;
end;;"
  );
  $self->do("grant all privileges on conv to public");
  $self->do ("create or replace FUNCTION unix_timestamp return number deterministic  AUTHID current_user is begin return to_number(sysdate - to_date('01-JAN-1970','DD-MON-YYYY')) * (86400); END ;"); 
  $self->do("grant all privileges on unix_timestamp to public");
  $self->do("create synonym unix_timestamp for alien_system.unix_timestamp");
  $self->do(
    "create or replace FUNCTION INSRT
(str1 in VARCHAR2, num1 in NUMBER, num2 in NUMBER, str2 in VARCHAR2)
return VARCHAR2
deterministic 
AUTHID current_user
is begin
if(num1<1)then return str1;
end if;
if(str1 is null or str2 is null) then return null;
end if;
return 
concat(concat(substr(str1, 1, num1-1), substr(str2, 1, num2)), substr(str1, num1+num2, length(str1)));
END INSRT;;"
  );

  $self->do(
    "create or replace
function now
return date as begin
return sysdate ;
end;;"
  );

  $self->do("grant execute on now to public");
  $self->do("create synonym now for alien_system.now");

  $self->do("grant execute on insrt to public");
  $self->do("create synonym insrt for alien_system.insrt");

#-return insrt(insrt(insrt(insrt(rawtohex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-');
  $self->do(
    "create or replace
function binary2string 
(my_uuid in raw) 
return VARCHAR2
deterministic 
AUTHID current_user
is
begin
return 
 concat (
 concat(
 concat(concat (concat(substr(my_uuid, 0, 8),'-'),
CONCAt(substr(my_uuid,9,4),'-')) , 
concat(substr(my_uuid, 13,4),'-')) ,
concat (substr(my_uuid,17,4), '-')), substr(my_uuid, 21))
; 
end binary2string;;"
  );

  $self->do("grant execute on binary2string to public");
  $self->do(
    "create or replace
function binary2date 
(my_uuid in raw)  
return varchar2
deterministic 
AUTHID current_user
as
begin
return 
substr(
upper(concat(concat(substr(substr
    (rawtohex(my_uuid),1,16),16-4+1),
substr
  (substr(rawtohex(my_uuid),1,12),12+4-1)),
  substr(rawtohex(my_uuid),1,8))), 1, 8);
end binary2date;;"
  );
  $self->do("grant execute on binary2date  to public");

  $self->do(
    "create or replace function string2binary
(my_uuid in varchar2)  
return raw
deterministic 
AUTHID current_user
as
begin
if(my_uuid like 'NULL')then return null;else 
return 
hextoraw(replace(my_uuid,'-',''));end if;
end string2binary;;"
  );
  $self->do("grant execute on string2binary  to public");
}

sub createGUIDFunctions {
  my $self = shift;
  $self->do(
    "create or replace FUNCTION INSRT
(str1 in VARCHAR2, num1 in NUMBER, num2 in NUMBER, str2 in VARCHAR2)
return VARCHAR2
deterministic 
AUTHID current_user
is begin
if(num1<1)then return str1;
end if;
if(str1 is null or str2 is null) then return null;
end if;
return 
concat(concat(substr(str1, 1, num1-1), substr(str2, 1, num2)), substr(str1, num1+num2, length(str1)));
END INSRT;;"
  );
  $self->do("grant execute on insrt to public");
  $self->do(
    "create or replace
function binary2string 
(my_uuid in raw) 
return VARCHAR2
deterministic 
AUTHID current_user
is
begin
--return insrt(insrt(insrt(insrt(rawtohex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-');

return 
 concat (
 concat(
 concat(concat (concat(substr(my_uuid, 0, 8),'-'),
CONCAt(substr(my_uuid,9,4),'-')) , 
concat(substr(my_uuid, 13,4),'-')) ,
concat (substr(my_uuid,17,4), '-')), substr(my_uuid, 21))
; 
end binary2string;;"
  );
  $self->do("grant execute on binary2string to public");

  $self->do(
    "create or replace
function binary2date 
(my_uuid in raw)  
return varchar
deterministic 
AUTHID current_user
as
begin
return 
upper(concat(concat(substr(substr
    (rawtohex(my_uuid),1,16),16-4+1),
substr
  (substr(rawtohex(my_uuid),1,12),12+4-1)),
  substr(rawtohex(my_uuid),1,8)));
end binary2date;;"
  );
  $self->do("grant execute on binary2date to public");

  $self->do(
"create or replace function string2date (my_uuid in varchar2)   return varchar deterministic  AUTHID current_user as begin if(my_uuid like 'NULL')then return null;else  return  substr( upper(   concat(  concat(substr(substr(my_uuid,1,18),18-4+1), substr(substr(my_uuid,1,13),13-4+1)),substr(my_uuid,1,8))),1,8);end if; end string2date;;
"
  );
  $self->do("grant execute on string2date to public");
}

sub lock {
  my $self = shift;
  my $lock = shift;

  # $DEBUG and $self->debug(1,"Database: In lock locking table $table.");
  $lock =~ /,/
    and $self->info("Oracle doesn't know how to lock multiple tables") and return 1;

  $self->_do("LOCK TABLE $lock IN ROW EXCLUSIVE MODE");
}

sub unlock {
  my $self  = shift;
  my $table = shift;

  $DEBUG and $self->debug(1, "Database: In lock unlocking tables.");

  $table and $table = " $table"
    or $table = "S";

  return "COMMIT";

}

#return the columns that are being added and not exist in the table yet

sub paginate {
  my $self   = shift;
  my $sql    = shift;
  my $limit  = shift;
  my $offset = shift;
  $offset or $offset = 0;
  if ($offset <= 0) { $offset = 1; }
  if ($limit and $limit >= 0) {

#return "select query.* from (select P.* ,rownum R from ($sql ) P ) query where R between $offset and $limit+$offset-1 ";
    return
"SELECT P.* FROM ($sql) P WHERE rownum BETWEEN $offset and $limit + $offset -1";
  } else {
    return $sql;
  }
}

sub optimizeTable {
  my $self  = shift;
  my $table = uc shift;
  $self->do("alter table $table move");
  my $indexes = $self->query(
"select INDEX_NAME from all_indexes where OWNER = upper(\'$self->{SCHEMA}\') and TABLE_NAME LIKE \'$table\'"
  );
  foreach my $ind (@$indexes) {
    $self->do(
      "begin exec_stmt(\'alter index $ind->{INDEX_NAME} rebuild\'); end;"    );
  }
}

sub schema {
  my $self = shift;
  return $self->{SCHEMA};
}

sub resetAutoincrement{
 return 1;
}
sub resetAutoincrement2 {
  my $self   = shift;
  my $table  = shift;
  my $sqName = $table . "_seq";
  $self->do(
    "begin 
exec_stmt(\'drop sequence $sqName\');
end;"
  );
  $self->do("
begin
exec_stmt(\'create sequence $sqName
start with 1 
increment by 1 
nomaxvalue\');
end;");
}

sub defineAutoincrement {
  my $self        = shift;
  my $tableName   = shift;
  my $field       = shift;
  my $sqName      = $tableName . "_seq";
  my $triggerName = $tableName . "_trigger";
  my $exists      = $self->queryValue(
" SELECT count(1) FROM all_sequences where upper(sequence_name)=upper('$sqName')  and sequence_owner like upper('$self->{SCHEMA}')"
  );
  $exists &= $self->queryValue(
" SELECT count(1) FROM all_triggers where upper(trigger_name)=upper('$triggerName')  and owner like upper('$self->{SCHEMA}')"
  );

  if (!$exists) {
    $self->do(" begin exec_stmt(\'create sequence $sqName start with 1  increment by 1 nomaxvalue\'); exec_stmt(\'create or replace trigger $triggerName before insert on $tableName for each row begin select $sqName.nextval into :new.$field from dual; end;\');end; ;");

  }
  return 1;
}

sub existsTable {
  my $self  = shift;
  my $table = shift;
  $table = uc($table);
  my $ref = $self->queryColumn(
"SELECT COUNT(*) FROM all_TABLES WHERE OWNER = upper(\'$self->{SCHEMA}\') and upper(table_name) like (?)", undef, {bind_values=>[$table]}
  );
  return $$ref[0];

  #return 0;
}

sub renameField {
  my $self  = shift;
  my $table = shift;
  my $old   = shift;
  my $new   = shift;
  $self->do(
    "begin exec_stmt(\'ALTER TABLE $table rename COLUMN  $old to  $new\'); end;"  );
}

sub quote_query {
  my $self = shift;
  my $stmt = shift;
  if ($stmt =~ /select(\s+)(\w+|\w+(\s*\,\s*\w+)+)(\s+)from/i) {
    my $cols = $2;
    my $old  = $cols;
    $cols = uc($cols);
    $cols =~ s/(\s*)\,(\s*)/\"\,\"/igx;
    $cols = "\"$cols\"";
    $stmt =~ s/SELECT(\s+)$old(\s+)(.)/SELECT$1$cols$2$3/i;
    return $stmt;
  } else {
    return;
  }

}

sub regexp {
  my $self    = shift;
  my $col     = shift;
  my $pattern = shift;
  return " regexp_like($col, '$pattern')";
}

sub dateFormat {
  my $self = shift;
  my $col  = shift;
  return "TO_CHAR($col, 'MON DD HH24:MI')  $col";
}

sub preprocess_where_delete {
  my $self  = shift;
  my $where = shift;

  my @new_where = split(/AND/i, $where);
  foreach (@new_where) {
    $_ =~ s/(\w+)(\s*)=(\s*)(\w+)/"\"". uc($1) . "\"=".$4/mexgi;
  }

  return join(" AND ", @new_where);
}

sub _connectSchema {
  my $self   = shift;
  my $schema = shift;

  #!$self->{DBH} and return;
  !$schema    # and !$self->{SCHEMA}
    and $self->{SCHEMA} = $self->{DB};

  $self->{SCHEMA} =~ s/(.+):(.+)/$2/i;

  $self->debug(1, "connecting to the current schema: $self->{SCHEMA}");

  $self->do("ALTER SESSION SET CURRENT_SCHEMA = $self->{SCHEMA}");
}

sub checkUser {
  my $self     = shift;
  my $user     = shift;
  my $pass     = shift;
  my $sqlError = "";
  $user = uc $user;
  my $res = $self->_queryDB(
    "SELECT USERNAME FROM ALL_USERS WHERE USERNAME LIKE upper(?)",
    undef, { bind_variables => [$user] });

  if ($res) {
    $self->do("ALTER USER $user IDENTIFIED BY \"$pass\"");

    return 1;
  }

  if ($pass) {
    $self->do(
"CREATE USER $user IDENTIFIED BY \"$pass\" DEFAULT TABLESPACE ALIEN_TABLESPACE  QUOTA UNLIMITED ON alien_tablespace ACCOUNT UNLOCK"
    );
    $self->do("GRANT ALIEN_OPER TO $user");
    return 1;
  }
  $@ and $sqlError = "The command died: $@";
  if ($@ =~ /ORA-01920/i) {
    $DEBUG
      and $self->info("This user already exists", 1)
      and return 1;    #if the user already exists , this is correct (?)
  }
  return 1;
}

sub process_zero_length_old {
  my $self = shift;
  my $stmt = shift;
  my $bind = shift;

  #case without binding values
  $stmt =~
s/(.*)(\!\=|\<\>|NOT\sLIKE)(\s*(\'\s?\') \s*)(.*)$/$1. " IS NOT NULL ". $4/gxei;

  $stmt =~ s/(.*)(\=|LIKE)(\s*\'\s?\' \s*)(.*)$/$1. " IS NULL ".$4/gxei;

  my @bind = @{$bind};
  if ($bind) {

    #  my @bind = @{$bind_values};

    #case binding values
    if (grep { /^$/ } @bind) {
      my @new_bind = ();
      my $left     = $stmt;
      my $new_stmt = " ";
      foreach (@bind) {

        #element with string length zero
        if ($_ =~ /^$/) {

#change the statement to consider if the column is null and remove it from the bind values
          if ($left =~
s/(.*)(\!\= |\<\>|NOT\sLIKE)(\s*\? )(.*)/$1 . " IS  NOT NULL ".$4 /xei
            )
          {
          } else {
            $left =~ s/(.*)(\=|LIKE)(\s*\? )(.*)/$1 . " IS NULL ".$4 /xei;
          }
          $new_stmt = $left;
        } else {    #case element with string no length zero
          push(@new_bind, $_);
          $left =~ s/(.*)(\s*\? )(.*)/$1  .$2 .$3/xei
            ;       # $new_stmt=$new_stmt.$left ; $left=$3
          $new_stmt = $left;
        }
      }
      @bind = @new_bind;
      $stmt = $new_stmt;
      return ($new_stmt, \@new_bind);
    } else {

      #case the binding values have not got zero length,do nothing}
      return ($stmt, \@bind);
    }
  }
  return ($stmt);
}

sub process_zero_length {
  my $self = shift;
  my $stmt = shift;
  my $b    = shift;

  #case without binding values
  while ($stmt =~ s/(\!\=|\<\>|NOT\sLIKE)(\s*\'\' \s*)/ " IS NOT NULL "/gxei) {
  }
  while ($stmt =~ s/(\=|LIKE)(\s*\'\s?\' \s*)/" IS NULL "/gxei) {
  }
  my @bind = @{$b};

  if ($b) {

    #case binding values
    if (grep { /^$/ } @bind) {
      my @new_bind = ();
      my $left     = $stmt;
      my $append   = " ";
      my $new_stmt = " ";
      foreach (reverse(@bind)) {

        #element with string length zero
        if ($_ =~ /^$/) {

#change the statement to consider if the column is null and remove it from the bind values
          if ($left =~
s/(.*)(\!\=|\<\>|NOT\s+LIKE)(\s*\? )(.*) /$1 . " IS NOT NULL ".$4 /xei
            )
          {
          } else {
            if ($left =~ m/WHERE(.*)(\=|LIKE)\s*\?/ix)
            {    #(\s*(or|and)\s*\w\s*(\=|LIKE)\s*\?)?/i){
              $left =~ s/(.*)(\=|LIKE)(\s*\? )(.*) /$1." is NULL ".$4 /xei;
            } elsif ($left =~ m/WHERE(.*)\(\s*\?\)/ix) {
              $left =~ s/(.*)(\(\s*\?\)) (.*)/$1 . " \( NULL\) ".$3 /xei;
            } else {
              $left =~ s/(.*)(\=|LIKE)(\s*\? )(.*) /$1 . "=NULL ".$4 /xei;
            }
          }
          $new_stmt = $left;

        } else {    #case element with string no length zero
          push(@new_bind, $_);
          $new_stmt = $left =~ s/(.*)(\s*\? )(.*)/$1/xei;
          $append = $2 . $3 . $append;    # $new_stmt=$new_stmt.$left ; $left=$3

          #   $new_stmt = $new_stmt."".$2."".$3;
        }

      }
      @new_bind and @bind = reverse(@new_bind) or @bind = ();
      $stmt = $left . $append;
      return ($stmt, \@bind);
    } else {

      #case the binding values have not got zero length,do nothing}
      return ($stmt, \@bind);
    }
  }
  return ($stmt);
}

sub _timeUnits {
  my $self    = shift;
  my $seconds = shift;
  return $seconds / 24 / 60 / 60;
}

sub dbGetAllTagNamesByPath {
  my $self    = shift;
  my $path    = shift;
  my $options = shift || {};

  my $rec  = "";
  my $rec2 = "";
  my @bind = ($path);
  if ($options->{r}) {
    $rec = " or path like concat(?, '%') ";
    push @bind, $path;
  }
  if ($options->{user}) {
    $self->debug(1, "Only for the user $options->{user}");
    $rec2 = " and user=?";
    push @bind, $options->{user};
  }

#return $self->query("SELECT DISTINCT TAGNAME,PATH FROM (SELECT TAGNAME,PATH,ENTRYID FROM TAG0 where  ? like concat(path,'%')   $rec  $rec2) where ENTRYID in((select min(ENTRYID) from TAG0 group by tagName) union (select max(ENTRYID) from TAG0 group by tagName)) ", undef, {bind_values=>\@bind});

#return $self->query("SELECT distinct TAGNAME,PATH from (select tagname,path, entryid  FROM TAG0 where (? like path) or  ( ? like concat(path,'%')  )  $rec  $rec2 and rownum <=1 order by entryId desc )", undef, {bind_values=>\@bind});
#return $self->query("SELECT distinct TAGNAME,PATH,entryid from (select tagname,path, entryid  FROM TAG0 where  ( ? like concat(path,'%')  )  $rec  $rec2 and rownum <=1 order by entryId desc )", undef, {bind_values=>\@bind});

  return $self->query(
"select tagname , path from ( select distinct tagname,path, length(path) LEN from tag0 where ?  like concat(path,'%')   $rec  $rec2 order by LEN desc )  where rownum <= 1",
    undef,
    { bind_values => \@bind }
  );
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
  if ($info < 100000) {
    $self->debug(1, "Only $info. We don't need to renumber");
    return 1;
  }

  $self->info("Let's renumber the table $table");

  $self->lock($lock);
  my $ok = 1;
  $self->do("alter table $table modify $index number(11)");
  $self->do(" alter table $table drop primary key");
  $self->do("alter table $table add new_index number(11)  primary key");
  $self->defineAutoincrement($table, "new_index");
  $self->do("create unique index $table" . "_uk on table $table (guidid)")
    or $ok = 0;

  if ($ok) {
    foreach my $t (@{ $options->{update} }) {
      $self->debug(1, "Updating $t");
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
    $self->do("alter table $table drop column $index");
    $self->renameField($table, "new_index", $index, " number(11) ");
    $self->defineAutoincrement($table, $index);
  } else {
    $self->info("The update didn't work. Rolling back");
    $self->do("alter table $table drop new_index");
    $self->do("alter table $table modify $index number(11) primary key");
    $self->defineAutoincrement($table, $index);
  }

  $self->unlock($table);

  return 1;

}

sub addTimeToToken {
  my $self  = shift;
  my $user  = shift;
  my $hours = shift;
  return $self->do(
"update TOKENS set Expires=(sysdate +INTERVAL '$hours' hour) where Username='$user'"
  );

}

sub _deleteFromTODELETE {
  my $self = shift;
  $self->do(
"delete from TODELETE WHERE todelete.entryid in (select todelete.entryid from Todelete join SE s on TODELETE.senumber=s.senumber where sename='no_se' and pfn like 'guid://%')"
  );

}

sub getTransfersForOptimizer {
  my $self = shift;
  return $self->query(
"SELECT transferid FROM TRANSFERS_DIRECT where (status='ASSIGNED' and  ctime< to_timestamp(now()- (30/1440))  or (status='TRANSFERRING' and to_timestamp(to_char(started),'DD.MM.YYYY:HH24:MI:SS')<to_timestamp(now()-(1/6))))"
  );
}

sub getToStage {
  my $self = shift;
  return $self->query(
"select s.queueid, jdl from STAGING s, QUEUE q where s.queueid=q.queueid and (staging_time+5/1440)<now()"
  );

}

sub unfinishedJobs24PerUser {
  my $self = shift;

  return $self->do(
"merge  into PRIORITY p using (select SUBSTR( submitHost, 1, instr(submitHost,'\@') -1)  \"USER\", count(1)  unfinishedJobsLast24h from queue q where (status='INSERTING' or status='WAITING' or status='STARTED' or status='RUNNING' or status='SAVING' or status='OVER_WAITING') and ( (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')   >=  To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + received / 86400,'DD.MM.YYYY HH24:Mi:ss')) and (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')  <=  To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + received / 86400 + 60*60*24,'DD.MM.YYYY HH24:Mi:ss'))) GROUP BY submithost ) c on (upper(p.\"USER\")=upper(c.\"USER\")) when matched then update set p.unfinishedjobslast24h=c.unfinishedjobslast24h"
  );
}



sub cpuCost24PerUser {
  my $self = shift;
  return $self->do(
    "merge  into PRIORITY pr using 
(select SUBSTR( submitHost, 1, instr(submitHost,'\@') -1)  \"USER\",sum(p.cost)  totalCpuCostLast24h, sum(p.runtimes) as totalRunningTimeLast24h 
from queue q , QUEUEPROC p where ( (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')   >=  
To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + q.received / 86400,'DD.MM.YYYY HH24:Mi:ss'))  
and (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')  <=   
To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + q.received / 86400 + 60*60*24,'DD.MM.YYYY HH24:Mi:ss'))) 
GROUP BY submithost ) c on (pr.\"USER\"=c.\"USER\") 
when matched then update set  pr.totalCpuCostLast24h=c.pr.totalCpuCostLast24h"
  );
}

sub execHost {
  return "SUBSTR( execHost, instr(execHost,'\\\@') + 1)";
}

sub changeOWtoW {
  my $self = shift;
  return $self->do(
    "merge  into QUEUE q using 
(select queueId
from queue qu join PRIORITY pr on ( pr.\"USER\" = SUBSTR( submitHost, 1, instr(submitHost,'\@') -1) )
where (pr.totalRunningTimeLast24h<pr.maxTotalRunningTime 
and pr.totalCpuCostLast24h<pr.maxTotalCpuCost) and qu.status=\'OVER_WAITING\' ) c 
on (q.queueId=c.queueId)
when matched then update set  q.status='WAITING'"
  );
}

sub changeWtoOW {
  my $self = shift;
  return $self->do(
    "merge  into QUEUE q using 
(select queueId
from queue qu join PRIORITY pr on ( pr.\"USER\" = SUBSTR( submitHost, 1, instr(submitHost,'\@') -1) )
where (pr.totalRunningTimeLast24h>=pr.maxTotalRunningTime 
and pr.totalCpuCostLast24h>=pr.maxTotalCpuCost) and qu.status='WAITING' ) c 
on (q.queueId=c.queueId)
when matched then update set  q.status=\'OVER_WAITING\'"
  );
}

sub updateFinalPrice {
  my $self     = shift;
  my $t        = shift;
  my $nominalP = shift;
  my $now      = shift;
  my $done     = shift;
  my $failed   = shift;
  return $self->do(
    "merge  into $t q using  (select si2k,$nominalP, qu.queueid
from queue qu, queueproc  where  (status=\'DONE\' AND si2k>0 AND chargeStatus!=\'$done\' AND chargeStatus!=\'$failed\')  ) c 
on (q.queueId=c.queueId) when matched then update  set   q.finalPrice= c.si2k * 1 * q.price, q.chargeStatus=\'$now\' "
  );
}

sub optimizerJobExpired {
  return
"((status='DONE') or (status='FAILED') or (status='EXPIRED') or (status like 'ERROR%')  )
and To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + received/86540 +7*85540) < (now()) ";
}

sub optimizerJobPriority {
  my $self       = shift;
  my $userColumn = "SUBSTR( submitHost, 1, instr (submitHost,\'@\' )-1 )";
  return $self->do(
"INSERT  INTO PRIORITY(\"USER\", priority, maxparallelJobs, nominalparallelJobs) SELECT distinct $userColumn, 1,200, 100 from QUEUE q where not exists (select * from priority where \"USER\"= $userColumn)"
  );
}

sub userColumn {
#return "SUBSTR( submitHost, 1, instr (submitHost,\'@\' )-1 )";
  return "SUBSTR( submitHost, 1, instr (\'@\' , submitHost)-1 )";
}

sub getMessages {
  my $self    = shift;
  my $service = shift;
  my $host    = shift;
  my $time    = shift;
  return $self->query(
"SELECT ID,TargetHost,Message,MessageArgs from MESSAGES WHERE TargetService = ? AND  ? like TargetHost AND (Expires > ? or Expires = 0) AND rownum <300 order by ID",
    undef,
    { bind_values => [ $service, $host, $time ] }
  );

}

sub createUser {
  my $self = shift;
  my $user = shift;
  my $pwd  = shift;
  $self->do(
"create user $user IDENTIFIED BY \"$pwd\" DEFAULT TABLESPACE ALIEN_TABLESPACE  quota unlimited on alien_tablespace"
  );
  $self->do("ALTER USER $user ACCOUNT UNLOCK");
  $self->do("GRANT ALIEN_OPER TO $user");
  return $self->do("COMMIT");
}

sub currentDate {
  return 'sysdate';
}

sub setUpdateDefaultCurrentTimestamp {
  my $self  = shift;
  my $table = shift;
  my $col   = shift;
  $self->setUpdateDefault($table, $col, "CURRENT_TIMESTAMP");

}

sub setUpdateDefault {
  my $self  = shift;
  my $table = shift;
  my $col   = shift;
  my $val   = shift;
  $self->do("create or replace TRIGGER  " 
      . $table
      . "_t_ctime BEFORE UPDATE ON  "
      . $table
      . " FOR EACH ROW BEGIN      select "
      . $val
      . " into :new."
      . $col
      . " from dual; END "
      . $table
      . "_t_ctime;;");
  return 1;
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
   select ?, seNumber,  ?, 0  from SE where upper( seName) LIKE upper(?)  ",
    { bind_values => [ $site, $rank, $seName, $seName, $site ] }
  );
}
#####
###Specific for Database/Catalogue/GUID
###

sub insertLFNBookedDeleteMirrorFromGUID {
  my $self     = shift;
  my $table    = shift;
  my $lfn      = shift;
  my $guid     = shift;
  my $role     = shift;
  my $pfn      = shift;
  my $guidId   = shift;
  my $seNumber = shift;
  $self->debug(3,
"The query is SELECT COUNT(*) FROM LFN_BOOKED WHERE lfn LIKE ?  AND guid = string2binary(?) AND pfn LIKE ? and the values $lfn $guid $pfn"
  );

  my $exists = $self->queryValue(
"SELECT COUNT(*) FROM LFN_BOOKED WHERE lfn LIKE ?  AND guid = string2binary(?) AND pfn LIKE ?",
    undef,
    { bind_values => [ $lfn, $guid, $pfn ], }
  );
  if (!$exists || $exists == 0) {
    return $self->do(
"INSERT INTO LFN_BOOKED(lfn, owner, expiretime, \"SIZE\", guid, gowner, user, pfn, se)
      select ?,g.owner,-1,g.\"SIZE\",string2binary(?),g.gowner,?,?,s.seName
      from " . $table . " g, " . $table . "_PFN g_p, SE s
      where g.guidId=g_p.guidId and g_p.guidId=? and g_p.seNumber=? and g_p.pfn=? and s.seNumber=g_p.seNumber",
      undef,
      { bind_values => [ $lfn, $guid, $role, $pfn, $guidId, $seNumber, $pfn ],
        zero_length => 0
      }
    );
  } else {
    return $self->do(
"UPDATE LFN_BOOKED SET (lfn, owner, expiretime, \"SIZE\", guid, gowner, user, pfn, se) = 
      (select ?,g.owner,-1,g.\"SIZE\",string2binary(?),g.gowner,?,?,s.seName
       from " . $table . " g, " . $table . "_PFN g_p, SE s
      where g.guidId=g_p.guidId and g_p.guidId=? and g_p.seNumber=? and g_p.pfn=? and s.seNumber=g_p.seNumber)",
      { bind_values => [ $lfn, $guid, $role, $pfn, $guidId, $seNumber, $pfn ],
        zero_length => 0
      }
    );
  }
}
####
# Specific for Database/IS
##
sub insertLFNBookedRemoveDirectory {
  my $self      = shift;
  my $lfn       = shift;
  my $tableName = shift, my $user = shift;
  my $tmpPath   = shift;

# my $values = $self->query("SELECT pfn,lfn,guid FROM   LFN_BOOKED LB,$tableName L WHERE  LB.lfn like concat(?,L.lfn) and LB.OWNER LIKE L.OWNER
#  AND LB.EXPIRETIME=-1 AND LB.\"SIZE\" = L.\"SIZE\" AND LB.GUID = L.GUID AND LB.GOWNER=L.GOWNER AND LB.\"USER\" like ? ",undef,{bind_values=>[$lfn,$user]});

  # if (!$exists || $exists == 0){
  return $self->do(
"INSERT INTO LFN_BOOKED(lfn, owner, expiretime,\"SIZE\", guid, gowner, \"USER\", pfn)
     SELECT concat('$lfn' , l.lfn), l.owner, -1, l.\"SIZE\", l.guid, l.gowner, ? , '*' FROM $tableName l WHERE l.type='f' AND l.lfn LIKE concat (?,'%')",
    { bind_values => [ $user, $tmpPath ], zero_length => 0 }
  );

#}else{
#  return $self->do("UPDATE LFN_BOOKED SET (lfn, owner, expiretime, \"SIZE\", guid, gowner, \"USER\", pfn)=
#   (SELECT concat('$lfn' , l.lfn), l.owner, -1, l.\"SIZE\", l.guid, l.gowner, ?,'*' FROM $tableName l WHERE l.type='f' AND l.lfn LIKE concat (?,'%')) $tmpPath",    {bind_values=>[$user,$tmpPath],zero_length=>0}) ;
#}
}

###
##Specific for Catalogue/Authorize
###
sub insertLFNBookedAndOptionalExistingFlagTrigger {
  my $self       = shift;
  my $lfn        = shift;    #$envelope->{lfn};
  my $user       = shift;    #$user;
  my $quota      = shift;    #, "1"
  my $md5sum     = shift;    #,$envelope->{md5}
  my $expiretime = shift;    #$lifetime,
  my $size       = shift;    #$envelope->{size},
  my $pfn        = shift;    #$envelope->{turl},
  my $se         = shift;    #$envelope->{se},$user,
  my $guid       = shift;    # $envelope->{guid},
  my $existing   = shift;    #$trigger,
  my $jobid      = shift;    #$jobid;
  $self->debug(3,
"The query is SELECT COUNT(*) FROM LFN_BOOKED WHERE lfn LIKE ?  AND guid = string2binary(?) AND pfn LIKE ? and the values $lfn $guid $pfn"
  );
  my $exists = $self->queryValue(
"SELECT COUNT(*) FROM LFN_BOOKED WHERE lfn LIKE ?  AND guid = string2binary(?) AND pfn LIKE ?",
    undef,
    { bind_values => [ $lfn, $guid, $pfn ] }
  );

  if (!$exists || $exists == 0) {
    return $self->do(
"INSERT INTO LFN_BOOKED (lfn, owner, quotaCalculated, md5sum, expiretime, \"SIZE\", pfn, se, gowner, guid, existing, jobid) VALUES (?,?,?,?,?,?,?,?,?,string2binary(?),?,?)",
      {
        bind_values => [
          $lfn, $user, $quota, $md5sum, $expiretime, $size,
          $pfn, $se,   $user,  $guid,   $existing,   $jobid
        ],
        zero_length => 0
      }
    );
  } else {
    return $self->do(
"UPDATE LFN_BOOKED SET (lfn, owner, quotaCalculated, md5sum, expiretime, \"SIZE\", pfn, se, gowner, guid, existing, jobid) VALUES (?,?,?,?,?,?,?,?,?,string2binary(?),?,?)",
      {
        bind_values => [
          $lfn, $user, $quota, $md5sum, $expiretime, $size,
          $pfn, $se,   $user,  $guid,   $existing,   $jobid
        ],
        zero_length => 0
      }
    );

  }
}

sub dbGetSEListFromSiteSECacheForWriteAccess{
 
   my $self=shift;
   my $user=shift ;
   my $fileSize=shift;
   my $type=shift;
   my $count=shift ;
   my $sitename=shift ;
   my $excludeList=(shift || "");
   
  my $query="SELECT DISTINCT SE.seName, rank FROM SERanks,SE WHERE "
       ." sitename=? and SERanks.seNumber = SE.seNumber ";

   my @queryValues = ();
   push @queryValues, $sitename;

   foreach(@$excludeList){   $query .= "and upper(SE.seName)<>upper(?) "; push @queryValues, $_;  }
   
   $query .=" and SE.seMinSize <= ? and SE.seQoS  LIKE concat('%,' , concat(? , ',%' )) "
    ." and (SE.seExclusiveWrite is NULL or SE.seExclusiveWrite  LIKE concat ('%,' , concat(? , ',%') ))"
    ." order by rank ASC  ";

     
   push @queryValues, $fileSize;
   push @queryValues, $type;
   push @queryValues, $user;

   my @column; 
   my $in= 0;
   my $result = $self->queryColumn($query, undef, {bind_values=>\@queryValues});
   while($in<$count){
    push @column,$result->[$in];
    $in++;
  }

 
   @$result = @$result[0..$count];
  return $result;
   
}
##############
###optimizer /SeSize
#############
sub updateVolumesInSESize {
  my $self = shift;
  $self->do(
"merge into SE_VOLUMES SV  using ( select seusedspace, sename  from se) S on (upper(S.sename)=upper(SV.sename)) when matched then update  set SV.usedspace =S.seusedspace/1024 "
  );
  $self->do(
" merge into SE_VOLUMES SV using ( select seusedspace, sename  from se) S on (upper(S.sename)=upper(SV.sename)) when matched then update SET SV.freespace =( SV.\"SIZE\" -SV.freespace) where SV.\"SIZE\" =!-1)"
  );
  return;
}

sub showLDLTables {
  my $self = shift;
  return $self->queryColum(
    "select table_name from all_tables where table_name like 'L%L'");
}

sub updateSESize {
  my $self = shift;
  return $self->do(
"merge into SE_VOLUMES SV  using ( select seusedspace, sename  from se) S on (upper(S.sename)=upper(SV.sename)) when matched then 
update  set SV.usedspace =S.seusedspace/1024 "
    )
    and $self->do(
" merge into SE_VOLUMES SV using ( select seusedspace, sename  from se) S on (upper(S.sename)=upper(SV.sename)) when matched then 
update SET SV.freespace =( SV.\"SIZE\" -SV.freespace) where SV.\"SIZE\" =!-1)"
    );
}

#######
## optimizer Job/priority
#####
sub getPriorityUpdate {
  my $self       = shift;
  my $userColumn = shift;
  return "update PRIORITY p  set
waiting=(select count(*) from QUEUE where status='WAITING' and p.\"USER\"=$userColumn ),
running=(select count(*) from QUEUE where (status='RUNNING' or status='STARTED' or status='SAVING') and p.\"USER\"= $userColumn ),
userload=(running/maxparallelJobs),
computedpriority= 
case when (p.RUNNING < p.maxparallelJobs)  then 
                   
                     case when (2-userload)*priority>0 
                     then  50.0*(2-userload)*priority 
                     else 1
                     end
                     
else 1 
end";
}

sub getJobAgentUpdate {
  my $self       = shift;
  my $userColumn = shift;
  return "UPDATE JOBAGENT j set 
priority= (SELECT p.computedPriority-(min(queueid)/nvl(max(queueid), 1))  
from PRIORITY p, QUEUE q where j.entryId=q.agentId and status='WAITING'
and $userColumn=p.\"USER\"
group by q.agentId, computedPriority)";
}

########
## optimizer Job/Expired
####

#sub getJobOptimizerExpiredQ1 {
#  my $self = shift;
#  return
#    " where  (status in ('DONE','FAILED','EXPIRED') or status like 'ERROR%'  )
#and ( mtime < (sysdate + INTERVAL '-10' day)  and split=0) ";
#}

sub getJobOptimizerExpiredQ2 {
  my $self = shift;
  return
" left join QUEUE q2 on q.split=q2.queueid where q.split!=0 and q2.queueid is null and q.mtime< (sysdate + INTERVAL '-10' day)";
}

sub getJobOptimizerExpiredQ3 {
  my $self = shift;
  return " where mtime <(sysdate + INTERVAL '-10' day) and split=0";
}

########
### optimizer Job/Zombies
####

sub getJobOptimizerZombies {
  my $self   = shift;
  my $status = shift;
  return
" q, QUEUEPROC p where $status and p.queueId=q.queueId and (sysdate +INTERVAL '-3600' SECOND)>lastupdate";
}
########
### optimizer Job/Charge
####

sub getJobOptimizerCharge {
  my $self           = shift;
  my $queueTable     = shift;
  my $nominalPrice   = shift;
  my $chargingNow    = shift;
  my $chargingDone   = shift;
  my $chargingFailed = shift;
  my $update = "UPDATE $queueTable q SET finalPrice =  ( select round(si2k*price*$nominalPrice ) from QUEUEPROC p where p.queueid=q.queueid and p.si2k >0 ) , chargeStatus=\'$chargingNow\'";

  my $where =" WHERE (status='DONE' AND chargeStatus!=\'$chargingDone\' AND chargeStatus!=\'$chargingFailed\') ";
  return $update . $where;
}
1;

