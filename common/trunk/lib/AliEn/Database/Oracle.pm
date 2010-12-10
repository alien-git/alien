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

#use Tie::CPHash;

use AliEn::SOAP;
use AliEn::Logger::LogObject;
use vars qw($DEBUG @ISA $INDEX);

$DEBUG = 0;

=head1 NAME

AliEn::Database::Oracle - database interface for Oracle driver for AliEn system 

=head1 DESCRIPTION

This module implements the database wrapper in case of using the driver Oracle. Sytanx and structure are different for each engine. This affects the code. The rest of the modules should finally abstract from SQL code. Therefore, instead we would ideally use calls to functions implemented in this module - case of Oracle. 

=cut

sub initialize{
  my $self = shift;
  $self->getTypes;
  defined $self->{ORACLE_USER} or $self->{ORACLE_USER}="ALIENSTANDARD";
}

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
  
  my %autoincrements= ();
  my %columns=%$columnsDef;
  my $desc2=$desc;
  $desc2 =~ s/^size$|^user$|^time$/"\"$desc2\""/ie; 
  $columns{$desc} =~ s/DEFAULT CHARACTER SET latin1|COLLATE latin1_general_cs|COLLATE latin1_general_ci//ig;
  $columns{$desc} =~ s/(\s*)([a-zA-Z]*)(\(|\s+|$)(.*)/$self->{TYPES}->{$2}$3$4/i;
  if ($columns{$desc} =~ s/auto_increment//){ $autoincrements{$table}=$desc;}
 # print "\nEsta es la primera columna : $desc y despues $columns{$desc} y estos son los componentes 1 $1 2$2 3$3 4$4";
  $desc = "$desc2 $columns{$desc}";
  $self->createTable( $table, "($desc) ", 1 ) or return;use Data::Dumper;
  print "\n\nThese are the autoincrements ".Dumper(%autoincrements);
 if(%autoincrements){foreach my $t(keys %autoincrements){
    $self->defineAutoincrement($t,$autoincrements{$t}) or return;
  }}
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
$self->setPrimaryKey( $table, $desc, $primaryKey, $index );
  #Ok, now let's take a look at the primary key
  #$primaryKey or return 1;
  

#  $desc =~ /not null/i or $self->{LOGGER}->error("Database", "Error: the table $table is supposed to have a primary key, but the index can be null!") and return;
}
=item C<createTable>

  $res = $dbh->createTable($table,$definition);

=cut

sub createTable {
  my $self       = shift;
  my $table      = shift;
  my $definition = shift;

  $DEBUG and $self->debug( 1,
"Database: In createTable creating table $table with definition $definition."
  );

  $self->_do("CREATE TABLE  $table  $definition  ")
    or $self->info( "In checkQueueTable creating table $table failed", 3 )
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

  my %autoincrements= (); 
  foreach my $desc( keys %columns ) {  
    my $desc2=$desc;
    $desc2 =~ s/^size$|^user$|^time$/"\"$desc2\""/ie; 
    $columns{$desc} =~ s/(DEFAULT)? CHARACTER SET latin1|COLLATE latin1_general_cs|COLLATE latin1_general_ci//ig;
    $columns{$desc} =~ s/(\s*)([a-zA-Z]*)(\(|\s+|$)(.*)/$self->{TYPES}->{$2}$3$4/i;
    if ($columns{$desc} =~ s/auto_increment//){ $autoincrements{$table}=$desc;}
   
    $alter .= " $desc2 $columns{$desc} ,";  
  }
  if(%autoincrements){foreach my $t(keys %autoincrements){
    $self->defineAutoincrement($t,$autoincrements{$t}) or return;
  }}
  if ( chop($alter) =~ /^,/i ) {
    $alter .= ")";

    return ( 1, $alter );
  }
  return;

}

=item C<getIndexes>

  $res = $dbh->getIndexes($table,);

Returns the keys of the table $table

=cut

sub getIndexes {
  my $self  = shift;
  my $table = uc shift
    ; # It is returned exactly in the same format as mysql, since this result is processed in the datbase interface
  return $self->query(
"SELECT DISTINCT MOD (INSTR(a1.uniqueness,'UNIQUE')+1, 2) AS \"Non_unique\" ,   a2.column_name as \"Column_name\", a1.index_name as \"Key_name\" FROM all_indexes A1 , all_ind_columns A2 WHERE (a1.index_name = a2.index_name)  and (a1.table_name=a2.table_name) and A1.table_name LIKE \'$table\'"
  );
}

=item C<dropIndex>

  $res = $dbh->dropIndex($index,$table,);

Drop the index $index from the database.

=cut

sub dropIndex {
  my $self  = shift;
  my $index = shift;
  my $table = shift;
  $self->do("drop index $index");
  $self->do("ALTER TABLE $table DROP INDEX $index");
}

=item C<createIndex>

  $res = $dbh->createIndex($index,$table,);

Create the index for the table. This index cannot be named automatically, like it is the case for mysql.

=cut

sub createIndex {
  my $self     = shift;
  my $index    = shift;
  my $table    = shift;
  my $i  = shift || $INDEX;
  my $sqlError = "";
  if ( $index =~ /^FOREIGN KEY/i ) {
    $self->do(
      "ALTER TABLE $table ADD CONSTRAINT FK_" . $table . "_$i $index" );
  }
  elsif ( $index =~ /^PRIMARY KEY/i ) {
    $self->do(
      "ALTER TABLE $table ADD CONSTRAINT  " . $table . "_pk $index" );
  }
  elsif ( $index =~ /^(.*)\((.*)\)/i ) {
    my $name   = $1;
    my $fields = $2;
    if ( !( $name =~ /(\w*)\s* INDEX\s+ (\w+)/xi ) ) {
      $name = "IDX_" . $table . "_$i";
    }
    $self->do( "CREATE $1 " . $name . " ON " . $table . "  ( $fields )" );
  }
  $INDEX++;
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

=item C<getConnectionChain>

  $res = $dbh->getConnectionChain($table,);

get the combination for connecting through DBI

=cut

sub getConnectionChain {
  my $self = shift;
  defined $ENV{'ORACLE_SID'} or $self->info( "The Oracle SID is not defined in your system. Normally it should be. We take 'alien' as ORACLE_SID by convention" );
  my $db = $ENV{ORACLE_SID} || "alien";

  ( $db, $self->{SCHEMA} ) = split( ":", uc $self->{DB} );
  if ( uc( $self->{SCHEMA} ) ne uc( $self->{ROLE} ) ) {
    if (    $self->{ROLE} !~ /^admin(ssl)?/i and $self->{SCHEMA} =~ /^admin?/i )
    {
      print STDERR "Only the administrator can access the admin schema\n";
      return;
    }
    elsif ( $self->{ROLE} !~ /^admin(ssl)?/i and $self->{SCHEMA} !~ /^admin?/i )
    {
      $self->{SCHEMA} = $self->{ORACLE_USER};
    }
  }

  # ($db, $schema) = split(":" ,$self->{DB});
  if ( $self->{HOST} =~ /^no_host$/i ) {
  return "DBI:Oracle:$db";
  }
  else {
  my ( $host, $port ) = split( ":", $self->{HOST} );
  return "DBI:Oracle:sid=$db;host=$host;port=$port";
  }
}

sub _queryDB {
  my ( $self, $stmt, $options, $already_tried ) = @_;
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
  $options->{bind_values} and push @bind, @{ $options->{bind_values} };
  $DEBUG
    and $self->debug( 2, "In _queryDB executing $stmt in database (@bind)." );

  ( $stmt, $b ) = $self->process_zero_length( $stmt, \@bind );
  @bind = @{$b};
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
  $arrRef = $sth->fetchall_arrayref( {} );
  $DBI::errstr and $sqlError .= "In fetch: $DBI::errstr\n";
  foreach (@$arrRef) {
    my %h;
    tie %h, 'Tie::CPHash';
    %h = %$_;
    $_ = \%h;
  }

  #  $sth->finish;
  #  $DBI::errstr and $sqlError.="In finish: $DBI::errstr\n";
      }
    };
    $@ and $sqlError = "The command died: $@";
    alarm(0);

    if ($sqlError) {
      my $found = 0;
      $sqlError =~
/(Unexpected EOF)|(Lost connection)|(Constructor didn't return a handle)|(No such object)|(Connection reset by peer)|(MySQL server has gone away at)|(_set_fbav\(.*\): not an array ref at)|(Constructor didn't return a handle)/
  and $found = 1;

      if ( $sqlError =~ /Died at .*AliEn\/UI\/Catalogue\.pm line \d+/ ) {
  die("We got a ctrl+c... :( ");
      }
      if ( $sqlError =~ /Maximum message size of \d+ exceeded/ ) {
  $self->info("ESTAMOS AQUI\n");
      }
      if ( $sqlError =~ /ORA-/ and !$already_tried ) {

#it could be because we are using a reserved word to select a field. We can quote all the fields in the selection.
  $stmt = $self->quote_query($stmt);

  #retry
  $already_tried = 1;
  $stmt
    and $self->_queryDB( $stmt, $options, $already_tried )
    and $found = 1;

      }
      $found
  or $self->info( "There was an SQL error: $sqlError", 1001 )
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
    and $self->debug( 1,
    "Query $stmt successfully executed. ($#{$arrRef}+1 entries)" );
  return $arrRef;
}

sub _do {
  my $self    = shift;
  my $stmt    = shift;
  my $options = ( shift or {} );

  my $oldAlarmValue = $SIG{ALRM};
  local $SIG{ALRM} = \&_timeout;

  local $SIG{PIPE} = sub {
    print STDERR
"Warning!! The connection to the AliEnProxy got lost while doing an insert\n";
    $self->reconnect();
  };

  $DEBUG
    and
    $self->debug( 2, "In _do checking is database connection still valid" );

  $self->_pingReconnect or return;
  my @bind_values;
  $options->{bind_values}
    and push @bind_values, @{ $options->{bind_values} }
    and $options->{prepare} = 1;
  my $result;

  while (1) {
    my $sqlError = "";

    $result = eval {
      alarm(600);
      my $tmp;
      if ( $options->{prepare} ) {
  $DEBUG and $self->debug( 2, "In _do doing $stmt @bind_values" );
  my $sth = $self->{DBH}->prepare_cached($stmt);
  $tmp = $sth->execute(@bind_values);
      }
      else {
  $DEBUG and $self->debug( 1, "In _do doing $stmt @bind_values" );
  $tmp = $self->{DBH}->do($stmt);
      }
      $DBI::errstr and $sqlError .= "In do: $DBI::errstr\n";
      $tmp;
    };
    my $error = $@;
    alarm(0);
    if ($error) {
      $sqlError .= "There is an error: $@\n";
      $options->{silent}
  or
  $self->info( "There was an SQL error  ($stmt): $sqlError", 1001 );
      return;
    }
    defined($result) and last;

    #this is an optimization for Oracle
    if ( $sqlError =~
      /ORA-00955: name is already used by an existing object/i
      or $sqlError =~ /already exists/i )
    {
      return 1;
    }
    else {
      my $found = 0;
      $sqlError =~
/(Unexpected EOF)|(Lost connection)|(MySQL server has gone away at)|(Connection reset by peer)/
  and $found = 1;
      if ( !$found ) {
  $oldAlarmValue and $SIG{ALRM} = $oldAlarmValue
    or delete $SIG{ALRM};
  chomp $sqlError;
  $options->{silent}
    or $self->info( "There was an SQL error  ($stmt): $sqlError",
    1001 );
  return;
      }
    }

    $self->reconnect() or return;
  }

  $oldAlarmValue and $SIG{ALRM} = $oldAlarmValue
    or delete $SIG{ALRM};

  $DEBUG
    and $self->debug( 1,
    "Query $stmt successfully executed with result: $result" );

  $result;
}

sub getTypes {
  my $self = shift;

  $self->{TYPES} = {
    'serial'    => 'number(19) ',
    'text'      => 'varchar2(4000)',
    'char'      => 'varchar2',
    'binary'    => 'raw',
    'int'    => 'number',
    'tinyint'   => 'number',
    'bigint'    => 'number',
    'smallint'  => 'number',
    'mediumint' => 'number',
    'datetime'      => 'date',
    'varchar'	=> 'varchar2',
    'timestamp' => 'timestamp', 
    'integer' => 'integer'
  };
  return 1;
}
sub binary2string {
  my $self = shift;
  my $column = shift || "guid";
  return " binary2string($column) ";

#return "insrt(insrt(insrt(insrt(rawtohex($column),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')";
}



sub grantPrivilegesToUser{
#The privileges are assigned to a unique user on the database. Every user in the application connects through this one.
return 1;}
sub grantExtendedPrivilegesToUser{return 1;}
sub grantPrivilegesToObject{
  my $self = shift;
  my $privs = shift;
  my $schema_from  = shift;
  $schema_from=~s/(.)*:(.)*/$2/i;
  my $object = shift; # if object == * that means all the objects in the schema.
  my $user_to = shift;
  my $pass = shift;
  if($schema_from eq $user_to) {return 1;}
  if($pass){ 
  $self->checkUser($user_to,$pass) ;
  } #the user already exists
  $DEBUG and $self->debug(1, "In grantPrivilegesToObject");

  my $success = 1;
  if ($object=~ m/(.*)\*(.*)/i ){

  my $s = $self->{DBH}->prepare("begin grant_whole_schema(\'$privs\',\'$schema_from\',\'$user_to\');end;") ;
  $s->execute; return 1; 
  #$self->do("begin grant_whole_schema(\'$privs\',\'$schema_from\',\'$user_to\');end;")  or $DEBUG and $self->debug (0, "Error adding privileges $privs to $user_to")
      #and return 0;
  }else{
  $object = "$schema_from\.$object";  
  $DEBUG and $self->debug (0, "Adding privileges $privs to $user_to");

  $self->_do("GRANT $privs ON $object TO $user_to")
     or $DEBUG and $self->debug (0, "Error adding privileges $privs to $user_to")
      and $success = 0;
  }
  return $success;
}


sub multiinsert {
  my $self = shift;
  my $table = shift;
  my $rarray = shift;
  my $options=shift;
  my $rloop;
  
  ###     statement checking is a temporary solution ... remove later!!!
  if ($table =~ /\s/){return $self->do($table);}
  
  my $rfields = @$rarray[0];
  
  my $query = "INSERT";

  my @fields = keys %$rfields;
  my $new_fields = $self->preprocessFields(\@fields) ; #for the reserved words
  my @new_f = @$new_fields;  
  $query.=" INTO $table (" . join(", ",@new_f) . ") VALUES ";
  my $quote="'";
  $options->{noquotes} and $quote="";
  #my @arr = values %$rfields;
  my @bind = ();
  
  foreach $rloop (@$rarray) {
   my $query2 = "(";@bind = ();
    foreach (keys %$rfields) {
      if(defined $rloop->{$_}){
      
  if ($quote) {
     $query2.="?,"; 
  } else {
    $rloop->{$_} =~ s/^([^'"]*)['"](.*)['"]([^'"]*)$/$2/;
    my $function="";
    my $functionend="";
    if($1 && $3){
      $function=$1 and $functionend=$3;
    }
    $query2 .= " $function ? $functionend,";
  }
  push @bind, $rloop->{$_};
      }else{
  $query2 .= "NULL,";
      }
    }
    chop($query2);
    
    $query2 .= ")";
    my $doOptions={bind_values=>\@bind};
    $options->{silent} and $doOptions->{silent}=1;
    $self->_do($query.$query2, $doOptions);
    if ($options->{ignore} && $DBI::errstr =~/ORA-00001: unique constraint/){
    my $delete = "delete from $table where ";my $i;
    for $i (0..$#new_f){
      $delete .= $new_f[$i] ." = $bind[$i]  AND "; 
    }
    $delete=~ s/(.*)AND $/$1/;

    $self->_do($delete); 
    $self->_do($query.$query2, $doOptions);
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
    "create or replace procedure grant_whole_schema
(privilegio IN varchar2 , schema_from  IN varchar2, schema_to IN varchar2)
is 
begin
for x in (select object_name from all_objects where owner like schema_from)
loop
execute immediate 'grant '|| privilegio ||' on '|| x.object_name || ' to '|| schema_to;
end loop;
end;
"
  );
  $self->do("grant all privileges on grant_whole_schema to public");
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
end;"
  );
  $self->do("grant all privileges on conv to public");
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
END INSRT;"
  );

  $self->do(
    "create or replace
function now
return date as begin
return sysdate ;
end;"
  );
  $self->do("grant all privileges on now to public");
  $self->do("create public synonym now for alien_system.now");

  $self->do("grant all privileges on insrt to public");
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
end binary2string;"
  );

  $self->do("grant all privileges on binary2string to public");
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
end binary2date;"
  );
  $self->do("grant all privileges on binary2date  to public");

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
end string2binary;"
  );
  $self->do("grant all privileges on string2binary  to public");
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
END INSRT;"
  );
  $self->do("grant all privileges on insrt to public");
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
end binary2string;"
  );
  $self->do("grant all privileges on binary2string to public");

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
end binary2date;"
  );
  $self->do("grant all privileges on binary2date to public");

  $self->do(
    "create or replace
function string2date 
(my_uuid in varchar2)  
return varchar
deterministic 
AUTHID current_user
as
begin
if(my_uuid like 'NULL')then return null;else 
return 
substr(
upper(
  concat( 
    concat(substr(substr(my_uuid,1,18),18-4+1), substr(substr(my_uuid,1,13),13-4+1)),substr(my_uuid,1,8))),1,8);end if;
end string2date;
"
  );
  $self->do("grant all privileges on string2date to public");
}
sub _createPrivilegesProcedure {
  my $self = shift;
  $self->do(
    " create or replace
procedure grant_whole_schema
(privilegio IN varchar2 , schema_from  IN varchar2, schema_to IN varchar2)
is 
begin
for x in (select object_name from all_objects where object_type=\'TABLE\' AND owner like  schema_from)
loop
  execute immediate \'grant \'\|\| privilegio \|\|\' on \'\|\| schema_from\|\| \'\.\'\|\| x\.object_name \|\| \' to \'\|\| schema_to;
end loop;
end;"
  );
  $self->do("grant all privileges on grant_whole_schema to public");
}

sub lock {
  my $self = shift;
  my $lock = shift;

  # $DEBUG and $self->debug(1,"Database: In lock locking table $table.");

  $self->_do("LOCK TABLE $lock  IN ROW EXCLUSIVE MODE");
}

sub unlock {
  my $self  = shift;
  my $table = shift;

  $DEBUG and $self->debug( 1, "Database: In lock unlocking tables." );

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
  if ( $offset <= 0 ) { $offset = 1; }
  if ( $limit and $limit >= 0 ) {

#return "select query.* from (select P.* ,rownum R from ($sql ) P ) query where R between $offset and $limit+$offset-1 ";
    return
"SELECT P.* FROM ($sql) P WHERE rownum BETWEEN $offset and $limit + $offset -1";
  }
  else {
    return $sql;
  }
}

sub optimizeTable {
  my $self  = shift;
  my $table = shift;
  $self->do("alter table $table move");
  my $indexes = $self->query(
"select INDEX_NAME from all_indexes where OWNER = \'$self->{SCHEMA}\' and TABLE_NAME LIKE \'$table\'"
  );
  foreach my $ind (@$indexes) {
    $self->do("alter index $ind->{INDEX_NAME} rebuild");
  }
}


sub schema {
  my $self = shift;
  return $self->{SCHEMA};
}

sub resetAutoincrement {
  my $self   = shift;
  my $table  = shift;
  my $sqName = $table . "_seq";
  $self->do("drop sequence $sqName");
  $self->do(
    "create sequence $sqName
start with 1 
increment by 1 
nomaxvalue"
  );
}


sub defineAutoincrement {
  my $self  = shift;
  my $tableName   = shift;
  my $field       = shift;
  my $sqName      = $tableName . "_seq";
  my $triggerName = $tableName . "_trigger";
  my $exists      = $self->queryValue(
" SELECT count(1) FROM all_sequences where upper(sequence_name)=upper('$sqName')  and sequence_owner like '$self->{SCHEMA}'"
  );
  $exists &= $self->queryValue(
" SELECT count(1) FROM all_triggers where upper(trigger_name)=upper('$triggerName')  and owner like '$self->{SCHEMA}'"
  );

  if ( !$exists ) {
    $self->do(
      "create sequence $sqName start with 1  increment by 1 nomaxvalue");
    $self->do("grant select on $sqName to public");
    $self->do(
"create trigger $triggerName before insert on $tableName for each row begin select $sqName.nextval into :new.$field from dual;end; "
    );
  }
  return 1;
}
sub existsTable{
#my $self = shift;
#my $table = shift;$table = uc($table);
#my $ref = $self->queryColumn("SELECT COUNT(*) FROM all_TABLES WHERE OWNER = \'$self->{SCHEMA}\' and table_name like '$table'");
#return $$ref[0];
return 0;
}
sub renameField {
  my $self  = shift;
  my $table = shift;
  my $old   = shift;
  my $new   = shift;
  $self->do("ALTER TABLE $table rename COLUMN  $old to  $new");
}

sub quote_query {
  my $self = shift;
  my $stmt = shift;
  if ( $stmt =~ /select(\s+)(\w+|\w+(\s*\,\s*\w+)+)(\s+)from/i ) {
    my $cols = $2;
    my $old  = $cols;
    $cols = uc($cols);
    $cols =~ s/(\s*)\,(\s*)/\"\,\"/igx;
    $cols = "\"$cols\"";
    $stmt =~ s/SELECT(\s+)$old(\s+)(.)/SELECT$1$cols$2$3/i;
    return $stmt;
  }
  else { return; }

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

  my @new_where = split( /AND/i, $where );
  foreach (@new_where) {
    $_ =~ s/(\w+)(\s*)=(\s*)(\w+)/"\"". uc($1) . "\"=".$4/mexgi;
  }

  return join( " AND ", @new_where );
}

sub _connectSchema {
  my $self   = shift;
  my $schema = shift;

  #!$self->{DBH} and return;
  !$schema    # and !$self->{SCHEMA}
    and $self->{SCHEMA} = $self->{DB};

  $self->{SCHEMA} =~ s/(.+):(.+)/$2/i;
  
  $self->debug( 1, "connecting to the current schema: $self->{SCHEMA}" );

  $self->do("ALTER SESSION SET CURRENT_SCHEMA = $self->{SCHEMA}");
}

sub checkUser {
  my $self     = shift;
  my $user     = shift;
  my $pass     = shift;
  my $sqlError = "";
  $user = uc $user;
  my $res = $self->_queryDB(
  "SELECT USERNAME FROM ALL_USERS WHERE USERNAME LIKE '$user'");

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
  if ( $@ =~ /ORA-01920/i ) {
    $DEBUG
      and $self->info( "This user already exists", 1 )
      and return 1;    #if the user already exists , this is correct (?)
  }
  return 1;
}

sub process_zero_length {
  my $self = shift;
  my $stmt = shift;
  my $b    = shift;

  #case without binding values
  while ( $stmt =~
s/(.*)(\!\=|\<\>|NOT\sLIKE)(\s*\'\' \s*)(.*)$/$1. " IS NOT NULL ". $4/gxei
    )
  {
  }
  while ( $stmt =~ s/(.*)(\=|LIKE)(\s*\'\' \s*)(.*)$/$1. " IS NULL ".$4/gxei )
  {
  }
  if ($b) {
    my @bind = @{$b};

    #case binding values
    if ( grep { /^$/ } @bind ) {
      my @new_bind = ();
      my $left     = $stmt;
      my $new_stmt = " ";
      foreach (@bind) {

  #element with string length zero
  if ( $_ =~ /^$/ ) {

#change the statement to consider if the column is null and remove it from the bind values
    if ( $left =~
s/(.*)(\!\= |\<\>|NOT\sLIKE)(\s*\? )(.*)/$1 . " IS  NOT NULL ".$4 /xei
  )
    {
    }
    else {
  $left =~
    s/(.*)(\=|LIKE)(\s*\? )(.*)/$1 . " IS NULL ".$4 /xei;
    }
    $new_stmt = $left;
  }
  else {    #case element with string no length zero
    push( @new_bind, $_ );
    $left =~ s/(.*)(\s*\? )(.*)/$1  .$2 .$3/xei
  ;    # $new_stmt=$new_stmt.$left ; $left=$3
    $new_stmt = $left;
  }
      }
      @bind = @new_bind;
      $stmt = $new_stmt;
      return ( $new_stmt, \@new_bind );
    }
    else {

      #case the binding values have not got zero length,do nothing}
      return ( $stmt, \@bind );
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
  if ( $options->{r} ) {
    $rec = " or path like concat(?, '%') ";
    push @bind, $path;
  }
  if ( $options->{user} ) {
    $self->debug( 1, "Only for the user $options->{user}" );
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
  if ( $info < 100000 ) {
    $self->debug( 1, "Only $info. We don't need to renumber" );
    return 1;
  }

  $self->info("Let's renumber the table $table");

  $self->lock($lock);
  my $ok = 1;
  $self->do("alter table $table modify $index number(11)");
  $self->do(" alter table $table drop primary key");
  $self->do("alter table $table add new_index number(11)  primary key");
  $self->defineAutoincrement( $table, "new_index" );
  $self->do( "create unique index $table" . "_uk on table $table (guidid)" )
    or $ok = 0;

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
    $self->do("alter table $table drop column $index");
    $self->renameField( $table, "new_index", $index, " number(11) " );
    $self->defineAutoincrement( $table, $index );
  }
  else {
    $self->info("The update didn't work. Rolling back");
    $self->do("alter table $table drop new_index");
    $self->do("alter table $table modify $index number(11) primary key");
    $self->defineAutoincrement( $table, $index );
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
"merge  into PRIORITY p using (select SUBSTR( submitHost, 1, instr(submitHost,'\@') -1)  \"USER\", count(1)  unfinishedJobsLast24h from queue q where (status='INSERTING' or status='WAITING' or status='STARTED' or status='RUNNING' or status='SAVING' or status='OVER_WAITING') and ( (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')   >=  To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + received / 86400,'DD.MM.YYYY HH24:Mi:ss')) and (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')  <=  To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + received / 86400 + 60*60*24,'DD.MM.YYYY HH24:Mi:ss'))) GROUP BY submithost ) c on (p.\"USER\"=c.\"USER\") when matched then update set p.unfinishedjobslast24h=c.unfinishedjobslast24h"
  );
}

sub totalRunninTimeJobs24PerUser {
  my $self = shift;
  return $self->do(
"merge  into PRIORITY pr using (select SUBSTR( submitHost, 1, instr(submitHost,'\@') -1)  \"USER\",sum(runtimes)  totalRunningTimeLast24h from queue q , QUEUEPROC p where ( (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')   >=   To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + q.received / 86400,'DD.MM.YYYY HH24:Mi:ss'))  and (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')  <=   To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + q.received / 86400 + 60*60*24,'DD.MM.YYYY HH24:Mi:ss'))) GROUP BY submithost ) c on (pr.\"USER\"=c.\"USER\") when matched then update set  pr.totalRunningTimeLast24h=c. pr.totalRunningTimeLast24h"
  );
}

sub cpuCost24PerUser {
  my $self = shift;
  return $self->do(
    "merge  into PRIORITY pr using 
(select SUBSTR( submitHost, 1, instr(submitHost,'\@') -1)  \"USER\",sum(p.cost)  totalCpuCostLast24h 
from queue q , QUEUEPROC p where ( (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')   >=  
To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + q.received / 86400,'DD.MM.YYYY HH24:Mi:ss'))  
and (to_char(sysdate, 'DD.MM.YYYY HH24:Mi:ss')  <=   
To_Char( To_Date( '01.01.1970 06:00:00','DD.MM.YYYY HH24:Mi:Ss') + q.received / 86400 + 60*60*24,'DD.MM.YYYY HH24:Mi:ss'))) 
GROUP BY submithost ) c on (pr.\"USER\"=c.\"USER\") 
when matched then update set  pr.totalCpuCostLast24h=c. pr.totalCpuCostLast24h"
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
  my $t  = shift;
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
  my $self = shift;
  return $self->do(
"INSERT  INTO PRIORITY(\"USER\", priority, maxparallelJobs, nominalparallelJobs) 
SELECT distinct SUBSTR( submitHost, 1, instr (\'@\' , submitHost)-1 ) , 1,200, 100 from QUEUE q where not exists
(select * from priority where \"USER\"=SUBSTR( submitHost, 1, instr (\'@\' , submitHost)-1 ) )"
  );
}

sub userColumn {
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

sub setUpdateDefault {
  my $self  = shift;
  my $table = shift;
  my $col   = shift;
  my $val   = shift;
  return $self->do(
  "create or replace TRIGGER  " 
      . $table
      . "trigger_ctime BEFORE UPDATE ON  "
      . $table
      . " FOR EACH ROW BEGIN      
  select " 
      . $val
      . " into :new."
      . $col
      . " from dual; END TRANSFERS_DIRECT_trigger_ctime;"
  );
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
1;

