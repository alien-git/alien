#!/bin/env alien-perl

use strict;
use Data::Dumper;
use AliEn::Database;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

### Set direct connection
$ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/rpub.pem";
$ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/rpriv.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/lpub.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/lpriv.pem";
$ENV{ALIEN_DATABASE_ROLE}='admin';
$ENV{ALIEN_DATABASE_PASSWORD}='pass';

### Get connections and DB objects
my $db = AliEn::Database->new({DRIVER => "mysql",
                               HOST   => Net::Domain::hostfqdn().":3307",
                               DB     => "alice_users",
                               ROLE   => "admin"});
my $cat = AliEn::UI::Catalogue::LCM->new({ROLE => "admin"});

### Get index table values for GUID and LFN
my $indexTable = $db->query("SELECT * FROM INDEXTABLE i, HOSTS h WHERE h.hostIndex=i.hostIndex ORDER BY db DESC, tableName ASC");
my $guidIndex  = $db->query("SELECT * FROM GUIDINDEX g, HOSTS h WHERE h.hostIndex=g.hostIndex ORDER BY db DESC, tableName ASC");
my $triggers   = $db->query("SELECT * FROM INDEXTABLE i, HOSTS h, INFORMATION_SCHEMA.TRIGGERS t
                                WHERE i.hostIndex=h.hostIndex and h.db=t.EVENT_OBJECT_SCHEMA and t.EVENT_OBJECT_TABLE RLIKE i.tableName
                                ORDER BY h.db DESC, i.tableName ASC");
my @failures;

### Get rid of unnecesary table
my $unn = $db->query("
          select concat(icc.TABLE_SCHEMA, '.', icc.TABLE_NAME) as uselessTable from INFORMATION_SCHEMA.TABLES icc 
          where icc.TABLE_NAME like 'L%L' 
          and concat(icc.TABLE_SCHEMA, '.', icc.TABLE_NAME) not in 
            (SELECT concat(h.db, '.L', i.tableName,'L') FROM INDEXTABLE i, HOSTS h where i.hostIndex=h.hostIndex) 
          order by icc.TABLE_NAME asc;");
print "Deleting unnecesary tables\n";
map { $db->do("DROP TABLE $_->{uselessTable}") } @$unn;

### Set new table names for the values
my $lfn_ctr = 0;
map { $_->{new_tableName} = $lfn_ctr++ } @$indexTable and $lfn_ctr--;
my $guid_ctr = 0;
map { $_->{new_tableName} = $guid_ctr++ } @$guidIndex and $guid_ctr--;

### Renumber and move L#L tables in place
print "Moving L#L tables into alice_users\n";
createIndexTable($db);
my $mct = shift @$triggers;
foreach my $row (@$indexTable) {
  print "[ $row->{new_tableName} / $lfn_ctr ]\tMoving $row->{db}.L$row->{tableName}L to L$row->{new_tableName}L for $row->{lfn}\n";
  $db->do("INSERT INTO INDEXTABLE(tableName,lfn) VALUES (?,?)", {bind_values=>[$row->{new_tableName}, $row->{lfn}]})
    or push (@failures, {tableName=>$row->{new_tableName}, lfn=>$row->{lfn}});
  $mct->{tableName} eq $row->{tableName} and $db->do("DROP TRIGGER IF EXISTS $mct->{db}.$mct->{TRIGGER_NAME}");
  $db->do("ALTER TABLE $row->{db}.L$row->{tableName}L RENAME alice_users.L$row->{new_tableName}L");
  $db->do("DROP TABLE IF EXISTS $row->{db}.L$row->{tableName}L_QUOTA");
  $db->do("DROP TABLE IF EXISTS $row->{db}.L$row->{tableName}L_broken");
  if( $mct->{tableName} eq $row->{tableName} ) {
    $db->do("CREATE TRIGGER TRIGGER_L$row->{tableName}L $mct->{ACTION_TIMING} $mct->{EVENT_MANIPULATION} 
             ON  L$row->{new_tableName}L FOR EACH ROW $mct->{ACTION_STATEMENT}");
    $mct = shift @$triggers || {tableName=>""};
  }
}
### Retry failures
map { print "Trying again --- $_->{lfn} == L$_->{tableName}L\n" and $db->do("INSERT INTO INDEXTABLE(lfn, tableName) VALUES(?,?)", {bind_values=>[$_->{lfn},$_->{tableName}]}) } @failures;

### Renumber and move G#L tables
print "Moving GUID tables to alice_temp\n";
createGuidIndex($db);
$db->do("CREATE SCHEMA IF NOT EXISTS alice_temp");
foreach my $row (@$guidIndex) {
  print "[ $row->{new_tableName} / $guid_ctr ]\tMoving $row->{db}.G$row->{tableName}L to alice_temp.G$row->{new_tableName}L\n";
  $db->do("INSERT INTO GUIDINDEX(tableName, guidTime) VALUES (?,?)", {bind_values=>[$row->{new_tableName}, $row->{guidTime}]});
  $db->do("ALTER TABLE $row->{db}.G$row->{tableName}L RENAME alice_temp.G$row->{new_tableName}L");
  $db->do("ALTER TABLE $row->{db}.G$row->{tableName}L_PFN RENAME alice_temp.G$row->{new_tableName}L_PFN");
  $db->do("ALTER TABLE $row->{db}.G$row->{tableName}L_REF RENAME alice_temp.G$row->{new_tableName}L_REF");
}
print "Removing the useless G#L from alice _users\n";
my $entries = $db->query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='alice_users' 
  AND ( TABLE_NAME REGEXP '^G[0-9].*L\$' OR TABLE_NAME REGEXP '^G[0-9].*L_PFN\$'  OR TABLE_NAME REGEXP '^G[0-9].*L_REF\$' )");
map { $db->do("ALTER TABLE alice_users.$_->{TABLE_NAME} RENAME alice_users.$_->{TABLE_NAME}_BACKUP") } @$entries;
#map { $db->do("DROP TABLE alice_users.$_->{TABLE_NAME} ") } @$entries;
print "Now moving the G#L back to alice _users\n";
$entries = $db->query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='alice_temp'");
map { $db->do("ALTER TABLE alice_temp.$_->{TABLE_NAME} RENAME alice_users.$_->{TABLE_NAME}") } @$entries;
$db->do("DROP SCHEMA IF EXISTS alice_temp");

### Move tags into place
my @schemas = ("alien_system", "alice_data");
foreach my $schema (@schemas) {
  my $tag0 = $db->query("SELECT DISTINCT tableName, tagName FROM $schema.TAG0");
  foreach my $tag (@$tag0) {
    defined $tag->{tableName} or next;
    print "Moving tag $tag->{tagName} defined in $schema.$tag->{tableName}\n";
    my $tagCols = $db->query("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? AND TABLE_SCHEMA=?", undef, {bind_values=>[$tag->{tableName}, $schema]});
    my $create = "CREATE TABLE IF NOT EXISTS alice_users.$tag->{tableName} (\n";
    my $cols = "";
    foreach my $col (@$tagCols) {
      $cols .= "`$col->{COLUMN_NAME}` $col->{COLUMN_TYPE}";
      $col->{IS_NULLABLE} eq "NO" and $cols .= " NOT NULL";
      $cols .= ",\n";
    }
    $cols =~ s{,\n$}{};
    $create .= "$cols)\nENGINE = MyISAM";
    $db->do($create);
    $cols = "";
    map { $cols.="`$_->{COLUMN_NAME}`, " } @$tagCols;
    $cols =~ s{entryId, }{};
    $cols =~ s{, $}{};
    $db->do("INSERT INTO $tag->{tableName}($cols) (SELECT $cols FROM $schema.$tag->{tableName})");
  }
}
print "Moving tags and adding entries into TAG0\n";
foreach my $schema (@schemas) {
  my $tag0 = $db->query("SELECT * FROM $schema.TAG0");
  foreach my $tag (@$tag0) {
    defined $tag->{tableName} or next;
    #print "Moving tag $tag->{tagName} defined for $tag->{path} in $schema to TAG0\n";
    $db->do("INSERT INTO TAG0(tagName, path, tableName, user) VALUES (?, ?, ?, ?)", {bind_values=>[$tag->{tagName}, $tag->{path}, $tag->{tableName}, $tag->{user}]});
  }
}

### Drop old schemas and clean tables
print "Droping old schemas and cleaning unnecesary tables\n";
$db->do("DROP TABLE HOSTS");
$db->do("DROP SCHEMA IF EXISTS alien_system");
$db->do("DROP SCHEMA IF EXISTS alice_data");

### Check tables and create all other tables -- Make LFN_QUOTA and LFN_broken
#map { $cat->{CATALOG}->{DATABASE}->checkLFNTable($_->{new_tableName}) } @$indexTable;
#map { $cat->{CATALOG}->{DATABASE}->checkGUIDTable($_->{new_tableName}) } @$guidIndex;


##############################################################################
#
# FUNCTIONS
#
##############################################################################

sub createIndexTable {
  my $dbObj = shift;
  $dbObj->do("DROP TABLE IF EXISTS INDEXTABLE");
  $dbObj->do("
    CREATE  TABLE IF NOT EXISTS INDEXTABLE (
      `lfn` VARCHAR(255) CHARACTER SET 'latin1' COLLATE 'latin1_general_cs' NULL DEFAULT NULL ,
      `tableName` INT(11) NOT NULL ,
  PRIMARY KEY (`tableName`) )
  ENGINE = MyISAM
  AUTO_INCREMENT = 4
  DEFAULT CHARACTER SET = latin1
  COLLATE = latin1_general_cs");
  $dbObj->do("CREATE UNIQUE INDEX `lfn` ON INDEXTABLE (`lfn` ASC)");
}

sub createGuidIndex {
  my $dbObj = shift;
  $dbObj->do("DROP TABLE IF EXISTS GUIDINDEX");
  $dbObj->do("
  CREATE  TABLE IF NOT EXISTS GUIDINDEX (
    `tableName` INT(11) NOT NULL ,
    `guidTime` VARCHAR(16) CHARACTER SET 'latin1' COLLATE 'latin1_general_cs' NULL DEFAULT '0' ,
  PRIMARY KEY (`tableName`) )
  ENGINE = MyISAM
  AUTO_INCREMENT = 2
  DEFAULT CHARACTER SET = latin1
  COLLATE = latin1_general_cs;");
  $dbObj->do("CREATE UNIQUE INDEX `guidTime` ON GUIDINDEX (`guidTime` ASC)");
}

