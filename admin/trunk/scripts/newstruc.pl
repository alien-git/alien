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
                               DB     => "al_syst",
                               ROLE   => "admin"});
my $cat = AliEn::UI::Catalogue::LCM->new({ROLE => "admin"});

### Get index table values for GUID and LFN
my $indexTable = $db->query("SELECT tableName FROM INDEXTABLE");
my $guidIndex  = $db->query("SELECT tableName FROM GUIDINDEX");
my $table;
my $chk=0;

$db->do("DROP TABLE IF EXISTS USERS,GROUPS1");
$db->do("CREATE TABLE USERS (uId MEDIUMINT not null PRIMARY KEY AUTO_INCREMENT, user varchar(20) not null UNIQUE ) CHARACTER SET latin1 COLLATE latin1_general_cs");
$db->do("CREATE TABLE GROUPS1 (gId MEDIUMINT not null PRIMARY KEY AUTO_INCREMENT, user varchar(20) not null UNIQUE ) CHARACTER SET latin1 COLLATE latin1_general_cs");
print "Created 2 new tables: USERS and GROUPS1\n";
print "\n".scalar(localtime(time))."\n";

#INDEXTABLE
foreach my $row (@$indexTable) {
  $table="L".$row->{tableName}."L";
  $db->do("INSERT IGNORE INTO USERS (user) (SELECT DISTINCT owner from $table)");
  $db->do("INSERT IGNORE INTO GROUPS1 (user) (SELECT DISTINCT gowner from $table)");
  #$db->do("ALTER TABLE $table DROP COLUMN uId, DROP COLUMN gId");
}
print "Doing the alteration in L#L tables\n";
print "\n".scalar(localtime(time))."\n";

my $status=0;
my $collation='latin1_general_cs';
foreach my $row (@$indexTable) {
  $table="L".$row->{tableName}."L";
  $db->do("ALTER TABLE $table ADD (ownerId MEDIUMINT , gownerId MEDIUMINT)");
  $status=$db->do("select 1 from information_schema.tables where table_name='$table' and table_collation='$collation'");
  ($status) or $status=0;
  if($status==0)
  { $db->do("ALTER TABLE $table COLLATE latin1_general_cs");}

  $db->do("UPDATE $table JOIN USERS ON $table.owner=USERS.user SET $table.ownerId=USERS.uId");
  $db->do("UPDATE $table JOIN GROUPS1 ON $table.gowner=GROUPS1.user SET $table.gownerId=GROUPS1.gId");
  $db->do("ALTER TABLE $table DROP COLUMN owner, DROP COLUMN gowner");
}
print "New Changes made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";


##GUIDINDEX
foreach my $row (@$guidIndex) {
  $table="G".$row->{tableName}."L";
  $db->do("INSERT IGNORE INTO USERS (user) (SELECT DISTINCT owner from $table)");
  $db->do("INSERT IGNORE INTO GROUPS1 (user) (SELECT DISTINCT gowner from $table )");
}
print "Doing the alteration in G#L tables\n";
print "\n".scalar(localtime(time))."\n";
foreach my $row (@$guidIndex) {
  $table="G".$row->{tableName}."L";
  $db->do("ALTER TABLE $table ADD (ownerId MEDIUMINT , gownerId MEDIUMINT)");
  $status=$db->do("select 1 from information_schema.tables where table_name='$table' and table_collation='$collation'");
  ($status) or $status=0;
  if($status==0)
  { $db->do("ALTER TABLE $table COLLATE latin1_general_cs");}

  $db->do("UPDATE $table join USERS ON $table.owner=USERS.user SET $table.ownerId= USERS.uId");
  $db->do("UPDATE $table join GROUPS1 ON $table.gowner=GROUPS1.user SET $table.gownerId= GROUPS1.gId");
  $db->do("ALTER TABLE $table DROP COLUMN owner, DROP COLUMN gowner");
}
print "New Changes made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";


print "Updating the GROUPS table to UGMAP with uId and gId & DROP columns Username and Groupname\n";
$db->do("ALTER TABLE GROUPS ADD (uId MEDIUMINT ,gId MEDIUMINT)");
$db->do("UPDATE GROUPS join USERS ON USERS.user=GROUPS.Username SET GROUPS.uId=USERS.uId ");
$db->do("UPDATE GROUPS join GROUPS1 ON GROUPS1.user=GROUPS.Groupname SET GROUPS.gId=GROUPS1.gId ");
#$db->do("ALTER TABLE GROUPS RENAME UGMAP");
#$db->do("ALTER TABLE UGMAP DROP COLUMN Username, DROP COLUMN Groupname");


print "\n".scalar(localtime(time))."\n";
