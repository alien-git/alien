#!/bin/env alien-perl

use strict;
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
my $db_now = shift;
(defined $db_now) or $db_now="alice_users";
my $no_threads = shift;
(defined $no_threads) or $no_threads=24;

my $db = AliEn::Database->new({DRIVER => "mysql",
                               HOST   => Net::Domain::hostfqdn().":3307",
                               DB     => "$db_now",
                               ROLE   => "admin"});

### Get index table values for GUID and LFN
my $indexTable = $db->query("SELECT tableName FROM INDEXTABLE");
my $guidIndex  = $db->query("SELECT tableName FROM GUIDINDEX");
my $table;
my $chk=0;

print "1. Create 2 new tables: USERS and GRPS\n";
$db->do("DROP TABLE IF EXISTS USERS,GRPS");
$db->do("CREATE TABLE USERS (uId MEDIUMINT UNSIGNED not null PRIMARY KEY AUTO_INCREMENT, Username varchar(20) not null UNIQUE ) 
         CHARACTER SET latin1 COLLATE latin1_general_cs");
$db->do("CREATE TABLE GRPS (gId MEDIUMINT UNSIGNED not null PRIMARY KEY AUTO_INCREMENT, Groupname varchar(20) not null UNIQUE ) 
         CHARACTER SET latin1 COLLATE latin1_general_cs");
#INDEXTABLE
foreach my $row (@$indexTable) {
  $table="L".$row->{tableName}."L";
  $db->do("INSERT IGNORE INTO USERS (Username) (SELECT DISTINCT owner from $table)");
  $db->do("INSERT IGNORE INTO GRPS (Groupname) (SELECT DISTINCT gowner from $table)");
}
print "1. DONE\n".scalar(localtime(time))."\n";

###################################################################################
##GUIDINDEX
foreach my $row (@$guidIndex) {
  $table="G".$row->{tableName}."L";
  $db->do("INSERT IGNORE INTO USERS (Username) (SELECT DISTINCT owner from $table)");
  $db->do("INSERT IGNORE INTO GRPS (Groupname) (SELECT DISTINCT gowner from $table )");
}

print "1. DONE\n".scalar(localtime(time))."\n";
print "\n".scalar(localtime(time))."\n";

print "4. Updating the GROUPS table to UGMAP with uId and gId & DROP columns Username and Groupname\n";
$db->do("ALTER TABLE GROUPS DROP COLUMN Userid");
$db->do("ALTER TABLE GROUPS ADD (Userid MEDIUMINT UNSIGNED,Groupid MEDIUMINT UNSIGNED)");
$db->do("UPDATE GROUPS join USERS ON USERS.Username=GROUPS.Username SET GROUPS.Userid=USERS.uId ");
$db->do("UPDATE GROUPS join GRPS ON GRPS.Groupname=GROUPS.Groupname SET GROUPS.Groupid=GRPS.gId ");
$db->do("ALTER TABLE GROUPS RENAME UGMAP");
$db->do("ALTER TABLE UGMAP DROP COLUMN Username, DROP COLUMN Groupname");
print "4. DONE\n".scalar(localtime(time))."\n";

