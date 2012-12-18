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
my $db_now = shift;
(defined $db_now) or $db_now="alice_users";
my $db = AliEn::Database->new({DRIVER => "mysql",
                               HOST   => Net::Domain::hostfqdn().":3307",
                               DB     => "$db_now",
                               ROLE   => "admin"});
my $cat = AliEn::UI::Catalogue::LCM->new({ROLE => "admin"});

### Get index table values for GUID and LFN
my $indexTable = $db->query("SELECT tableName FROM INDEXTABLE");
my $guidIndex  = $db->query("SELECT tableName FROM GUIDINDEX");
my $table;
my $chk=0;

#=cut1
$db->do("DROP TABLE IF EXISTS USERS,GRPS");
$db->do("CREATE TABLE USERS (uId MEDIUMINT UNSIGNED not null PRIMARY KEY AUTO_INCREMENT, Username varchar(20) not null UNIQUE ) CHARACTER SET latin1 COLLATE latin1_general_cs");
$db->do("CREATE TABLE GRPS (gId MEDIUMINT UNSIGNED not null PRIMARY KEY AUTO_INCREMENT, Groupname varchar(20) not null UNIQUE ) CHARACTER SET latin1 COLLATE latin1_general_cs");
print "Created 2 new tables: USERS and GRPS\n";
print "\n".scalar(localtime(time))."\n";

#INDEXTABLE
foreach my $row (@$indexTable) {
  $table="L".$row->{tableName}."L";
  $db->do("INSERT IGNORE INTO USERS (Username) (SELECT DISTINCT owner from $table)");
  $db->do("INSERT IGNORE INTO GRPS (Groupname) (SELECT DISTINCT gowner from $table)");
  #$db->do("ALTER TABLE $table DROP COLUMN uId, DROP COLUMN gId");
}
print "Doing the alteration in L#L tables\n";
print "\n".scalar(localtime(time))."\n";

my $status=0;
my $collation='latin1_general_cs';
foreach my $row (@$indexTable) {
  $table="L".$row->{tableName}."L";
  print "$table\n";
  $db->do("ALTER TABLE $table ADD (ownerId MEDIUMINT UNSIGNED  , gownerId MEDIUMINT UNSIGNED), ADD FOREIGN KEY (ownerId) REFERENCES USERS(uId),ADD FOREIGN KEY (gownerId) REFERENCES GRPS(gId)" , {timeout=>[60000]} );
  $status=$db->do("select 1 from information_schema.tables where table_name='$table' and table_collation='$collation'");
  ($status) or $status=0;
  if($status==0){
    print "Changing the collation of table $table\n";
    $db->do("ALTER TABLE $table CONVERT TO CHARACTER SET latin1 COLLATE latin1_general_cs");
    $db->do("ALTER TABLE $table COLLATE latin1_general_cs");
  }

  $db->do("UPDATE $table JOIN USERS ON $table.owner=USERS.Username JOIN GRPS ON $table.gowner=GRPS.Groupname SET $table.ownerId=USERS.uId, $table.gownerId=GRPS.gId",{timeout=>[60000]} );
  #  or $db->do("UPDATE $table JOIN USERS ON BINARY $table.owner=USERS.Username SET $table.ownerId=USERS.uId") ;
  # $db->do("UPDATE $table JOIN GRPS ON $table.gowner=GRPS.Groupname SET $table.gownerId=GRPS.gId") 
  #  or $db->do("UPDATE $table JOIN GRPS ON BINARY $table.gowner=GRPS.Groupname SET $table.gownerId=GRPS.gId");
  $db->do("ALTER TABLE $table DROP COLUMN owner, DROP COLUMN gowner",{timeout=>[60000]});
}
print "New Changes made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";


##GUIDINDEX
foreach my $row (@$guidIndex) {
  $table="G".$row->{tableName}."L";
  $db->do("INSERT IGNORE INTO USERS (Username) (SELECT DISTINCT owner from $table)");
  $db->do("INSERT IGNORE INTO GRPS (Groupname) (SELECT DISTINCT gowner from $table )");
}
print "Doing the alteration in G#L tables\n";
print "\n".scalar(localtime(time))."\n";
foreach my $row (@$guidIndex) {
  $table="G".$row->{tableName}."L";
  print "$table\n";
  $db->do("ALTER TABLE $table ADD (ownerId MEDIUMINT UNSIGNED , gownerId MEDIUMINT UNSIGNED), ADD FOREIGN KEY (ownerId) REFERENCES USERS(uId),ADD FOREIGN KEY (gownerId) REFERENCES GRPS(gId)" ,{timeout=>[60000]});
  $status=$db->do("select 1 from information_schema.tables where table_name='$table' and table_collation='$collation'");
  ($status) or $status=0;
  if($status==0){
    print "Changing the collation of table $table\n";
    $db->do("ALTER TABLE $table CONVERT TO CHARACTER SET latin1 COLLATE latin1_general_cs");
    $db->do("ALTER TABLE $table COLLATE latin1_general_cs");
  }

  $db->do("UPDATE $table JOIN USERS ON $table.owner=USERS.Username JOIN GRPS ON $table.gowner=GRPS.Groupname SET $table.ownerId=USERS.uId, $table.gownerId=GRPS.gId" ,{timeout=>[60000]}); 
  # or $db->do("UPDATE $table JOIN USERS ON BINARY $table.owner=USERS.Username SET $table.ownerId=USERS.uId") ;
  # $db->do("UPDATE $table JOIN GRPS ON $table.gowner=GRPS.Groupname SET $table.gownerId=GRPS.gId");
  # or $db->do("UPDATE $table JOIN GRPS ON BINARY $table.gowner=GRPS.Groupname SET $table.gownerId=GRPS.gId");
  # $db->do("UPDATE $table join USERS ON $table.owner=USERS.Username SET $table.ownerId= USERS.uId");
  # $db->do("UPDATE $table join GRPS ON $table.gowner=GRPS.Groupname SET $table.gownerId= GRPS.gId");
  $db->do("ALTER TABLE $table DROP COLUMN owner, DROP COLUMN gowner" ,{timeout=>[60000]});
}
print "New Changes made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";


print "Updating the GROUPS table to UGMAP with uId and gId & DROP columns Username and Groupname\n";
$db->do("ALTER TABLE GROUPS DROP COLUMN Userid");
$db->do("ALTER TABLE GROUPS ADD (Userid MEDIUMINT UNSIGNED,Groupid MEDIUMINT UNSIGNED)");
$db->do("UPDATE GROUPS join USERS ON USERS.Username=GROUPS.Username SET GROUPS.Userid=USERS.uId ");
$db->do("UPDATE GROUPS join GRPS ON GRPS.Groupname=GROUPS.Groupname SET GROUPS.Groupid=GRPS.gId ");
$db->do("ALTER TABLE GROUPS RENAME UGMAP");
$db->do("ALTER TABLE UGMAP DROP COLUMN Username, DROP COLUMN Groupname");


print "\n".scalar(localtime(time))."\n";
#=cut
