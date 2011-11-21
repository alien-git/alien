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

my $db_old = shift; 
my $db_new = shift;
(defined $db_old) or $db_old="alice_users_sync";
(defined $db_new) or $db_new="alice_users";
print "Old Schema: $db_old" ; 
print "New Schema: $db_new" ; 

### Get connections and DB objects
my $db = AliEn::Database->new({DRIVER => "mysql",
                               HOST   => Net::Domain::hostfqdn().":3307",
                               DB     => "$db_new",
                               ROLE   => "admin"});
my $cat = AliEn::UI::Catalogue::LCM->new({ROLE => "admin"});

### Get index table values for GUID and LFN
my $indexTable = $db->query("SELECT tableName FROM INDEXTABLE");
my $guidIndex  = $db->query("SELECT tableName FROM GUIDINDEX");
my $table;
my $chk=0;

my $update_status=0;
my $status=0;
my $chkTableexists=0;
my $collation='latin1_general_cs';
my $update_time = "2011-11-08 12:26:41";
print "Doing the alteration in L#L tables\n";
print "\n".scalar(localtime(time))."\n";


foreach my $row (@$indexTable) {
  
  $table="L".$row->{tableName}."L";
  print "$table\n";
  
  #checking first if the table exists in db_old
  $chkTableexists = $db->do("SELECT 1 FROM information_schema.tables WHERE table_schema='$db_old' AND table_name='$table' ");
  ($chkTableexists) or $chkTableexists =0;

  if($chkTableexists==0){
    print "Table $table : Doesnt exists \n";
    print "Table $table : Making all the changes \n";
    $db->do("INSERT IGNORE INTO $db_old.USERS (Username) (SELECT DISTINCT owner from $table)");
    $db->do("INSERT IGNORE INTO $db_old.GRPS (Groupname) (SELECT DISTINCT gowner from $table)");
    #Adding the columns
    $db->do("ALTER TABLE $table ADD (ownerId MEDIUMINT UNSIGNED  , gownerId MEDIUMINT UNSIGNED ), ADD FOREIGN KEY (ownerId) 
      REFERENCES USERS(uId),ADD FOREIGN KEY (gownerId) REFERENCES GRPS(gId)" , {timeout=>[60000]} );
    $status=$db->do("select 1 from information_schema.tables where table_name='$table' and table_collation='$collation'");
    ($status) or $status=0;
    if($status==0){
      print "Changing the collation of table $table\n";
      $db->do("ALTER TABLE $table CONVERT TO CHARACTER SET latin1 COLLATE latin1_general_cs");
      $db->do("ALTER TABLE $table COLLATE latin1_general_cs");
    }
    #Updating the columns
    $db->do("UPDATE $table JOIN $db_old.USERS ON $table.owner=$db_old.USERS.Username JOIN $db_old.GRPS ON $table.gowner=$db_old.GRPS.Groupname 
      SET $table.ownerId=$db_old.USERS.uId, $table.gownerId=$db_old.GRPS.gId",{timeout=>[60000]} );
    #Deleting the columns
    $db->do("ALTER TABLE $table DROP COLUMN owner, DROP COLUMN gowner",{timeout=>[60000]});
    #Renaming the table to db_old
    $db->do("ALTER TABLE $db_new.$table RENAME $db_old.$table ",{timeout=>[60000]});
    next;
  }

  print "Table $table exists: Lets chk what we can do!!\n";
  $update_status=$db->do("SELECT 1 FROM information_schema.tables WHERE (table_schema='$db_new' AND table_name='$table') AND 
    (UPDATE_TIME>= (SELECT UPDATE_TIME FROM information_schema.tables WHERE table_schema='$db_old' and table_name='$table') OR 
    (SELECT count(1) FROM $db_old.$table)!=(SELECT count(1) FROM $db_new.$table ))");
  ($update_status) or $update_status=0;
  if($update_status==0){
    #$db->do("DROP TABLE $db_new.$table ",{timeout=>[60000]});
    #$db->do("ALTER TABLE $db_old.$table RENAME $db_new.$table ",{timeout=>[60000]});
    print "Table $table : No Changes (Hence, preserving the $table)\n";
    next;
  }
  
  print "Table $table : Only Changes to be made)\n";
  #updating the USERS and GRPS tables
  $db->do("INSERT IGNORE INTO $db_old.USERS (Username) (SELECT DISTINCT owner from $table)");
  $db->do("INSERT IGNORE INTO $db_old.GRPS (Groupname) (SELECT DISTINCT gowner from $table)");
  #deleting the entries from the table
  $db->do("DELETE FROM $db_old.$table USING $db_old.$table 
     LEFT JOIN $db_new.$table 
     ON ( $db_old.$table.entryId=$db_new.$table.entryId  AND  $db_old.$table.lfn=$db_new.$table.lfn  AND  $db_old.$table.guid=$db_new.$table.guid )
     WHERE $db_new.$table.lfn IS NULL",{timeout=>[60000]});
  #inserting the entries which are not present in old table.
  $db->do("INSERT IGNORE INTO $db_old.$table (entryId,replicated,ctime,jobid,guidtime,lfn,broken,expiretime,size,dir,type,guid,md5,perm,ownerId,gownerId)
     SELECT entryId,replicated,ctime,jobid,guidtime,lfn,broken,expiretime,size,dir,type,guid,md5,perm, $db_old.USERS.uId, $db_old.GRPS.gId 
     FROM $db_new.$table 
     JOIN $db_old.USERS ON $db_old.USERS.Username=$db_new.$table.owner JOIN $db_old.GRPS ON $db_old.GRPS.Groupname=$db_new.$table.gowner ");
  #$db->do("DROP TABLE alice_users_sync.$table ",{timeout=>[60000]});
  #$db->do("ALTER TABLE alice_users.$table RENAME alice_users_sync.$table DROP COLUMN owner, DROP COLUMN gowner",{timeout=>[60000]});

}
$db->do("DROP TABLE IF EXISTS $db_old.INDEXTABLE ",{timeout=>[60000]});
$db->do("ALTER TABLE $db_new.INDEXTABLE RENAME $db_old.INDEXTABLE ",{timeout=>[60000]});
print "New Changes for L#L tables made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";


#####################################################################################################
#####################################################################################################

print "Doing the alteration in G#L tables\n";
print "\n".scalar(localtime(time))."\n";
foreach my $row (@$guidIndex) {
  
  $table="G".$row->{tableName}."L";
  print "$table\n";
  
  #checking first if the table exists in db_old
  $chkTableexists = $db->do("SELECT 1 FROM information_schema.tables WHERE table_schema='$db_old' AND table_name='$table' ");
  ($chkTableexists) or $chkTableexists =0;

  if($chkTableexists==0){
    print "Table $table : Doesnt exists \n";
    print "Table $table : Making all the changes \n";
    $db->do("INSERT IGNORE INTO $db_old.USERS (Username) (SELECT DISTINCT owner from $table)");
    $db->do("INSERT IGNORE INTO $db_old.GRPS (Groupname) (SELECT DISTINCT gowner from $table)");
    #Adding the columns
    $db->do("ALTER TABLE $table ADD (ownerId MEDIUMINT UNSIGNED  , gownerId MEDIUMINT UNSIGNED ), ADD FOREIGN KEY (ownerId) 
      REFERENCES USERS(uId),ADD FOREIGN KEY (gownerId) REFERENCES GRPS(gId)" , {timeout=>[60000]} );
    $status=$db->do("select 1 from information_schema.tables where table_name='$table' and table_collation='$collation'");
    ($status) or $status=0;
    if($status==0){
      print "Changing the collation of table $table\n";
      $db->do("ALTER TABLE $table CONVERT TO CHARACTER SET latin1 COLLATE latin1_general_cs");
      $db->do("ALTER TABLE $table COLLATE latin1_general_cs");
    }
    #Updating the columns
    $db->do("UPDATE $table JOIN $db_old.USERS ON $table.owner=$db_old.USERS.Username JOIN $db_old.GRPS ON $table.gowner=$db_old.GRPS.Groupname 
      SET $table.ownerId=$db_old.USERS.uId, $table.gownerId=$db_old.GRPS.gId",{timeout=>[60000]} );
    #Deleting the columns
    $db->do("ALTER TABLE $table DROP COLUMN owner, DROP COLUMN gowner",{timeout=>[60000]});
    #Renaming the table to db_old
    $db->do("ALTER TABLE $db_new.$table RENAME $db_old.$table ",{timeout=>[60000]});
    next;
  }

  print "Table $table exists: Lets chk what we can do!!\n";
  $update_status=$db->do("SELECT 1 FROM information_schema.tables WHERE (table_schema='$db_new' AND table_name='$table') AND 
    (UPDATE_TIME>= (SELECT UPDATE_TIME FROM information_schema.tables WHERE table_schema='$db_old' and table_name='$table') OR 
    (SELECT count(1) FROM $db_old.$table)!=(SELECT count(1) FROM $db_new.$table ))");
  ($update_status) or $update_status=0;
  if($update_status==0){
    #$db->do("DROP TABLE $db_new.$table ",{timeout=>[60000]});
    #$db->do("ALTER TABLE $db_old.$table RENAME $db_new.$table ",{timeout=>[60000]});
    print "Table $table : No Changes (Hence, preserving the $table)\n";
    next;
  }
  
  print "Table $table : Only Changes to be made)\n";
  #updating the USERS and GRPS tables
  $db->do("INSERT IGNORE INTO $db_old.USERS (Username) (SELECT DISTINCT owner from $table)");
  $db->do("INSERT IGNORE INTO $db_old.GRPS (Groupname) (SELECT DISTINCT gowner from $table)");
  #deleting the entries from the table
  $db->do("DELETE FROM $db_old.$table USING $db_old.$table 
     LEFT JOIN $db_new.$table 
     ON ( $db_old.$table.guidId=$db_new.$table.guidId  AND  $db_old.$table.seStringlist=$db_new.$table.seStringlist  AND  $db_old.$table.guid=$db_new.$table.guid )
     WHERE $db_new.$table.guid IS NULL",{timeout=>[60000]});
  #inserting the entries which are not present in old table.
  $db->do("INSERT IGNORE INTO $db_old.$table (guidId, ctime,ref,jobid,seStringlist,seAutoStringlist, aclId,expiretime,size,guid,type,md5,perm,ownerId,gownerId)
     SELECT guidId,ctime,ref,jobid,seStringlist,seAutoStringlist,aclId,expiretime,size,guid,type,md5,perm, $db_old.USERS.uId, $db_old.GRPS.gId 
     FROM $db_new.$table 
     JOIN $db_old.USERS ON $db_old.USERS.Username=$db_new.$table.owner JOIN $db_old.GRPS ON $db_old.GRPS.Groupname=$db_new.$table.gowner ");
  #$db->do("DROP TABLE alice_users_sync.$table ",{timeout=>[60000]});
  #$db->do("ALTER TABLE alice_users.$table RENAME alice_users_sync.$table DROP COLUMN owner, DROP COLUMN gowner",{timeout=>[60000]});

}
$db->do("DROP TABLE IF EXISTS $db_old.GUIDINDEX ",{timeout=>[60000]});
$db->do("ALTER TABLE $db_new.GUIDINDEX RENAME $db_old.GUIDINDEX ",{timeout=>[60000]});
print "New Changes for L#L tables made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";

print "Updating the GROUPS table to UGMAP with uId and gId & DROP columns Username and Groupname\n";
$db->do("ALTER TABLE GROUPS DROP COLUMN Userid");
$db->do("ALTER TABLE GROUPS ADD (Userid MEDIUMINT UNSIGNED  ,Groupid MEDIUMINT UNSIGNED )");
$db->do("UPDATE GROUPS join $db_old.USERS ON $db_old.USERS.Username=GROUPS.Username SET GROUPS.Userid=$db_old.USERS.uId ");
$db->do("UPDATE GROUPS join $db_old.GRPS ON $db_old.GRPS.Groupname=GROUPS.Groupname SET GROUPS.Groupid=$db_old.GRPS.gId ");
$db->do("ALTER TABLE GROUPS RENAME UGMAP");
$db->do("ALTER TABLE UGMAP DROP COLUMN Username, DROP COLUMN Groupname");
$db->do("DROP TABLE IF EXISTS $db_old.UGMAP ");
$db->do("ALTER TABLE $db_new.UGMAP RENAME $db_old.UGMAP");

print "\n".scalar(localtime(time))."\n";

