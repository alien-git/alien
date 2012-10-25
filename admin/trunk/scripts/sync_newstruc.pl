#!/bin/env alien-perl

use strict;
use Data::Dumper;
use AliEn::Database;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

my $destDB = "alice_data_test";
my $orgDB  = "alice_users_test";

### Get connections and DB objects
my $db = AliEn::Database::Catalogue->new({
                               ROLE   => "admin"}) or exit(-2);
$db->do("use $destDB");
                              
$db->createCatalogueTables();

print "Inserting into tables \n";
print "\n".scalar(localtime(time))."\n";
  $db->do("INSERT IGNORE INTO USERS (Username) (SELECT user from $orgDB.FQUOTAS)");
  $db->do("INSERT IGNORE INTO GRPS (Groupname) (SELECT user from $orgDB.FQUOTAS)");

  $db->do("alter table $orgDB.FQUOTAS convert to character set latin1 collate latin1_general_cs;");
  $db->do("INSERT IGNORE INTO FQUOTAS SELECT uId, maxNbFiles, nbFiles, tmpIncreasedTotalSize, maxTotalSize, tmpIncreasedNbFiles, totalSize from $orgDB.FQUOTAS join USERS on user=Username");

  $db->do("INSERT IGNORE INTO TAG0 SELECT 0, uId, path, tagName, tableName from $orgDB.TAG0 join USERS on user=Username");

  $db->do("INSERT IGNORE INTO INDEXTABLE SELECT tableName, lfn from $orgDB.INDEXTABLE");
  $db->do("INSERT IGNORE INTO LL_STATS SELECT tableNumber, max_time, min_time from $orgDB.LL_STATS");
  $db->do("INSERT IGNORE INTO LL_ACTIONS SELECT tableNumber, time, action, extra from $orgDB.LL_ACTIONS");
  
  $db->do("INSERT IGNORE INTO SE SELECT seNumber,seMinSize,seExclusiveWrite,seDemoteRead,seName,seQoS,seStoragePath,seType,seNumFiles,seUsedSpace,seExclusiveRead,seDemoteWrite,seioDaemons,seVersion from $orgDB.SE");
  $db->do("INSERT IGNORE INTO SE_VOLUMES SELECT volume,0,usedspace,mountpoint,size,seNumber,method,freespace from $orgDB.SE_VOLUMES join SE on $orgDB.SE_VOLUMES.seName=SE.seName");

  $db->do("INSERT IGNORE INTO GUIDINDEX SELECT tableName, guidTime from $orgDB.GUIDINDEX");
  $db->do("INSERT IGNORE INTO GL_STATS SELECT tableNumber, seNumFiles, seNumber, seUsedSpace from $orgDB.GL_STATS");
  $db->do("INSERT IGNORE INTO GL_ACTIONS SELECT tableNumber, time, action, extra from $orgDB.GL_ACTIONS");
  
  $db->do("INSERT IGNORE INTO TODELETE SELECT entryId,pfn,seNumber,guid from $orgDB.TODELETE");
  
  $db->do("INSERT IGNORE INTO COLLECTIONS SELECT * from $orgDB.COLLECTIONS");
  $db->do("INSERT IGNORE INTO COLLECTIONS_ELEM SELECT collectionId,localName,data,origLFN,guid from $orgDB.COLLECTIONS_ELEM");

  $db->do("INSERT IGNORE INTO ACTIONS SELECT action,todo from $orgDB.ACTIONS");

  $db->do("INSERT IGNORE INTO PACKAGES SELECT fullPackageName,lfn,packageName,username,size,platform,packageVersion from $orgDB.PACKAGES");
  
  $db->do("INSERT IGNORE INTO LFN_UPDATES SELECT guid,entryId,action from $orgDB.LFN_UPDATES");
  
  $db->do("INSERT IGNORE INTO PFN_TODELETE SELECT pfn,retry from $orgDB.PFN_TODELETE");
  
  $db->do("INSERT IGNORE INTO TRIGGERS SELECT lfn,entryId,triggerName from $orgDB.TRIGGERS");
  $db->do("INSERT IGNORE INTO TRIGGERS_FAILED SELECT lfn,entryId,triggerName from $orgDB.TRIGGERS_FAILED");
  
  $db->do("INSERT IGNORE INTO UGMAP SELECT uId,1,uId from USERS");
  
#  $db->do("alter table $orgDB.LFN_BOOKED convert to character set latin1 collate latin1_general_cs;");
#  $db->do("alter table $orgDB.LFN_BOOKED modify se varchar(100) COLLATE latin1_general_ci DEFAULT NULL;");
#  $db->do("insert into GRPS select distinct gowner from $orgDB.LFN_BOOKED where gowner not in (select Groupname from GRPS)");
#  $db->do("update $orgDB.LFN_BOOKED set se='no_se' where se is null");
#  $db->do("INSERT IGNORE INTO LFN_BOOKED SELECT lfn,quotaCalculated,existing,jobid,md5sum,expiretime,size,pfn,gId,seNumber,guid,uId from $orgDB.LFN_BOOKED join USERS on Username=owner join GRPS on gowner=Groupname join SE on se=seName");
  
  $db->do("drop table $orgDB.TAG0, $orgDB.TODELETE, $orgDB.TRIGGERS, $orgDB.TRIGGERS_FAILED, L0L_QUOTA, L0L_broken, L0L, G0L_REF, G0L_PFN, G0L_QUOTA, G0L");

  my ($myTTables) = $db->queryColumn("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$orgDB' and table_name like 'T%'");
  foreach my $table (@$myTTables){
    $db->do("alter table $orgDB.".$table." rename $destDB.".$table."");
  }
  
print "Finished inserting into tables, going to order \n";
print "\n".scalar(localtime(time))."\n";  

my $cpus=4;

### Get index table values for GUID and LFN
my $indexTable = $db->queryColumn("SELECT tableName FROM INDEXTABLE join information_schema.TABLES on (concat('L', tableName, 'L')= table_name and table_schema='alice_data_test') order by table_rows desc;");
my $guidIndex  = $db->queryColumn("SELECT tableName FROM GUIDINDEX join information_schema.TABLES on (concat('G', tableName, 'L')= table_name and table_schema='alice_data_test') order by table_rows desc;");
my $table;
my $chk=0;

# Order
#my %tablesL = ();
#my %tablesG = ();
#
#for my $table (@$indexTable){
#  my $count = $db->queryValue("select count(*) as count from L".$table."L");
#  $tablesL{$table}->{count}=$count;
#}
#
#foreach my $table (sort {$tablesG{$a}->{count} <=> $tablesG{$b}->{count} } keys %tablesG) {
#
#for my $table (@$guidIndex){
#  my $count = $db->queryValue("select count(*) as count from G".$table."L");
#  $tablesG{$table}->{count}=$count;
#}

print "Doing the alteration in L#L tables\n";
print "\n".scalar(localtime(time))."\n";

my $status=0;
my $update_status=0;
my $count1=0;
my $count2=0;
my $collation='latin1_general_cs';
my $update_time = "2011-11-08 12:26:41";

my $kid=0;
my $splitLFN={};
for my $table (@$indexTable){
  $splitLFN->{$kid} or $splitLFN->{$kid}={LFN=>[], GUID=>[]};
  push @{$splitLFN->{$kid}->{LFN}}, "L${table}L";
  $kid++;
  $kid==$cpus and $kid=0;
}

for my $table (@$guidIndex){
  $splitLFN->{$kid} or $splitLFN->{$kid}={LFN=>[], GUID=>[]};
  push @{$splitLFN->{$kid}->{GUID}}, "G${table}L";
  $kid++;
  $kid==$cpus and $kid=0;
}


for ($kid=0; $kid<$cpus; $kid++){
  my $pid=fork();
  ($pid) or last;
}
if ($kid==$cpus){
  print "The father finishes\n";
  exit(-1);
}

$db->{LOGGER}->redirect("/home/alienmaster/.alien/mysql/salida_kid_".$kid."_$$");
my $counter=0;
$db->do("use $destDB");

foreach my $table (@{$splitLFN->{$kid}->{LFN}}) {
  print "KID $kid doing   $table ($counter out of $#{$splitLFN->{$kid}->{LFN}}\n";
#  if ($db->do("select gownerId from ${table} limit  1")){
#   print "DONE SOMETHING\n";
#   $db->do("update $table set ownerId=(select uId from USERS where Username='admin') where ownerId is null");
#   $db->do("update $table set gownerid=(select gId from GRPS where Groupname='admin') where gownerId is null");
#   next;
#   $db->do("drop table $table");
#   $db->do("alter table ${table}_OLD rename $table");
#  }
  $counter++;
#  $db->do("ALTER TABLE $orgDB.".$table." rename $orgDB.".$table."_OLD");
  $db->checkLFNTable($table, 'noindex');
  $db->do("INSERT INTO $destDB.$table ( entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gownerId, type, guid, md5, ownerId, perm) 
   select  entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gId, type, guid, md5, uId, perm from ${orgDB}.${table} old 
left JOIN USERS ON old.owner=USERS.username left JOIN GRPS ON old.gowner=GRPS.groupname ");

  $db->do("INSERT IGNORE INTO ${destDB}.${table}_QUOTA ( userId, nbFiles, totalSize ) select uId, nbFiles, totalSize from ${orgDB}.${table}_QUOTA old 
  left JOIN USERS on old.user=USERS.username");
  $db->do("INSERT IGNORE INTO ${destDB}.${table}_broken ( entryId ) select entryId from ${orgDB}.${table}_broken old");
  #$db->do("update $destDB.$table set ownerId=(select uId from USERS where Username='admin') where ownerId is null");
  #$db->do("update $destDB.$table set gownerId=(select gId from GRPS where Groupname='admin') where gownerId is null");
  $db->checkLFNTable($table);

  #$db->do("drop table ${orgDB}.${table}");
  #$db->do("drop table ${orgDB}.${table}_QUOTA");
  #$db->do("drop table ${orgDB}.${table}_broken");
  print scalar(localtime(time))."\n";
}


print "New Changes made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";


print " $kid Doing the alteration in G#L tables\n";
print "\n".scalar(localtime(time))."\n";

$counter=0;
foreach my $table (@{$splitLFN->{$kid}->{GUID}}) {
  print "KID $kid doing $table  ($counter out of $#{$splitLFN->{$kid}->{GUID}})\n";
#  if ($db->do("select gownerId from ${table} limit 1")){
#   print "DONE SOMETHING\n";
#    $db->do("drop table $table");
#    $db->do("alter table ${table}_OLD rename $table");
#    $db->checkGUIDTable($table);
#
#    next;
#  }
#  print "$table was not converted!!\n";
#  $db->do("alter table $table rename ${table}_OLD");

  $counter++;
  $db->checkGUIDTable($table, 'noindex');
  
  print "After noindex: ".scalar(localtime(time))."\n";
  
  $db->do("insert into $destDB.$table ( guidId,ctime,ref,jobid,sestringlist, seautostringlist,expiretime,size,gownerId,guid,type, md5, ownerId, perm)
select  guidId,ctime,ref,jobid,sestringlist, seautostringlist,expiretime,size,gId,guid,type, md5, uId, perm from ${orgDB}.${table} old left JOIN USERS on old.owner=USERS.username left JOIN GRPS on old.gowner=GRPS.groupname");
  
  $db->do("INSERT IGNORE INTO ${destDB}.${table}_PFN ( guidId, pfn, seNumber) select guidId, pfn, seNumber from ${orgDB}.${table}_PFN old");
  $db->do("DELETE FROM ${destDB}.${table}_PFN where seNumber not in (select seNumber from SE)");
  #$db->do("INSERT IGNORE INTO ${destDB}.${table}_REF ( guidId, lfnRef) select guidId, lfnRef from ${orgDB}.${table}_REF old");
  $db->do("INSERT IGNORE INTO ${destDB}.${table}_QUOTA ( userId, nbFiles, totalSize) select uId, nbFiles, totalSize from ${orgDB}.${table}_QUOTA old 
  left JOIN USERS on old.user=USERS.username");
  
  print "After insert: ".scalar(localtime(time))."\n";
  
  #$db->do("update $destDB.$table set ownerId=(select uId from USERS where Username='admin') where ownerId is null");
  #$db->do("update $destDB.$table set gownerId=(select gId from GRPS where Groupname='admin') where gownerId is null");
  $db->checkGUIDTable($table);
  
  #$db->do("drop table ${orgDB}.${table}_PFN");
  #$db->do("drop table ${orgDB}.${table}_REF");
  #$db->do("drop table ${orgDB}.${table}_QUOTA");
  #$db->do("drop table ${orgDB}.${table}");
}

print "New Changes made successfully by $kid!!!\n"; 
print "\n".scalar(localtime(time))."\n";
# drop table G71L_REF, G71L_QUOTA, G71L_PFN, G71L, G70L_REF, G70L_QUOTA, G70L_PFN, G70L, G69L_REF, G69L_QUOTA, G69L_PFN, G69L, G86L_REF, G86L_QUOTA, G86L_PFN, G86L, G34L_REF, G34L_QUOTA, G34L_PFN, G34L, G47L_REF, G47L_QUOTA, G47L_PFN, G47L, G50L_REF, G50L_QUOTA, G50L_PFN, G50L, G68L_REF, G68L_QUOTA, G68L_PFN, G68L;
