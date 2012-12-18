#!/bin/env alien-perl

use strict;
use Data::Dumper;
use AliEn::Database;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

my $destDB = "alice_data_test";
my $orgDB  = "alice_users";
my $orgDB2 = "alice_data";

### Get connections and DB objects
my $db = AliEn::Database::Catalogue->new({ROLE=>'admin', PASSWD=>'XI)B^nF7Ft','user'=>'admin'}) or exit(-2);
$db->do("use $destDB");
   
print "Creating tables: ".scalar(localtime(time))."\n";                         
$db->createCatalogueTables();
$db->do("drop table L0L_broken,L0L_QUOTA,L0L, G0L_REF,G0L_PFN,G0L_QUOTA,G0L");
$db->do("delete from INDEXTABLE");
$db->do("delete from GUIDINDEX");
$db->do("update $orgDB.INDEXTABLE set tableName=9 where tableName=10 and hostIndex=7");
$db->do("alter table $orgDB2.L10L rename $orgDB2.L9L");                                 
$db->do("alter table $orgDB2.L10L_QUOTA rename $orgDB2.L9L_QUOTA");                      
$db->do("alter table $orgDB2.L10L_broken rename $orgDB2.L9L_broken"); 
$db->do("drop table $orgDB2.L0L_broken,$orgDB2.L0L_QUOTA,$orgDB2.L0L");

print "Inserting into tables: ".scalar(localtime(time))."\n";

  #SI
  $db->do("INSERT IGNORE INTO USERS (Username) (SELECT user from $orgDB.FQUOTAS)");
  $db->do("INSERT IGNORE INTO GRPS (Groupname) (SELECT user from $orgDB.FQUOTAS)");

  #SI
  $db->do("alter table $orgDB.FQUOTAS convert to character set latin1 collate latin1_general_cs;");
  $db->do("INSERT IGNORE INTO FQUOTAS SELECT uId, maxNbFiles, nbFiles, tmpIncreasedTotalSize, maxTotalSize, tmpIncreasedNbFiles, totalSize from $orgDB.FQUOTAS join USERS on user=Username");

  #SI
  $db->do("INSERT IGNORE INTO SE SELECT seNumber,seMinSize,seExclusiveWrite,seDemoteRead,seName,seQoS,seStoragePath,seType,seNumFiles,seUsedSpace,seExclusiveRead,seDemoteWrite,seioDaemons,seVersion from $orgDB.SE");
  $db->do("INSERT IGNORE INTO SE_VOLUMES SELECT volume,0,usedspace,mountpoint,size,seNumber,method,freespace from $orgDB.SE_VOLUMES join SE on $orgDB.SE_VOLUMES.seName=SE.seName");

  #SI
  $db->do("INSERT IGNORE INTO INDEXTABLE SELECT tableName, lfn from $orgDB.INDEXTABLE where hostIndex=7");
  $db->do("update INDEXTABLE set tableName=9 where tableName=10");
  $db->do("INSERT IGNORE INTO INDEXTABLE SELECT tableName, lfn from $orgDB.INDEXTABLE where hostIndex=8");
  $db->do("insert ignore into LL_STATS (tableName,max_time,min_time) select tableNumber as tableName,max_time,min_time from $orgDB.LL_STATS where tableNumber in (select tableName from INDEXTABLE)");
  $db->do("update $orgDB2.LL_STATS set tableNumber=9 where tableNumber=10");
  $db->do("delete from $orgDB2.LL_STATS where tableNumber=0");
  $db->do("insert ignore into LL_STATS (tableName,max_time,min_time) select tableNumber as tableName,max_time,min_time from $orgDB2.LL_STATS where tableNumber in (select tableName from INDEXTABLE)");
  $db->do("INSERT IGNORE INTO LL_ACTIONS (tableName,time,action,extra) SELECT tableNumber as tableName, time, action, extra from $orgDB.LL_ACTIONS where action!='QUOTA' and tableNumber in (select tableName from INDEXTABLE)");
  $db->do("update $orgDB2.LL_ACTIONS set tableNumber=9 where tableNumber=10");  
  $db->do("delete from $orgDB2.LL_ACTIONS where tableNumber=0");
  $db->do("INSERT IGNORE INTO LL_ACTIONS (tableName,time,action,extra) SELECT tableNumber as tableName, time, action, extra from $orgDB2.LL_ACTIONS where action!='QUOTA' and tableNumber in (select tableName from INDEXTABLE)");

  #SI
  $db->do("INSERT IGNORE INTO GUIDINDEX SELECT tableName, guidTime from $orgDB.GUIDINDEX");
  $db->do("INSERT IGNORE INTO GL_STATS (tableName,seNumFiles,seNumber,seUsedSpace) SELECT tableNumber as tableName,t1.seNumFiles,t1.seNumber,t1.seUsedSpace from $orgDB.GL_STATS t1 join SE using(seNumber) where tableNumber in (select tableName from GUIDINDEX)");
  $db->do("INSERT IGNORE INTO GL_ACTIONS (tableName,time,action,extra) SELECT tableNumber as tableName,time,action,extra from $orgDB.GL_ACTIONS where action!='QUOTA' and tableNumber in (select tableName from GUIDINDEX)");
  
  #SI
  $db->do("INSERT IGNORE INTO TODELETE (entryId,pfn,seNumber,guid) SELECT entryId,pfn,seNumber,guid from $orgDB.TODELETE join SE using(seNumber)");
  
  #SI 5min
  $db->do("INSERT IGNORE INTO COLLECTIONS (collectionId,collGUID) SELECT collectionId,collGUID from $orgDB.COLLECTIONS");
  $db->do("INSERT IGNORE INTO COLLECTIONS_ELEM (collectionId,localName,data,origLFN,guid) SELECT collectionId,localName,data,origLFN,guid from $orgDB.COLLECTIONS_ELEM");
  $db->do("set \@maxC :=  (select max(collectionId) from COLLECTIONS)");
  $db->do("insert into COLLECTIONS (collectionId, collGUID) select collectionId+\@maxC,collGUID from $orgDB2.COLLECTIONS;");
  $db->do("insert ignore into COLLECTIONS_ELEM (collectionId,origLFN,localName,guid,data) select collectionId+\@maxC,origLFN,localName,guid,data from $orgDB2.COLLECTIONS_ELEM");
  
  #SI
  $db->do("INSERT IGNORE INTO ACTIONS (action,todo) SELECT action,todo from $orgDB.ACTIONS");

  #SI
  $db->do("INSERT IGNORE INTO PACKAGES (fullPackageName,lfn,packageName,username,size,platform,packageVersion) SELECT fullPackageName,lfn,packageName,username,size,platform,packageVersion from $orgDB.PACKAGES");
  
  #SI
  $db->do("INSERT IGNORE INTO LFN_UPDATES (guid,entryId,action) SELECT guid,entryId,action from $orgDB.LFN_UPDATES");
  
  #SI
  $db->do("INSERT IGNORE INTO PFN_TODELETE (pfn,retry) SELECT pfn,retry from $orgDB.PFN_TODELETE");
  
  #SI
  $db->do("INSERT IGNORE INTO TRIGGERS (lfn,entryId,triggerName) SELECT lfn,entryId,triggerName from $orgDB.TRIGGERS");
  $db->do("INSERT IGNORE INTO TRIGGERS_FAILED (lfn,entryId,triggerName) SELECT lfn,entryId,triggerName from $orgDB.TRIGGERS_FAILED"); #5min
  
  #SI
  $db->do("INSERT IGNORE INTO UGMAP SELECT uId,1,uId from USERS");
  
  #SI 4min users, 11min data no insert - uId,gId,seNumber null o inexistentes excluidos
  $db->do("alter table $orgDB.LFN_BOOKED modify owner varchar(20) COLLATE latin1_general_cs DEFAULT NULL");
  #$db->do("alter table $orgDB2.LFN_BOOKED modify owner varchar(20) COLLATE latin1_general_cs DEFAULT NULL")";
  $db->do("INSERT IGNORE INTO LFN_BOOKED SELECT lfn,quotaCalculated,existing,jobid,md5sum,expiretime,size,pfn,gId,seNumber,guid,uId from $orgDB.LFN_BOOKED join USERS on Username=owner join GRPS on gowner=Groupname join SE on se=seName");
  #$db->do("INSERT IGNORE INTO LFN_BOOKED SELECT lfn,quotaCalculated,existing,jobid,md5sum,expiretime,size,pfn,gId,seNumber,guid,uId from $orgDB2.LFN_BOOKED join USERS on Username=owner join GRPS on gowner=Groupname join SE on se=seName");

  #SI
  #TABLAS TV NO EN TAG0s: select table_name from information_schema.TABLES where table_schema='alice_data_test' and table_name like 'T%V%' and table_name not in (select distinct tableName from TAG0);
  $db->do("INSERT IGNORE INTO TAG0 (entryId,userId,path,tagName,tableName) SELECT 0,uId,path,tagName,tableName from $orgDB.TAG0 join USERS on user=Username");
  $db->do("INSERT IGNORE INTO TAG0 (entryId,userId,path,tagName,tableName) SELECT 0,uId,path,tagName,concat(tableName, '_data') from $orgDB2.TAG0 join USERS on user=Username");
  
  my ($myTTables) = $db->queryColumn("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$orgDB' and table_name like 'T%V%'");
  foreach my $table (@$myTTables){
    $db->do("alter table ${orgDB}.${table} rename ${destDB}.${table}");
    $db->do("alter table ${destDB}.${table} engine='InnoDB'");
  }
  my ($myTTablesData) = $db->queryColumn("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$orgDB2' and table_name like 'T%V%'");
  foreach my $table (@$myTTablesData){
    $db->do("alter table ${orgDB2}.${table} rename ${destDB}.${table}_data");
    $db->do("alter table ${destDB}.${table}_data engine='InnoDB'");
  }
   
print "Finished inserting into tables, going to query index tables ".scalar(localtime(time))."\n";  

my $cpus=32;

### Get index table values for GUID and LFN
my $guidIndex  = $db->queryColumn("SELECT tableName FROM GUIDINDEX join information_schema.TABLES on (concat('G', tableName, 'L')= table_name and table_schema='$orgDB') order by table_rows desc;");
my $indexTable = $db->queryColumn("SELECT tableName FROM $orgDB.INDEXTABLE join information_schema.TABLES on (concat('L', tableName, 'L')= table_name and table_schema='$orgDB' and hostIndex=8) order by table_rows desc;");
my $indexTable2 = $db->queryColumn("SELECT tableName FROM $orgDB.INDEXTABLE join information_schema.TABLES on (concat('L', tableName, 'L')= table_name and table_schema='$orgDB2' and hostIndex=7) order by table_rows desc;");

my $kid=0;
my $splitLFN={};
my $count=0;

for my $table (@$guidIndex){
  $splitLFN->{$kid} or $splitLFN->{$kid}={LFN=>[], LFN2=>[], GUID=>[]};
  push @{$splitLFN->{$kid}->{GUID}}, "G${table}L";
  $kid++;
  $kid==$cpus and $kid=0;
}

for my $table (@$indexTable){
  $splitLFN->{$kid} or $splitLFN->{$kid}={LFN=>[], LFN2=>[], GUID=>[]};
  push @{$splitLFN->{$kid}->{LFN}}, "L${table}L";
  $kid++;
  $kid==$cpus and $kid=0;
}

for my $table (@$indexTable2){
  $splitLFN->{$kid} or $splitLFN->{$kid}={LFN=>[], LFN2=>[], GUID=>[]};
  push @{$splitLFN->{$kid}->{LFN2}}, "L${table}L";
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

$db->{LOGGER}->redirect("/home/mysql/salida_kid_full_".$kid."_$$");
my $counter=0;
$db->do("use $destDB");

# SQL for insert and indexes
open(FILE, "> ia_${kid}.sql");
print FILE "use ${destDB};\n";

print "$kid doing the alteration in G#L tables ".scalar(localtime(time))."\n";

foreach my $table (@{$splitLFN->{$kid}->{GUID}}) {
  print "KID $kid doing $table ($counter out of $#{$splitLFN->{$kid}->{GUID}})\n";

  $counter++;
  $db->checkGUIDTable($table, 'noindex');
  print FILE "select now() from DUAL;  
insert into $destDB.$table ( guidId,ctime,ref,jobid,sestringlist, seautostringlist,expiretime,size,gownerId,guid,type, md5, ownerId, perm) select  guidId,ctime,ref,jobid,sestringlist, seautostringlist,expiretime,size,gId,guid,type, md5, uId, perm from ${orgDB}.${table} old left JOIN USERS ON old.owner=USERS.username left JOIN GRPS ON old.gowner=GRPS.groupname;
ALTER TABLE $destDB.$table ADD UNIQUE INDEX (guid), ADD INDEX(seStringlist), ADD INDEX(ctime),   ADD FOREIGN KEY (ownerId) REFERENCES ${destDB}.USERS(uId) ON DELETE CASCADE, ADD FOREIGN KEY (gownerId) REFERENCES ${destDB}.GRPS(gId) ON DELETE CASCADE;

select 'START PFN', now() from DUAL;
INSERT IGNORE INTO ${destDB}.${table}_PFN ( guidId, pfn, seNumber) select guidId, pfn, seNumber from ${orgDB}.${table}_PFN old join SE using (senumber) join $destDB.$table using (guidId);
ALTER TABLE ${destDB}.${table}_PFN ADD INDEX guid_ind (guidId), ADD FOREIGN KEY (guidId) REFERENCES ${destDB}.${table}(guidId) ON DELETE CASCADE,   ADD FOREIGN KEY (seNumber) REFERENCES ${destDB}.SE(seNumber) on DELETE CASCADE;

select 'START QUOTA', now() from DUAL;

INSERT IGNORE INTO ${destDB}.${table}_QUOTA ( userId, nbFiles, totalSize) select uId, nbFiles, totalSize from ${orgDB}.${table}_QUOTA old  left JOIN USERS on old.user=USERS.username;
ALTER TABLE ${destDB}.${table}_QUOTA ADD INDEX user_ind (userId), ADD foreign key (userId) references ${destDB}.USERS(uId) on delete cascade;

ALTER TABLE ${destDB}.${table}_REF ADD INDEX guidId(guidId), ADD INDEX lfnRef(lfnRef), ADD FOREIGN KEY (guidId) REFERENCES ${destDB}.${table}(guidId) ON DELETE CASCADE;
select ' FINISH $table', now() from DUAL;
\n\n";
  
  print "Checked table ${table} ".scalar(localtime(time))."\n";
}

print "$kid doing the alteration in L#L tables ".scalar(localtime(time))."\n";

foreach my $table (@{$splitLFN->{$kid}->{LFN}}) {
  print "KID $kid doing $table ($counter out of $#{$splitLFN->{$kid}->{LFN}}\n";
  $counter++;
  $db->checkLFNTable($table, 'noindex');
  
  print FILE "select now() from DUAL;
INSERT INTO $destDB.$table ( entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gownerId, type, guid, md5, ownerId, perm) 
   select  entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gId, type, guid, md5, uId, perm from ${orgDB}.${table} old 
   left JOIN USERS ON old.owner=USERS.username left JOIN GRPS ON old.gowner=GRPS.groupname;
ALTER TABLE $destDB.$table ADD INDEX(entryId), ADD UNIQUE INDEX (lfn), ADD INDEX(dir), ADD INDEX(guid), ADD INDEX(type), ADD INDEX(ctime),
   ADD INDEX(guidtime), ADD foreign key (ownerId) references ${destDB}.USERS(uId) on delete cascade, ADD foreign key (gownerId) references ${destDB}.GRPS(gId) on delete cascade;

select 'START QUOTA', now() from DUAL;
ALTER TABLE ${destDB}.${table}_QUOTA ADD INDEX user_ind (userId), ADD foreign key (userId) references ${destDB}.USERS(uId) on delete cascade;

select 'START broken', now() from DUAL;
ALTER TABLE ${destDB}.${table}_broken ADD foreign key (entryId) references ${destDB}.${table}(entryId) on delete cascade;

select ' FINISH $table', now() from DUAL;
\n\n";

# INSERT IGNORE INTO ${destDB}.${table}_QUOTA ( userId, nbFiles, totalSize ) select uId, nbFiles, totalSize from ${orgDB}.${table}_QUOTA old 
#   left JOIN USERS on old.user=USERS.username; 
#  print FILE "INSERT IGNORE INTO ${destDB}.${table}_broken ( entryId ) select entryId from ${orgDB}.${table}_broken old;\n";
  
  print "Checked table ${table} ".scalar(localtime(time))."\n";
}


foreach my $table (@{$splitLFN->{$kid}->{LFN2}}) {
  print "KID $kid doing $table ($counter out of $#{$splitLFN->{$kid}->{LFN2}}\n";
  $counter++;
  $db->checkLFNTable($table, 'noindex');

  print FILE "select now() from DUAL;
INSERT INTO $destDB.$table ( entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gownerId, type, guid, md5, ownerId, perm) 
   select  entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gId, type, guid, md5, uId, perm from ${orgDB2}.${table} old 
   left JOIN USERS ON old.owner=USERS.username left JOIN GRPS ON old.gowner=GRPS.groupname;
ALTER TABLE $destDB.$table ADD INDEX(entryId), ADD UNIQUE INDEX (lfn), ADD INDEX(dir), ADD INDEX(guid), ADD INDEX(type), ADD INDEX(ctime),
   ADD INDEX(guidtime), ADD foreign key (ownerId) references ${destDB}.USERS(uId) on delete cascade, ADD foreign key (gownerId) references ${destDB}.GRPS(gId) on delete cascade;

select 'START QUOTA', now() from DUAL;
ALTER TABLE ${destDB}.${table}_QUOTA ADD INDEX user_ind (userId), ADD foreign key (userId) references ${destDB}.USERS(uId) on delete cascade;

select 'START broken', now() from DUAL;
ALTER TABLE ${destDB}.${table}_broken ADD foreign key (entryId) references ${destDB}.${table}(entryId) on delete cascade;

select ' FINISH $table', now() from DUAL;
\n\n";

# INSERT IGNORE INTO ${destDB}.${table}_QUOTA ( userId, nbFiles, totalSize ) select uId, nbFiles, totalSize from ${orgDB2}.${table}_QUOTA old 
#   left JOIN USERS on old.user=USERS.username; 
#  print FILE "INSERT IGNORE INTO ${destDB}.${table}_broken ( entryId ) select entryId from ${orgDB}.${table}_broken old;\n";

  print "Checked table ${table} ".scalar(localtime(time))."\n";
}

close FILE;
print "New Changes made successfully by $kid!!! ".scalar(localtime(time))."\n";
