#!/bin/env alien-perl

use strict;
use Data::Dumper;
use AliEn::Database;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

### Get connections and DB objects
my $db = AliEn::Database::Catalogue->new({
                               ROLE   => "admin"}) or exit(-2);
$db->do("use alice_users_test");
                              
$db->createUserTables();

print "\n".scalar(localtime(time))."\n";
  $db->do("INSERT IGNORE INTO USERS (Username) (SELECT user from FQUOTAS)");
  $db->do("INSERT IGNORE INTO GRPS (Groupname) (SELECT user from FQUOTAS)");

  $db->do("  alter table FQUOTAS ADD userId mediumint(8) unsigned NOT NULL FIRST;");
  $db->do("  alter table FQUOTAS convert to character set latin1 collate latin1_general_cs;");
  $db->do("  update FQUOTAS set userId = (select uId from USERS where user=Username );");
  $db->do("  alter table FQUOTAS DROP COLUMN user;");

  $db->do("  alter table TAG0 ADD userId mediumint(8) unsigned DEFAULT NULL AFTER entryId;");
  $db->do("  alter table TAG0 modify tagName varchar(50) COLLATE latin1_general_cs DEFAULT NULL AFTER path;");  
  $db->do("  update TAG0 set userId = (select uId from USERS where user=Username );");
  $db->do("  alter table TAG0 DROP COLUMN user;");

  $db->do("  delete from GL_STATS where seNumber not in (select seNumber from SE)");


exit(-2);
$db->createCatalogueTables();


my $cpus=4;

# Make all the tables InnoDB
#my ($myIsamTables) = $db->queryColumn("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'alice_users_test' AND ENGINE='MyISAM'");
#foreach my $table (@$myIsamTables){
#  $db->do("ALTER TABLE $table ENGINE = INNODB");
#}

### Get index table values for GUID and LFN
my $indexTable = $db->queryColumn("SELECT tableName FROM INDEXTABLE");
my $guidIndex  = $db->queryColumn("SELECT tableName FROM GUIDINDEX");
my $table;
my $chk=0;


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
  $kid==$cpus and $kid=0; # > ?
}


for ($kid=0; $kid<$cpus; $kid++){
  my $pid=fork();
  ($pid) or last;
}
if ($kid==$cpus){
  print "The father finishes\n";
  exit(-1);
}
my $counter=0;
foreach my $table (@{$splitLFN->{$kid}->{LFN}}) {
  print "KID $kid doing   $table ($counter out of $#{$splitLFN->{$kid}->{LFN}}\n";
  if ($db->do("select gownerId from ${table} limit  1")){
   print "DONE SOMETHING\n";
   $db->do("update $table set ownerId=(select uId from USERS where Username='admin') where ownerId is null");
   $db->do("update $table set gownerid=(select gId from GRPS where Groupname='admin') where gownerId is null");
   next;
   $db->do("drop table $table");
   $db->do("alter table ${table}_OLD rename $table");
  }
  $counter++;
  $db->do("ALTER TABLE $table rename ${table}_OLD");
  $db->checkLFNTable($table, 'noindex');
  $db->do("INSERT INTO $table ( entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gownerId, type, guid, md5, ownerId, perm) 
   select  entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gId, type, guid, md5, uId, perm from ${table}_OLD old 
left JOIN USERS ON old.owner=USERS.username left JOIN GRPS ON old.gowner=GRPS.groupname ");
  $db->do("update $table set ownerId=(select uId from USERS where Username='admin') where ownerId is null");
  $db->do("update $table set gownerId=(select gId from GRPS where Groupname='admin') where gownerId is null");

  $db->checkLFNTable($table);

  $db->do("drop table ${table}_OLD");
  print scalar(localtime(time))."\n";
}


print "New Changes made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";
#exit(-2);


print " $kid Doing the alteration in G#L tables\n";
print "\n".scalar(localtime(time))."\n";

$counter=0;
foreach my $table (@{$splitLFN->{$kid}->{GUID}}) {
  print "KID $kid doing $table  ($counter out of $#{$splitLFN->{$kid}->{GUID}})\n";
  #next;
  $counter++;
  if ($db->do("select gownerId from ${table} limit 1")){
   print "DONE SOMETHING\n";
#   $db->do("drop table $table");
#   $db->do("alter table ${table}_OLD rename $table");
    $db->checkGUIDTable($table);

    next;
  }
  #print "$table was not converted!!\n";
  $db->do("alter table $table rename ${table}_OLD");

  $db->checkGUIDTable($table, 'noindex');
  $db->do("insert into $table ( guidId,ctime,ref,jobid,sestringlist, seautostringlist,expiretime,size,gownerId,guid,type, md5, ownerId, perm)
select  guidId,ctime,ref,jobid,sestringlist, seautostringlist,expiretime,size,gId,guid,type, md5, uId, perm from ${table}_OLD old left JOIN USERS on old.owner=USERS.username left JOIN GRPS on old.gowner=GRPS.groupname");
  $db->do("update $table set ownerId=(select uId from USERS where Username='admin') where ownerId is null");
  $db->do("update $table set gownerId=(select gId from GRPS where Groupname='admin') where gownerId is null");
  $db->checkGUIDTable($table);
  
  $db->do("drop table ${table}_OLD");
}

print "New Changes made successfully by $kid!!!\n"; 

