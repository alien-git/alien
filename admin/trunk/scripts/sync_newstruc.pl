#!/bin/env alien-perl

use strict;
use Data::Dumper;
use AliEn::Database;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

### Get connections and DB objects
my $db = AliEn::Database::Catalogue->new({
                               ROLE   => "admin"}) or exit(-2);
$db->createCatalogueTables();
exit();
my $cpus=4;

### Get index table values for GUID and LFN
my $indexTable = $db->queryColumn("SELECT tableName FROM INDEXTABLE");
my $guidIndex  = $db->queryColumn("SELECT tableName FROM GUIDINDEX");
my $table;
my $chk=0;

print "\n".scalar(localtime(time))."\n";
  $db->do("INSERT IGNORE INTO USERS (username) (SELECT user from FQUOTAS)");
  $db->do("INSERT IGNORE INTO GRPS (groupname) (SELECT user from FQUOTAS)");

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
  $kid>$cpus and $kid=0;
}


print Dumper($splitLFN);

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
  next;
  if ($db->do("select count(*) from ${table}_OLD")){
   print "DONE SOMETHING\n";
   $db->do("drop table $table");
   $db->do("alter table ${table}_OLD rename $table");
  }
  $counter++;
  $db->do("ALTER TABLE $table rename ${table}_OLD");
  $db->checkLFNTableNoIndex($table);
  $db->do("INSERT INTO $table ( entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gownerid, type, guid, md5, ownerId, perm) 
   select  entryId, replicated, ctime, jobid,guidtime,lfn, broken, expiretime, size, dir, gid, type, guid, md5, uId, perm from ${table}_OLD old 
JOIN USERS ON old.owner=USERS.username JOIN GRPS ON old.gowner=GRPS.groupname ");
  $db->checkLFNTable($table);

  print scalar(localtime(time))."\n";
}
print "New Changes made successfully !!!\n";
print "\n".scalar(localtime(time))."\n";


print "Doing the alteration in G#L tables\n";
print "\n".scalar(localtime(time))."\n";

$counter=0;
foreach my $table (@{$splitLFN->{$kid}->{GUID}}) {
#  print "$table\n";
#  $update_time = $db->do("SELECT UPDATE_TIME FROM information_schema.tables WHERE table_schema='alice_users_sync' and table_name='$table'");
#  $update_status=$db->do("SELECT 1 FROM information_schema.tables WHERE table_schema='alice_users' AND table_name='$table' AND UPDATE_TIME>='$update_time'");
#  $count1=$db->do("SELECT count(*) FROM alice_users.$table");
#  $count2=$db->do("SELECT count(*) FROM alice_users_sync.$table");
#  ($update_status) or $update_status=0;
#  if($update_status==0 and $count1==$count2){
#    next;
#  }
  print "KID $kid doing $table  ($counter out of $#{$splitLFN->{$kid}->{GUID}})\n";
  $counter++;
  if ($db->do("select count(*) from ${table}_OLD")){
   print "DONE SOMETHING\n";
   $db->do("drop table $table");
   $db->do("alter table ${table}_OLD rename $table");
  }

  $db->do("alter table $table rename ${table}_OLD");

  $db->checkGUIDTable($table);
  $db->do("insert into $table ( guidId,ctime,ref,jobid,sestringlist, seautostringlist,expiretime,size,gownerid,guid,type, md5, ownerid, perm)
select  guidId,ctime,ref,jobid,sestringlist, seautostringlist,expiretime,size,gid,guid,type, md5, uid, perm from ${table}_OLD old left JOIN USERS on old.owner=USERS.username left JOIN GRPS on old.gowner=GRPS.groupname");

}
print "New Changes made successfully by $kid!!!\n";
