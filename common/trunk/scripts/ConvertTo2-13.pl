use AliEn::Database::Catalogue;

use strict;

my $db=AliEn::Database::Catalogue->new(
				       {
#					DB=>"alien_system", 
#					HOST=>"a.cern.ch:3307",
#					DRIVER=>"mysql", 
					PASSWD=>"pass", 
					USE_PROXY=>0,
					ROLE=>"admin",}) or exit(-2);

print "Connected\n";



my $hosts=$db->{LFN_DB}->queryColumn("SELECT hostIndex from HOSTS");


print "First, let's create the functions\n";
#createFunctions($db, $hosts) or exit(-2);

print "OK, let's split the GUID table\n";
my $entriesPerGUIDTable=100;
#splitGUIDTable($db, $entriesPerGUIDTable) or exit(-2);

print "Now, lets put all the info of all the guid tables\n";
fillGUIDpermissions($db , $hosts);

print "Finally, let's populate also all the pfns\n";
#populatePFNS($db);

sub populatePFNS {
  my $db=shift;
  my $ses=$db->{LFN_DB}->queryColumn("show databases");

  my $maxguid=$db->{LFN_DB}->queryValue("SELECT max(tableName) from GUIDINDEX");


  foreach my $dbName (@$ses){
    my $s=$dbName;
    $s =~ s/^se_// or next;
    $s =~ s/_/::/ or next;

    my $seNumber=$db->{LFN_DB}->queryColumn("select seNumber from SE where se='$s'");
    print "Doing the database $s ($seNumber)\n";
    for (my $guid=0; $guid<=$maxguid; $guid++){
      my $g="alien_system.G${guid}";

      $seNumber->do("insert into ${g}_PFN g (pfn, guidId, seNumber) select pfn,guidId, $seNumber from $g g, $dbName.FILES f where f.guid=g.guid");
      $seNumber->do("update $g g set seStringList=concat(seStringList,'$seNumber,')");
    }

  }

}

print "YUHUUU!!\n";

sub fillGUIDpermissions {
  my $db=shift;
  my $hosts=shift;

  my $maxguid=$db->{LFN_DB}->queryValue("SELECT max(tableName) from GUIDINDEX");
  print ("We have to look up to guid table $maxguid");
  foreach my $index (@$hosts){
    my ($lfndb, $table)=$db->{LFN_DB}->reconnectToIndex($index);
    my $tables=$lfndb->queryColumn("show tables");
    foreach my $lfnTable (@$tables){
      $lfnTable =~ /^D\d+L$/ or next;
      for (my $guid=0; $guid<=$maxguid; $guid++){
	$lfndb->do("update alien_system.G${guid}L g, $lfnTable l set g.owner=l.owner, g.ctime=l.ctime, ref=ref+1, g.size=l.size, g.gowner=l.gowner, g.md5=l.md5,g.perm=l.perm  where g.guid=l.guid");
      }
      $lfndb->do("alter table $lfnTable drop column seStringList");
      $lfndb->do("alter table $lfnTable drop column md5");
      my $newName=$lfnTable;
      $newName =~ s/^D/L/;
      $lfndb->do("rename table $lfnTable to $newName");
      
      print "   Table $lfnTable done!!\n";
    }

  }
  print "GUID PERMISSIONS FINISHED!!!! :)\n";

}

sub createFunctions{
  my $db=shift;
  my $hosts=shift;
  my ($todo, $done)=(0,0);
  foreach my $entry (@$hosts){
    print "Checking in $entry\n";
    $todo++;
    my $dd=$db->reconnectToIndex($entry) or print "Error reconnecting to $entry\n" and last;
    $dd->do("create function string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))");
    $dd->do("create function binary2string (my_uuid binary(16)) returns varchar(36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')");


    $dd->do("create function string2date (my_uuid varchar(36)) returns char(16) deterministic sql security invoker return upper(concat(right(left(my_uuid,18),4), right(left(my_uuid,13),4),left(my_uuid,8)))");

    $dd->do("create function binary2date (my_uuid binary(16))  returns char(16) deterministic sql security invoker
return upper(concat(right(left(hex(my_uuid),16),4), right(left(hex(my_uuid),12),4),left(hex(my_uuid),8)))");
    $done++;
  }
  if ($todo<$done){
    print "Error: we were supposed  to do $todo, but only did $done\n";
    return;
  }
  return 1;
}

sub splitGUIDTable {
  my $info=shift;
  my $db=$info->{GUID_DB};
  my $maxEntries=shift;
  print "Creating the index\n";
  $db->createCatalogueTables() or exit(-2);
  my $table=0;
  my $error=0;

  $db->do(" alter table GUID add column (guidTime char(16))");
  $db->do("update GUID set guidTime=binary2date(guid)");
  $db->do("alter table GUID add index (guidTime)");
  $db->do("TRUNCATE GUIDINDEX");
  $db->do("INSERT INTO GUIDINDEX(hostIndex, guidTime,tableName) values  ('1','', $table)") or return;

  while(1) {
    my $time=$db->queryValue("select guidTime from GUID order by 1 limit $maxEntries,1") or last;

    my @stmts=("INSERT INTO G${table}L(guid,ref)  select guid,0 from GUID where guidTime<'$time'",
	       "DELETE FROM  GUID where guidTime<'$time'",
	       "INSERT INTO GUIDINDEX(hostIndex, guidTime, tableName) values ('1', '$time', $table+1)",
	      );
    $db->checkGUIDTable($table) or print "Error checking the table" and last;
    $table++;
    foreach my $stmt(@stmts){
      if (!$db->do($stmt)) {
	$error=1;
	print "Error doing $stmt\n";
	last;
      }
    }
    print "table finished \n\n";
    $error and last;

  }
  $error and print "ERROR :(" and return;
  
  #Ok, let's do the last update
  print "Doing the last update\n";
  $db->checkGUIDTable($table);
  my @stmts=( "INSERT INTO G${table}L (guid,ref) select guid,0 from GUID", "DELETE FROM GUID");
  foreach my $stmt(@stmts){
    if (!$db->do($stmt)) {
      $error=1;
      print "Error doing $stmt\n";
      last;
    }
  }

  $error and print "ERROR :(" and return;

  print "Done!!\n";
}

sub grantPersmissions{
  my $db=shift;
  my $users=$db->{LFN_DB}->queryColumn("select username from ADMIN.TOKENS");

  foreach my $user (@$users){

    my $c=$db->{LFN_DB}->queryColumn("show grants for $user");
    foreach my $f (@$c){
      $f =~ s/`D(\d+L)/`L$1/ or next;
      print "Got $f\n";;
      $db->{LFN_DB}->do($f);
      $f=~ s/grant/revoke/i;;
      $f=~ s/ to / from /i;
      $f =~ s/`L(\d+L)/`D$1/ or next;
      print "AND $f\n";
      $db->{LFN_DB}->do($f);
    }
    for(my $i=0;$i<26; $i++){
      $db->{LFN_DB}->do("grant insert,delete,update on G${i}L to $user");
      $db->{LFN_DB}->do("grant insert,delete,update on G${i}L_PFN to $user");
    }

  }

}
