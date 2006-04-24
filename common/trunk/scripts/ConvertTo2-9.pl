use AliEn::Database::Catalogue;

use strict;

my $db=AliEn::Database::Catalogue->new({DB=>"alien_system", 
					HOST=>"aliendb5.cern.ch:3307",
					DRIVER=>"mysql", 
					PASSWD=>"<putpassword>", 
					USE_PROXY=>0,
					ROLE=>"admin",}) or exit(-2);

print "Connected\n";
my $caseSensitive=1;
my $fixPath=1;
my $fixGUID=1;


my $hosts=$db->query("SELECT hostIndex from HOSTS");

if ($caseSensitive){
  print "First, let's make the tables case sensitive\n";
  foreach my $host (@$hosts){
    print "Reconnecting to $host->{hostIndex}\n";
    $db=$db->reconnectToIndex($host->{hostIndex}) or 
      print "Error reconnecting\n" and exit(-2);
    
    my $tables=$db->queryColumn("SHOW TABLES");
    foreach my $table (@$tables){
      my $query="alter table $table CONVERT TO CHARACTER set   latin1 COLLATE latin1_general_cs";
      print "\tDoing $query\n";
      $db->do($query);
      
    }
  }
}



if ($fixPath) {
  my $tables=$db->query("SELECT tableName, hostIndex, count(*) as total from INDEXTABLE group by  tableName having total>1");
  
  foreach my $table (@$tables) {
    print "We have to fix table $table->{tableName}\n";
    my $lfns=$db->queryColumn("SELECT lfn from INDEXTABLE where tableName=$table->{tableName} and hostIndex=$table->{hostIndex}");
    my $first=1;
    foreach my $lfn (@$lfns){
      if ($first){
	print "\tSkipping $lfn\n";
	$first=0;
    }else {
      print "\tMoving $lfn to another table\n";
    }
    }
    
  }
  
  print "Ok, now let's remove the beginning of the entries\n";
  
  $hosts=$db->query("SELECT tableName, hostIndex,length(lfn)+1 as l, lfn from  INDEXTABLE");
  
  foreach my $host (@$hosts){
    print "Fixing table $host->{lfn}\n";
    my $s=$db->reconnectToIndex($host->{hostIndex}) or print "Error reconnecting to $host->{hostIndex}\n" and next;
    $s->do("update D$host->{tableName}L set lfn=substring(lfn, $host->{l}) where lfn like '$host->{lfn}%'");
    $s->do("repair table  D$host->{tableName}L");
    
  }
}

if ($fixGUID){
  print "Finally, let's fix the guid\n";
  foreach my $host (@$hosts){
    next;
    print "Reconnecting to $host->{hostIndex}\n";
    $db=$db->reconnectToIndex($host->{hostIndex}) or 
      print "Error reconnecting\n" and exit(-2);
#    $db->do("create function string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))");
#    $db->do("create function binary2string (my_uuid binary(16)) returns varchar(36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')");
    my $tables=$db->queryColumn("SHOW TABLES");
    foreach my $table (@$tables){
      $table=~ /^D[0-9]+L$/ or next;
      
      my $info=$db->query("describe $table");
      my $type="";
      my $done=1;
      foreach my $entry( @$info) {
	$entry->{Field} eq "guid" and $type=$entry->{Type};
	$entry->{Field} eq "guidString" and $done=0;
      }
      $type=~ /^binary/ and $done and 
	print "\t\tTable $table has been done\n" and next;

      my @queries=(
#		   "alter table $table change column guid guidString varchar(36)",
#		   "alter table $table add column guid binary(16)",
		   "update $table  set guid=string2binary(guidString) where guid is null",
		   "alter table $table drop column guidString",
		   "create index ${table}_guid on $table (guid)",
		   "repair table $table");
      my $ok=1;
      foreach my $query(@queries){
	if ($ok) {
	  print "\tDoing $query\n";
	  $db->do($query) or $ok=0;;
	}
      }

    }
  }

  print "and also the se databases...\n";
  my $alldb=$db->queryColumn("show databases like 'se_%'");
  foreach my $dbName (@$alldb){
#    $db->do("create function $dbName.string2binary (my_uuid varchar(36)) returns binary(16) deterministic sql security invoker return unhex(replace(my_uuid, '-', ''))") or next;
#    $db->do("create function $dbName.binary2string (my_uuid binary(16)) returns varchar(36) deterministic sql security invoker return insert(insert(insert(insert(hex(my_uuid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')") or next;
    print "In $dbName\n";
    foreach my $table ("FILES", "TODELETE", "BROKENLINKS", "FILES2",){
      my @queries=(
		   "alter table $dbName.$table change column guid guidString varchar(36)",
		   "alter table  $dbName.$table add column guid binary(16)",
		   "update  $dbName.$table  set guid=string2binary(guidString) where guid is null",
		   "alter table  $dbName.$table drop column guidString",
		   "repair table  $dbName.$table",
		  );

      my $info=$db->query("describe $dbName.$table");
      my $type="";
      my $string=1;
      foreach my $entry( @$info) {
	$entry->{Field} eq "guid" and $type=$entry->{Type};
	$entry->{Field} eq "guidString" and $string=0;
      }
      ($type=~ /^binary/) and $string and 
	print "\t\tTable $table has been done\n" and next;
      my $ok=1;
      foreach my $query(@queries){
	if ($ok){
	  print "\tDoing $query\n";
	  $db->do($query) or $ok=0;
	}
      }
    }
  }
}


sub populateGUID{
  my $db=shift;

  $db->do("TRUNCATE GUID");
  my $hosts=$db->query("SELECT hostIndex from HOSTS");
  foreach my $host (@$hosts){
    print "Reconnecting to $host->{hostIndex}\n";
    $db=$db->reconnectToIndex($host->{hostIndex}) or 
      print "Error reconnecting\n" and exit(-2);
    
    my $tables=$db->queryColumn("SHOW TABLES");
    foreach my $table (@$tables){
      $table =~ /^D[0-9]+L$/ or next;
      print "doing table $table\n";
      my $tableName="$host->{hostIndex}_$table";
      my @queries=(
		   "UPDATE alien_system.GUID g, $table t  set g.lfn=concat(g.lfn,'$tableName,') where g.guid=t.guid ",
		   "INSERT IGNORE INTO alien_system.GUID(guid, lfn) select t.guid,',$tableName,' from $table t  where guid is not null and t.lfn not like '%/'",);
      foreach my $query(@queries) {
	print "\tDoing $query\n";
	$db->do($query);
      }
    }
  }

}
