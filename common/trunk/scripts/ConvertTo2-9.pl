use AliEn::Database::Catalogue;

use strict;

my $db=AliEn::Database::Catalogue->new({DB=>"alien_system", 
					HOST=>"aliendb5.cern.ch:3307",
					DRIVER=>"mysql", 
					PASSWD=>"XI)B^nF7Ft", 
					USE_PROXY=>0,
					ROLE=>"admin",}) or exit(-2);

print "Connected\n";

print "First, let's make the tables case sensitive\n";
my $hosts=$db->query("SELECT hostIndex from HOSTS");
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



exit(-2);
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
#  $s->do("update D$host->{tableName}L set lfn=substring(lfn, $host->{l}) where lfn like '$host->{lfn}%'");
  $s->do("repair table  D$host->{tableName}L");

}
