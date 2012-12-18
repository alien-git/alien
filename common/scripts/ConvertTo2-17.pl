use AliEn::Database::Catalogue;

use strict;
use Data::Dumper;

my $db=AliEn::Database::Catalogue->new(
				       {
					DB=>"alice_users", 
					HOST=>"aliendb9.cern.ch:3306",
					DRIVER=>"mysql", 
					PASSWD=>"XXXXXXXX",
					USE_PROXY=>0,
					#DEBUG=>2,
					ROLE=>"admin",}) or exit(-2);

print "Connected\n";



my $hosts=$db->{LFN_DB}->query("SELECT * from HOSTS");

print Dumper($hosts);


foreach my $h (@$hosts){
#  $h->{hostIndex}<8 and next;

  print "Ready to do $h->{db}\n";

#  fixLFNtables($h);
#  fixGUIDtables($h);
  optimizeTables($h);
  last;
}

sub optimizeTables{
  my $h=shift;
  my $index=$h->{hostIndex};

  my ($db2, $tabl)=$db->{GUID_DB}->reconnectToIndex($index,"", $h);
  my $allTables=$db2->queryColumn("show tables");
  for my $t (@$allTables){
    print "Optimizing $t\n";
    $db2->do("optimize table $t");
  }
}

sub fixGUIDtables{
  my $h=shift;

  checkTables($h, 'GUID_DB', "G", "GUIDINDEX", "checkGUIDTable");
  return 1;
}
sub fixLFNtables{
  my $h=shift;

  checkTables($h, 'LFN_DB', "L", "INDEXTABLE", undef, "set guidtime=binary2date(guid)");

  return 1;
}


sub checkTables{
  my $h=shift;
  my $type=shift;
  my $name=shift;
  my $indexTable=shift;
  my $method=shift || "checkLFNTable";
  my $extra=shift;


  my $index=$h->{hostIndex};
  my ($db2, $tabl)=$db->{$type}->reconnectToIndex($index,"", $h);

  $db2->createCatalogueTables();

  my $allTables=$db2->queryColumn("show tables like '$name\%L'");

  my $n=$db2->queryColumn("SELECT tableName from $indexTable where hostindex=?",undef,{bind_values=>[$index]});
  my $realTables={};
  foreach my $s (@$n){
    $realTables->{"$name${s}L"}=1;
  }

  foreach  my $t (@$allTables){
    $t=~ /^$name\d+L$/ or next;
    if (not $realTables->{$t}){
      print "The table $t is not in the index. Dropping it\n";
      $db2->do("DROP TABLE $t");
      next;
    }
    print "  Table $t \n";
    my $triggers=$db2->query("show triggers like ?", undef, {bind_values=>[$t]});
    foreach my $trigger (@$triggers){
      $trigger->{Timing} =~ /BEFORE/ or next;
      print "    Dropping trigger $trigger->{Trigger}\n";
      $db2->do("drop trigger $trigger->{Trigger}");
    }
    $db2->$method($t);

    print "Creating the broken field\n";
##    $db2->do("alter table $t add broken tinyint default 1") or next;
#    my $number=$t;
#    $number =~ s/L//g;
#    my $oldGUIDList=$db2->getPossibleGuidTables($number);
#    foreach my $elem (@$oldGUIDList){
#      my $gtable="$elem->{db}.G$elem->{tableName}L";
#      $db2->do("update $t l join $gtable using (guid) set broken=0 where l.guidtime>=(select left(guidtime,8) from GUIDINDEX where tablename=? and hostIndex=? )",{bind_values=>[ $elem->{tableName}, $elem->{hostIndex}]});
#    }
#    $db2->do("update $t set broken=0 where type='d'");
    $extra and $db2->do("update $t $extra");
    $db2->do("optimize table $t");
  }



}

