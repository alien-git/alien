package AliEn::Service::Optimizer::Catalogue::SEsize;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  $self->$method(@info, "The SE optimizer starts");
  (-f "$self->{CONFIG}->{TMP_DIR}/AliEn_TEST_SYSTEM")  or $self->{SLEEP_PERIOD}=3600;
  my $dbs=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->queryColumn("show databases like 'se_%'");
  my $guiddb=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{GUID_DB};

  my $guids=$guiddb->query("select * froM GUIDINDEX");
  foreach my $f (@$guids){
    $self->info("Checking the table $f->{tableName}");
    my ($db2, $path2)=$guiddb->reconnectToIndex( $f->{hostIndex}) or next; 
    $db2->checkGUIDTable($f->{tableName});
    $db2->updateStatistics($f->{tableName});
  }
  my $hosts=$guiddb->query("select distinct hostIndex from  GUIDINDEX");
  foreach my $db (@$dbs){
    $self->info("Calculating the size of $db");
    use Data::Dumper;
    print Dumper($db);
    $self->checkSESize($db, $hosts);
  }

  return;
}


sub checkSESize{
  my $self=shift;
  my $db=shift;
  my $guids=shift;

  $db=~ /^se_([^_]*)_([^_]*)_(.*)$/ or $self->info("Error parsing '$db'") and return;
  my $dbName="${1}::${2}::$3";
  
  my $guiddb=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{GUID_DB};
  my $index=$guiddb->getSENumber($dbName, {existing=>1});
  $index or $self->info("Error getting the index number of $dbName") and return;
  $self->debug(1, "Hello $dbName and $index");
  my $size=0;
  my $counter=0;

  foreach my $guid (@$guids){
    my ($db2, $path2)=$guiddb->reconnectToIndex( $guid->{hostIndex}) 
      or $self->info("Error reconnecting") and next;

    
    my $info=$db2->queryRow("select sum(seNumFiles) total, sum(seUsedSpace) size from GL_STATS where seNumber=? group by seNumber", undef, {bind_values=>[$index]}) 
      or $self->info("Error doing the query") and next;
    
    $info->{size} and $size+=$info->{size};
    $info->{total} and $counter+=$info->{total};
    
  }
  $self->info("The SE has $counter files and $size bytes");
  $guiddb->update("SE", {seNumFiles=>$counter, seUsedSpace=>$size}, "seNumber = ? ", {bind_values=>[$index]});
  return 1;
}
1;
