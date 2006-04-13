package AliEn::Service::Optimizer::Catalogue;

use strict;

use vars qw (@ISA);


use AliEn::Service::Optimizer;
use AliEn::Database::Catalogue;
use AliEn::Database::SE;
use AliEn::GUID;

#use AliEn::UI::Catalogue::LCM;
@ISA=qw(AliEn::Service::Optimizer);

use Data::Dumper;

my $self;


sub initialize {
  $self=shift;
  my $options =(shift or {});

  $self->{SERVICE}="Catalogue";

  $self->{DB_MODULE}="AliEn::Database::Catalogue";

  $self->SUPER::initialize(@_) or return;

  $self->{GUID}=new AliEn::GUID or return;


  return $self;
}

sub checkDTables {
  my $self=shift;
  
  $self->info("Let's check all the D tables");
  my $hosts=$self->{DB}->getAllHosts("address,db,driver");

  defined $hosts
    or $self->{LOGGER}->error("CatOpt","Error fetching all hosts from database")
      and return;
  foreach my $host (@$hosts){
    #my ($address, $db, $driver)=split ("###", $host);
    $self->info("Checking the d tables in $host->{address}, $host->{driver}, $host->{db}");
    $self->{DB}->reconnect($host->{address}, $host->{db}, $host->{driver}) or
      $self->info("Error connecting to $host->{address}, $host->{driver}, $host->{db}")
	and next;
    $self->info("Updating D tables");
    my $tables=$self->{DB}->queryColumn("show tables like 'D\%L'");
    foreach my $table (@$tables){
      $table =~ /^D[0-9]+L$/ or $self->info("Ignoring $table") and next;
      $self->info("Checking  $table ");
      $self->{DB}->checkDLTable($table) or $self->info("PROBLEMS!!!!");
    }
  }

  return 1;
}
sub checkWakesUp {
  my $this=shift;
  my $silent=shift;
  
  my $method="info";
  $silent and $method="debug";


  if (!$self->{DONE}) {
    $self->info("Let's check all the catalogue tables");
    $self->checkDTables();
    $self->{DONE}=1;
  }
  $self->{LOGGER}->$method("CatOpt", "Checking if there is anything to do");

  $self->checkHostsTable($silent);

  my $done2=$self->checkGUID($silent);

	#    my $done=$self->checkWaitingJobs($silent);
  $self->checkDeletedEntries();

  $self->{LOGGER}->$method("CatOpt", "Going back to sleep");

  (-f "$self->{CONFIG}->{TMP_DIR}/AliEn_TEST_SYSTEM") or
    $self->info("Sleeping for a looooooonnnngggg time") and sleep(2*3600);
  return;
}
#
# This subroutine looks for all the entries in the SE database that are not
# pointed at anymore from the catalogue
#
#
#
sub checkDeletedEntries {
  my $self=shift;
  $self->info("Checking the entries from the local databases that do not exist anymore in the central catalogue");

  my $databases=$self->{DB}->queryColumn("SHOW DATABASES");

  foreach (@$databases){
    print "Lets se $_\n";
    /^se_/ or next;
    /^se_alice_2005/ and next;

    $self->checkSEdatabase($_) or exit(-2);
  }
#  $self->info("Sleeping for two hours");
#  sleep (2*3600);
  return 1;


}

sub checkSEdatabase {
  my $self=shift;
  my $seName=shift;

  my $se=AliEn::Database::SE->new({DB=>$seName,
				   HOST=>$self->{DB}->{HOST},
				   DRIVER=>$self->{DB}->{DRIVER},
				  }) or exit();
  $self->info("Connected to $seName");

  $seName=~ s/^se_//;
  $seName=~ s/_/::/g;
  my $seNumber=$self->{DB}->getSENumber($seName)
    or $self->info("Error getting the number of the SE $seName") and return;

  my $dbs=$self->{DB}->getFieldsFromHostsEx() or return;

  $se->do("TRUNCATE FILES2");
  foreach   my $db (@$dbs) {
    $self->info("Reconnecting to $db->{address}, $db->{db}, $db->{driver}");
    $self->{DB}->reconnect($db->{address}, $db->{db}, $db->{driver}) or 
      $self->info("Error reconnecting") and return;
    
    my $tables=$self->{DB}->queryColumn("SELECT tableName from INDEXTABLE where hostIndex=$db->{hostIndex}") or return;
    foreach my $table (@$tables){
      my $name="$db->{db}.D${table}L";
      $se->do("INSERT IGNORE INTO FILES2 SELECT guid FROM $name where seStringlist like '%,$seNumber,%'") or return;
    }
  }
  $se->do("TRUNCATE TODELETE");
  $se->do("INSERT IGNORE INTO TODELETE(guid,pfn) select f.guid, f.pfn from FILES f left join  FILES2 f2 on f.guid=f2.guid where f2.guid is null");
  $se->do("INSERT IGNORE INTO BROKENLINKS(guid) select f2.guid from FILES f right join  FILES2 f2 on f.guid=f2.guid where f.guid is null");

  $self->info("DATABASE FINISHED!!");
  return 1;
}


sub checkHostsTable {
  my $self=shift;
  my $silent=shift;
  my $method="info";
  $silent and $method="debug";

  my $hosts=$self->{DB}->getAllHosts("address,db,driver");

  defined $hosts
    or $self->{LOGGER}->error("CatOpt","Error fetching all hosts from database")
      and return;

  foreach my $host (@$hosts){
    #my ($address, $db, $driver)=split ("###", $host);
    $self->info("Checking the host  $host->{address}, $host->{driver}, $host->{db}");
    $self->{DB}->reconnect($host->{address}, $host->{db}, $host->{driver}) or
      $self->info("Error connecting to $host->{address}, $host->{driver}, $host->{db}")
	and next;
    $self->info("Updating HOSTS table");
    $self->{DB}->checkHostsTable;
  }
}

sub checkGUID {
  my $self=shift;
  my $silent=shift;
  my $doit = 1;
  my $method="info";
  $silent and $method="debug";

  $self->{LOGGER}->$method("CatOpt", "Checking if all the entries have guid");

  my $hosts=$self->{DB}->getFieldsFromHostsEx("address,db,driver,hostIndex","WHERE organisation is NULL");

  defined $hosts
    or $self->debug(1,"Error fetching hosts from database")
      and return;

  foreach my $host (@$hosts){
    $doit = 1;
    #my ($address, $db, $driver)=split ("###", $host);
    $self->{LOGGER}->$method("CatOpt", "Checking the host $host->{address}, $host->{driver}, $host->{db}");
    $self->{DB}->reconnect($host->{address}, $host->{db}, $host->{driver}) or
      $self->info( "Error connecting to $host->{address}, $host->{driver}, $host->{db}")
	and next;
    $self->info("Getting all the tables of this host");

    my $tablesRef=$self->{DB}->queryColumn("SELECT tableName from INDEXTABLE where hostIndex=$host->{hostIndex}");

    foreach my $table (@$tablesRef){
      my $tableName="D${table}L";
      my $entries=$self->{DB}->queryColumn("SELECT entryId FROM $tableName WHERE ( (guid is NULL) or (guid = '') ) and lfn not like '%/' and lfn not like '' limit 10000");

      defined $entries
	or $self->debug(1,"Error fetching entries from D0")
	  and return;
      $#{$entries}>-1 or next;
      $self->info("Updating ". $#{$entries}." entries");
      my $i=1;
      foreach my $entry (@$entries) {
	my $guid=$self->{GUID}->CreateGuid() or next;
	$self->info("Setting $entry and $guid");

	$self->{DB}->update($tableName, {guid=>"string2binary(\"$guid\")"},
			    "entryId=$entry", {noquotes=>1});
	$i++;
	($i %100) or   $self->info("Already checked $i files");
      }
    }

    $self->info("Finished!!");
  }

  return ;
}
return 1;

