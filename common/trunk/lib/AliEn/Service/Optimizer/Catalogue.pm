package AliEn::Service::Optimizer::Catalogue;

use strict;

use vars qw (@ISA);


use AliEn::Service::Optimizer;
use AliEn::Database::Catalogue;
use AliEn::Database::SE;
use AliEn::GUID;

use AliEn::UI::Catalogue::LCM;
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
  $options->{ROLE}=$options->{role}="admin";

  $self->{CATALOGUE} = AliEn::UI::Catalogue::LCM->new($options);

  ( $self->{CATALOGUE} )
    or $self->{LOGGER}->error( "JobOptimizer", "In initialize error creating AliEn::UI::Catalogue::LCM instance" )
      and return;


  my @optimizers=("SE","Trigger", "Expired" ,"Deleted", "Packages", "SEsize");
#  @optimizers=("SEsize");
  $self->StartChildren(@optimizers) or return;

  return $self;
}

sub checkLTables {
  my $self=shift;
  my $silent=shift;
  my $db=shift;

  $self->info("Let's check all the L tables");

  $self->info("Updating D tables");
  my $tables=$db->queryColumn("show tables like 'L\%L'");
  foreach my $table (@$tables){
    $table =~ /^L[0-9]+L$/ or $self->info("Ignoring $table") and next;
    $self->info("Checking  $table ");
    $db->checkDLTable($table) or $self->info("PROBLEMS!!!!");
  }

  return 1;
}
sub checkWakesUp {
  my $this=shift;
  my $silent=shift;
  
  my $method="info";
  $silent and $method="debug";


  $self->{LOGGER}->$method("CatOpt", "Checking if there is anything to do");


  my ($hosts) = $self->{DB}->getAllHosts();
  defined $hosts
    or return;
  foreach my $tempHost (@$hosts) {
    my ($db, $Path2)=$self->{DB}->{LFN_DB}->reconnectToIndex( $tempHost->{hostIndex},"",$tempHost) or $self->info("Error doing $tempHost->{db}") and next;;
    $self->info("Doing $tempHost->{db}");

    $self->checkGUID($silent, $db);

    if (!$self->{DONE}) {
      $self->info("Let's check all the catalogue tables");
      $self->checkLTables($silent, $db);
      $self->{DONE}=1;
    }

  }

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

sub checkGUID {
  my $self=shift;
  my $silent=shift;
  my $method="info";
  $silent and $method="debug";
  my $db=shift;

  $self->info("Getting all the tables of this host");

  my $tablesRef=$db->queryColumn("SELECT tableName from INDEXTABLE where hostIndex=?", undef, {bind_values=>[$db->{CURHOSTID}]});
  
  foreach my $table (@$tablesRef){
    my $tableName="D${table}L";
    my $entries=$db->queryColumn("SELECT entryId FROM $tableName WHERE ( (guid is NULL) or (guid = '') ) and lfn not like '%/' and lfn not like '' limit 10000");
    
    defined $entries
      or $self->debug(1,"Error fetching entries from D0")
	and return;
    $#{$entries}>-1 or next;
    $self->info("Updating ". $#{$entries}." entries");
    my $i=1;
    foreach my $entry (@$entries) {
      my $guid=$self->{GUID}->CreateGuid() or next;
      $self->info("Setting $entry and $guid");

      $db->update($tableName, {guid=>"string2binary(\"$guid\")"},
		  "entryId=?", {noquotes=>1, bind_values=>[$entry]});
      $i++;
      ($i %100) or   $self->info("Already checked $i files");
    }
  }
  $self->info("Finished!!");
  return ;
}

return 1;

