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


  my @optimizers=("Trigger", "Expired" ,"Deleted", "Packages", "SEsize", "LDAP");

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


  $self->info( "Checking if there is anything to do");
  
  return;

}


return 1;

