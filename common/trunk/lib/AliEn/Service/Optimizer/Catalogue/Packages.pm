package AliEn::Service::Optimizer::Catalogue::Packages;
 
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

  $self->$method(@info, "The packages optimizer starts");

  $self->{SLEEP_PERIOD}=10;
  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='PACKAGES'");
  $todo or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='PACKAGES'");

  my $Fsilent="";
  my @userPackages=$self->{CATALOGUE}->execute("find", $Fsilent, $self->{CONFIG}->{USER_DIR}, "/packages/*");
  my @voPackages=$self->{CATALOGUE}->execute("find", $Fsilent, "\L/$self->{CONFIG}->{ORG_NAME}/packages", "*");
  my @packages;
  my $org="\L$self->{CONFIG}->{ORG_NAME}\E";
  foreach my $pack (@userPackages, @voPackages) {
    $self->debug(2,  "FOUND $pack");
    if ($pack =~ m{^$self->{CONFIG}->{USER_DIR}/?./([^/]*)/packages/([^/]*)/([^/]*)/([^/]*)$}) {
      push @packages,{'fullPackageName'=> "$1\@${2}::$3",
		 packageName=>$2,
		 username=>$1, 
		 packageVersion=>$3,
		 platform=>$4,};
    }elsif ($pack =~ m{^/$org/packages/([^/]*)/([^/]*)/([^/]*)$}) {
      push @packages,{'fullPackageName'=> "VO_\U$org\E\@${1}::$2",
		 packageName=>$1,
		 username=>"VO_\U$org\E", 
		 packageVersion=>$2,
		 platform=>$3,};
    }else {
      $self->info("Don't know what to do with $pack");
    }

  }
  $self->info("READY TO INSERT @packages\n");
  $self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->lock('PACKAGES');
  $self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->delete('PACKAGES', "1");
  $self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->multiinsert('PACKAGES', \@packages,);
  $self->{CATALOGUE}->{CATALOG}->{DATABASE_FIRST}->unlock();

  return 1;
}



return 1;
