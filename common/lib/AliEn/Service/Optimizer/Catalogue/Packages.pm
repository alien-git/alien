package AliEn::Service::Optimizer::Catalogue::Packages;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;
use Data::Dumper;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
#  $silent and $method="debug" and  @info=1;

  $self->$method(@info, "The packages optimizer starts");
  
#  $self->{NDH} or 
#    $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$self->{DDB},HOST=> $self->{HOSTDB},DRIVER => $self->{DRIVERDB},ROLE=>'admin'}) 
#    and $self->info("Created new DBH") 
#    and $self->{NDH}=1;
#  
#  $self->{DB}->{LFN_DB} or $self->info("We don't have a handler!") and return;
#  $self->{CATALOGUE} or $self->info("We don't have a catalogue!") and return;
  
  $self->{CATALOGUE}->{CONFIG}->{CACHE_SERVICE_ADDRESS}="";
  
  $self->{MONITOR} and 
    $self->{MONITOR}->sendParameters("$self->{CONFIG}->{SITE}_Optimizer_Packages", "$self->{HOST}", { "pid" => $$ });
  
  $self->{SLEEP_PERIOD}=10;
  
  my $todo=$self->{DB}->{LFN_DB}->queryValue("SELECT todo from ACTIONS where action='PACKAGES'");
  $todo or $self->info("Nothing to do") and return;

#  my $Fsilent="";
 # my @userPackages=$self->{CATALOGUE}->execute("find", $Fsilent, $self->{CONFIG}->{USER_DIR}, "/packages/*");
  $self->info("Going to find \L/$self->{CONFIG}->{ORG_NAME}/packages *");
  
  my @voPackages;
  eval {
	  $self->{CATALOGUE}->setDebug(5);
	  @voPackages=$self->{CATALOGUE}->execute("find", "\L/$self->{CONFIG}->{ORG_NAME}/packages", "*");
	  $self->{CATALOGUE}->setDebug(0);
  };
  if ($@){
  	$self->info("The find failed: ".Dumper($@));
  	return;
  }
  
  scalar(@voPackages) or 
    $self->info("We have an empty result, returning") and return;
  
  my @packages;
  my $org="\L$self->{CONFIG}->{ORG_NAME}\E";
  
  $self->info("Going to iterate packages of $org and ".scalar(@voPackages));
  
  foreach my $pack ( @voPackages) {
    $self->debug(2,  "FOUND $pack");
    if ($pack =~ m{^$self->{CONFIG}->{USER_DIR}/?./([^/]*)/packages/([^/]*)/([^/]*)/([^/]*)$}) {
      push @packages,{'fullPackageName'=> "$1\@${2}::$3",
		      packageName=>$2,
		      username=>$1, 
		      packageVersion=>$3,
		      platform=>$4,
		      lfn=>$pack};
    }elsif ($pack =~ m{^/$org/packages/([^/]*)/([^/]*)/([^/]*)$}) {
      push @packages,{'fullPackageName'=> "VO_\U$org\E\@${1}::$2",
		      packageName=>$1,
		      username=>"VO_\U$org\E", 
		      packageVersion=>$2,
		      platform=>$3,
		      lfn=>$pack};
    }else {
      $self->info("Don't know what to do with $pack");
    }

  }
  $self->info("READY TO INSERT");#.Dumper(@packages));
  $self->{DB}->{LFN_DB}->lock('PACKAGES');
  $self->{DB}->{LFN_DB}->delete('PACKAGES', "1");
  @packages and 
  $self->{DB}->{LFN_DB}->multiinsert('PACKAGES', \@packages,);
  $self->{DB}->{LFN_DB}->unlock();

  $self->info("Insertion done.");

  $self->info("Updating ACTIONS on PACKAGES (to 0)");
  $self->{DB}->{LFN_DB}->update("ACTIONS", {todo=>0}, "action='PACKAGES'");
    
  $self->info("Returning...");

  return 1;
}



return 1;
