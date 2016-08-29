package AliEn::Service::Optimizer::Job::Packages;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::Database::Catalogue;
use AliEn::GUID;
use AliEn::PackMan;
use Data::Dumper;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  $self->{SLEEP_PERIOD}=3600;

  $self->{CAT_DB} or $self->{CAT_DB}=AliEn::Database::Catalogue->new();
  $self->{CAT_DB} or $self->info("Error connecting to the catalogue") and return;


  if (!$self->{PACKMAN} && defined $self->{CONFIG}->{PACKMAN_ENVIRONMENT} && $self->{CONFIG}->{PACKMAN_ENVIRONMENT}=~/PACKOPT/i ){
    $self->{PACKTYPE} = (split '=',$self->{CONFIG}->{PACKMAN_ENVIRONMENT})[1];
    my $options={PACKMAN_METHOD=>$self->{PACKTYPE}};
    $self->info("Creating custom PackMan: $self->{PACKTYPE}");
    $self->{PACKMAN} = AliEn::PackMan->new($options);
    $self->{PACKMAN} or 
      $self->info("Error creating PackMan/$self->{PACKTYPE}") and return;
  }

  my $method="info";
#  $silent and $method="debug";
#  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
#  $self->{INSERTING_COUNTING}++;
#  if ($self->{INSERTING_COUNTING}>10){
#    $self->{INSERTING_COUNTING}=0;
#  }else {
#    $method="debug";
#  }

  $self->{LOGGER}->$method("Packages", "The packages optimizer starts");

  my $agents=$self->{DB}->query("select count(1) c, sum(counter) total , packages from JOBAGENT group by packages");
  $agents or $self->info("ERROR asking for the JOBAGENTS") and return;
  my @allPackages=$self->loadPackages() or $self->info("Error getting the list of packages") and return;
  
  foreach my $agent (@$agents){
      $self->info("Checking if the packages '$agent->{packages}' still exist");
      my @packages=split(/,/, $agent->{packages});
      my $exists=1;
      foreach my $package (@packages){
         $package=~ /%/ and next;
         $self->existsPackage($package, @allPackages) or $exists=0;
         $exists or last;
      }
      if (! $exists){
         $self->info("We should remove all the jobagents ($agent->{c} and $agent->{total})\n");
         $self->moveToError($agent->{packages});
      }

  }
  $self->info("We can go back to sleep");
  return;

}

sub moveToError {
  my $self=shift;
  my $req=shift;

  my $waitingStatusId=5;
  my $jobs=$self->{DB}->queryColumn("SELECT queueid from QUEUE join JOBAGENT on (agentid=entryid) where packages=? and statusid=$waitingStatusId", undef, {bind_values=>[$req]});

  foreach my $queueid (@$jobs){
    $self->info("We have to kill the job $queueid");
    $self->{DB}->updateStatus($queueid, "WAITING", "ERROR_E");
    $self->putJobLog($queueid, "state", "Job move to ERROR_E: one of the packages '$req' does not exist");
  }


}


sub loadPackages {
  my $self=shift;
  $self->info("We have to load all the packages");
  my $allPackages={};
  my $status;
  my @infopack;
  my $infodb;
  my @info;
  
  # Getting packages
  $infodb=$self->{CAT_DB}->{LFN_DB}->queryColumn("SELECT distinct fullPackageName p from PACKAGES");
  @info = @{$infodb};
  
  if($self->{PACKMAN}){
  	 $self->info("Getting packages from special PackMan ($self->{PACKTYPE})");
    ($status,@infopack) = $self->{PACKMAN}->getListPackages("-s");
    $status and @infopack and scalar(@infopack)>10 
      or $self->info("Couldn't retrieve packages from $self->{PACKTYPE}") and last; 
    
    $self->info("We have a special PackMan ($self->{PACKTYPE}), showing diffs");    
    my @missing;
    foreach my $dbpackage (@info){
      $dbpackage ~~ @infopack or push(@missing,$dbpackage);
    }
    @missing and
      $self->info("Packages missing in $self->{PACKTYPE}\n".Dumper(@missing));
         
    @info = @infopack;
  }

  @info or $self->info("Error getting the list of packages") and return;
  print "RETURNING LOADED: ".Dumper(@info);
#  foreach my $d (@info){
#    $self->debug(1, "The package $d exists");
#    $allPackages->{lc($d)}=1;
#  }
  $self->info("List of packages loaded");

  return @info; #$allpackages
}


sub existsPackage {
  my $self=shift;
  my $package=shift;
  my @allPackages=@_;
  
  $self->debug(1,"Checking if the package '$package' exists");
#  $allPackages->{lc($package)} and return 1;

  $package =~ /@/  or $package = ".*\@$package";
  $package =~ /::/ or $package = "${package}::.*";

  my @name = grep (/^$package.*$/i, @allPackages);
  
  scalar(@name) and return 1;  

  $self->info("The package '$package' does not exist!!");
  return 0;
}

1;



