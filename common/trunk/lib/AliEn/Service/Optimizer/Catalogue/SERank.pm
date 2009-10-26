package AliEn::Service::Optimizer::Catalogue::SERank;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;
use LWP::UserAgent;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  $self->$method(@info, "The SE Rank optimizer starts");
  $self->{SLEEP_PERIOD}=7200;

  my $catalogue=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};

  my $sites = $catalogue->queryColumn("select distinct sitename from SERanks");

  $self->updateRanksForSites($sites,$silent);

  $self->info("Going back to sleep");
  return;
}


sub updateRanksForSites{
  my $self=shift;
  my $sites=(shift|| return 0);
  my $silent=(shift || 1);
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  my @updatedSites = ();

  $self->info("Going to handle an updateRanksForSites request for sites: @$sites");

  foreach my $site (@$sites) {

    AliEn::Service::Optimizer::Catalogue::SERank::processUpdateRanksForSite($self,$site,$silent) and push @updatedSites, $site; 

  }

  if(scalar(@updatedSites) gt 0) {

     my $query = "delete from SERanks where updated=0 and (sitename = '$updatedSites[0]'";
   
     for my $siteC(1..$#updatedSites) {
        $query .= " or sitename = '$updatedSites[$siteC]'";
     }
     $query .= ")";

     $self->$method(@info,$query);
  
     my $catalogue=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};

     my $stat  = $catalogue->do($query);

     $stat and  $self->$method(@info, "SE Rank Optimizer, deleted old entries");

     $stat = $catalogue->do(" update SERanks set updated=0");

     $stat and  $self->$method(@info, "SE Rank Optimizer, set new entries as old ones from now on.");
  }


  return 1;
}





sub processUpdateRanksForSite{
  my $self=shift;
  my $site=(shift || return 0);
  my $silent=(shift || 1);
  my $stat = 0;
  my @info;
  my $method="info";

  $silent and $method="debug" and  @info=1;

  my $selist = AliEn::Service::Optimizer::Catalogue::SERank::rankStorageElementsWithMonAlisa($self,$site,$silent) or $self->info("Error calling MonALISA by HTTP") and return 0;
  
  scalar(@$selist) gt 0 or $self->info("MonALISA didn't supply any SE for site: $site") and return 0;

  my $catalogue=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->{FIRST_DB};

  my $status = 1;
  $self->$method(@info, "SE Rank Optimizer, the ses for site $site are: @$selist");


  for my $rank(0..$#{$selist}) {
   
     $stat = $catalogue->do("REPLACE INTO SERanks (sitename,seNumber,rank,updated) values (?,(select seNumber from SE where seName=?),?,1);", {bind_values=>[$site,$$selist[$rank],$rank]});
     $self->$method(@info, "SE Rank Optimizer, updating for site $site the SE ".$$selist[$rank]." with rank $rank");
     $stat and  $self->$method(@info, "SE Rank Optimizer, setting ".$$selist[$rank]." to $rank was OK");
     $status = $status && $stat;

  }
  return $status;

}




sub rankStorageElementsWithMonAlisa{
   my $self=shift;
   my $sitename=(shift || "");
   my $silent=(shift || "");


   $self->{CONFIG}->{SEDETECTMONALISAURL} or return 0;

   my $url=$self->{CONFIG}->{SEDETECTMONALISAURL}."?";

   ($sitename and $sitename ne "") and $url .= "site=$sitename&";
   $url .= "dumpall=true";


   my @info;

   my $method="info";
   $silent and $method="debug" and  @info=1;

   $self->$method(@info, "SE Rank Optimizer, gonna ask MonAlisa for: $url");

   my $monua = LWP::UserAgent->new();
   $monua->timeout(120);
   $monua->agent( "AgentName/0.1 " . $monua->agent );
   my $monreq = HTTP::Request->new("GET" => $url);
   $monreq->header("Accept" => "text/html");
   my $monres = $monua->request($monreq);
   my $monoutput = $monres->content;
   my @selist = ();
   ( $monres->is_success() ) and push @selist, split (/\n/, $monoutput);

   $self->$method(@info, "SE Rank Optimizer, MonAlisa replied with se list: @selist");

   return (\@selist);
}

1;
