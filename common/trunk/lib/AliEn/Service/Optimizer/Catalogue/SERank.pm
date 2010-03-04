
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
  AliEn::Service::Optimizer::Catalogue::SERank->updateRanksForAllSites($self,$catalogue,$silent);
  $self->info("Going back to sleep");
  return;
}

sub updateRanksForAllSites{
  my $proto = shift;
  my $class = ref($proto) || $proto;

  my $self=shift;
  my $catalogue=(shift || return 0); 
  my $silent=(shift || 0);
  my @info;
  my $method="info";
  $silent and $method="debug" and  @info=1;


  my $sites = $catalogue->queryColumn("select distinct sitename from SERanks;");
  $self->info("Going to handle an updateRanksForSites request for sites: @$sites");
 
  my $stat=1;
   
  foreach my $site (@$sites) {
    $stat = $stat and AliEn::Service::Optimizer::Catalogue::SERank->updateRanksForSite($self,$site,$catalogue,$silent);
  }

  $stat and  $catalogue->query("delete from SERanks where updated=0;");
  $stat and  $self->$method(@info, "SE Rank Optimizer, deleted old entries;");

  $stat = $catalogue->query(" update SERanks set updated=0;");
  $stat and  $self->$method(@info, "SE Rank Optimizer, set new entries as old ones from now on.");

  return $stat;

}


sub updateRanksForSite{
  my $proto = shift;
  my $class = ref($proto) || $proto;

  my $self=shift;
  my $site=(shift || return 0);
  my $catalogue=(shift || return 0); 
  my $silent=(shift || 0);
  my $stat = 1;
  my @info;
  my $method="info";
  $silent and $method="debug" and  @info=1;

  my $selist = AliEn::Service::Optimizer::Catalogue::SERank->rankStorageElementsWithMonAlisa($self,$site,$catalogue,$silent);

  if ($selist and scalar(@$selist) gt 0) {
     $self->$method(@info, "SE Rank Optimizer, the SEs for site $site are: @$selist");
    for my $rank(0..$#{$selist}) {
      $stat = $stat && $catalogue->query("REPLACE INTO SERanks (sitename,seNumber,rank,updated)"
          ." values (?,(select seNumber from SE where seName=?),?,1);", undef, {bind_values=>[$site,$$selist[$rank],$rank]});
          $stat and  $self->$method(@info, "SE Rank Optimizer, setting ".$$selist[$rank]." to $rank was OK");
    }
    return $stat;
  }
  $self->info( "ERROR in SE Rank Optimizer, we didn't get any SEs (neither MonALISA nor SE table) while updating site $site.");
  return 0;
}


sub rankStorageElementsWithMonAlisa{
   my $proto = shift;
   my $class = ref($proto) || $proto;

   my $self=shift;
   my $sitename=(shift || return 0);
   my $catalogue=(shift || return 0); 
   my $silent=(shift || 0);

   my @info;
   my $method="info";
   $silent and $method="debug" and  @info=1;
   my @selist = ();

   if($self->{CONFIG}->{SEDETECTMONALISAURL}) {
  
     my $url=$self->{CONFIG}->{SEDETECTMONALISAURL}."?";
  
     ($sitename and $sitename ne "") and $url .= "site=$sitename&";
     $url .= "dumpall=true";
  
     $self->$method(@info, "SE Rank Optimizer, gonna ask MonAlisa for: $url");
  
     my $monua = LWP::UserAgent->new();
     $monua->timeout(120);
     $monua->agent( "AgentName/0.1 " . $monua->agent );
     my $monreq = HTTP::Request->new("GET" => $url);
     $monreq->header("Accept" => "text/html");
     my $monres = $monua->request($monreq);
     my $monoutput = $monres->content;
     ( $monres->is_success() ) and push @selist, split (/\n/, $monoutput);
  
     $self->$method(@info, "SE Rank Optimizer, MonAlisa replied for site $sitename with se list: @selist");
  } else {
     $self->$method(@info,"MonALISA for SE Discovery isn't configured in LDAP->SEDETECTMONALISAURL");
  }  

  if (scalar(@selist) eq 0) {
     $self->$method(@info,"We couldn't get any SEs from MonALISA for site: $sitename");
     $self->$method(@info,"Therefore we will add all listed SEs in table SE for site: $sitename");
     @selist = @{$catalogue->queryColumn("select distinct seName from SE;")};
  }

   return (\@selist);
}


sub updateRanksForOneSite{
  my $proto = shift;
  my $class = ref($proto) || $proto;

  my $self=shift;
  my $site=(shift|| return 0);
  my $catalogue=(shift || return 0); 
  my $silent=(shift || 0);
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  $self->$method(@info,"Going to handle an updateRanksForOneSite request for site: $site");

  AliEn::Service::Optimizer::Catalogue::SERank->updateRanksForSite($self,$site,$catalogue,$silent) or return 0;

  $catalogue->query("delete from SERanks where updated=0 and sitename=? ;", undef,  {bind_values=>[ $site ]});

  return $catalogue->query("update SERanks set updated=0 where sitename=? ;", undef,  {bind_values=>[ $site ]});

}




1;

