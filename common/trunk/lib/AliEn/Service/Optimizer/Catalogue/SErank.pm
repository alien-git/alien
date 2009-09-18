package AliEn::Service::Optimizer::Catalogue::SErank;
 
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

  $self->$method(@info, "SE Rank Optimizer, the sites are: @$sites");

  foreach my $site (@$sites) {

     my $selist = $self->rankStorageElementsWithMonAlisa($site,$silent) or return 0;
  
     $self->$method(@info, "SE Rank Optimizer, the ses for site $site are: @$selist");

     for my $rank(0..$#{$selist}) {
      
        $catalogue->do(" update SERanks set SERanks.rank=$rank,SERanks.updated=1 where SERanks.seName='$$selist[$rank]' and SERanks.sitename='$site'");
     }
  
     $catalogue->do("delete from SERanks where SERanks.updated=0 and SERanks.sitename='$site'");
     $catalogue->do(" update SERanks set SERanks.updated=0 where SERanks.sitename='$site'");
  }

  $self->info("Going back to sleep");
  return;
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
