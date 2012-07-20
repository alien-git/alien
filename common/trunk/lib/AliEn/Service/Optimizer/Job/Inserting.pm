package AliEn::Service::Optimizer::Job::Inserting;

use strict;

use AliEn::Service::Optimizer::Job;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  $self->{SLEEP_PERIOD}=10;
  my $method="info";
  $silent and $method="debug";
  my @data;
  $silent and push @data, 1;
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
  $self->{INSERTING_COUNTING}++;
  if ($self->{INSERTING_COUNTING}>10){
    $self->{INSERTING_COUNTING}=0;
  }else {
    $method="debug";
    @data=(1);
  }
  $self->$method(@data, "The inserting optimizer starts");
  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='INSERTING'");
  $todo or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='INSERTING'");
  my $q = "1' and upper(origjdl) not like '\% SPLIT = \"\%";
  $self->{DB}->{DRIVER}=~/Oracle/i and $q = "1 and REGEXP_REPLACE(upper(origjdl), '\\s*', '') not like '\%SPLIT=\"\%";
  my $done=$self->checkJobs($silent,$q, "updateInserting");

  $self->$method(@data, "The inserting optimizer finished");
  return;
}

sub updateInserting {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  my $status="WAITING";

  $self->info( "\n\nInserting a new job $queueid" );

  my ($host)= $self->{DB}->getFieldFromQueue($queueid,"submitHost")
    or $self->info( "Job $queueid doesn't exist" )
      and return;

  my $user = "";
  ( $host =~ /^(.*)\@/ ) and ( $user = $1 );
  my $set={};
  eval {
    if ( !$job_ca->isOK() ) {
      die("incorrect JDL input");
    }

    my $done=$self->copyInput($queueid, $job_ca, $user) or 
      die("error copying the input\n");

    my ($ok, $req)=$job_ca->evaluateExpression("Requirements");
    ($ok and $req) or
      die("error getting the requirements of the jdl");
    $self->debug(1,  "Let's create the entry for the jobagent");
    $req =~ s{ \&\& \( other.LocalDiskSpace > \d+ \)}{}g;

    $done->{requirements} and $req.=" && $done->{requirements}";

    $ok=$job_ca->set_expression("Requirements", $req) or 
      die("ERROR SETTING THE REQUIREMENTS TO $req");
    $set->{origjdl}=$job_ca->asJDL();

    ($ok, my $stage)=$job_ca->evaluateExpression("Prestage");
    if ($stage){
      $self->putJobLog($queueid, "info", "The job asks for its data to be pre-staged");
      $status="TO_STAGE";
      $req.="  && other.TO_STAGE==1 ";
    }

    ($status) = $self->checkRequirements($req,$queueid,$status);

    if($status ne "FAILED"){
	    $req=$self->getJobAgentRequirements($req, $job_ca);
	    $set->{agentId}=$self->{DB}->insertJobAgent($req)
	      or die("error creating the jobagent entry\n");
    }
  };
  my $return=1;
  if ($@) {
    $self->info( "Error inserting the job: $@");
    $status="ERROR_I";
    $self->{DB}->deleteJobToken($queueid);

    undef $return;
  }
  if (! $self->{DB}->updateStatus($queueid,"INSERTING", $status, $set)) {
    $self->{DB}->updateStatus($queueid,"INSERTING", "ERROR_I");
    $self->info( "Error updating status for job $queueid" );
    return;
  }
  $self->putJobLog($queueid,"state", "Job state transition from INSERTING to $status");

  $return and $self->debug(1, "Command $queueid inserted!" );
  return $return


}


sub checkRequirements {
	my $self = shift;
	my $tmpreq = shift;
	my $queueid = shift;
	my $status = shift;
	
	# READ THE REQUISITES
	my @sitename; my @nositename;
	my @no_ce; my @cename; my @nocename;
	my @ef_site; my @ef_ce; my @def_site;
		
    while ($tmpreq =~ s/!member\(other.CloseSE,"([^:]*::)([^:]*)(::[^:]*)"\)//si) {
      grep { /$1/ } @nositename or push @nositename, $1.$2.$3;
    }
       
    while ($tmpreq =~ s/member\(other.CloseSE,"([^:]*::)([^:]*)(::[^:]*)"\)//si) {
      grep { /$1/ } @sitename or push @sitename, $1.$2.$3;
    }
    	    
    while ($tmpreq =~ s/!other.ce\s*==\s*"([^"]*)"//i) {
      grep { /$1/ } @no_ce or push @no_ce, (split("::", $1))[1] and push @nocename, $1;
    }	
    	   
    while ($tmpreq =~ s/other.ce\s*==\s*"([^"]*)"//i) {
      grep { /$1/ } @cename or push @cename, $1;
    }
    
    # FILTER THE REQUISITES
    foreach my $s (@sitename){
    	grep { /$s/ } @nositename or push @ef_site, (split("::", $s))[1];
    }
    
    foreach my $s (@cename){
    	grep { /$s/ } @nocename or push @ef_ce, (split("::", $s))[1];
    }
    
    foreach my $s (@ef_ce){
        grep { /$s/ } @ef_site and push @def_site, $s;	
    }
    
    my $count=0;
    foreach my $s (@ef_site){
    	grep { /$s/ } @no_ce and $count++;
    }
    
    # ERROR CASES
    my $msg ="";
    ( ( @cename and !@ef_ce and $msg="conflict with CEs" ) or
    ( @sitename and !@ef_site and $msg="conflict with SEs" ) or
    ( !@cename and @ef_site and @ef_site==$count and $self->numberCESite(@no_ce) <= @no_ce and $msg="conflict with SEs and !CEs" ) or
    ( @ef_ce and @ef_site and !@def_site and $msg="conflict with CEs and SEs" ) )
        and ( $self->putJobLog($queueid,"state", "Job going to FAILED, problem with requirements: $msg") and $status="FAILED");
    
#    use Data::Dumper;
#    $self->info("NO_CE: ".Dumper(@no_ce)." - NOSITENAME: ".Dumper(@nositename)." - SITENAME: ".Dumper(@sitename)." - CENAME: ".Dumper(@cename)." - NOCENAME: ".Dumper(@nocename)."");
#    $self->info("EF_SITE: ".Dumper(@ef_site)." - EF_CE: ".Dumper(@ef_ce)." - DEF_SITE: ".Dumper(@def_site)." - COUNT: ".$count." - TAMCENAME: ".@cename);
#    $self->info("SIZE NOCE: ".@no_ce." - NUMERO: ".$self->numberCESite(@no_ce));
    
    return $status;
}

sub numberCESite {
	my $self = shift;
	my @no_ce = shift;
	my $ces = 0;
	
	foreach my $s (@no_ce){
		my ($tmpce) = $self->{DB}->getFieldFromSiteQueueEx("count(1) as count","where site like '%$s%'");
		$ces += @$tmpce[0];
	}
	
	return $ces;
}


1
