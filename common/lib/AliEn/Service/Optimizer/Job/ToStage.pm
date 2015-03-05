package AliEn::Service::Optimizer::Job::ToStage;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::GUID;
use Data::Dumper;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp  {
  my $self=shift;
  my $silent = shift;
  
  #$self->{SLEEP_PERIOD}=10;
  my $method="info";
  $silent and $method="debug";
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
  $self->{INSERTING_COUNTING}++;
  if ($self->{INSERTING_COUNTING}>10){
    $self->{INSERTING_COUNTING}=0;
  }else {
    $method="debug";
  }
   
  $self->info("The ToStage optimizer starts");
     
  my $done=$self->{DB}->queryColumn("SELECT queueId from QUEUE where statusId=17");
  $done and @$done or $self->info("No jobs in TO_STAGE") and return;
  
  # available CES
  my $ces = $self->{DB}->getFieldFromSiteQueueEx("site","where status like 'jobagent-no-match' and blocked like 'open'");
  my $ces_matched = $self->{DB}->getFieldFromSiteQueueEx("site","where status like 'jobagent-matched' and blocked like 'open'");
  push @$ces, @$ces_matched;
  
  $ces and @$ces or $self->info("No CEs available") and return;    
      
  foreach my $job (@$done){  	  	
  	my $jdl=$self->{DB}->queryValue("select origJdl from QUEUEJDL where queueid=?", undef, {bind_values => [$job]});
  	my $ca=Classad::Classad->new($jdl);
    my ($ok, $req)=$ca->evaluateExpression("Requirements");
  	$ok or $self->info("Could not get Requirements from job $job") and next;
  	
  	my @se_sites; my @ses;
    while ($req =~ s/member\(other.CloseSE,"([^:]*::)([^:]*)(::[^:]*)"\)//si) {
        push @se_sites, $2;
        push @ses, $1.$2.$3;
    }
    
    
    my $place; my $site;
    for($a=0;$a<@se_sites;$a++){
        grep /$se_sites[$a]/i, @$ces and $self->info("Found in $ses[$a]") and $place=$ses[$a] and $site=$se_sites[$a] and last;
    }
    
    if($place){
    	my $reqCE; my $no_error=1;
    	foreach (@$ces){ $_ =~ /$site/ and $reqCE=$_ and last; }
    	
    	($ok, my @inputData) = $ca->evaluateAttributeVectorString("InputData"); 

        $self->copyInputCollection($ca, $job, \@inputData)
          or $self->info("Error copying the inputCollection") 
            and $self->{DB}->updateStatus($job, "TO_STAGE", "FAILED") 
            and $self->putJobLog($job,"state", "Job state transition from TO_STAGE to FAILED")
            and next;          
    	    	
        foreach my $file ( @inputData ) {
        	$self->info("Going to stage file $file in $place ($job)");
        	$file =~ s/,nodownload$//; $file =~ s/^LF://i;
            $self->{CATALOGUE}->stage("-se",$place,$file) or $self->info("Failed staging $file") and $no_error=0 and last;
        }
 
  	    if($no_error){
            my ($ok, $req)=$ca->evaluateExpression("Requirements");
            $ca->set_expression("Requirements", $req." && (other.CE==\"$reqCE\")");
            $self->{DB}->update("QUEUEJDL", {origJdl => $ca->asJDL()}, "queueId=?", {bind_values=>[$job]}) or $self->info("Error doing the jdl update");
                    
            $self->{DB}->updateStatus($job, "TO_STAGE", "STAGING");
            $self->{DB}->update("ACTIONS", {todo=>1}, "action='STAGING'");
            $self->putJobLog($job,"state", "Job state transition from TO_STAGE to STAGING");
  	    }        
    }
    else{
      $self->info("No place to stage files from job $job");
    }
    
  }

  return;

}

1

