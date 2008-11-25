package AliEn::Service::Optimizer::Job::Expired;

use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");


sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";

  $self->{SLEEP_PERIOD}=3600;
  $self->{LOGGER}->$method("Expired", "In checkWakesUp .... optimizing the QUEUE table ...");
  my  $now = time; 


  $self->archiveJobs("received < (? - 865400)", "10 days","QUEUEEXPIRED" );

  $self->archiveJobs("( ( (status='DONE') || (status='FAILED') || (status='EXPIRED') || (status like 'ERROR%')  ) && ( received < (? - 7*86540) ) )", "1 week" ,$self->{DB}->{QUEUEARCHIVE});


  $self->{LOGGER}->$method("Expired", "In checkWakesUp going back to sleep");

  return;
}

sub archiveJobs{
  my $self=shift;
  my $query=shift;
  my $time=shift;
  my $table=shift;

  my  $now = time; 
  $self->info("Archiving the jobs older than $time");
  my $data= $self->{DB}->getFieldsFromQueueEx("*","where $query ORDER by queueId", {bind_values=>[$now]});

  my $columns=$self->{DB}->query("describe QUEUEPROC");

  my $c="";
  my $c2="";
  foreach my $column (@$columns){
    $c.="$column->{Field}, ";
    $c2.="q.$column->{Field}, ";
  }
  $c=~ s/, $//;
  $c2=~ s/, $//;
  for my $q ("insert into ${table}PROC ($c) select $c2 from ",
	     "delete from q using" ){
    $self->{DB}->do("$q QUEUEPROC q, QUEUE s where s.queueid=q.queueid and $query", {bind_values=>[$now]});
  }


  foreach my $job (@$data){
    $self->info(" Found standard job $job->{queueId} > $time old in status $job->{status}");
    
    # insert master job into the archive
    $self->{DB}->insertEntry($table,$job) or
      print STDERR "Expired: cannot copy entry $job->{queueId} to $table\n" and next;
    
    # delete master job from the regular queue
    $self->{DB}->deleteJob("$job->{queueId}") or
      print STDERR "Expired: cannot delete entry $job->{queueId} from QUEUE\n"
	and next;
    
    # remove /proc entry
    if ( (defined $job->{queueId}) && ( $job->{queueId} ne "") && ( $job->{queueId} > 0 ) ) {
      my $procDir = AliEn::Util::getProcDir(undef, $job->{submitHost}, $job->{queueId});
      $self->info("   Removing $procDir directory");
      $self->{CATALOGUE}->execute("rmdir",$procDir,"-r") or $self->info("Error deleting the directory $procDir");
    }
  }


  return 1;
}

1;

