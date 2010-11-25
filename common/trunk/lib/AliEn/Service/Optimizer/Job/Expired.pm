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
   

  #Completed master Jobs older than 10 days are moved to the archive
  $self->archiveJobs("where (( status in ('DONE','FAILED','EXPIRED') || status like 'ERROR%'  ) 
                      and ( mtime < addtime(now(), '-10 00:00:00')  and split=0) )", "10 days" ,$self->{DB}->{QUEUEARCHIVE});



# #This is to archive the subjobs 'select count(*) from QUEUE q left join QUEUE q2
  $self->archiveJobs("left join QUEUE q2 on q.split=q2.queueid where 
                      q.split!=0 and q2.queueid is null and q.mtime<addtime(now(), '-10 00:00:00')", 
                      " subjobs ", $self->{DB}->{QUEUEARCHIVE});
  
    #This is slightly more than ten days, and we move it to another table
  $self->archiveJobs("where mtime < addtime(now(), '-10 00:00:00') and split=0", "10 days","QUEUEEXPIRED" );



  $self->{LOGGER}->$method("Expired", "In checkWakesUp going back to sleep");

  return;
}

sub archiveJobs{
  my $self=shift;
  my $query=shift;
  my $time=shift;
  my $table=shift;

  
  $self->info("Archiving the jobs older than $time");
  my $data= $self->{DB}->getFieldsFromQueueEx("*","q $query ORDER by q.queueId");

  my $columns=$self->{DB}->query("describe QUEUEPROC");

  my $c="";
  my $c2="";
  foreach my $column (@$columns){
    $c.="$column->{Field}, ";
    $c2.="s.$column->{Field}, ";
  }
  $c=~ s/, $//;
  $c2=~ s/, $//;
  for my $q ("insert into ${table}PROC ($c) select $c2 from ",
	     "delete from s using" ){
    $self->{DB}->do("$q QUEUEPROC s, QUEUE q $query and s.queueid=q.queueid");
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
    
#    # remove /proc entry
#    if ( (defined $job->{queueId}) && ( $job->{queueId} ne "") && ( $job->{queueId} > 0 ) ) {
#      my $procDir = AliEn::Util::getProcDir(undef, $job->{submitHost}, $job->{queueId});
#      $self->info("   Removing $procDir directory");
#      $self->{CATALOGUE}->execute("rmdir",$procDir,"-r") or $self->info("Error deleting the directory $procDir");
#    }
  }


  return 1;
}

1;

