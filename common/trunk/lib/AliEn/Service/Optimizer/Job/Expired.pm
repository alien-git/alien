package AliEn::Service::Optimizer::Job::Expired;

use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA);
use POSIX qw(strftime);

push (@ISA, "AliEn::Service::Optimizer::Job");
sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";

  $self->{SLEEP_PERIOD}=3600;
  $self->{LOGGER}->$method("Expired", "In checkWakesUp .... optimizing the QUEUE table ...");
   
  my $time = strftime "%Y-%m-%d %H:%M:%S",  localtime(time - 240*3600 );
  #Completed master Jobs older than 10 days are moved to the archive
  $self->archiveJobs("where (( status in ('DONE','FAILED','EXPIRED') || status like 'ERROR%'  ) 
                      and ( mtime < '$time')  and split=0) ", "10 days in final state" ,$self->{DB}->{QUEUEARCHIVE});



# #This is to archive the subjobs 'select count(*) from QUEUE q left join QUEUE q2
  $self->archiveJobs("left join QUEUE q2 on q.split=q2.queueid where 
                      q.split!=0 and q2.queueid is null and q.mtime<'$time'", 
                      "10 days without subjobs ", $self->{DB}->{QUEUEARCHIVE});
  
    #This is slightly more than ten days, and we move it to another table
  $time = strftime "%Y-%m-%d %H:%M:%S",  localtime(time - 2*240*3600 );
  $self->archiveJobs("where mtime < '$time' and split=0", "20 days in any state","QUEUEEXPIRED" );



  $self->{LOGGER}->$method("Expired", "In checkWakesUp going back to sleep");

  return;
}

sub archiveJobs{
  my $self=shift;
  my $query=shift;
  my $time=shift;
  my $table=shift;

  
  $self->info("Archiving the jobs older than $time");

  my $done=$self->{DB}->do("insert into ${table}PROC select p.* from QUEUE q join QUEUEPROC p using (queueid) $query");
  my $done2=$self->{DB}->do("insert into ${table} select q.* from QUEUE q $query");
  my $done3=$self->{DB}->do("delete from p using  QUEUE q join QUEUEPROC p using (queueid) $query");
  my $done4=$self->{DB}->do("delete from q using QUEUE q $query");

  $self->info("AT THE END, WE HAVE $done and $done2 and $done3 and $done4");
  return 1;
}

1;

