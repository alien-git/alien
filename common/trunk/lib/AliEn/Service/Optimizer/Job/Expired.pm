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
 
  my $time = strftime "%Y-%m-%d %H:%M:%S", localtime(time - 240*3600);
  my $mtime = "q.mtime";
  $self->{DB}->{DRIVER}=~ /Oracle/i and $time=" to_timestamp(\'$time\','YYYY-MM-DD HH24:Mi:ss') " or $time="\'$time\'";
  
  # Completed master Jobs older than 10 days are moved to the archive
  $self->archiveJobs("where ((statusId=15 or statusId=-13 or statusId=-12 or statusId=-1 or statusId=-2 or statusId=-3 or statusId=-4 or statusId=-5
  or statusId=-7 or statusId=-8 or statusId=-9 or statusId=-10 or statusId=-11 or statusId=-16 or statusId=-17 or statusId=-18) 
  and ( $mtime < $time) and split=0) ", "10 days in final state", $self->{DB}->{QUEUEARCHIVE});

  # This is to archive the subjobs
  $self->archiveJobs("left join QUEUE q2 on q.split=q2.queueid where $mtime<$time", # and q.split!=0 and q2.queueid is null ? 
                      "10 days without subjobs", $self->{DB}->{QUEUEARCHIVE});
                      
  # This is slightly more than ten days, and we move it to another table
#  $time = strftime "%Y-%m-%d %H:%M:%S",  localtime(time - 2*240*3600 );
#  $self->{DB}->{DRIVER}=~ /Oracle/i and $time=" to_timestamp(\'$time\','YYYY-MM-DD HH24:Mi:ss') " or $time = "\'$time\'";
#  $self->archiveJobs("where $mtime < $time and split=0", "20 days in any state","QUEUEEXPIRED" );

  $self->{LOGGER}->$method("Expired", "In checkWakesUp going back to sleep");

  return;
}

sub archiveJobs{
  my $self=shift;
  my $query=shift;
  my $time=shift;
  my $table=shift;

  $self->info("Archiving the jobs older than $time");
    
  my ($jobs)=$self->{DB}->queryColumn("select q.queueId from QUEUE q $query");
  $self->info("There are ".scalar(@$jobs)." expired jobs");
  scalar(@$jobs) or return 1;
  
  my $ids="";
  foreach my $j (@$jobs){
  	$ids.="$j,";
  }
  $ids=~ s/,$//;
  
  my $columns=$self->{DB}->describeTable($table);
  my $c="";
  foreach my $column (@$columns){
    $column->{Field} !~ /queueid/i and $c.="$column->{Field}, ";
  }
  $c=~ s/, $//;
    
  my $done=$self->{DB}->do("insert into ${table} (queueId, $c) select q.queueId, $c from QUEUE q join QUEUEPROC p on q.queueId=p.queueId 
                                                                                                 join QUEUEJDL j on q.queueId=j.queueId 
                                                                                                 where q.queueId in ($ids)");

  my $done2=$self->{DB}->do("insert into JOBMESSAGES (timestamp, jobId, procinfo, tag) 
                             select unix_timestamp(), queueId, 'Job moved to the archived table', 'state' from ${table} where queueId in ($ids)");

  my $done3=$self->{DB}->do("delete from QUEUE WHERE queueId in (SELECT queueId FROM ${table}) ");
  
  $self->info("AT THE END, WE HAVE $done and $done2 and $done3");
  return 1;
}

1;
