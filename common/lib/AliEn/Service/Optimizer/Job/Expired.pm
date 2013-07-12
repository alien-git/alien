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
  $self->{DB}->{DRIVER}=~ /Oracle/i and $time=" to_timestamp(\'$time\','YYYY-MM-DD HH24:Mi:ss') " or $time="\'$time\'";
  
  my $finalStatus="(15,-13,-12,-1,-2,-3,-4,-5,-7,-8,-9,-10,-11,-16,-17,-18)";
  
  # Completed single Jobs older than 10 days are moved to the archive
  $self->archiveJobs("where statusId in $finalStatus and q.mtime<$time and split=0");                   

  $self->{LOGGER}->$method("Expired", "In checkWakesUp going back to sleep");

  return;
}

sub archiveJobs{
  my $self=shift;
  my $query=shift;
  my $table=$self->{DB}->{QUEUEARCHIVE};

  $self->info("Archiving the jobs older than 10 days");
    
  my ($jobs)=$self->{DB}->queryColumn("select q.queueId from QUEUE q $query");
  scalar(@$jobs) or $self->info("There are 0 expired jobs") and return 1;
  
  my $ids="";
  foreach my $j (@$jobs){
  	# We add the subjobs
  	my ($subjobs) = $self->{DB}->queryColumn("select queueId from QUEUE where split=?",undef, {bind_values=>[$j]});
  	scalar(@$subjobs) and push @$jobs, @$subjobs; 
  	  
  	$ids.="$j,";
  }
  $ids=~ s/,$//;
  
  $self->info("There are ".scalar(@$jobs)." expired jobs"); 
  
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
