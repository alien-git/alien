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
  
  $self->{NOT_FIRST} 
    or sleep(30) and $self->{NOT_FIRST}=1;
  
  $self->{LOGGER}->$method("Expired", "In checkWakesUp .... optimizing the QUEUE table ...");
 
  my $time = strftime "%Y-%m-%d %H:%M:%S", localtime(time - 240*3600);
  $self->{DB}->{DRIVER}=~ /Oracle/i and $time=" to_timestamp(\'$time\','YYYY-MM-DD HH24:Mi:ss') " or $time="\'$time\'";
  
  my $finalStatus="(15,-13,-12,-1,-2,-3,-4,-5,-7,-8,-9,-10,-11,-16,-17,-18)";
  
  # Completed Jobs older than 10 days are moved to the archive
  $self->archiveJobs("where statusId in $finalStatus and q.mtime<$time and split=0");
                      
  $self->{LOGGER}->$method("Expired", "In checkWakesUp going back to sleep");

  return;
}

sub archiveJobs{
  my $self=shift;
  my $query=shift;
  my $table=$self->{DB}->{QUEUEARCHIVE};
  my $limit = 10000;

  eval {
	  my ($jobs)=$self->{DB}->queryColumn("select q.queueId from QUEUE q $query");
	  scalar(@$jobs) or $self->info("There are 0 expired jobs") and return 1;
	
	  $self->info("Archiving the jobs older than 10 days");
	  
	  while (scalar(@$jobs)) { 
	    $self->{DB}->do("drop table if exists QUEUE_TMP_EXP");
	    $self->{DB}->do("create table if not exists QUEUE_TMP_EXP as select queueId from QUEUE limit 0");
	    my $total=0;    
	    foreach my $job (@$jobs){
	      $self->{DB}->do("insert into QUEUE_TMP_EXP values ($job)");
	      $self->{DB}->do("insert into QUEUE_TMP_EXP select queueId from QUEUE where split=?",{bind_values=>[$job]});
	      $total=$self->{DB}->queryValue("select count(1) from QUEUE_TMP_EXP");
	      $total >= $limit and last;
	    }
	    $self->info("There are $total expired jobs");
	    #use Data::Dumper;
	    #$self->info("JOBS: ".Dumper($jobs).Dumper($total).Dumper($query));
	
	    $total or return 1;
	  
	    my $columns=$self->{DB}->describeTable($table);
	    my $c="";
	    foreach my $column (@$columns){
	      $column->{Field} =~ /agentid/i and next;
	      $column->{Field} !~ /queueid/i and $c.="$column->{Field}, ";
	    }
	    $c=~ s/, $//;
	    
	    my $done=$self->{DB}->do("insert ignore into ${table} (queueId, $c) select q.queueId, $c from QUEUE q join QUEUEPROC p using (queueid )
	                                                                                                 join QUEUEJDL j using (queueid)
	    join QUEUE_TMP_EXP using (queueid)") ;
	
	    my $done2=$self->{DB}->do("insert into JOBMESSAGES (timestamp, jobId, procinfo, tag) 
	                             select unix_timestamp(), queueId, 'Job moved to the archived table', 'state' from QUEUE_TMP_EXP");
	
	    my $done3=$self->{DB}->do("delete from QUEUE using QUEUE join QUEUE_TMP_EXP using (queueid)");
	  
	    $self->info("AT THE END, WE HAVE $done and $done2 and $done3");
	    
	    ($jobs)=$self->{DB}->queryColumn("select q.queueId from QUEUE q $query");
    }
  };
  if($@){
  	$self->info("There was a problem!: $@");
  }
  
  return 1;
}

1;

