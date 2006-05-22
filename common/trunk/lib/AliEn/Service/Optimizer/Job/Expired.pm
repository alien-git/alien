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


  $self->{LOGGER}->$method("Expired", "In checkWakesUp .... optimizing the QUEUE table ...");
  my  $now = time; 
  my $allkilled = $self->{DB}->getFieldsFromQueueEx("count(*) as count","where ( ($now-sent) > (7*86540) ) and ( status='KILLED' )");
  my $allinqueue = $self->{DB}->getFieldsFromQueueEx("count(*) as count","");

  my $olderthanoneyear = $self->{DB}->getFieldsFromQueueEx("count(*) as count","where ( ($now-sent) > (365*86540) ) ORDER by queueId");

  if ( (! defined $allkilled)  || (! defined $allinqueue ) ) {
    $self->{LOGGER}->info("Expired", "I cannot retrieve the number of entries in the queue .... Aborting check!");
    return ;
  }

  my $ratio;
  $ratio =0 ;

  if ( (defined @$allinqueue[0]->{count} )  && (@$allinqueue[0]->{count} != 0)) {
    $ratio = 100.0 * int (@$allkilled[0]->{count}) / int (@$allinqueue[0]->{count});
  }

  $self->{LOGGER}->$method("Expired", "Found @$allkilled[0]->{count} killed Processes out of @$allinqueue[0]->{count} [Ratio $ratio]");

  $self->{LOGGER}->$method("Expired", "Found @$olderthanoneyear[0]->{count} Processes out of @$allinqueue[0]->{count} older than 1 year");

  if ( ($ratio < 0) || ($ratio > 80)) {
    $self->{LOGGER}->$method("Expired", "I better do nothing, the ratio [$ratio] is out of limit .... Aborting check!");
    return;
  }

  $self->{LOGGER}->$method("Expired", "Getting 10000 old killed jobs to expire");

  my $data = $self->{DB}->getFieldsFromQueueEx("*","where ( ($now-sent) > (7*86540) ) and ( status='KILLED' ) ORDER by queueId limit 10000 ");

  my $cnt = 0 ;
  foreach (@$data) {
      $cnt++;
      $self->{LOGGER}->$method("Expired","[$cnt] Found Job $_->{queueId} >7 days old in status $_->{status}");
      my $value;
      # disabled for the moment	
      $self->{DB}->insertEntry("QUEUEEXPIRED",$_) or 
	  print STDERR "Expired: cannot copy entry $_->{queueId} to QUEUEEXPIRED\n"
	      and return;
      $self->{DB}->deleteJob("$_->{queueId}") or
	  print STDERR "Expired: cannot delete entry $_->{queueId} from QUEUE\n"
	      and return;
  }

  $self->{LOGGER}->$method("Expired", "Getting 10000 jobs older than 1 year to expire");

  undef $data;

  $data = $self->{DB}->getFieldsFromQueueEx("*","where ( ($now-sent) > (365*86540) ) ORDER by queueId");
  $cnt=0;
  foreach (@$data) {
      $cnt++;
      $self->{LOGGER}->info("Expired","[$cnt] Found Job $_->{queueId} > 1 year old in status $_->{status}");
      my $value;
      # disabled for the moment	
      $self->{DB}->insertEntry("QUEUEEXPIRED",$_) or 
	  print STDERR "Expired: cannot copy entry $_->{queueId} to QUEUEEXPIRED\n"
	      and return;
      $self->{DB}->deleteJob("$_->{queueId}") or
	  print STDERR "Expired: cannot delete entry $_->{queueId} from QUEUE\n"
	      and return;
  }

  $self->{LOGGER}->$method("Expired", "Archiving finished split jobs older than 1 week");

  undef $data;

  $data = $self->{DB}->getFieldsFromQueueEx("*","where ( (status='DONE' ) && ( jdl like '%split%') && ( ($now-received) > (7*86540) ) ) ORDER by queueId");
  $cnt=0;
  foreach (@$data) {
      $cnt++;
      $self->{LOGGER}->info("Expired","[$cnt] Found split job $_->{queueId} > 7 days finished  in status $_->{status}");
      my $childjobs = $self->{DB}->getFieldsFromQueueEx("*","where (split='$_->{queueId}') ORDER by queueId");
      my $child;
      $self->{LOGGER}->info("Expired","[$cnt] Moving master job $_->{queueId} to archive $self->{DB}->{QUEUEARCHIVE} ..");

      foreach $child (@$childjobs) {
	  $self->{LOGGER}->info("Expired","[$cnt] Moving childjob $child->{queueId} of master job $_->{queueId} to archive ..");
	  # insert child jobs into the archive
	  $self->{DB}->insertEntry("$self->{DB}->{QUEUEARCHIVE}",$child) or
	      print STDERR "Expired: cannot copy entry $child->{queueId} to $self->{DB}->{QUEUEARCHIVE}\n"
		  and next;
	  
	  # delete child job from the regular queue
	  $self->{DB}->deleteJob("$child->{queueId}") or
	      print STDERR "Expired: cannot delete entry $child->{queueId} from QUEUE\n"
		  and next;

	  # remove /proc entry
	  if ( (defined $child->{queueId}) && ( $child->{queueId} ne "") && ( $child->{queueId} > 0 ) ) {
	    my $procDir = AliEn::Util::getProcDir(undef, $child->{submitHost}, $child->{queueId});
	    $self->{LOGGER}->info("Expired","[$cnt] Removing $procDir directory");
	    $self->{CATALOGUE}->execute("rmdir",$procDir,"-r") or $self->{LOGGER}->info("Expired", "Error deleting the directory $procDir");
	  }
      }

      # insert master job into the archive
      $self->{DB}->insertEntry("$self->{DB}->{QUEUEARCHIVE}",$_) or
	  print STDERR "Expired: cannot copy entry $_->{queueId} to $self->{DB}->{QUEUEARCHIVE}\n"
	      and return;

      # delete master job from the regular queue
      $self->{DB}->deleteJob("$_->{queueId}") or
          print STDERR "Expired: cannot delete entry $_->{queueId} from QUEUE\n"
              and return;
      
      # remove /proc entry
      if ( (defined $_->{queueId}) && ( $_->{queueId} ne "") && ( $_->{queueId} > 0 ) ) {
        my $procDir = AliEn::Util::getProcDir(undef, $_->{submitHost}, $_->{queueId});
	$self->{LOGGER}->info("Expired","[$cnt] Removing $procDir directory");
	$self->{CATALOGUE}->execute("rmdir",$procDir,"-r") or $self->{LOGGER}->info("Expired", "Error deleting the directory $procDir");
      }
  }

  undef $data;

  $self->{LOGGER}->$method("Expired", "Archiving all finished jobs older than 4 weeks");

  undef $data;

  $data = $self->{DB}->getFieldsFromQueueEx("*","where ( ( (status='DONE') || (status='FAILED') || (status='EXPIRED') || (status like 'ERROR%') || (status='KILLED') ) && ( ($now-received) > (28*86540) ) ) ORDER by queueId");
  $cnt=0;
  foreach (@$data) {
      $cnt++;
      $self->{LOGGER}->info("Expired","[$cnt] Found standard job $_->{queueId} > 4 weeks finished  in status $_->{status}");

      # insert master job into the archive
      $self->{DB}->insertEntry("$self->{DB}->{QUEUEARCHIVE}",$_) or
	  print STDERR "Expired: cannot copy entry $_->{queueId} to $self->{DB}->{QUEUEARCHIVE}\n"
	      and next;

      # delete master job from the regular queue
      $self->{DB}->deleteJob("$_->{queueId}") or
          print STDERR "Expired: cannot delete entry $_->{queueId} from QUEUE\n"
              and return;

      # remove /proc entry
      if ( (defined $_->{queueId}) && ( $_->{queueId} ne "") && ( $_->{queueId} > 0 ) ) {
        my $procDir = AliEn::Util::getProcDir(undef, $_->{submitHost}, $_->{queueId});
	$self->{LOGGER}->info("Expired","[$cnt] Removing $procDir directory");
	$self->{CATALOGUE}->execute("rmdir",$procDir,"-r") or $self->{LOGGER}->info("Expired", "Error deleting the directory $procDir");
      }
  }

  undef $data;

  $self->{LOGGER}->$method("Expired", "In checkWakesUp going back to sleep");

  return;
}


1
