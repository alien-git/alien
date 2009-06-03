package AliEn::LQ::LCG;

use AliEn::LQ;
use vars qw (@ISA);
@ISA = qw( AliEn::LQ);

use strict;
use AliEn::Database::CE;
use AliEn::Classad::Host;
use Data::Dumper;
use File::Basename;
use Net::LDAP;
use AliEn::TMPFile;
use POSIX ":sys_wait_h";
use Time::Local;

sub initialize {

   my $self=shift;
   $self->SUPER::initialize() or return;
   $self->{DB}=AliEn::Database::CE->new();
   $ENV{X509_CERT_DIR} and $self->{LOGGER}->debug("LCG","X509: $ENV{X509_CERT_DIR}");
   my $host= `/bin/hostname` || $self->{CONFIG}->{HOST};
   chomp $host;
 
   $self->{CONFIG}->{VOBOX} = $host.':8084';
   $ENV{ALIEN_CM_AS_LDAP_PROXY} and $self->{CONFIG}->{VOBOX} = $ENV{ALIEN_CM_AS_LDAP_PROXY};
   $self->info("This VO-Box is $self->{CONFIG}->{VOBOX}, site is \'$ENV{SITE_NAME}\'");
   $self->{CONFIG}->{VOBOXDIR} = "/opt/vobox/\L$self->{CONFIG}->{ORG_NAME}";
   $self->{UPDATECLASSAD} = 0;
   
   my $cmds = {  SUBMIT_CMD  => 'glite-wms-job-submit',
                 STATUS_CMD  => 'glite-wms-job-status',
		 KILL_CMD    => 'glite-wms-job-cancel',
		 CLEANUP_CMD => 'glite-wms-job-output',
                 DELEGATION_CMD => 'glite-wms-job-delegate-proxy',
		 MATCH_CMD   => 'glite-wms-job-list-match' };
			 
   $self->{$_} = $cmds->{$_} || $self->{CONFIG}->{$_} || '' foreach (keys %$cmds);
   
   unless ( $ENV{LCG_GFAL_INFOSYS} ) {
     $self->{LOGGER}->error("\$LCG_GFAL_INFOSYS not defined in environment");
     return;
   }    
   $self->{CONFIG}->{CE_SITE_BDII} = '';
   if ($ENV{CE_SITE_BDII}) {
     $self->{CONFIG}->{CE_SITE_BDII} = $ENV{CE_SITE_BDII};
   } else {
      $self->info("No site BDII defined in \$ENV, querying $ENV{LCG_GFAL_INFOSYS}");
      my $IS = "ldap://$ENV{LCG_GFAL_INFOSYS}";
      my $DN = "mds-vo-name=$ENV{SITE_NAME},mds-vo-name=local,o=grid";
      $self->debug(1,"Querying $IS/$DN");
      if (my $ldap =  Net::LDAP->new($IS)) {
        if ($ldap->bind()) {
	  my $result = $ldap->search( base   => $DN,
  		                      filter => "GlueServiceType=bdii_site");
          my $code = $result->code;
	  unless ($code) {
	    my $entry  = $result->entry(0);
	    my $thisDN = $entry->dn;
	    $self->debug(1,"Found $thisDN");
            my $found = $entry->get_value("GlueServiceEndpoint");
  	    $self->{CONFIG}->{CE_SITE_BDII} = $found;
	  } else {
	    my $msg = $result->error();
	    $self->{LOGGER}->error("LCG","Error querying: $code ($msg)");
	  }			 
	  $ldap->unbind();	
        } else {
	  $self->{LOGGER}->error("LCG","Could not bind to $IS");
	}
     } else {
       $self->{LOGGER}->error("LCG","Could not contact $IS");
     }
   }    
   if ($self->{CONFIG}->{CE_SITE_BDII}) {
     $self->info("Site BDII is $self->{CONFIG}->{CE_SITE_BDII}"); 
   } else {
     $self->{LOGGER}->warning("LCG","No site BDII defined and could not find one in IS");
   }  
   if ( $ENV{CE_LCGCE} ) {
     $self->info("Taking the list of CEs from \$ENV: $ENV{CE_LCGCE}");
     my $string = $ENV{CE_LCGCE};
     my @sublist = ($string =~ /\(.+?\)/g);
     $string =~ s/\($_\)\,?// foreach (@sublist);
     push  @sublist, split(/,/, $string);
     $self->{CONFIG}->{CE_LCGCE_LIST} = \@sublist;
   }
   
   # Flat-out sublist in CE list
   my @flatlist = ();
   foreach my $CE ( @{$self->{CONFIG}->{CE_LCGCE_LIST}} ) {
     $CE =~ s/\s*//g;
     if (  $CE =~ m/\(.*\)/ ) {
       $CE =~ s/\(//; $CE =~ s/\)//;
       push @flatlist, split (/,/,$CE);
     } else {
       push @flatlist, $CE;
     }
   }
   $self->{CONFIG}->{CE_LCGCE_LIST_FLAT} = \@flatlist;
      
   # Read RB list and generate config files if needed

   unlink "$self->{CONFIG}->{LOG_DIR}/lastGoodRB" if ( -e "$self->{CONFIG}->{LOG_DIR}/lastGoodRB");
   my @list;
   my @wmslist;
   if ( $ENV{CE_RBLIST} ) { 
       $self->info("Taking the list of RBs from \$ENV: $ENV{CE_RBLIST}");
       @list=split(/,/,$ENV{CE_RBLIST});
       @wmslist=split(/:/,$ENV{CE_RBLIST});
       $self->{CONFIG}->{CE_RB_LIST} = \@list;
   }
 
   $self->renewProxy(100000);

   foreach my $thisWMS (@wmslist){
       
       if( !-e "$self->{CONFIG}->{LOG_DIR}/$thisWMS.vo.conf" ){
	   $self->info("In addition, Config file for $thisWMS  not there, creating it.");
	   my $wmsurl = "\"https://$thisWMS:7443/glite_wms_wmproxy_server\"";
	   open WMSVOCONF, ">$self->{CONFIG}->{LOG_DIR}/$thisWMS.vo.conf" or return;
	   print WMSVOCONF "[
           VirtualOrganisation = \"alice\";
           EnableServiceDiscovery  =  false;
           WMProxyEndpoints    = {$wmsurl};
           MyProxyServer       = \"myproxy.cern.ch\";\n]\n";
	   close WMSVOCONF;
       }
  #     my @command = ($self->{DELEGATION_CMD},"-c","$self->{CONFIG}->{LOG_DIR}/$thisWMS.vo.conf","-d","$proxy_delegated");
    my @command = ($self->{DELEGATION_CMD},"-c","$self->{CONFIG}->{LOG_DIR}/$thisWMS.vo.conf","-d","$ENV{DEL_PROXY}");   
   my @output = $self->_system(@command);
       my $error = $?;
      print "AQUI::::::::::::::::::::::::::::::::::::::::::::::::::;@output\n";

   }

   opendir CONFDIR, $self->{CONFIG}->{LOG_DIR};
   while (my $name = readdir CONFDIR){
       my @tmp_individualwms = ();
       foreach my $thisRB ( @{$self->{CONFIG}->{CE_RB_LIST}} ) {
	   (my $wmslist = $thisRB) =~ s/:/_/g;
	   
	   my @tmp_vectorwms = ();
	   push (@tmp_vectorwms,"\"https://$_:7443/glite_wms_wmproxy_server\"") foreach (split(/:/,$thisRB));
	   
	   if( !-e "$self->{CONFIG}->{LOG_DIR}/$wmslist.vo.conf" ){ 
	       my $streamwms = join ",",@tmp_vectorwms;
	       $self->info("Config file for $wmslist not there, creating it."); 
	       open STVOCONF, ">$self->{CONFIG}->{LOG_DIR}/$wmslist.vo.conf" or return;
	       print STVOCONF "[
           VirtualOrganisation = \"alice\";
           EnableServiceDiscovery  =  false;
           WMProxyEndpoints    = {$streamwms};
           MyProxyServer       = \"myproxy.cern.ch\";\n]\n";
	       close STVOCONF;
	   }
       }
       
   }
   
   $self->{CONFIG}->{CE_MINWAIT} = 180; #Seconds
   defined $ENV{CE_MINWAIT} and $self->{CONFIG}->{CE_MINWAIT} = $ENV{CE_MINWAIT};
   $self->info("Will wait at least $self->{CONFIG}->{CE_MINWAIT}s between submission loops.");
   $self->{LASTCHECKED} = time-$self->{CONFIG}->{CE_MINWAIT};
   
   return 1;
}

sub submit {
  my $self = shift;
  my $jdl = shift;
  my $command = shift;
  
  my $timelimit = 3600; # 1 hour
  my $testtime = time;
  my $proxy_delegated = $self->{CONFIG}->{DEL_PROXY};

  my $startTime = time;
  my @args=();
  $self->{CONFIG}->{CE_SUBMITARG_LIST} and @args = @{$self->{CONFIG}->{CE_SUBMITARG_LIST}};
  my $jdlfile = $self->generateJDL($jdl, $command);
  $jdlfile or return;

   my $timediff = $testtime - $self->{INITIME};

   print "CHECKING THE TIME:::::::::::::::::::::::::$timediff, $testtime, $self->{INITIME}\n";
   if ($timediff >= $timelimit){
      print "RENEW THE PROXY ONTO THE SUBMISSION TOOL\n"; 
      $self->renewProxy(100000); 



       my @wmslist;
       if ( $ENV{CE_RBLIST} ) {
          $self->info("Taking the list of RBs from \$ENV: $ENV{CE_RBLIST}");
          @wmslist=split(/:/,$ENV{CE_RBLIST});
       }
      foreach my $thisWMS (@wmslist){
	  my @command = ($self->{DELEGATION_CMD},"-c","$self->{CONFIG}->{LOG_DIR}/$thisWMS.vo.conf","-d","$ENV{DEL_PROXY}");
	   
      }
   } 	

  $self->info("Submitting to LCG with \'@args\'.");
  my $now = time;
  my $logFile = AliEn::TMPFile->new({filename=>"job-submit.$now.log"}) 
     or return;

  my $contact = '';
  if ( defined $ENV{CE_RBLIST} ) {
    my $lastGoodRB = $self->{CONFIG}->{CE_RB_LIST}->[0];
    $lastGoodRB =~ s/:/_/g;
    $self->debug(1,"Default RB is $lastGoodRB");
    if ( -e "$self->{CONFIG}->{LOG_DIR}/lastGoodRB") {
      my $timestamp = (stat("$self->{CONFIG}->{LOG_DIR}/lastGoodRB"))[9];
      my $elapsed = (time-$timestamp)/60;
      $self->debug(1,"Last RB was first used $elapsed minutes ago.");
      if ($elapsed > 120) {  ##minutes
	unlink "$self->{CONFIG}->{LOG_DIR}/lastGoodRB";    
      } else {
	if (open LASTGOOD, "<$self->{CONFIG}->{LOG_DIR}/lastGoodRB") {
          my $last = <LASTGOOD>;
          chomp $last;
          $self->info("Last RB was $last");
          foreach (@{$self->{CONFIG}->{CE_RB_LIST}}) {
            if ($_ eq $last) {
              $lastGoodRB = $last;
              last;
            } #Don't use it if it's not in the current list
          }
          close LASTGOOD;
	} else {
          $self->{LOGGER}->error("LCG","Could not open $self->{CONFIG}->{LOG_DIR}/lastGoodRB");
	}
      } 
    } else {
      if ( open LASTGOOD, ">$self->{CONFIG}->{LOG_DIR}/lastGoodRB" ) {
	$self->debug(1,"Saving $lastGoodRB in $self->{CONFIG}->{LOG_DIR}/lastGoodRB");
	print LASTGOOD "$lastGoodRB\n";
	close LASTGOOD;
	$self->{LOGGER}->error("LCG","Could not save $self->{CONFIG}->{LOG_DIR}/lastGoodRB");
      }
    }
    $self->info("Will use $lastGoodRB");
    $contact = $self->wrapSubmit($lastGoodRB, $logFile, $jdlfile, @args);

    unless ( $contact ) {
      redoit:foreach ( @{$self->{CONFIG}->{CE_RB_LIST}} ) { 
	next redoit if ( $_ eq $lastGoodRB ); ##This one just failed
	$contact = $self->wrapSubmit($_, $logFile, $jdlfile, @args);
	next redoit unless $contact; 
	if ( open LASTGOOD, ">$self->{CONFIG}->{LOG_DIR}/lastGoodRB" ) {
          $self->debug(1,"Found a good one, will use $_ from now on");
          print LASTGOOD "$_\n";
          close LASTGOOD;
	} else {
          $self->{LOGGER}->error("LCG","Could not save $self->{CONFIG}->{LOG_DIR}/lastGoodRB");
	}
	last;
      }
    }
  } else {
    $self->info("Failover submission not configured.");
    $contact = $self->wrapSubmit("", $logFile, $jdlfile, @args);
  } 

  $self->info("LCG JobID is $contact");
  $self->{LAST_JOB_ID} = $contact;
  open JOBIDS, ">>$self->{CONFIG}->{LOG_DIR}/CE.db/JOBIDS";
  print JOBIDS "$now,$contact\n";
  close JOBIDS;

  my $submissionTime = time - $startTime;
  $self->info("Submission took $submissionTime sec.");
  return 0;
}

sub kill {
     my $self    = shift;
     my $queueid = shift;
     $queueid or return;
     my ($contact )= $self->getContactByQueueID($queueid);
     if (!$contact) {
       $self->{LOGGER}->error("LCG", "The job $queueid is not here");
       return 1;
     }

     $self->info("Killing job $queueid, JobID is $contact");

     my $error = system( $self->{KILL_CMD},  "--noint","$contact" );
     return $error;
}

sub getBatchId {
   my $self=shift;
   return $self->{LAST_JOB_ID};
}

sub getStatus { ### This is apparently unused
     my $self = shift;
     my $queueid = shift;
     $queueid or return;
     $self->info("GetStatus: getting status from LCG for $queueid");
     my $LCGStatus =  $self->getJobStatus($self->getContactByQueueID($queueid));
     $LCGStatus or return 'DEQUEUED';
     chomp $LCGStatus;


     $self->debug(1,"LCG Job $queueid is $LCGStatus");
     if ( $LCGStatus eq "Aborted" ||
         $LCGStatus eq "Cleared" ||
         $LCGStatus eq "Done(Failed)" ||
         $LCGStatus eq "Done(Cancelled)" ||
         $LCGStatus eq "Cancelled")  {
          return 'DEQUEUED';
     }
     if ( $LCGStatus eq "Done(Success)" )  {
        my $outdir = "$self->{CONFIG}->{TMP_DIR}/JobOutput.$queueid";
        my ($contact )= $self->getContactByQueueID($queueid);
        $contact or return;
        $self->info("Will retrieve OutputSandbox for job $queueid, JobID is $contact");
        system("mkdir -p $outdir");
        system("$self->{CE_CLEANUPCMD} --noint --dir $outdir $contact"); 
        return 'DEQUEUED';
     }
     return 'QUEUED';
}

sub getAllBatchIds {
  my $self = shift;
  $self->{DB}->delete("JOBAGENT", "batchId is null");
  my $jobIds = $self->{DB}->queryColumn("SELECT batchId FROM JOBAGENT where status<>'DEAD'");
  my %queuedJobs = ();
  $queuedJobs{$_}=1 foreach @$jobIds;
  my $before = scalar keys %queuedJobs;
  while ( @$jobIds ) { 
    my @someJobs = splice(@$jobIds,0,25);
    my $logfile = AliEn::TMPFile->new({ ttl => '12 hours'});
    my @output=$self->_system($self->{STATUS_CMD}, "--noint", 
                                                   "-v", "2",
                                                   "--logfile", $logfile,
						   @someJobs);
    my $status = '';
    my $JobId = '';
    my $time = '';
    my @result = ();
    my $newRecord = 1;
    foreach ( @output ) {
      chomp;
      if (m/\*\*\*\*\*\*\*\*/) {
	if ($newRecord) { # First line of record, reset
	  $time     = 0;
          $status   = '';
          $JobId    = '';
          $newRecord = 0;
	} else { # Last line of record, dump
	  my $elapsed = (time-$time)/60;
          $self->info("Job $JobId is $status since $elapsed minutes");
	  my $RB = $JobId;
	  $RB =~ s/^https:\/\///;
	  $RB =~ s/:.*$//;
	  if ($status =~ m/\s*(Done\(Success\))|(Done\(Failed\))|(Aborted)|(Cleared)|(Cancelled)/) {
            $self->info("Marking job $JobId as dead");
	    delete($queuedJobs{$JobId});    
            $self->{DB}->update("JOBAGENT", {status=>"DEAD"}, "batchId=?", {bind_values=>[$JobId]});
          } elsif ($status =~ m/\s*Waiting/ && $elapsed>120) { ###
	    $self->{LOGGER}->error("LCG","Job $JobId has been \'Waiting\' for $elapsed minutes");
            $self->info("Marking job $JobId as dead");
	    delete($queuedJobs{$JobId});    
            my $logfile = AliEn::TMPFile->new({ ttl      => '12 hours',
	                                        filename => "edg-job-cancel.log"});            
	    my @output = $self->_system( $self->{KILL_CMD}, "--noint", "--logfile", $logfile, "$JobId" ); 
            $self->{DB}->update("JOBAGENT", {status=>"DEAD"}, "batchId=?", {bind_values=>[$JobId]});
	  }
	  $newRecord = 1;
	}
	next;
      }
      if (m/Status info for the Job/) {
	(undef,$JobId) = split /:/,$_,2;  
	$JobId =~ s/\s//g; 
	next;
      } elsif ( m/Current Status/) {
	(undef,$status) = split /:/;
	$status =~ s/\s//g;     
	next;
      } elsif ( m/Stateentertime/) { 
        (undef,$time) = split /=/,$_,2;
        $time =~ s/^\s+//;    
	my ( undef, $m, $d, $hrs, $min, $sec, $y ) = 
	   ($time =~ /([A-Za-z]+)\s+([A-Za-z]+)\s+(\d+)\s+(\d+):(\d+):(\d+)\s+(\d+)/);
	$m = { Jan => 0, Feb => 1, Mar => 2, Apr => 3,
	       May => 4, Jun => 5, Jul => 6, Aug => 7,
	       Sep => 8, Oct => 9, Nov => 10, Dec => 11 }->{"$m"};
	$time = timelocal($sec,$min,$hrs,$d,$m,$y-1900); 
       next;
      }
    }  
  }
  return keys %queuedJobs;
}

sub getQueueStatus { ##Still return values from the local DB
  my $self = shift;
  my $value = $self->{DB}->queryValue("SELECT COUNT (*) FROM JOBAGENT WHERE status<>'DEAD'");
  $value or $value = 0;
  return $value;
}

sub getNumberRunning() {
  my $self = shift;
  my $now = time;
  if ( $now < $self->{LASTCHECKED}+$self->{CONFIG}->{CE_MINWAIT} ) {
    my $still = $self->{LASTCHECKED}+$self->{CONFIG}->{CE_MINWAIT}-$now;
    $self->info("Checking too early, still $still sec to wait");
    return;
  }
  my ($run,$wait) = $self->getCEInfo(qw(GlueCEStateRunningJobs GlueCEStateWaitingJobs ));
  my $value = $self->getQueueStatus();
  $value or $value = 0;
  $run or $run=0;
  $wait or $wait=0;

  $self->info("Jobs: $run running, $wait waiting from IS, $value from local DB");
  if ( $run =~ m/4444/ || $wait =~ m/4444/ ) {
    $self->{LOGGER}->error("LCG","IS failure 4444");
    return;
  }
  return $run+$wait;    
}

sub getNumberQueued() {
  my $self=shift;
  my ($wait,$cpu) = $self->getCEInfo(qw(GlueCEStateWaitingJobs));
  $wait or $wait=0;
  my $value = $self->{DB}->queryValue("SELECT COUNT (*) FROM JOBAGENT where status='QUEUED'");
  $value or $value = 0;
  $self->info("Queued: $wait from IS, $value from local DB");
  if ( $wait =~ m/4444/ ) {
    $self->{LOGGER}->error("LCG","IS failure 4444");
    return;
  }
  return $wait;
}

sub cleanUp {
  my $self = shift;
  ### The following in principle should not be needed
  my $todelete = $self->{DB}->queryValue("SELECT COUNT (*) FROM JOBAGENT where status='DEAD'");
  if ($todelete) {
    $self->info("Will remove $todelete dead job agents from DB");
    $self->{DB}->delete("JOBAGENT", "status='DEAD'");
  }
  my $jobIds = $self->{DB}->query("SELECT batchId,timestamp FROM TOCLEANUP");
  
  foreach ( splice (@$jobIds,0,20) ) { #Up to 20 at a time
    my $age = (time - $_->{'timestamp'});
    if ( $age < 60*60*24*3 ) {
      if ( $_->{'batchId'} ) {
	my $status = $self->getJobStatus($_->{'batchId'});
	if ( $status eq 'Aborted' || $status eq 'Cancelled' || $status eq 'Done(Failed)') {
	  $self->info("Job $_->{'batchId'} was aborted, cancelled or failed, no logs to retrieve");
	} elsif ( $status eq 'Running' || $status eq 'Waiting' || $status eq 'Scheduled') {
          $self->info("Job $_->{'batchId'} is still \'$status\'");
          next;
	} elsif ( $status eq 'Cleared' ) {
          $self->info("Output for job $_->{'batchId'} was already downloaded, this is funny...");
          $self->info("Will remove DB entry for $_->{batchId}");
          $self->{DB}->delete("TOCLEANUP","batchId=\'$_->{batchId}\'") if $_->{batchId};
          next;
	} else {
	  $self->info("Will retrieve OutputSandbox for $status job $_->{'batchId'}");
          my $logfile = AliEn::TMPFile->new({ ttl      => '24 hours',
                                              filename => "edg-job-get-output.log"});
          my $outdir = dirname($logfile); 
	  my @output = $self->_system($self->{CLEANUP_CMD},"--noint",
                                                	   "--logfile", $logfile,
					        	   "--dir", $outdir,
					        	   $_->{'batchId'} );
	  						   
	  if ( $? ) {						     
	    $self->info("Could not retrieve output for $_->{'batchId'}: @?");
            next;
	  }
	}
      } else {
	$self->{LOGGER}->error("LCG","There is no LCG JobID in this entry!");
	next;
      }
    } else {
      	$self->info("Job $_->{'batchId'} is more than three days old ($age) and is beginning to smell");
    } 
    $self->info("OK, will remove DB entry for $_->{batchId}");
    $self->{DB}->delete("TOCLEANUP","batchId=\'$_->{batchId}\'") if $_->{batchId};
  }
  return 1;
}

sub needsCleaningUp {
  return 1;
}

#
#---------------------------------------------------------------------
#

sub wrapSubmit {
  my $self = shift;
  my $RB = shift;
  my $logFile = shift;
  my $jdlfile = shift;
#  my $proxy_delegated = "AliceProxyDel";
  my @args = @_ ;  
  my @command = ( $self->{SUBMIT_CMD}, 
                  "--noint", 
		  "--nomsg");
  @command = ( @command,		   
	       "--config", "$self->{CONFIG}->{LOG_DIR}/$RB.vo.conf") if $RB;
  @command = ( @command,
	       "--logfile", $logFile, 
	       @args, "$ENV{DEL_PROXY}",
	       "$jdlfile");
  my @output = $self->_system(@command);
  my $error = $?;
  (my $jobId) = grep { /https:/ } @output;
  return if ( $error || !$jobId);
  $jobId =~ m/(https:\/\/[A-Za-z0-9.-]*:9000\/[A-Za-z0-9_-]{22})/;
  $jobId = $1;
  chomp $jobId;
  return $jobId;
}

sub getCEInfo {
  my $self = shift;
  my @items = @_;
  my %results = ();
  my $someAnswer = 0;
  $self->debug(1,"Querying all CEs, requested info: @items");
  foreach my $CE ( @{$self->{CONFIG}->{CE_LCGCE_LIST}} ) {
    # If it's a sublist take only the first one to avoid 
    # double counting (all CEs in sublist see the same resources)
    $CE =~ s/\s*//g; $CE =~ s/\(//; $CE =~ s/\)//;
    ($CE, undef) = split (/,/,$CE,2);
    ($CE, undef) = split (/=/,$CE,2) if ($CE =~/=/);

    $self->debug(1,"Querying for $CE");
    (my $host,undef) = split (/:/,$CE);    
    my $res = $self->queryBDII($CE,'',"GlueVOViewLocalID=\L$self->{CONFIG}->{ORG_NAME}\E,GlueCEUniqueID=$CE",@_);
    if ( $res ) {
      $someAnswer++;
      $results{$_}+=$res->{$_} foreach (@items);
    } else { 
      $self->{LOGGER}->warning("LCG","Query for $CE failed.");
      next;
    }
  }  
  unless ($someAnswer) {
    $self->{LOGGER}->error("LCG","No CE answered our queries!");
    return;
  } 
  $self->debug(1,"Got $someAnswer answers from CEs");
  my @values = ();
  push (@values,$results{$_}) foreach (@items);
  $self->debug(1,"Returning: ".Dumper(@values));
  return @values;
}

sub queryBDII {
  my $self = shift;
  my $CE = shift;
  my $filter = shift;
  $filter or $filter = "objectclass=*";
  my $base = shift;
  $base or $base = "GlueVOViewLocalID=\L$self->{CONFIG}->{ORG_NAME}\E,GlueCEUniqueID=$CE";
  my @items = @_;
  my %results = ();
  my $someAnswer = 0;
  $self->info("Querying $CE for @items");
  $self->debug(1,"DN string is $base");
  $self->debug(1,"Filter is $filter");
  (my $host,undef) = split (/:/,$CE);    
  my @IS  = (
              "ldap://$host:2170,mds-vo-name=resource,o=grid", # Resource BDII
            );
  @IS = (@IS,$self->{CONFIG}->{CE_SITE_BDII}) if ( defined $self->{CONFIG}->{CE_SITE_BDII} );  
#  if ( defined $ENV{LCG_GFAL_INFOSYS} ) { # Top-level BDII
#    @IS = (@IS,"ldap://$ENV{LCG_GFAL_INFOSYS},mds-vo-name=$ENV{SITE_NAME},mds-vo-name=local,o=grid"); 
#  }
  my $ldap = '';
  foreach (@IS) {
     my ($GRIS, $BaseDN) = split (/,/,$_,2);
     $self->debug(1,"Asking $GRIS/$BaseDN");
     unless ($ldap =  Net::LDAP->new($GRIS)) {
       $self->info("$GRIS/$BaseDN not responding (1), trying next.");
       next;
     }
     unless ($ldap->bind()) {
       $self->{LOGGER}->info("$GRIS/$BaseDN not responding (2), trying next.");
       next;
     }
     my $result = $ldap->search( base	=> "$base,$BaseDN",
  				 filter => "$filter");
     my $code = $result->code;				 
     my $msg = $result->error;				 
     if ($code) {
       $self->{LOGGER}->warning("LCG","\"$msg\" ($code) from $GRIS/$BaseDN, trying next.");
       next;
     }
     if ( ($result->all_entries)[0] ) {
       foreach (@items) {
  	 my $value = (($result->all_entries)[0])->get_value("$_");
  	 $self->debug(1, "$_ for $CE is $value");
  	 $results{$_} = $value;
       }
       my $message = "OK, got an answer from $GRIS/$BaseDN: ";
       $message = $message.$results{$_}." " foreach keys %results;
       $self->info($message);
       $someAnswer++;
       last;
     } else {
       $self->{LOGGER}->warning("LCG","The query to $GRIS/$BaseDN did not return any value, trying next.");
     }
     $ldap->unbind();
  }
  unless ($someAnswer) {
    $self->{LOGGER}->error("LCG","No BDII answered our queries!");
    return;
  } 
  $self->debug(1,"Returning: ".Dumper(\%results));
  return \%results;
}

sub getJobStatus {
   my $self = shift;
   my $contact = shift;
   my $pattern = shift;
   $contact or return;
   $self->info("Getting status from LCG for $contact");
   $pattern or $pattern = 'Current Status:';
   my $user = getpwuid($<);
   my @args=();
   $self->{CONFIG}->{CE_STATUSARG} and
     @args=split (/\s+/, $self->{CONFIG}->{CE_STATUSARG});
   my $logfile = AliEn::TMPFile->new({ ttl => '12 hours'});

   my @output=$self->_system($self->{STATUS_CMD}, "-noint", "--logfile", $logfile, @args,
                             "\"$contact\" | grep \"$pattern\"" );
   my $status = $output[0];
   $status or return;
   chomp $status;
   $status =~ s/$pattern//;
   $status =~ s/ //g;
   return $status;
}

sub getContactByQueueID {
   my $self = shift;
   my $queueid = shift;
   $queueid or return;
   my $contact = '';
   return $contact;
}

sub renewProxy {
   my $self = shift;
    

    $self->{INITIME}=time; 
   my $gracePeriod = shift;
   $gracePeriod or $gracePeriod = 0;
   my $duration = shift;
   $duration or $duration=$self->{CONFIG}->{CE_TTL};
   $duration or $duration = 172800; #in seconds
   $self->info("Checking whether to renew proxy for $duration seconds");
   $ENV{X509_USER_PROXY} and $self->debug(1,"\$X509_USER_PROXY is $ENV{X509_USER_PROXY}");
   my $ProxyRepository = "$self->{CONFIG}->{VOBOXDIR}/proxy_repository";
   my $voName=$ENV{ALIEN_VOBOX_ORG} || $self->{CONFIG}->{ORG_NAME};
   my $command = "vobox-proxy --vo \L$voName\E query";
   
   my @lines = $self->_system($command);
   my $dn = '';
   my $proxyfile = '';
   my $timeLeft = '';
   foreach (@lines) {
     chomp;
     m/^DN:/ and ($dn) = m/^DN:\s*(.+)\s+$/;
     m/^File:/ and ($proxyfile) = m/^File:\s*(.+)\s+$/;
     m/^Proxy Time Left/ and ($timeLeft) = m/^Proxy Time Left \(seconds\):\s*(.+)\s+$/;
   }
   $dn or $self->{LOGGER}->error("LCG","No valid proxy found.") and return;
   $self->debug(1,"DN is $dn");
   $self->debug(1,"Proxy file is $proxyfile");
   my $thres = $duration-$gracePeriod;
   $self->info("Proxy timeleft is $timeLeft (threshold is $thres)");
   return 1 if ( $gracePeriod && $timeLeft>$thres );
   # I apparently cannot pass this via an argument
   my $currentProxy = $ENV{X509_USER_PROXY};
   $self->{LOGGER}->warning("LCG","\$X509_USER_PROXY different from the proxy we are renewing") if ($currentProxy ne $proxyfile);
   $self->{LOGGER}->warning("LCG","$currentProxy and $proxyfile ") if ($currentProxy ne $proxyfile);
   $ENV{X509_USER_PROXY} = "$self->{CONFIG}->{VOBOXDIR}/renewal-proxy.pem";
   $self->debug(1,"Renewing proxy for $dn for $duration seconds");
   my @command=("$ENV{LCG_LOCATION}/bin/lcg-proxy-renew", "-a", "$proxyfile",
	        "-d", "-t",int($duration/3600).":", #in hours
	        "-o", "/tmp/tmpfile.$$" , "--cert", $ENV{X509_USER_PROXY}, 
	        "--key", $ENV{X509_USER_PROXY});
   my $oldPath=$ENV{PATH};
   my $pattern="$ENV{ALIEN_ROOT}"."[^:]*:";
   $ENV{PATH}=~ s/$pattern//g;
   unless ( $self->_system(@command) ) {
     $ENV{PATH}=$oldPath;
      $self->{LOGGER}->error("LCG","unable to renew proxy");
      $ENV{X509_USER_PROXY} = $currentProxy;
      return;
   }
   $ENV{PATH}=$oldPath;

   @command = ("mv", "-f", "/tmp/tmpfile.$$", "$proxyfile");
   if ( $self->_system(@command) ) {
     $self->{LOGGER}->error("LCG","unable to move new proxy");
     $ENV{X509_USER_PROXY} = $currentProxy;
     return;
   }  
   $command = "vobox-proxy --vo \L$self->{CONFIG}->{ORG_NAME}\E --dn \'$dn\' query-proxy-timeleft";
   ( my $realDuration ) = $self->_system($command);
   chomp $realDuration;
   $self->{LOGGER}->error("LCG","asked for $duration sec, got only $realDuration") if ( $realDuration < 0.9*$duration);
   $ENV{X509_USER_PROXY} = $currentProxy;

   return 1;
}

sub updateClassAd {
  my $self = shift;
  $self->debug(1,"Updating host classad from IS...");
  my $classad = shift;
  $classad or return;
  my ($maxRAMSize, $maxSwapSize) = (0,0);
  foreach my $CE (@{$self->{CONFIG}->{CE_LCGCE_LIST_FLAT}}) {
    $self->debug(1,"Getting RAM and swap info for $CE");
    my $res = $self->queryBDII($CE,'',"GlueCEUniqueID=$CE",'GlueForeignKey');
    $res or return;
    my $cluster = $res->{'GlueForeignKey'};
    $cluster =~ s/^GlueClusterUniqueID=//;
    $self->debug(1,"Cluster name from IS is $cluster");
    $res = $self->queryBDII($CE,'(GlueHostMainMemoryRAMSize=*)',"GlueClusterUniqueID=$cluster",qw(GlueHostMainMemoryRAMSize GlueHostMainMemoryVirtualSize));
    $res or return;
    $maxRAMSize  = $res->{'GlueHostMainMemoryRAMSize'}  if ($res->{'GlueHostMainMemoryRAMSize'}>$maxRAMSize );
    $maxSwapSize = $res->{'GlueHostMainMemoryVirtualSize'} if ($res->{'GlueHostMainMemoryVirtualSize'}>$maxSwapSize );
  }  
  $self->{UPDATECLASSAD} = time();    
  $self->info("Updating host ClassAd from IS (RAM,Swap) = ($maxRAMSize,$maxSwapSize)" );
  $classad->set_expression("Memory",$maxRAMSize*1024);
  $classad->set_expression("Swap",$maxSwapSize*1024);
  $classad->set_expression("FreeMemory",$maxRAMSize*1024);
  $classad->set_expression("FreeSwap",$maxSwapSize*1024);
  return $classad;
  return;
}

sub translateRequirements {
  my $self = shift;
  my $ca = shift;
  my $requirements= shift || "";;

  $ca or return $requirements;

  my ($ok, $memory) =  $ca->evaluateAttributeString("Memory");
  if ($memory) {
    $self->info("Translating \'Memory\' requirement ($ok,$memory)");
    $requirements .= "&& other.GlueHostMainMemoryRAMSize>=$memory";
  }
  ($ok, my $swap) =  $ca->evaluateAttributeString("Swap");
  if ($swap) {
    $self->info("Translating \'Swap\' requirement ($ok,$swap)");
    $requirements .= "&& other.GlueHostMainMemoryVirtualSize>=$swap";
  }
  ($ok,my  $ttl)= $ca->evaluateAttributeString("Requirements");
  if ($ttl and $ttl =~ /TTL\s*[=>]*\s*(\d+)/ ) {
     $self->info("Translating \'TTL\' requirement ($1)");
     $requirements .= "&& other.GlueCEPolicyMaxWallClockTime>=".$1/60; #minutes
   }
  return $requirements;
}

sub generateJDL {
  my $self = shift;
  my $ca = shift;
  my $command=shift;
  my $bdiiReq=shift;
  my $currenttime = time;
  my $delaytime = 900; #15 minutos

  my $requirements = $self->translateRequirements($ca, $bdiiReq);

  # implementation for the WMS: Avoid any resubmission of jobs
  my $newtime = $currenttime + $delaytime;

  my $exeFile = AliEn::TMPFile->new({filename=>"dg-submit.$$.sh"})
    or return;

  open( BATCH, ">$exeFile" )
    or print STDERR "Can't open file '$exeFile': $!"
      and return;
  print BATCH "\#!/bin/sh
\# Script to run AliEn on LCG
\# Automatically generated by AliEn running on $ENV{HOSTNAME}

export OLDHOME=\$HOME
export HOME=`pwd`
export ALIEN_LOG=$ENV{ALIEN_LOG}
echo --- hostname, uname, whoami, pwd --------------
hostname
uname -a
whoami
pwd
echo --- ls -la ------------------------------------
ls -lart
echo --- df ----------------------------------------
df -h
echo --- free --------------------------------------
free

";

  my $exec="alien";
  if ( $self->{CONFIG}->{CE_INSTALLMETHOD}) {
    $exec= "\$HOME/bootsh /opt/alien/bin/alien";
    my $method="installWith".$self->{CONFIG}->{CE_INSTALLMETHOD};
    eval {
      ($exec, my $print)=$self->$method();
      print BATCH $print;
    };
    if ($@){
      $self->info("Error calling $method: $@");
      return;

    };
  } else {
    print BATCH "export PATH=\$PATH:\$VO_ALICE_SW_DIR/alien/bin\n";
  }

  print BATCH "
cd \${TMPDIR:-.}
echo --- env ---------------------------------------
echo \$PATH
echo \$LD_LIBRARY_PATH

echo --- alien --printenv --------------------------
$exec -printenv
echo --- alien proxy-info ---------------------------
$exec proxy-info
echo --- Run ---------------------------------------
$exec RunAgent
rm -f dg-submit.*.sh
ls -lart
";

  close BATCH or return;
  
  my $jdlFile = AliEn::TMPFile->new({filename=>"dg-submit.$$.jdl"})
    or return;
  open( BATCH, ">$jdlFile" )
    or print STDERR "Can't open file '$jdlFile': $!"
      and return;
  my $voName=$ENV{ALIEN_VOBOX_ORG}|| $self->{CONFIG}->{ORG_NAME};
  print BATCH "\# JDL automatically generated by AliEn
Executable = \"/bin/sh\";
Arguments = \"-x dg-submit.$$.sh\";
StdOutput = \"std.out\";
StdError = \"std.err\";
RetryCount = 0;
ShallowRetryCount = 0;
VirtualOrganisation = \"\L$voName\E\";
InputSandbox = {\"$exeFile\"};
ExpiryTime = $newtime;
OutputSandbox = { \"std.err\" , \"std.out\" };
Environment = {\"ALIEN_CM_AS_LDAP_PROXY=$self->{CONFIG}->{VOBOX}\",\"ALIEN_JOBAGENT_ID=$ENV{ALIEN_JOBAGENT_ID}\", \"ALIEN_USER=$ENV{ALIEN_USER}\"};
";
  if (scalar @{$self->{CONFIG}->{CE_LCGCE_LIST_FLAT}}) {
      
      my $range = 100;
      my $random_number = int(rand($range));
      my $list_of_ces = join(" ",@{$self->{CONFIG}->{CE_LCGCE_LIST_FLAT}});
      
      my %ce_hash;
      my $ce_name;
      my $ces;
      my @celist;
      if ($list_of_ces =~/=/){
	  my %ce_hash = $list_of_ces =~ /([\w:\/\-\_\.]+)=(\d+)/g;
	  my $total = 0;
	  foreach my $key (sort{ $ce_hash{$a} cmp $ce_hash{$b}} keys %ce_hash){
      
	      my @v = ($total..$total+$ce_hash{$key});    
	      $total += $ce_hash{$key};
	      foreach my $valor (@v){
		  if ($valor == $random_number){
		      $ce_name=$key;
                      $self->debug(1,"Random CE selection: $random_number $ce_hash{$key} $key $total @v");
		  }
	      }            
	  }

	  $ces="other.GlueCEUniqueID==\"$ce_name\"";
      }
      
      else{
      
	  @celist = map {"other.GlueCEUniqueID==\"$_\""} @{$self->{CONFIG}->{CE_LCGCE_LIST_FLAT}};
	  $ces=join (" || ", @celist);
      }
      print BATCH "Requirements = ( $ces )";     
      print BATCH " && ".$requirements if $requirements;
      print BATCH ";\n";
  } else {

      print BATCH "Requirements = $requirements;\n" if $requirements;
  }
  close BATCH;
  return $jdlFile;
}

sub installWithParrot {
  my $self=shift;

  return "\$HOME/bootsh /opt/alien/bin/alien", "rm -rf bootsh
wget -O bootsh http://alien.cern.ch/bootsh
chmod +x bootsh
export PATH=/opt/alien/bin:\$PATH\n";
}

sub installWithTorrent {
  my $self=shift;
  $self->info("The worker node will install with the torrent method!!!");

  return "$self->{CONFIG}->{TMP_DIR}/alien_installation.\$\$/alien/bin/alien","DIR=$self->{CONFIG}->{TMP_DIR}/alien_installation.\$\$
mkdir -p \$DIR
echo \"Ready to install alien\"
date
cd \$DIR
wget http://alien.cern.ch/alien-torrent-installer -O alien-auto-installer
export ALIEN_INSTALLER_PREFIX=\$DIR/alien
chmod +x alien-auto-installer
./alien-auto-installer -version v2-16 -skip_rc  -type workernode -batch
echo \"Installation completed!!\"

";
}

sub installWithLocal {
  my $self=shift;
  my $version=$self->{CONFIG}->{VERSION};
  $version=~ s{\..*$}{};
  my $vo_dir="VO_".uc($self->{CONFIG}->{ORG_NAME})."_SW_DIR";

  return "alien", "IDIR=\$HOME/alien_auto_install

if [ -n \"\$$vo_dir\" ]
then
    echo \"Let's try to use $vo_dir=\$$vo_dir\"
    [ -d \$$vo_dir ] || mkdir \$$vo_dir
    touch \$$vo_dir/user.\$UID.lock
    if [ \$? = \"0\" ]
    then
      echo 'The lock worked!! :)'
      rm \$$vo_dir/user.\$UID.lock
      IDIR=\$$vo_dir/alien_auto_install
    fi
fi


if [ -e \$IDIR/lock ]
then
    echo \"The lock \$IDIR/lock exists. Is anybody installing alien?\"
    echo \"Let's exit so that we do not interfere\"
    exit -2
fi
IDIR=\$IDIR/$version
if  [ -d \$IDIR  ]  && [ -f \$IDIR/bin/alien ]
then
    echo \"The installation already exists\"
else

    echo \"Let's install everything\" 
    touch \$IDIR/lock
    rm -rf alien-installer


    wget -O alien-installer http://alien.cern.ch/alien-installer
    chmod +x alien-installer
    mkdir -p \$HOME/.alien \$IDIR \${IDIR}_cache

    case `uname -m` in
      i*86*)
        PLATFORM=i686-pc-linux-gnu
       ;;
      x86_64)
        PLATFORM=x86_64-unknown-linux-gnu
        ;;
      powerpc)
        PLATFORM=powerpc-apple-darwin8.1.0
        ;;
      apple*)
        PLATFORM=i686-apple-darwin8.6.1
        ;;
      ia64)
        PLATFORM=ia64-unknown-linux-gnu
        ;;
       *)
        echo 'Unknown or unsupported platform: ' `uname -m`
        exit 1
        ;;
    esac


    wget http://alien.cern.ch/BitServers -O BitServers

    echo \"This platform is \$PLATFORM\"
    URL=`grep -v -e '^#' BitServers | grep \$PLATFORM  |awk -F \\| '{print \$2}'| awk '{print \$1}'`
    echo \"It will download from \$URL\"

    cat  >\$HOME/.alien/installer.rc <<EOF
ALIEN_INSTALLER_HOME=\${IDIR}_cache
ALIEN_INSTALLER_PREFIX=\$IDIR
ALIEN_INSTALLER_AUTODETECT=false
ALIEN_INSTALLER_TYPE=wn
ALIEN_INSTALLER_PLATFORM=\$PLATFORM
ALIEN_DIALOG=dialog
ALIEN_RELEASE=$version
ALIEN_BITS_URL=\$URL/\$PLATFORM/$version/download/
EOF
    echo \"Starting the installation\"
    date
    ./alien-installer --trace   update
    if [  $? ne 0 ]
    then
       echo \"The installer wasn't happy. Removing the installation\"
       rn -rf \$IDIR
    fi  
    rm \$IDIR/lock
    echo \"Installation finished!!\"
    date
    ls \$IDIR
    echo \"And the size\"
    du -sh \$IDIR
fi
export ALIEN_ROOT=\$IDIR
export PATH=\$ALIEN_ROOT/bin:\$PATH
";
}

sub _system {
  my $self=shift;
  my $command=join (" ", @_);
  $self->info("Doing '$command'");

  my $pid;
  local $SIG{ALRM} =sub {
    print "$$ timeout while doing '$command'\n";
    $pid and print "Killing the process $pid\n" and CORE::kill(9, $pid);

    print "Let's try to close the file handler\n";
    close FILE;
    print " $$ File closed";

    die("timeout!! ");
  };
  my @output;
  eval {
    alarm(300);
    $pid=open(FILE, "$command |") or
      die("Error doing '$command'!!\n$!");
    @output=<FILE>;

    if (! close FILE){
      #We have to check that the proces do^?^?
      print "The system call failed  PID $pid\n";
      if (CORE::kill 0,$pid) {
        my $kid;
        do {
  	  $kid = waitpid($pid, WNOHANG);
        }   until $kid > 0;
      }
    }
    alarm(0);
  };
  if ($@) {
    $self->info("Error: $@");
    close FILE;
    $pid and print "Killing the process $pid\n" and CORE::kill(9, $pid);
    alarm(0);
    return;
  }
  return @output;
}


return 1;
