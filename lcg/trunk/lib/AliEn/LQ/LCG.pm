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

   $self->{SUBMIT_CMD} = ( $self->{CONFIG}->{CE_SUBMITCMD} or "edg-job-submit" );

   $self->{STATUS_CMD} = ( $self->{CONFIG}->{CE_STATUSCMD} or "edg-job-status" );

   $self->{KILL_CMD}   = ( $self->{CONFIG}->{CE_KILLCMD} or "edg-job-cancel" );

   $self->{MATCH_CMD}  = ( $self->{CONFIG}->{CE_MATCHCMD} or '' );
   $self->{CONFIG}->{CE_MATCHARG} and  $self->{MATCH_CMD} .= " $self->{CONFIG}->{CE_MATCHARG}";
   $self->{PRESUBMIT}  = $self->{MATCH_CMD};
   
   if ( $ENV{CE_LCGCE} ) {
     $self->info("Taking the list of CEs from \$ENV: $ENV{CE_LCGCE}");
     my @list=split(/,/,$ENV{CE_LCGCE});
     $self->{CONFIG}->{CE_LCGCE_LIST} = \@list;
   }
   
   if ( $ENV{CE_RBLIST} ) { 
     $self->info("Taking the list of RBs from \$ENV: $ENV{CE_RBLIST}");
     my @list=split(/,/,$ENV{CE_LCGCE});
     $self->{CONFIG}->{CE_RB_LIST} = \@list;
   } 

   $self->{CONFIG}->{CE_MINWAIT} = 180; #Seconds
   defined $ENV{CE_MINWAIT} and $self->{CONFIG}->{CE_MINWAIT} = $ENV{CE_MINWAIT};
   $self->info("Will wait at least $self->{CONFIG}->{CE_MINWAIT}s between submission loops.");
   $self->{LASTCHECKED} = time-$self->{CONFIG}->{CE_MINWAIT};
   
   $self->renewProxy();
   
   return 1;
}

sub submit {
  my $self = shift;
  my $jdl = shift;
  my $command = shift;
  my $arguments  = join " ", @_;
  my $startTime = time;

  my @args=();
  $self->{CONFIG}->{CE_SUBMITARG_LIST} and
    @args = @{$self->{CONFIG}->{CE_SUBMITARG_LIST}};
   
  my @conf = ();
  $self->{CONFIG}->{CE_EDG_WL_UI_CONF} and
    @conf = ("--config-vo",$self->{CONFIG}->{CE_EDG_WL_UI_CONF}); 
  $ENV{CE_EDG_WL_UI_CONF} and
    @conf = ("--config-vo",$ENV{CE_EDG_WL_UI_CONF}); 
  push @args,@conf;
  my $jdlfile = $self->generateJDL($jdl, $command);
  $jdlfile or return;

  $self->renewProxy(10000); ####
  if ($self->{PRESUBMIT}){
    $self->info("Checking if there are resources that match");
    my @info=$self->_system($self->{PRESUBMIT}, $jdlfile);

    if (!grep (/The following CE\(s\) matching your job requirements have been found/ , @info)){
      $self->info("No CEs matched the requirements!!\n@info\n\n***** We don't submit the jobagent");
      return -1;
    }
  }
  $self->info("Submitting to LCG with \'@args\'.");
  my $now = time;
  my $logFile = AliEn::TMPFile->new({filename=>"job-submit.$now.log"}) ## Or better a configurable TTL?
     or return;
   
  my @command = ( $self->{SUBMIT_CMD}, "--noint", "--nomsg", "--logfile", $logFile, @args, "$jdlfile");
  my @output=$self->_system(@command) or return -1;
  my $contact="";
  @output and $contact=$output[$#output];
  $contact and chomp $contact;
  if ($contact !~ /^https:\// ) {
    $self->{LOGGER}->warning("LCG","Error submitting the job. Log file $contact");
    if ($contact){
      open (LOG, "<$contact");
      print <LOG>;
      close LOG;
    }
    return -2;
  }

  $self->info("LCG JobID is $contact");
  $self->{LAST_JOB_ID} = $contact;
  open JOBIDS, ">>$self->{CONFIG}->{LOG_DIR}/CE.db/JOBIDS";
  print JOBIDS "$now,$contact\n";
  close JOBIDS;

  my $submissionTime = time - $startTime;
  $self->info("Submission took $submissionTime sec.");
  return 0;#$error;
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
        system("edg-job-get-output --noint --dir $outdir $contact"); ####
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
	  $time     = '';
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
          } elsif ($status =~ m/\s*Waiting/ && $elapsed>120) {
	    $self->{LOGGER}->error("LCG","Job $JobId has been \'Waiting\' for $elapsed minutes");
            $self->info("Marking job $JobId as dead");
	    delete($queuedJobs{$JobId});    
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
      } elsif ( m/reached on/) {
        (undef,$time) = split /:/,$_,2;
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
  ## Number of CPUs will be wrong for multiple-CE sites; 
  ## we only check it against zero to check if GRIS is working
  my $now = time;
  if ( $now < $self->{LASTCHECKED}+$self->{CONFIG}->{CE_MINWAIT} ) {
    my $still = $self->{LASTCHECKED}+$self->{CONFIG}->{CE_MINWAIT}-$now;
    $self->info("Checking too early, still $still sec to wait");
    return;
  }
  my ($run,$wait,$cpu) = $self->getInfoFromGRIS(qw(GlueCEStateRunningJobs GlueCEStateWaitingJobs GlueCEInfoTotalCPUs));
  my $value = $self->getQueueStatus();
  $value or $value = 0;
#  $self->debug(1,"Jobs: $run+$wait from GRIS, $value from local DB");
  $self->info("Jobs: $run running, $wait waiting from GRIS, $value from local DB");
  if ( $cpu == 0 ) {
    $self->{LOGGER}->error("LCG","GRIS not responding, returning value from local DB");
    return $value;
  }
  if ( $run == 4444 || $wait == 4444 ) {
    $self->{LOGGER}->error("LCG","GRIS failure 4444, returning value from local DB");
    return $value;
  }
  return $run+$wait;    
}

sub getNumberQueued() {
  my $self=shift;
  ## Number of CPUs will be wrong for multiple-CE sites; 
  ## we only check it against zero to check if GRIS is working
  my ($wait,$cpu) = $self->getInfoFromGRIS(qw(GlueCEStateWaitingJobs GlueCEInfoTotalCPUs));
  my $value = $self->{DB}->queryValue("SELECT COUNT (*) FROM JOBAGENT where status='QUEUED'");
  $value or $value = 0;
#  $self->debug(1,"Queued: $wait from GRIS, $value from local DB");
  $self->info("Queued: $wait from GRIS, $value from local DB");
  if ( $cpu == 0 ) {
    $self->{LOGGER}->error("LCG","GRIS not responding, returning value from local DB.");
    return $value;
  }
  if ( $wait == 4444 ) {
    $self->{LOGGER}->error("LCG","GRIS failure 4444, returning value from local DB");
    return $value;
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
	if ( $status eq 'Aborted' || $status eq 'Cancelled') {
	  $self->info("Job $_->{'batchId'} was aborted or cancelled, no logs to retrieve");
	} elsif ( $status eq 'Running' ) {
          $self->info("Job $_->{'batchId'} has not yet reported being finished");
          next;
	} else {
	  $self->info("Will retrieve OutputSandbox for $status job $_->{'batchId'}");
          my $logfile = AliEn::TMPFile->new({ ttl      => '24 hours',
                                              filename => "edg-job-get-output.$_->{'batchId'}.log"});
          my $outdir = dirname($logfile); 
	  my @output = $self->_system("edg-job-get-output","--noint",
                                                	   "--logfile", $logfile,
					        	   "--dir", $outdir,
					        	   $_->{'batchId'} );
	  if ( $? ) {						     
	    my $errmesg = (split(/\s+/,(grep(/\*\*\*\* Error: /,@output))[0]))[2];						     
	    $self->info("Could not retrieve output for $_->{'batchId'}: $errmesg");
	    next unless ($errmesg =~ m/NS_JOB_OUTPUT_RETRIEVED/);
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

sub getInfoFromGRIS {
  my $self = shift;
  my @items = @_;
  my %results = ();
  foreach my $CE ( @{$self->{CONFIG}->{CE_LCGCE_LIST}} ) {
    $self->debug(1,"Querying for $CE");
    (my $host,undef) = split (/:/,$CE);
    my $GRIS = "ldap://$host:2135";
    my $BaseDN = "mds-vo-name=local,o=grid";
    $self->debug(1,"Asking $GRIS/$BaseDN");
    my $ldap =  Net::LDAP->new($GRIS) or return;
    $ldap->bind() or return;
    my $result = $ldap->search( base   =>  $BaseDN,
                                filter => "(&(objectClass=GlueCEState)(GlueCEUniqueID=$CE))");
    $result->code && return;
    if ( ($result->all_entries)[0] ) {
      foreach (@items) {
        my $value = (($result->all_entries)[0])->get_value("$_");
        $self->debug(1, "$_ for $CE is $value");
        $results{$_}+=$value;
      }
    } else {
    	$self->{LOGGER}->error("LCG","The GRIS query for $CE did not return any value");
    }
    $ldap->unbind();
  }
  my @values = ();
  push (@values,$results{$_}) foreach (@items);
  return @values;
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
   my $gracePeriod = shift;
   $gracePeriod or $gracePeriod = 0;
   my $duration = shift;
   $duration or $duration=$self->{CONFIG}->{CE_TTL};
   $duration or $duration = 100000; #in seconds
   $self->info("Checking whether to renew proxy for $duration seconds");
   $self->debug(1,"\$X509_USER_PROXY is $ENV{X509_USER_PROXY}");
   my $ProxyRepository = "$self->{CONFIG}->{VOBOXDIR}/proxy_repository";
   my $command = "vobox-proxy --vo \L$self->{CONFIG}->{ORG_NAME}\E query";
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
   my @command = ( 'myproxy-get-delegation',
                   "-a", "$proxyfile",
                   "-d",
                   "-t",int($duration/3600), #in hours
                   "-o", "/tmp/tmpfile.$$");
    		   
    unless ( $self->_system(@command) ) {
      $self->{LOGGER}->error("LCG","unable to renew proxy");
      $ENV{X509_USER_PROXY} = $currentProxy;
      return;
   }   
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
  $self->debug(1,"Updating host classad from BDII...");
  my $classad = shift;
  $classad or return;
  my $BDII = $self->{CONFIG}->{CE_LCG_GFAL_INFOSYS};
  $BDII = "ldap://$ENV{LCG_GFAL_INFOSYS}" if defined $ENV{LCG_GFAL_INFOSYS};
  $self->debug(1,"BDII is $BDII");
  my $ldap =  Net::LDAP->new($BDII) or return;
  $ldap->bind() or return;
  my ($maxRAMSize, $maxSwapSize) = (0,0);
  foreach my $CE (@{$self->{CONFIG}->{CE_LCGCE_LIST}}) {
    $self->debug(1,"Getting info for $CE");
    my $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
                                filter => "GlueCEUniqueID=$CE");
    if (! $result or $result->code){
      $self->info("Couldn't get the CE info from ldap");
      $ldap->unbind();
       return;
    }
    my @entry = $result->all_entries();
    ($entry[0]) or next;
    my $cluster = $entry[0]->get_value("GlueForeignKey");
    $cluster =~ s/^GlueClusterUniqueID=//;
    $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
                             filter => "GlueSubClusterUniqueID=$cluster");
    if (! $result or $result->code){
      $self->info("Couldn't get the Subcluster info from ldap");
      $ldap->unbind();
       return;
    }
    @entry = $result->all_entries();
    if ($entry[0]){
      my $RAMSize = $entry[0]->get_value("GlueHostMainMemoryRAMSize");
      my $SwapSize = $entry[0]->get_value("GlueHostMainMemoryVirtualSize");
      $self->debug(1,"$cluster: $RAMSize,$SwapSize");
      $maxRAMSize = $RAMSize if ($RAMSize>$maxRAMSize );
      $maxSwapSize = $SwapSize if ($SwapSize>$maxSwapSize );
    }
  }
  $ldap->unbind();
  $self->debug(1,"Memory, Swap: $maxRAMSize,$maxSwapSize");
  $classad->set_expression("Memory",$maxRAMSize*1024);
  $classad->set_expression("Swap",$maxSwapSize*1024);
  $classad->set_expression("FreeMemory",$maxRAMSize*1024);
  $classad->set_expression("FreeSwap",$maxSwapSize*1024);
  $self->{UPDATECLASSAD} = time();
  return $classad;
}

sub translateRequirements {
  my $self = shift;
  my $ca = shift;
  $ca or return ();

  my $requirements = '';

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

#  ($ok, my $freeMemory) =  $ca->evaluateAttributeString("FreeMemory");
#  $self->info("Translating \'FreeMemory\' requirement ($ok,$freeMemory)") if $freeMemory;
#  ($ok, my $freeSwap) =  $ca->evaluateAttributeString("FreeSwap");
#  $self->info("Translating \'FreeSwap\' requirement ($ok,$freeSwap)") if $freeSwap;
  return $requirements;
}

sub generateJDL {
  my $self = shift;
  my $ca = shift;
  my $requirements = $self->translateRequirements($ca);
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

  print BATCH "\# JDL automatically generated by AliEn
Executable = \"/bin/sh\";
Arguments = \"-x dg-submit.$$.sh\";
StdOutput = \"std.out\";
StdError = \"std.err\";
RetryCount = 0;
Rank = 1000*(other.GlueCEInfoTotalCPUs - other.GlueCEStateWaitingJobs)/other.GlueCEInfoTotalCPUs;
FuzzyRank = True;
VirtualOrganisation = \"\L$self->{CONFIG}->{ORG_NAME}\E\";
InputSandbox = {\"$exeFile\"};
OutputSandbox = { \"std.err\" , \"std.out\" };
Environment = {\"ALIEN_CM_AS_LDAP_PROXY=$ENV{ALIEN_CM_AS_LDAP_PROXY}\",\"ALIEN_JOBAGENT_ID=$ENV{ALIEN_JOBAGENT_ID}\"};
";
  if (scalar @{$self->{CONFIG}->{CE_LCGCE_LIST}}) {
    my @celist = map {"other.GlueCEUniqueID==\"$_\""} @{$self->{CONFIG}->{CE_LCGCE_LIST}};
    my $ces=join (" || ", @celist);
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

sub installWithLocal {
  my $self=shift;
  my $version=$self->{CONFIG}->{VERSION};
  $version=~ s{_.*$}{};
  return "alien", "rm -rf alien-installer
wget -O alien-installer http://alien.cern.ch/alien-installer
chmod +x alien-installer
./alien-installer --release $version  --prefix \$HOME/alien_install --type wn  update
export ALIEN_ROOT=\$HOME/alien_install
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


