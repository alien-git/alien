package AliEn::LQ::CREAM;

use AliEn::LQ;
use vars qw (@ISA);
push @ISA,   qw( AliEn::LQ);

use strict;
use AliEn::Database::CE;
use AliEn::Classad::Host;
use File::Basename;
use Net::LDAP;
use AliEn::TMPFile;
use POSIX ":sys_wait_h";
use Sys::Hostname;
use List::Util qw( max min sum );
use Switch;
use Data::Dumper;

sub initialize {
   my $self=shift;
   $self->{LOGGER}->warning("CREAM","CE restarted.");
   $self->{DB}=AliEn::Database::CE->new();
   $ENV{X509_CERT_DIR} and $self->{LOGGER}->debug("LCG","X509: $ENV{X509_CERT_DIR}");
   
   $self->{CONFIG}->{VOBOX} = "$self->{CONFIG}->{HOST}:8084";
   $ENV{ALIEN_CM_AS_LDAP_PROXY} and $self->{CONFIG}->{VOBOX} = $ENV{ALIEN_CM_AS_LDAP_PROXY};
   $self->info("This VO-Box is $self->{CONFIG}->{VOBOX}, site is \'$ENV{SITE_NAME}\'");
   $self->{CONFIG}->{LCGVO} = $ENV{ALIEN_VOBOX_ORG}|| $self->{CONFIG}->{ORG_NAME};
   $self->{CONFIG}->{VOBOXDIR} = "/opt/vobox/\L$self->{CONFIG}->{LCGVO}";
   $self->info("Running as \'$ENV{ALIEN_USER}\' using $ENV{X509_USER_PROXY}");
   $self->{UPDATECLASSAD} = 0;

   my $fix_env ='LD_LIBRARY_PATH=$GLITE_LOCATION${LD_LIBRARY_PATH#*$GLITE_LOCATION}:/opt/c-ares/lib'; 

   my $cmds = {  SUBMIT_CMD     => "$fix_env glite-ce-job-submit",
                 STATUS_CMD     => 'glite-ce-job-status',
		 KILL_CMD       => 'glite-ce-job-cancel',
                 DELEGATION_CMD => 'glite-ce-delegate-proxy'};
			 
   $self->{$_} = $cmds->{$_} || $self->{CONFIG}->{$_} || '' foreach (keys %$cmds);
   unless ($self->readCEList()) {
      $self->{LOGGER}->error("LCG","Error reading CE list");
      return;
   } 
   # Some optionally configurable values
   $ENV{CE_PROXYDURATION} and $self->{CONFIG}->{CE_PROXYDURATION} = $ENV{CE_PROXYDURATION};
   $self->{CONFIG}->{CE_PROXYDURATION} or $self->{CONFIG}->{CE_PROXYDURATION} = 172800;
   $ENV{CE_PROXYTHRESHOLD} and $self->{CONFIG}->{CE_PROXYTHRESHOLD} = $ENV{CE_PROXYTHRESHOLD};
   $self->{CONFIG}->{CE_PROXYTHRESHOLD} or $self->{CONFIG}->{CE_PROXYTHRESHOLD} = 165600;
   $self->info("Proxies will be renewed for $self->{CONFIG}->{CE_PROXYDURATION} sec, with a threshold of $self->{CONFIG}->{CE_PROXYTHRESHOLD} sec.");
   $ENV{CE_DELEGATIONINTERVAL} and $self->{CONFIG}->{CE_DELEGATIONINTERVAL} = $ENV{CE_DELEGATIONINTERVAL};
   $self->{CONFIG}->{CE_DELEGATIONINTERVAL} or $self->{CONFIG}->{CE_DELEGATIONINTERVAL} = 72000;
   $self->{DELEGATIONTIME} = 0;
   $self->info("Delegations will be renewed with an interval of $self->{CONFIG}->{CE_DELEGATIONINTERVAL} sec"); 
		
   return 1;
}

sub prepareForSubmission {
  my $self = shift;
  $self->debug(1,"Preparing for submission...");
  my $classad = shift;
  $classad or return;
  $self->renewProxy($self->{CONFIG}->{CE_PROXYDURATION},$self->{CONFIG}->{CE_PROXYTHRESHOLD});
  $self->renewDelegation($self->{CONFIG}->{CE_DELEGATIONINTERVAL}); 

  my ($maxRAMSize, $maxSwapSize) = (0,0);
  
  foreach my $cluster ( @{$self->{CE_CLUSTERSTATUS}} ) {
    foreach my $CE ( keys %{$cluster} ) {
 
       if  ($cluster->{$CE} == -1) {
         $self->info("$CE is blacklisted, skipping.");
         next;
       }
       my $status = $self->getCEStatus($CE);
         $self->info("$CE is in \"$status\" mode");
       unless ($status =~ /^Production$/i) {
         $self->{LOGGER}->warning("CREAM","$CE is in \"$status\" mode, blacklisted.");
         $self->info("$CE is in \"$status\" mode, blacklisted.");
	 $cluster->{$CE} = -1;
	 next;
       }
       $self->debug(1,"Getting queued jobs for $CE");
       $self->info("Getting queued jobs for $CE");
       my $res = $self->queryBDII($CE,'',"GlueVOViewLocalID=\L$self->{CONFIG}->{LCGVO}\E,GlueCEUniqueID=$CE","GlueCEStateWaitingJobs");
       $cluster->{$CE} = 0;
       $cluster->{$CE} = $self->{CONFIG}->{CE_MAXQUEUEDJOBS} - $res->{"GlueCEStateWaitingJobs"}
          if ($self->{CONFIG}->{CE_MAXQUEUEDJOBS}-$res->{"GlueCEStateWaitingJobs"}>0);
       $self->info("Available slots for $CE: ".$cluster->{$CE});
       $res = '';
       $self->debug(1,"Getting RAM and swap info for $CE");
       $res = $self->queryBDII($CE,'',"GlueCEUniqueID=$CE",'GlueForeignKey');
       $res or return $classad;
       my $cluster = $res->{'GlueForeignKey'};
       $cluster =~ s/^GlueClusterUniqueID=//;
       $self->debug(1,"Cluster name from IS is $cluster");
       $res = $self->queryBDII($CE,'(GlueHostMainMemoryRAMSize=*)',"GlueClusterUniqueID=$cluster",qw(GlueHostMainMemoryRAMSize GlueHostMainMemoryVirtualSize));
       $res or return $classad;
       $maxRAMSize  = $res->{'GlueHostMainMemoryRAMSize'}  if ($res->{'GlueHostMainMemoryRAMSize'}>$maxRAMSize );
       $maxSwapSize = $res->{'GlueHostMainMemoryVirtualSize'} if ($res->{'GlueHostMainMemoryVirtualSize'}>$maxSwapSize );
    }  
  }
  $self->{UPDATECLASSAD} = time();    
  $self->info("Updating host ClassAd from IS (RAM,Swap) = ($maxRAMSize,$maxSwapSize)" );
  $classad->set_expression("Memory",$maxRAMSize*1024);
  $classad->set_expression("Swap",$maxSwapSize*1024);
  $classad->set_expression("FreeMemory",$maxRAMSize*1024);
  $classad->set_expression("FreeSwap",$maxSwapSize*1024);
  return $classad;
}
   
sub submit {
  my $self = shift;
  my $jdl = shift;
  my $command = shift;

  my $startTime = time;
  my @args=();
  $self->{CONFIG}->{CE_SUBMITARG_LIST} and @args = @{$self->{CONFIG}->{CE_SUBMITARG_LIST}};
  my $jdlfile = $self->generateJDL($jdl, $command);
  $jdlfile or return;
  
  #pick a random CE from the list
  my $theCE = '';
  my @CEList = $self->getCEList();
  $self->debug(1,"Cluster status:\n".Dumper $self->{CE_CLUSTERSTATUS});

  until ($theCE || !@CEList) {
    my $i = int(rand(@CEList));
    if ($self->getCESlots($CEList[$i]) <= 0) {
      splice(@CEList,$i,1);
      next;
     }
     $theCE = $CEList[$i];
   } 

  unless ($theCE) {
     $self->{LOGGER}->error("CREAM","No suitable CE found for submission!");
     $self->info("No more slots in the queues?");
     return;
  }
  push @args, ("-r", $theCE);
  push @args, ("-D", "$self->{CONFIG}->{DELEGATION_ID}");

  $self->info("Submitting to CREAM with \'@args\'.");
  my $now = time;
  my $logFile = AliEn::TMPFile->new({filename=>"job-submit.$now.log"}) or return;

  my $contact = '';
  $contact = $self->wrapSubmit($logFile, $jdlfile, @args);
  $self->{LAST_JOB_ID} = $contact;
  return unless $contact;
  $self->setCESlots($theCE,$self->getCESlots($theCE)-1);
  $self->info("LCG JobID is $contact");
  open JOBIDS, ">>$self->{CONFIG}->{LOG_DIR}/CE.db/JOBIDS";
  print JOBIDS "$now,$contact\n";
  close JOBIDS;

  my $submissionTime = time - $startTime;
  $self->info("Submission took $submissionTime sec.");
  return 0;
}

#
#-------------------------------------------------------------------------------------------
#

sub getBatchId {
  #Don't trust this too much...
  my $self = shift;
  my $id = $ENV{PBS_JOBNAME} or return;
  my $ce = $ENV{GLITE_WMS_LOG_DESTINATION} or return;
  (undef, $id) = split /_/,$id;
  $id or return;
  return "https://$ce:8443/CREAM$id";
}

sub wrapSubmit {
  my $self = shift;
  my $logFile = shift;
  my $jdlfile = shift;
  my @args = @_ ;  
  my @command = ( $self->{SUBMIT_CMD}, "--noint", "--nomsg");
  @command = ( @command, "--logfile", $logFile, @args, "$jdlfile");
  my @output = $self->_system(@command);
  my $error = $?;
  (my $jobId) = grep { /https:/ } @output;
  return if ( $error || !$jobId);
  $jobId =~ m/(https:\/\/[A-Za-z0-9.-]*:8443\/CREAM\d+)/;
  $jobId = $1; chomp $jobId;
  return $jobId;
}

sub getAllBatchIds {
  my $self = shift;
  return getCREAMStatus('RUNNING:REALLY-RUNNING:REGISTERED:PENDING:IDLE:HELD', $self->getCEList());
}

sub getNumberRunning() {
  my $self = shift;
  my ($run,$wait) = $self->getCEInfo('SUM',qw(GlueCEStateRunningJobs GlueCEStateWaitingJobs ));
  unless (defined $run && defined $wait) {
     $self->info("Could not get number of running/waiting jobs");
     return undef;
  }
  $self->info("JobAgents running, waiting: $run,$wait");
  $self->debug(1,"(Returning value from BDII)");
  return $run+$wait;       
}

sub getNumberQueued() {
  my $self = shift;
  (my $wait) = $self->getCEInfo('MIN',qw(GlueCEStateWaitingJobs));
  unless (defined $wait) {
     $self->info("Could not get number of waiting jobs");
     return undef;
  }
  $self->info("JobAgents waiting: $wait");
  $self->debug(1,"(Returning value from BDII)");
  return $wait;
}

sub getCREAMStatus {
  my $self = shift;
  my $statusString = shift;
  # Active states: 'RUNNING:REALLY-RUNNING:REGISTERED:PENDING:IDLE:HELD'
  # Final states:  'DONE-OK:DONE-FAILED:CANCELLED:ABORTED'
  # There is also: 'UNKNOWN'
  $statusString or return;
  my $CEList = shift;
  $CEList or return;
  my @allJobs = ();
  my $logfile = AliEn::TMPFile->new({ ttl => '12 hours'});
  foreach my $CE ( @$CEList ) {
    (my $endpoint, undef) = split /\//,$CE;
    $self->info("Asking $CE for JobAgents that are $statusString");
    my @output=$self->_system($self->{STATUS_CMD}, "--nomsg",
                                                   "--logfile", $logfile,
	  					   "--endpoint", $endpoint,
		    				   "--all",
						   "--status", $statusString);
    my $nJobs  = grep (/^\*\*\*\*\*\*  JobID=/,@output);
    $self->info("Got $nJobs jobIds.");                                             
    foreach (@output) {
      next unless m/^\*\*\*\*\*\*  JobID=/;
      chomp;
      (my $id = $_) =~ s/^.*\[//;
      $id  =~ s/].*$//;
      push (@allJobs, $id);
    } 
  }
  return @allJobs;
}

sub renewDelegation {
  my $self = shift;
  my $interval = shift;
  $interval or $interval = $self->{CONFIG}->{CE_DELEGATIONINTERVAL};
  my $still = $interval-(time-$self->{DELEGATIONTIME});
  my $dbg = ""; $dbg = "-d" if ($self->{LOGGER}->getDebugLevel());
  if ($still<=0) {
    $self->{DELEGATIONTIME} = time;
    $self->{CONFIG}->{DELEGATION_ID} = "$self->{CONFIG}->{CE_FULLNAME}:".time();
    $self->info("New delegation ID is  $self->{CONFIG}->{DELEGATION_ID}");
  } else {
    $self->info("Delegation timeleft is $still seconds (requested interval is $interval)");
  }

  $self->info("Checking proxy delegations...");
  foreach my $cluster ( @{$self->{CE_CLUSTERSTATUS}} ) {
    foreach ( keys %{$cluster} ) {
      (my $CE, my $queue) = split /\//;
      if ($cluster->{$_} == -1) {
          my @command = ($self->{DELEGATION_CMD},"-e",$CE,$dbg,"$self->{CONFIG}->{DELEGATION_ID}");
          my @output = $self->_system(@command);
          my $error = $?;
          if ($error) {
            $self->info("Error $error delegating the proxy to $CE, blacklisted");
          } else {
	    $self->info("Proxy successfully delegated to $CE ($self->{CONFIG}->{DELEGATION_ID})");
	    $cluster->{$_} = 0;
          }
       } else {
         if ($still >0) {
	   $self->debug(1,"No need to renew the delegation for $CE yet, still $still seconds to go (requested interval is $interval)");
          next;
         } else {
           my @command = ($self->{DELEGATION_CMD},"-e",$CE,$dbg,"$self->{CONFIG}->{DELEGATION_ID}");
 #          my @command = ("glite-ce-proxy-renew","-e",$CE,$dbg,"$self->{CONFIG}->{DELEGATION_ID}");
           my @output = $self->_system(@command);
           my $error = $?;
	   if ($error) {
             $self->info("Error $error renewing the delegation to $CE, blacklisted");
	     $cluster->{$_} = -1;
	   } else {
	     $self->info("Delegation for $CE successfully renewed ($self->{CONFIG}->{DELEGATION_ID})");
             $self->{DELEGATIONTIME} = time;
	   }
	 }
       }
     }
   }
   return 1;
}

sub readCEList {
   my $self = shift;
   unless ($ENV{CE_LCGCE}) {
     $self->{LOGGER}->error("LCG", "No CE list defined in \$ENV");
     return;
   }
   my $string = $ENV{CE_LCGCE};
   my $clusters = [];
   my @sublists = ($string =~ /\(.+?\)/g);
   $string =~ s/\($_\)\,?// foreach (@sublists);
   push  @sublists, split(/,/, $string);
   foreach (@sublists) {
     s/\s*//g;
     s/\(//g;
     s/\)//g;
     my @list = split /,/;
     my $hash = {};
     $hash->{$_} = 0 foreach @list;
     push @$clusters,$hash;
   }
   $self->{CE_CLUSTERSTATUS} = $clusters;
   $self->info("Clusters configuration:\n".Dumper $self->{CE_CLUSTERSTATUS});
   return 1;
}

sub getCEList {
  my $self = shift;
  my @list = ();
  push @list, keys %$_ foreach (@{$self->{CE_CLUSTERSTATUS}});
  return @list;
}

sub getCESlots {
  my $self = shift;
  my $CE = shift;
  foreach my $cluster ( @{$self->{CE_CLUSTERSTATUS}} ) {
    return $cluster->{$CE} if defined $cluster->{$CE};
  }
  return;
}

sub setCESlots {
  my $self = shift;
  my $CE = shift;
  my $slots = shift;
  foreach my $cluster ( @{$self->{CE_CLUSTERSTATUS}} ) {
    if (defined $cluster->{$CE}){
      $cluster->{$CE} = $slots;
      return 1;
    }
  }
  return;
}

sub queryBDII {
  my $self = shift;
  my $CE = shift; #GlueCEUniqueID
  my $filter = shift;
  $filter or $filter = "objectclass=*";
  my $base = shift;
  $base or $base = "GlueVOViewLocalID=\L$self->{CONFIG}->{LCGVO}\E,GlueCEUniqueID=$CE";
  my @items = @_;
  my %results = ();
  $self->info("Querying $CE for @items");
  $self->debug(1,"DN string is $base");
  $self->debug(1,"Filter is $filter");
  (my $host,undef) = split (/:/,$CE);    
  my $IS  = "ldap://$host:2170,mds-vo-name=resource,o=grid"; # Resource BDII
  $IS = $self->{CONFIG}->{CE_SITE_BDII} if ( defined $self->{CONFIG}->{CE_SITE_BDII} );  
  my $ldap = '';
  my ($GRIS, $BaseDN) = split (/,/,$IS,2);
  $self->debug(1,"Asking $GRIS/$BaseDN");
  unless ($ldap =  Net::LDAP->new($GRIS)) {
    $self->info("$GRIS/$BaseDN not responding (1)");
    return;
  }
  unless ($ldap->bind()) {
    $self->{LOGGER}->info("$GRIS/$BaseDN not responding (2)");
    next;
  }
  my $result = $ldap->search( base   => "$base,$BaseDN",
                              filter => "$filter");
  my $code = $result->code;                           
  my $msg = $result->error;                           
  if ($code) {
    $self->{LOGGER}->warning("LCG","\"$msg\" ($code) from $GRIS/$BaseDN");
    return;
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
  } else {
    $self->{LOGGER}->warning("LCG","The query to $GRIS/$BaseDN did not return any value");
    return;
  }
  $ldap->unbind();
  $self->debug(1,"queryBDII() returning: ".Dumper(\%results));
  return \%results;
}

sub getCEStatus {
  my $self = shift;
  my $theCE = shift;  #GlueCEUniqueId
  my $object = shift; 
  $object or $object = "GlueCEStateStatus";
  $self->debug(1,"Checking status of $theCE");
  my $result = $self->queryBDII($theCE,'',"GlueCEUniqueID=$theCE",$object);
  my $status = $result->{$object};
  $self->debug(1, "$object for $theCE is \"$status\"");
  return $status;
}

sub getCEInfo {
  my $self = shift;
  my $mode = shift;
  my @items = @_;
  $self->info("Querying CEs, mode: $mode, requested info: @items");
  my @list  = $self->getCEList();
  my $values = ();
  foreach ( @items ) {
     $values->{$_} = [];
  }
  foreach my $cluster ( @{$self->{CE_CLUSTERSTATUS}} ) {
    foreach my $CE ( keys %{$cluster} ) {
      # next if ($cluster->{$CE}<0);
      $self->debug(1,"Querying for $CE");
      my $res = $self->queryBDII($CE,'',"GlueVOViewLocalID=\L$self->{CONFIG}->{LCGVO}\E,GlueCEUniqueID=$CE",@_);
      $self->debug(1,"getCEInfo() got: ".Dumper($res));
      if ( $res ) {
        foreach (@items) {
          if ($res->{$_} =~ m/444444/) {
            $self->{LOGGER}->warning("LCG","Query for $CE gave $_=444444.");
            $self->info("Query for $CE gave $_=444444, blacklisting.");
	    $self->setCESlots($CE,-1);
            next;
          }
          push @{$values->{$_}}, $res->{$_} if (defined $res->{$_});  
        }
      } else { 
        $self->{LOGGER}->warning("LCG","Query for $CE failed, blacklisting.");
        $self->info("Query for $CE failed, blacklisting.");
	$self->setCESlots($CE,-1);
        next;
      }
      last;   
    }
  }  
  my @return;
  foreach (@items) {
    my $val = '';
    switch ($mode) {
      case "SUM" { $val = sum @{$values->{$_}}; }
      case "MIN" { $val = min @{$values->{$_}}; }
      case "MAX" { $val = max @{$values->{$_}}; }
      else  { $self->{LOGGER}->error('LCG',"Mode $mode not supported in querying CE"); }
    }  
      push @return, $val;
  }
  $self->debug(1,"getCEInfo() returning: ".Dumper(@return));
  return @return;
}

sub getJobStatus { #This is actually unused
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
   my $duration = shift;
   $duration or $duration=$self->{CONFIG}->{CE_TTL};
   $duration or $duration = 172800; #in seconds
   my $thres = shift;
   $thres or $thres = 0;
   $self->info("Checking whether to renew proxy for $duration seconds");
   $ENV{X509_USER_PROXY} and $self->debug(1,"\$X509_USER_PROXY is $ENV{X509_USER_PROXY}");
   my $ProxyRepository = "$self->{CONFIG}->{VOBOXDIR}/proxy_repository";
   my $command = "vobox-proxy --vo \L $self->{CONFIG}->{LCGVO}\E query";
   
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
   $self->info("Proxy timeleft is $timeLeft (threshold is $thres)");
   return 1 if ( $thres>0 && $timeLeft>$thres );
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
   $command = "vobox-proxy --vo \L$self->{CONFIG}->{LCGVO}\E --dn \'$dn\' query-proxy-timeleft";
   ( my $realDuration ) = $self->_system($command);
   chomp $realDuration;
   $self->{LOGGER}->error("LCG","asked for $duration sec, got only $realDuration") if ( $realDuration < 0.9*$duration);
   $ENV{X509_USER_PROXY} = $currentProxy;

   return 1;
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

sub _system {
  my $self=shift;
  my $command=join (" ", @_);

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
    $self->setEnvironmentForLCG();
    $self->debug(1, "Doing '$command'");
    my $dbg = "2>/dev/null"; $dbg = "" if ($self->{LOGGER}->getDebugLevel()>1);
    $pid=open(FILE, "$command $dbg|") or
      die("Error doing '$command'\n$!");
    @output=<FILE>;

    if (! close FILE){
      #We have to check that the proces do^?^?
      $self->{LOGGER}->error("LCG","The system call failed  PID $pid");
      if (CORE::kill 0,$pid) {
        my $kid;
        do {
          $kid = waitpid($pid, WNOHANG);
        }   until $kid > 0;
      }
    }
    alarm(0);
  };
  my $error=$?;
  $self->unsetEnvironmentForLCG();
  if ($error) {
    $self->{LOGGER}->error("LCG","Error in system call: $error");
    close FILE;
    $pid and $self->info("Killing the process $pid") and CORE::kill(9, $pid);
    alarm(0);
    return;
  }
  return @output;
}

sub setEnvironmentForLCG{
  my $self=shift;
  $self->debug(1,"Setting the environment for an LCG call");
  $self->{LCG_ENV}={};
  foreach  my $v ("GLOBUS_LOCATION", "X509_CERT_DIR", "MYPROXY_LOCATION"){
    $self->{LCG_ENV}->{$v}=$ENV{$v};
    delete $ENV{$v};
  }
  $self->{LCG_ENV}->{PATH}=$ENV{PATH};
  $ENV{PATH}=~ s/$ENV{ALIEN_PATH}//;
  $self->{LCG_ENV}->{LD_LIBRARY_PATH}=$ENV{LD_LIBRARY_PATH};
  $ENV{LD_LIBRARY_PATH}=~ s/$ENV{ALIEN_LD_LIBRARY_PATH}//;
  $ENV{GLOBUS_LOCATION}="/opt/globus";
}

sub unsetEnvironmentForLCG{
  my $self=shift;
  $self->debug(1,"Back to the normal environment");
  foreach my $v (keys %{$self->{LCG_ENV}}){
    $ENV{$v}=$self->{LCG_ENV}->{$v};
  }
}

sub generateStartAgent{
  my $self=shift;
  my $command=shift;

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
  $self->info("Writing the command that we got from the standard method: $command");
  my $info=$command;
  if (-f $command){
    open (FILE, "<$command") or $self->info("Error opening $command!") and return;
    $info=join("",<FILE>);
    close FILE;
  }


  print BATCH "$info\n";
  print BATCH "
cd \${TMPDIR:-.}
echo --- env ---------------------------------------
echo \$PATH
echo \$LD_LIBRARY_PATH

export ALIEN_USER=$ENV{ALIEN_USER}
echo --- alien --printenv --------------------------
\$ALIEN_ROOT/bin/$exec -printenv
echo --- alien proxy-info ---------------------------
\$ALIEN_ROOT/bin/$exec proxy-info
echo --- Run ---------------------------------------
ls -lart
";

  close BATCH or return;
  return $exeFile;
}

sub generateJDL {
  my $self = shift;
  my $ca = shift;
  my $command=shift;
  my $bdiiReq=shift;
  my $requirements = $self->translateRequirements($ca, $bdiiReq);

  my $exeFile=$self->generateStartAgent( $command) or return;

  my $jdlFile = AliEn::TMPFile->new({filename=>"dg-submit.$$.jdl"}) or return;
  my $tmpDir = dirname($jdlFile)."/job-output";
  unless ( -e $tmpDir ) {
    mkdir $tmpDir, 0755 or $self->{LOGGER}->warning("CREAM","The directory for the CREAM output cannot be created");
  }
  open( BATCH, ">$jdlFile" )
    or print STDERR "Can't open file '$jdlFile': $!"
      and return;

  my $now = gmtime()." "."$$"; 
  $now =~ s/\s+/\_/g;
  my $host_name = hostname;
  print BATCH "\# JDL automatically generated by AliEn
  Executable = \"/bin/sh\";
  Arguments = \"-x dg-submit.$$.sh\";
  StdOutput = \"std.out\";
  StdError = \"std.err\";
  InputSandbox = {\"$exeFile\"};
  Environment = {\"ALIEN_CM_AS_LDAP_PROXY=$self->{CONFIG}->{VOBOX}\",\"ALIEN_JOBAGENT_ID=$ENV{ALIEN_JOBAGENT_ID}\", \"ALIEN_USER=$ENV{ALIEN_USER}\"};
  ";

  if ($self->{LOGGER}->getDebugLevel()) {
    print BATCH "OutputSandbox = { \"std.err\" , \"std.out\" };\n";
    print BATCH "Outputsandboxbasedesturi = \"gsiftp://localhost\";\n";
  }
  print BATCH "Requirements = $requirements;\n" if $requirements;
  close BATCH;
  return $jdlFile;
}

return 1;
