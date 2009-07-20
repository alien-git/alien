package AliEn::LQ::WMS;

use AliEn::LQ::LCG;
use vars qw (@ISA);
@ISA = qw( AliEn::LQ::LCG);

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
   
   if ($self->{CONFIG}->{CE_SITE_BDII}) {
     $self->info("Site BDII is $self->{CONFIG}->{CE_SITE_BDII}, will be used for queries"); 
   } 
   
   unless ($self->readCEList()) {
      $self->{LOGGER}->error("LCG","Error reading CE list");
      return;
   } 

   # Read RB list and generate config files if needed
   my @list;
   my @wmslist;
   if ( $ENV{CE_RBLIST} ) { 
       $self->info("Taking the list of RBs from \$ENV: $ENV{CE_RBLIST}");
       @list=split(/,/,$ENV{CE_RBLIST});
       @wmslist=split(/:/,$ENV{CE_RBLIST});
       $_ =~ s/:/_/g foreach @list;
       $self->{CONFIG}->{CE_RB_LIST} = \@list;
       $self->{CURRENTRB} = $self->{CONFIG}->{CE_RB_LIST}->[0];
       $self->{RBTIME} = time;
   }
 
   $self->renewProxy(172800);   

   foreach my $thisWMS (@wmslist){
       
       if( !-e "$self->{CONFIG}->{LOG_DIR}/$thisWMS.vo.conf" ){
	   $self->info("Config file for $thisWMS  not there, creating it.");
	   my $wmsurl = "\"https://$thisWMS:7443/glite_wms_wmproxy_server\"";
	   open WMSVOCONF, ">$self->{CONFIG}->{LOG_DIR}/$thisWMS.vo.conf" or return;
	   print WMSVOCONF "[
           VirtualOrganisation = \"alice\";
           EnableServiceDiscovery  =  false;
           WMProxyEndpoints    = {$wmsurl};
           MyProxyServer       = \"myproxy.cern.ch\";\n]\n";
	   close WMSVOCONF;
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
           VirtualOrganisation     = \"alice\";
           EnableServiceDiscovery  =  false;
           Requirements            = other.GlueCEStateStatus == \"Production\";
           WMProxyEndpoints        = {$streamwms};
           MyProxyServer           = \"myproxy.cern.ch\";\n]\n";
	       close STVOCONF;
	   }
       }
       
   }
   
   $self->{CONFIG}->{DELEGATION_ID} = "$self->{CONFIG}->{CE_FULLNAME}:".time();
    my @command = ($self->{DELEGATION_CMD},"-c","$self->{CONFIG}->{LOG_DIR}/$thisWMS.vo.conf","-d","$self->{CONFIG}->{DELEGATION_ID}");   
    my @output = $self->_system(@command);
    my $error = $?;
    if ($error) {
      $self->{LOGGER}->error("LCG","Error $error delegating the proxy to $thisWMS");
      return 1;
    }
   }
   return 1;
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

  $self->renewProxy(172800,169200); 

  $self->info("Submitting to LCG with \'@args\'.");
  my $now = time;
  my $logFile = AliEn::TMPFile->new({filename=>"job-submit.$now.log"}) 
     or return;

  my $contact = '';
  if ( defined $ENV{CE_RBLIST} ) {
     my $elapsed = time - $self->{RBTIME};
     if ($elapsed > 120*60) {
      $self->info("This RB has been in use for $elapsed minutes, trying to revert to default.");
      $self->{CURRENTRB} = $self->{CONFIG}->{CE_RB_LIST}->[0];
      $self->{RBTIME} = time;
    }    
    my $lastGoodRB = $self->{CURRENTRB};
    $self->info("Will use $lastGoodRB");
    $contact = $self->wrapSubmit($lastGoodRB, $logFile, $jdlfile, @args);

    unless ( $contact ) {
      redoit:foreach ( @{$self->{CONFIG}->{CE_RB_LIST}} ) { 
	next redoit if ( $_ eq $lastGoodRB ); ##This one just failed
	$contact = $self->wrapSubmit($_, $logFile, $jdlfile, @args);
	next redoit unless $contact; 
        $self->{CURRENTRB} = $lastGoodRB;
        $self->{RBTIME} = time;
	last;
      }
    }
  } else {
    $self->info("Failover submission not configured, using default WMS.");
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

sub getNumberRunning() {
  my $self = shift;
  my $run = '';
  my $wait = '';
  my $string = "IS"; 
  if (defined $ENV{CE_GETRUNNING} && defined $ENV{CE_GETWAITING}) {
    ($run)  = $self->_system($ENV{CE_GETRUNNING}); 
    chomp $run;
    ($wait) = $self->_system($ENV{CE_GETWAITING}); 
    chomp $wait;
    $string = "LRMS";
  } else {
    ($run,$wait) = $self->getCEInfo(qw(GlueCEStateRunningJobs GlueCEStateWaitingJobs ));
  }  
  my $value = $self->getQueueStatus();
  $value or $value = 0;
  $run or $run = 0;
  $wait or $wait = 0;

  $self->info("Jobs: $run running, $wait waiting from $string, $value from local DB");
  if ( $run =~ m/44444/ || $wait =~ m/44444/ ) {
    $self->{LOGGER}->error("LCG","IS failure 44444");
    return;
  }
  return $run+$wait;    
}

sub getNumberQueued() {
  my $self=shift;
  my $wait = '';
  my $string = "IS"; 
  if (defined $ENV{CE_GETWAITING}) {
    ($wait) = $self->_system($ENV{CE_GETWAITING}); 
    chomp $wait;
    $string = "LRMS";
  } else {
    ($wait) = $self->getCEInfo(qw(GlueCEStateWaitingJobs ));
  }  
  $wait or $wait=0;
  my $value = $self->{DB}->queryValue("SELECT COUNT (*) FROM JOBAGENT where status='QUEUED'");
  $value or $value = 0;
  $self->info("Queued: $wait from $string, $value from local DB");
  if ( $wait =~ m/44444/ ) {
    $self->{LOGGER}->error("LCG","IS failure 44444");
    return;
  }
  return $wait;
}

sub cleanUp {
  my $self = shift;
  $self->{CLEANUP_CMD} or return;
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
  my @args = @_ ;  
  my @command = ( $self->{SUBMIT_CMD}, 
                  "--noint", 
		  "--nomsg");
  @command = ( @command,		   
	       "--config", "$self->{CONFIG}->{LOG_DIR}/$RB.vo.conf") if $RB;
  @command = ( @command,
	       "--logfile", $logFile, 
	       "-d", "$self->{CONFIG}->{DELEGATION_ID}",
               @args, 
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

sub getContactByQueueID {
   my $self = shift;
   my $queueid = shift;
   $queueid or return;
   my $contact = '';
   return $contact;
}

sub generateJDL {
  my $self = shift;
  my $ca = shift;
  my $command=shift;
  my $bdiiReq=shift;

  my $requirements = $self->translateRequirements($ca, $bdiiReq);

  # implementation for the WMS: Avoid any resubmission of jobs
  my $delaytime = 900; #15 minutos
  my $expirationtime = time + $delaytime;

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
ExpiryTime = $expirationtime;
OutputSandbox = { \"std.err\" , \"std.out\" };
Environment = {\"ALIEN_CM_AS_LDAP_PROXY=$self->{CONFIG}->{VOBOX}\",\"ALIEN_JOBAGENT_ID=$ENV{ALIEN_JOBAGENT_ID}\", \"ALIEN_USER=$ENV{ALIEN_USER}\"};
";
  if (scalar @{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}}) {
      
      my $range = 100;
      my $random_number = int(rand($range));
      my $list_of_ces = join(" ",@{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}});
      
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
      
	  @celist = map {"other.GlueCEUniqueID==\"$_\""} @{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}};
	  $ces=join (" || ", @celist);
      }
      print BATCH "Requirements = ( $ces )";     
      print BATCH " && ".$requirements if $requirements;
      print BATCH ";\n";
  } else {

      print BATCH "Requirements = $requirements;\n" if $requirements;
  }
  print BATCH "Rank = $ENV{CE_RANKING};\n" if ($ENV{CE_RANKING});
  close BATCH;
  return $jdlFile;
}

return 1;
