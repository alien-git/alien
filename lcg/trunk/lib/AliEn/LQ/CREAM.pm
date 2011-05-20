package AliEn::LQ::CREAM;

use AliEn::LQ::LCG;
use vars qw (@ISA);
push @ISA, qw(AliEn::LQ::LCG); 

use strict;
use AliEn::Database::CE;
use File::Basename;
use Net::LDAP;
use AliEn::TMPFile;
use POSIX ":sys_wait_h";
use Sys::Hostname;

my @CEBlackList;

sub initialize {
   my $self=shift;
   $self->{DB}=AliEn::Database::CE->new();
   $ENV{X509_CERT_DIR} and $self->{LOGGER}->debug("LCG","X509: $ENV{X509_CERT_DIR}");
   
   $self->{CONFIG}->{VOBOX} = "$self->{CONFIG}->{HOST}:8084";
   $ENV{ALIEN_CM_AS_LDAP_PROXY} and $self->{CONFIG}->{VOBOX} = $ENV{ALIEN_CM_AS_LDAP_PROXY};
   $self->info("This VO-Box is $self->{CONFIG}->{VOBOX}, site is \'$ENV{SITE_NAME}\'");
   $self->{CONFIG}->{LCGVO} = $ENV{ALIEN_VOBOX_ORG}|| $self->{CONFIG}->{ORG_NAME};
   $self->{CONFIG}->{VOBOXDIR} = "/opt/vobox/\L$self->{CONFIG}->{LCGVO}";
   $self->{UPDATECLASSAD} = 0;
   my @newCEList;
   my $queue;

   my $fix_env ='LD_LIBRARY_PATH=$GLITE_LOCATION${LD_LIBRARY_PATH#*$GLITE_LOCATION}:/opt/c-ares/lib'; 
   
   my $cmds = {  SUBMIT_CMD     => "$fix_env glite-ce-job-submit",
                 STATUS_CMD     => 'glite-ce-job-status',
		 KILL_CMD       => 'glite-ce-job-cancel',
		 CLEANUP_CMD    => '',
                 DELEGATION_CMD => 'glite-ce-delegate-proxy'};
			 
   $self->{$_} = $cmds->{$_} || $self->{CONFIG}->{$_} || '' foreach (keys %$cmds);
   unless ($self->readCEList()) {
      $self->{LOGGER}->error("LCG","Error reading CE list");
      #return;
   } 
   # Some optionally configurable values
   $ENV{CE_PROXYDURATION} and $self->{CONFIG}->{CE_PROXYDURATION} = $ENV{CE_PROXYDURATION};
   $self->{CONFIG}->{CE_PROXYDURATION} or $self->{CONFIG}->{CE_PROXYDURATION} = 172800;
   $ENV{CE_PROXYTHRESHOLD} and $self->{CONFIG}->{CE_PROXYTHRESHOLD} = $ENV{CE_PROXYTHRESHOLD};
   $self->{CONFIG}->{CE_PROXYTHRESHOLD} or $self->{CONFIG}->{CE_PROXYTHRESHOLD} = 165600;
   $self->info("Proxies will be renewed for $self->{CONFIG}->{CE_PROXYDURATION} sec, with a threshold of $self->{CONFIG}->{CE_PROXYTHRESHOLD} sec.");
   $ENV{CE_DELEGATIONINTERVAL} and $self->{CONFIG}->{CE_DELEGATIONINTERVAL} = $ENV{CE_DELEGATIONINTERVAL};
   $self->{CONFIG}->{CE_DELEGATIONINTERVAL} or $self->{CONFIG}->{CE_DELEGATIONINTERVAL} = 72000;
   $self->info("Delegations will be renewed with an interval of $self->{CONFIG}->{CE_DELEGATIONINTERVAL} sec"); 
		
   $self->renewProxy($self->{CONFIG}->{CE_PROXYDURATION});
   $self->{CONFIG}->{DELEGATION_ID} = "$self->{CONFIG}->{CE_FULLNAME}:".time();
   $self->info("Delegation ID is  $self->{CONFIG}->{DELEGATION_ID}");
   
   foreach ( @{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}} ) {
     (my $CE, my $queue) = split /\//;
     my @command = ($self->{DELEGATION_CMD},"-e",$CE,
                                            "-d","$self->{CONFIG}->{DELEGATION_ID}");
     my @output = $self->_system(@command);
     my $error = $?;
     if ($error) {
        $self->info(" CE $CE to the black list");
       push(@CEBlackList,$CE."/".$queue);
       $self->{LOGGER}->error("LCG","Error $error delegating the proxy to $CE");
     } else {
       push(@newCEList,$CE."/".$queue);
     }
     $self->{DELEGATIONTIME} = time;
   }
   $self->{CONFIG}->{CE_LCGCE_FLAT_LIST} = \@newCEList;

   #use Data::Dumper;
   #print Dumper( $self->{CONFIG}->{CE_LCGCE_FLAT_LIST});

   $ENV{CE_LIST} = join(",",@newCEList);

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
  
  #pick a random CE from the list
  my $theCE = $self->{CONFIG}->{CE_LCGCE_FLAT_LIST}->[int(rand(@{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}}))];
  push @args, ("-r", $theCE);
  push @args, ("-D", "$self->{CONFIG}->{DELEGATION_ID}");

  my $status = $self->getCEStatus($theCE);
  $self->debug(1,"$theCE is in \"$status\" mode");
  unless ($status =~ /^Production$/i) {
     $self->{LOGGER}->warning("CREAM","$theCE is in \"$status\" mode, will not submit.");
     return;
  }
 
  $self->renewProxy($self->{CONFIG}->{CE_PROXYDURATION},$self->{CONFIG}->{CE_PROXYTHRESHOLD});
  $self->renewDelegation($self->{CONFIG}->{CE_DELEGATIONINTERVAL}); 

  $self->info("Submitting to LCG with \'@args\'.");
  my $now = time;
  my $logFile = AliEn::TMPFile->new({filename=>"job-submit.$now.log"}) or return;

  my $contact = '';
  $contact = $self->wrapSubmit($logFile, $jdlfile, @args);
  $self->{LAST_JOB_ID} = $contact;
  return unless $contact;
  $self->info("LCG JobID is $contact");
  open JOBIDS, ">>$self->{CONFIG}->{LOG_DIR}/CE.db/JOBIDS";
  print JOBIDS "$now,$contact\n";
  close JOBIDS;

  my $submissionTime = time - $startTime;
  $self->info("Submission took $submissionTime sec.");
  return 0;
}

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
  #OutputSandbox = { \"std.err\" , \"std.out\" };
  #Outputsandboxbasedesturi = \"gsiftp://localhost\";
  Environment = {\"ALIEN_CM_AS_LDAP_PROXY=$self->{CONFIG}->{VOBOX}\",\"ALIEN_JOBAGENT_ID=$ENV{ALIEN_JOBAGENT_ID}\", \"ALIEN_USER=$ENV{ALIEN_USER}\"};
  ";

  print BATCH "Requirements = $requirements;\n" if $requirements;
  close BATCH;
  return $jdlFile;
}

sub getAllBatchIds {
  my $self = shift;
  return getCREAMStatus('RUNNING:REALLY-RUNNING:REGISTERED:PENDING:IDLE:HELD',
                        $self->{CONFIG}->{CE_LCGCE_FIRSTS_LIST});
}

sub cleanUp {
  return 1;
}

sub needsCleaningUp {
  return 0;
}

sub getNumberRunning() {
  my $self = shift;
  my ($run,$wait) = $self->getCEInfo(qw(GlueCEStateRunningJobs GlueCEStateWaitingJobs ));
  $run or $run=0;
  $wait or $wait=0;
  $self->info("JobAgents running, waiting: $run,$wait");
  if ($ENV{CE_USE_BDII} ) {
     $self->info("(Returning value from BDII)");
     return $run+$wait;    
  } else {
    $self->{LOGGER}->error("CREAM","Only info from BDII available in this release");
    return;
  }   
}

sub getNumberQueued() {
  my $self = shift;
  (my $wait) = $self->getCEInfo(qw(GlueCEStateWaitingJobs));
  $wait or $wait=0;
  $self->info("JobAgents waiting: $wait");
  if ($ENV{CE_USE_BDII} ) {
    $self->info("(Returning value from BDII)");
    return $wait;
  } else {
    $self->{LOGGER}->error("CREAM","Only info from BDII available in this release");
    return;
  }   
}

#
#---------------------------------------------------------------------
#

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
  $interval or $interval = 2*60*60; #2 hours
  my @newBlackCEList;
  my @newCEList;

  foreach ( @CEBlackList ) {
     (my $CE, my $queue) = split /\//;
     my @command = ("glite-ce-delegate-proxy","-e",$CE,
                                            "-d","$self->{CONFIG}->{DELEGATION_ID}");
     my @output = $self->_system(@command);
     my $error = $?;
     if ($error) {
       $self->info("CE $CE still in the blacklist ");
       $self->{LOGGER}->error("LCG","Error $error delegating the proxy to $CE. The CE is still not working");
       push(@newBlackCEList,$CE."/".$queue);
     } else {
       $self->info("CE $CE out of the blacklist");
       push(@{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}},$CE."/".$queue);
     }
#     $i = $i+1;
     $self->{DELEGATIONTIME} = time;
   }
   @CEBlackList = @newBlackCEList;

  my $still = $interval-(time-$self->{DELEGATIONTIME});

  if ( $still<=0 ) {
    $self->info("Renewing proxy delegation for all CEs");

    foreach ( @{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}} ) {
      (my $CE, my $queue) = split /\//;
      my @command = ("glite-ce-proxy-renew","-e",$CE,
                                            "-d","$self->{CONFIG}->{DELEGATION_ID}");
      $self->info("CE is $CE");
      my @output = $self->_system(@command);
      my $error = $?;
      if ($error) {
        $self->{LOGGER}->error("LCG","Error $error renewing the delegation to $CE");
        push(@CEBlackList,$CE."/".$queue);
      } else {
        push(@newCEList,$CE."/".$queue);
      }

   }
    $self->{CONFIG}->{CE_LCGCE_FLAT_LIST} = \@newCEList;
    $ENV{CE_LIST} = join(",",@newCEList);
    $self->{DELEGATIONTIME} = time;
  } else {
    $self->debug(1,"No need to renew the delegation yet, still $still seconds to go (requested interval is $interval)");
  }
  #$ENV{CE_LIST} = join(",",@newCEList);

  return 1;
}

#sub OLDrenewDelegation {
#  my $self = shift;
#  my $interval = shift;
#  $interval or $interval = 2*60*60; #2 hours
#  my $still = $interval-(time-$self->{DELEGATIONTIME});
#  if ( $still<=0 ) {
#    $self->info("Renewing proxy delegation for all CEs");
#    foreach ( @{$self->{CONFIG}->{CE_LCGCE_FLAT_LIST}} ) {
#      (my $CE, undef) = split /\//;
#      my @command = ("glite-ce-proxy-renew","-e",$CE,
#                                            "-d","$self->{CONFIG}->{DELEGATION_ID}");
#      my @output = $self->_system(@command);
#      my $error = $?;
#      if ($error) {
#        $self->{LOGGER}->error("LCG","Error $error renewing the delegation to $CE");
#      return;
#     }
#   }
#    $self->{DELEGATIONTIME} = time;
#  } else {
#    $self->debug(1,"No need to renew the delegation yet, still $still seconds to go (requested interval is $interval)");
#  }
#  return 1;
#}


return 1;

