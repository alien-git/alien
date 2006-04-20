package AliEn::LQ::LCG;

use AliEn::LQ;

@ISA = qw( AliEn::LQ);

use strict;
use AliEn::Database::CE;
use AliEn::Classad::Host;
use Data::Dumper;
use Net::LDAP;

sub initialize {
   my $self=shift;
   $self->SUPER::initialize() or return;
   $self->{DB}=AliEn::Database::CE->new();
   $ENV{X509_CERT_DIR} and $self->{LOGGER}->debug("LCG","X509: $ENV{X509_CERT_DIR}");
   my $error = !defined $ENV{GLOBUS_LOCATION};
   $self->{CONFIG}->{VOBOX} = $ENV{HOST}.':8084';
   $ENV{ALIEN_CM_AS_LDAP_PROXY} and $self->{CONFIG}->{VOBOX} = $ENV{ALIEN_CM_AS_LDAP_PROXY};
   $self->info("VO Box is $self->{CONFIG}->{VOBOX}");
   $self->{CONFIG}->{VOBOXDIR} = "/opt/vobox/\L$self->{CONFIG}->{ORG_NAME}";
   $self->{UPDATECLASSAD} = 0;
   return 1;
}

sub submit {
     my $self = shift;
     my $jdl = shift;
     my $command = shift;
     my $arguments  = join " ", @_;
     my $startTime = time;

     my @args=();
     $self->{CONFIG}->{CE_SUBMITARG} and
      	@args=split (/\s+/, $self->{CONFIG}->{CE_SUBMITARG});
     my $jdlfile = $self->generateJDL($jdl);
     $jdlfile or return;

     $self->renewProxy(90000);
     $self->info("Submitting to LCG with \'@args\'.");
     my @command = ( "edg-job-submit", "--noint", "--nomsg", "--config-vo", "/home/alicesgm/edg_wl_ui.conf", @args, "$jdlfile" );
     $self->debug(1,"Doing @command\n");

     open SAVEOUT,  ">&STDOUT";
     if ( !open STDOUT, ">$self->{CONFIG}->{TMP_DIR}/stdout" ) {
         return 1;
     }
     my $error=system( @command );
     close STDOUT;
     open STDOUT, ">&SAVEOUT";

     open (FILE, "<$self->{CONFIG}->{TMP_DIR}/stdout") or return 1;
     my $contact=<FILE>;
     close FILE;
     $contact and
       chomp $contact;

     if ($error) {
       $contact or $contact="";
       $self->{LOGGER}->warning("LCG","Error submitting the job. Log file '$contact'\n");
       $contact and system ('cat', $contact, '>/dev/null 1>&2');
       return $error;
     } else {
       $self->info("LCG JobID is $contact");
       $self->{LAST_JOB_ID} = $contact;
       open JOBIDS, ">>$self->{CONFIG}->{LOG_DIR}/CE.db/JOBIDS";
       print JOBIDS "$contact\n";
       close JOBIDS;
     }
     my $submissionTime = time - $startTime;
     $self->info("Submission took $submissionTime sec.");
     return $error;
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

     my $error = system( "edg-job-cancel",  "--noint","$contact" );
     return $error;
}

sub getBatchId {
   my $self=shift;
   return $self->{LAST_JOB_ID};
}

sub getStatus {
     my $self = shift;
     my $queueid = shift;
     $queueid or return;
     $self->info("GetStatus: getting status from LCG for $queueid");
     my $LCGStatus =  $self->getJobStatus($queueid);
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
        system("edg-job-get-output --noint --dir $outdir $contact");
        return 'DEQUEUED';
     }
     return 'QUEUED';
}

sub getAllBatchIds {
  my $self = shift;
  my $jobIds = $self->{DB}->queryColumn("SELECT batchId FROM JOBAGENT");
  my @queuedJobs = ();
  foreach (@$jobIds) {
     $_ or next;
     open LB,"edg-job-status $_|" or next;
     my @output = <LB>;     
     close LB;
     grep m/^Current Status:\s*(Running)|(Ready)|(Scheduled)|(Waiting)/,@output 
        or next;
     push @queuedJobs,$_;
  }
  return @queuedJobs;
}

sub getQueueStatus {
  my $self = shift;
  my $value = $self->{DB}->queryValue("SELECT COUNT (*) FROM JOBAGENT");
  $value or $value = '';
  return $value;
}

sub getFreeSlots {
  my $self = shift;
  my $value = 0;
  my $list = $self->{CONFIG}->{CE_LCGCE_LIST};
  foreach my $CE (@$list) {
    (my $host,undef) = split (/:/,$CE);
    my $GRIS = "ldap://$host:2135";
    my $ldap =  Net::LDAP->new($GRIS) or die $@;
    $ldap->bind() or return;
    my $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
                                filter => "(&(objectClass=GlueCEState)(GlueCEUniqueID=$CE))"); 
    $result->code && return;
    foreach my $entry ($result->all_entries) {
      my $RunningJobs = $entry->get_value("GlueCEStateRunningJobs");
      my $WaitingJobs = $entry->get_value("GlueCEStateWaitingJobs");
      my $FreeCPUs   = $entry->get_value("GlueCEStateFreeCPUs");
      $self->debug(1,"Jobs for $CE: $RunningJobs,$WaitingJobs,$FreeCPUs");
      $self->info("Jobs for $CE: $RunningJobs,$WaitingJobs,$FreeCPUs");#####
      $value += $FreeCPUs;
      last;
    }  
    $ldap->unbind();
  }
  
  return $value + $self->getQueueStatus();
}

sub getNumberRunning() {
  my $self = shift;
  return $self->getQueueStatus();
}

#
#---------------------------------------------------------------------
#

sub getJobStatus {
   my $self = shift;
   my $queueid = shift;
   my $pattern = shift;
   $self->info("GetJobStatus: getting status from LCG for $queueid");
   $queueid or return;
   $pattern or $pattern = 'Current Status:';
   my ($contact)= $self->getContactByQueueID($queueid);
   $contact or return;
   my $user = getpwuid($<);
   my @args=();
   $self->{CONFIG}->{CE_STATUSARG} and
     @args=split (/\s+/, $self->{CONFIG}->{CE_STATUSARG});
   open( OUT, "/opt/edg/bin/edg-job-status -noint @args \"$contact\"| grep \"$pattern\"|" );
   my @output = <OUT>;
   close(OUT);
   my $status = $output[0];
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
   $duration or $duration = 100000; #in seconds
   my $ProxyRepository = "$self->{CONFIG}->{VOBOXDIR}/proxy_repository";
   my $command = "vobox-proxy --vo \l$self->{CONFIG}->{ORG_NAME} query-dn";
   $self->debug(1,"Doing $command");
   my $dn = `$command`;
   $self->debug(1,"DN is $dn");
   $command = "vobox-proxy --vo \l$self->{CONFIG}->{ORG_NAME} --dn \'$dn\' query-proxy-filename";
   $self->debug(1,"Doing $command");
   my $proxyfile = `$command`;
   $? and $self->{LOGGER}->error("LCG","No valid proxy found.") and return;   
   chomp $proxyfile;
   $self->debug(1,"Proxy file is $proxyfile");
   # I apparently cannot pass this via an argument
   $ENV{X509_USER_PROXY} = "$self->{CONFIG}->{VOBOXDIR}/renewal-proxy.pem";
   ## All proxies, just to be safe
   my $error = 0;
   foreach my $cert (`ls $ProxyRepository`) {
     chomp $cert;
     my $command = "$ENV{GLOBUS_LOCATION}/bin/grid-proxy-info -f $ProxyRepository/$cert -subject";
     $self->debug(1,"Doing $command");
     my $proxyDN = `$command`;
     chomp $proxyDN;
     $command = "vobox-proxy --vo \l$self->{CONFIG}->{ORG_NAME} --dn \'$proxyDN\' query-proxy-timeleft";
     $self->debug(1,"Doing $command");
     my $timeLeft = `$command`;
     $self->debug(1,"Proxy timeleft is $timeLeft");
     next if ($gracePeriod && $timeLeft>$gracePeriod);
     $self->debug(1,"Renewing proxy for $proxyDN for $duration seconds");
     my @command = ( 'myproxy-get-delegation', 
                     "-a", "$ProxyRepository/$cert",
                     "-d", 
		     "-t",int($duration/3600), #in hours
	             "-o", "/tmp/tmpfile.$$");
     $self->debug(1,"Doing @command");
     $error = system(@command);
     $error and $self->{LOGGER}->error("LCG","unable to renew proxy") and last;
     @command = ("mv", "-f", "/tmp/tmpfile.$$", "$ProxyRepository/$cert");
     $self->debug(1,"Doing @command");
     $error = system(@command);
     $error and $self->{LOGGER}->error("LCG","unable to renew proxy") and last;
   }  
   $ENV{X509_USER_PROXY} = $proxyfile;
   $error and return;  
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
  my $ldap =  Net::LDAP->new($BDII) or die $@;
  $ldap->bind() or die $@;
  my ($maxRAMSize, $maxSwapSize) = (0,0);
  foreach my $CE (@{$self->{CONFIG}->{CE_LCGCE_LIST}}) {
    $self->debug(1,"Getting info for $CE");
    my $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
                                filter => "GlueCEUniqueID=$CE");
    $result->code && die $result->error;
    my @entry = $result->all_entries();
    ($entry[0]) or next;
    my $cluster = $entry[0]->get_value("GlueForeignKey");
    $cluster =~ s/^GlueClusterUniqueID=//;
    $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
                             filter => "GlueSubClusterUniqueID=$cluster");
    $result->code && die $result->error;
    @entry = $result->all_entries();
    my $RAMSize = $entry[0]->get_value("GlueHostMainMemoryRAMSize");
    my $SwapSize = $entry[0]->get_value("GlueHostMainMemoryVirtualSize");
    $self->debug(1,"$cluster: $RAMSize,$SwapSize"); 
    $maxRAMSize = $RAMSize if ($RAMSize>$maxRAMSize );
    $maxSwapSize = $SwapSize if ($SwapSize>$maxSwapSize );
    
  }
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
  $ca->print();
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
   my $file = "dg-submit.$$";
   my $tmpdir = "$self->{CONFIG}->{TMP_DIR}";
   if ( !( -d $self->{CONFIG}->{TMP_DIR} ) ) {
     my $dir = "";
     foreach ( split ( "/", $self->{CONFIG}->{TMP_DIR} ) ) {
       $dir .= "/$_";
       mkdir $dir, 0777;
     }
   }

   open( BATCH, ">$tmpdir/$file.jdl" )
       or print STDERR "Can't open file '$tmpdir/$file.jdl': $!"
       and return;

   print BATCH "
\# JDL automatically generated by AliEn
Executable = \"/bin/sh\";
Arguments = \"-x $file.sh\";
StdOutput = \"std.out\";
StdError = \"std.err\";
RetryCount = 0;
Rank = 1000*(other.GlueCEInfoTotalCPUs - other.GlueCEStateWaitingJobs)/other.GlueCEInfoTotalCPUs;
FuzzyRank = True;
VirtualOrganisation = \"\L$self->{CONFIG}->{ORG_NAME}\E\";
InputSandbox = {\"$tmpdir/$file.sh\"};
OutputSandbox = { \"std.err\" , \"std.out\" };
Environment = {\"ALIEN_CM_AS_LDAP_PROXY=$ENV{ALIEN_CM_AS_LDAP_PROXY}\",\"ALIEN_JOBAGENT_ID=$ENV{ALIEN_JOBAGENT_ID}\"};
";
   my $list = $self->{CONFIG}->{CE_LCGCE_LIST};
   if ($list) {
     my $first = 1;
     print BATCH "Requirements = (";
     foreach my $CE (@$list) {
       print BATCH " || " unless $first; $first = 0;
       print BATCH "other.GlueCEUniqueID==\"$CE\"";
       $self->debug(1,"Adding $CE to the list");
     }
     print BATCH ")";
     print BATCH $requirements;
     print BATCH ";";
   }
   print BATCH "\n";  
   close BATCH;
   open( BATCH, ">$tmpdir/$file.sh" )
       or print STDERR "Can't open file '$tmpdir/$file.sh': $!"
       and return;
   print BATCH "
\#!/bin/sh
\# Script to run AliEn on LCG
\# Automatically generated by AliEn running on $ENV{HOSTNAME}

export PATH=\$PATH:\$VO_ALICE_SW_DIR/alien/bin
cd \${TMPDIR:-.}
export OLDHOME=\$HOME
export HOME=`pwd`
export ALIEN_LOG=$ENV{ALIEN_LOG}
echo --- hostname, uname, whoami, pwd --------------
hostname   
uname -a
whoami
pwd
echo --- env ---------------------------------------
echo \$PATH
echo \$LD_LIBRARY_PATH
echo --- alien --printenv --------------------------
alien -printenv
echo --- ls -la ------------------------------------
ls -lart
echo --- df ----------------------------------------
df -h
echo --- free --------------------------------------
free
echo --- alien proxy-info ---------------------------
alien proxy-info
echo --- Run ---------------------------------------
alien RunAgent

rm -f dg-submit.*.sh
ls -lart
";

   close BATCH or return;
   return $tmpdir."/".$file.".jdl";
}


return 1;
