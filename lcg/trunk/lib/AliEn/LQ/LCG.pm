package AliEn::LQ::LCG;

use AliEn::LQ;

@ISA = qw( AliEn::LQ);

use strict;
use AliEn::Database::CE;
use Data::Dumper;

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
   return 1;

}

sub submit {
     my $self       = shift;
     my $command = shift;
     my $arguments  = join " ", @_;
     my $startTime = time;

     my @args=();
     $self->{CONFIG}->{CE_SUBMITARG} and
      	@args=split (/\s+/, $self->{CONFIG}->{CE_SUBMITARG});
#
     my $jdlfile = $self->generateJDL();
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

sub getQueueStatus {
  my $self = shift;
  print "Hello! I\' m here!\n";
  my $value = $self->{DB}->queryValue("SELECT COUNT (*) FROM JOBAGENT");
  $value or $value = '';
  return $value;
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

sub prepareProxy {
  my $self = shift;
  return 1;
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
     my @command = ( $ENV{GLOBUS_LOCATION}.'/bin/myproxy-get-delegation', 
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

sub generateJDL {
   my $self = shift;
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
\#Requirements = Member(\"VO-alice-AliEn\",other.GlueHostApplicationSoftwareRunTimeEnvironment);
Requirements = other.GlueCEUniqueID==\"lxgate15.cern.ch:2119/jobmanager-lcglsf-grid_alice\";
";

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
hostname -f
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
echo --- alien ProxyInfo ---------------------------
alien ProxyInfo
echo --- Software ----------------------------------
echo --- Run ---------------------------------------
alien RunAgent

rm -f dg-submit.*.sh
ls -lart
";

   close BATCH or return;
   return $tmpdir."/".$file.".jdl";
}


return 1;

