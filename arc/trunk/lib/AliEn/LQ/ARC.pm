package AliEn::LQ::ARC;

use AliEn::LQ;
use AliEn::Config;
@ISA = qw( AliEn::LQ);
use strict;

sub submit {
     my $self       = shift;
     my $classad=shift;
     my $executable = shift;
     my $arguments  = join " ", @_;
     
	 my $file = "dg-submit.$$";
     my $tmpdir = "$self->{CONFIG}->{TMP_DIR}";
     if ( !( -d $self->{CONFIG}->{TMP_DIR} ) ) {
        my $dir = "";
        foreach ( split ( "/", $self->{CONFIG}->{TMP_DIR} ) ) {
            $dir .= "/$_";
            mkdir $dir, 0777;
        }
     }
	 
	 #my $requirements=$self->GetJobRequirements();
     #$requirements or return;
	 #$self->debug(1,"Requirements:$requirements \n");
	 
	 
	 my $xrslfile = $self->generateXRSL($classad);
     $xrslfile or return;
	 
     my @args=();
     $self->{CONFIG}->{CE_SUBMITARG} and
        @args=split (/\s+/, $self->{CONFIG}->{CE_SUBMITARG});
     
   
     if (-e $xrslfile) {
	 $self->debug(1,"File $xrslfile exists!\n");
	 
	 }
	  
     $self->info("Submitting to ARC  with \'@args\'.");
     
     my @command = ( "$self->{CONFIG}->{CE_ARC_LOCATION}/bin/ngsub -f $xrslfile", "@args");
     
     $self->info("Submitting to ARC with: @command\n");
     
     open SAVEOUT,  ">&STDOUT";
     if ( !open STDOUT, ">$self->{CONFIG}->{TMP_DIR}/stdout" ) {
         return 1;
     }
     my $error=system("@command");
     close STDOUT;
     open STDOUT, ">&SAVEOUT";
	  
     open (FILE, "<$self->{CONFIG}->{TMP_DIR}/stdout") or return 1;
     my $contact=<FILE>;
     close FILE;
     $contact and
       chomp $contact;
	 my @cstring = split(":",$contact);
	 $contact = "$cstring[1]:$cstring[2]:$cstring[3]";

     if ($error) {
       $contact or $contact="";
       $self->{LOGGER}->warning("ARC","Error submitting the job. Log file '$contact'\n");
       $contact and system ('cat', $contact, '>/dev/null 1>&2');
       return $error;
     } else {
       $self->info("ARC JobID is $contact");
       $self->{LAST_JOB_ID} = $contact;
       open JOBIDS, ">>$self->{CONFIG}->{LOG_DIR}/CE.db/JOBIDS";
       print JOBIDS "$contact\n";
       close JOBIDS;
     }
     
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

     #my $error = system( "edg-job-cancel",  "--noint","$contact" );
     my $error = system( "$self->{CONFIG}->{CE_ARC_LOCATION}/bin/ngkill","$contact" );
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
     $self->info("GetStatus: getting status from ARC for $queueid");
     my $ARCStatus =  $self->getJobStatus($queueid);
     $ARCStatus or return 'DEQUEUED';
     chomp $ARCStatus;


     $self->debug(1,"ARC Job $queueid is $ARCStatus");
     if ( $ARCStatus eq "CANCELING" ||
         $ARCStatus eq "FINISHING" ||
         $ARCStatus eq "DELETED")  {
          return 'DEQUEUED';
     }
     if ( $ARCStatus eq "FINISHED" )  {
        my $outdir = "$self->{CONFIG}->{TMP_DIR}/JobOutput.$queueid";
        my ($contact )= $self->getContactByQueueID($queueid);
        $contact or return;
        $self->info("Will retrieve OutputSandbox for job $queueid, JobID is $contact");
        system("mkdir -p $outdir");
        system("$self->{CONFIG}->{CE_ARC_LOCATION}/bin/ngget -dir $outdir $contact");
        return 'DEQUEUED';
     }
     return 'QUEUED';
}

sub getAllBatchIds {
  my $self = shift;
#  my $jobIds = $self->{TXT}->queryColumn("SELECT batchId FROM JOBS");
  my @queuedJobs = ();
#  foreach (@$jobIds) {
#     $_ or next;
     open LB,"$self->{CONFIG}->{CE_ARC_LOCATION}/bin/ngstat -a |" or next;
     my @output = <LB>;
     close LB;
	 print "\n START DEBUGGING \n";
     my ($id, $status);
     foreach my $entry (@output){
        $entry =~ /^Job (gsiftp.*)$/ and $id=$1 and next;
        if ($entry=~ /^  Status:\s*(\S+)/){
          $status=$1;
          $id or print "Error found a status, but there is no id\n" and next;
          print "Id $id has status $status\n";
          if ($status !~ /^(CANCELING)|(FINISHED)|(FINISHING)|(DELETED)|(FAILED)$/){
            print "The job is queued ($status)\n";
	    push @queuedJobs, $id;
          }	
	  undef $id;
        }       
     }
#  }
  print "The queuedJobs are @queuedJobs\n";
  return @queuedJobs;
}

sub getNumberRunning() {
  my $self = shift;
  return $self->getAllBatchIds();
}

#
#---------------------------------------------------------------------
#

sub generateXRSL {
   my $self = shift;
   my $ca = shift;
  #my $requirements = $self->translateRequirements($ca);
#   my $file = "dg-submit.$$";
#   my $tmpdir = "$self->{CONFIG}->{TMP_DIR}";
#   if ( !( -d $self->{CONFIG}->{TMP_DIR} ) ) {
#     my $dir = "";
#     foreach ( split ( "/", $self->{CONFIG}->{TMP_DIR} ) ) {
#       $dir .= "/$_";
#       mkdir $dir, 0777;
#     }
#   }
   my ($ok, $ttl)=$ca->evaluateAttributeString("TTL");
  if ($ttl){
    $self->info("We are supposed to send a job with $ttl seconds");
    $ttl="(cpuTime=\"$ttl\")";
  }
   my $fullName=AliEn::TMPFile->new({filename=>"arc-submit.$$"})
    or return;
   my $file=$fullName;
   $file=~ s/^.*([^\/]*)$/$1/;
   open( BATCH, ">$fullName.xrsl" )
       or print STDERR "Can't open file '$fullName.xrsl': $!"
       and return;
   print BATCH "&
(jobName = \"AliEn-$file\")  
(executable = \"$file.sh\")
(stdout = \"std.out\")
$ttl
(stderr = \"std.err\")
(inputFiles = (\"$file.sh\" \"/tmp/$file.sh\"))
(outputFiles = ( \"std.err\" \"\") (  \"std.out\"  \"\") (  \"gm.log\"  \"\")(\"$file.sh\" \"\"))
(environment = (ALIEN_JOBAGENT_ID $ENV{ALIEN_JOBAGENT_ID})(ALIEN_CM_AS_LDAP_PROXY $ENV{ALIEN_CM_AS_LDAP_PROXY})(ALIEN_SE_MSS $ENV{ALIEN_SE_MSS})(ALIEN_SE_FULLNAME $ENV{ALIEN_SE_FULLNAME})(ALIEN_SE_SAVEDIR $ENV{ALIEN_SE_SAVEDIR}))
"
#$requirements
;
   close BATCH;
   open( BATCH, ">$fullName.sh" )
       or print STDERR "Can't open file '$fullName.sh': $!"
       and return;
   print BATCH "
\#!/bin/sh
\# Script to run AliEn on ARC
\# Automatically generated by AliEn running on $ENV{HOSTNAME}
pwd
ls -alrt
less $file.sh
#$self->{CONFIG}->{CE_ALIEN_LOCATION}/bin/alien proxy-init -valid 720:00
$self->{CONFIG}->{CE_ALIEN_LOCATION}/bin/alien RunAgent
";

   close BATCH or return;
   return "$fullName.xrsl";
}

return 1;

