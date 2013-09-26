package AliEn::LQ::ARC;

use AliEn::LQ;
use AliEn::Config;
@ISA = qw( AliEn::LQ);
use strict;

use AliEn::Database::CE;

my $nAllJobs = 0;
my $nQueuedJobs = 0;
my $getAllBatchIds_timestamp = 0;

sub initialize {
    my $self = shift;
    
    $self->{LOCALJOBDB}=new AliEn::Database::CE or return;

    return 1;
}

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
 
     my @args=();
     $self->{CONFIG}->{CE_SUBMITARG} and
        @args=@{$self->{CONFIG}->{CE_SUBMITARG_LIST}};

     my @xrsl=grep (/^xrsl:/, @args);
     map {s/^xrsl://} @xrsl;
     @args=grep (! /^xrsl:/, @args); 

     my $xrslfile = $self->generateXRSL($classad, @xrsl);
     $xrslfile or return;
  
     $self->info("Submitting to ARC  with \'@args\'.");
     
     my @command = ( "arcsub -f $xrslfile", "@args");
     $self->info("Submitting to ARC with: @command\n");
     
     open SAVEOUT,  ">&STDOUT";
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
     if ($contact=~ /gsiftp:\/\//){
         my @cstring = split(":",$contact);
         $contact = "$cstring[1]:$cstring[2]:$cstring[3]";
     } else {
       $error=1;
     }

     if ($error) {
       $contact or $contact="";
       $self->{LOGGER}->warning("ARC","Error submitting the job. Log file '$contact'\n");
       $contact and $contact !~/^:*$/ and system ('cat', $contact, '>/dev/null 1>&2');
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

     my $error = system( "arckill","$contact" );
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
     if ( $ARCStatus =~ /^((FINISHED)|(FAILED))$/ )  {
        my ($contact )= $self->getContactByQueueID($queueid);
        $contact or return;
        return 'DEQUEUED';
     }
     return 'QUEUED';
}


sub getOutputFile {
    my $self = shift;
    my $queueId = shift;
    my $output = shift;

    $self->debug(2,"QUEUEID: $queueId");
    
    my $batchId = $self->getContactByQueueID($queueId);
    $self->debug(2, "The job Agent is: $batchId" );
    
    system("arccp $batchId/alien-job-$queueId/$output /tmp/alien-output-$queueId");
    
    open OUTPUT, "/tmp/alien-output-$queueId";
    my @data = <OUTPUT>;
    close OUTPUT;

    system->("rm /tmp/alien-output-$queueId");

    return join("",@data);
}

sub getAllBatchIds {
    my $self = shift;

    
    # Running arcstat can sometimes be slow, and the situation rarely changes much in a few seconds
    if (time() <= $getAllBatchIds_timestamp + 5) {
        $self->info("Reusing old arcstat result; found $nAllJobs jobs, of which $nQueuedJobs queueing");
        return ($nAllJobs, $nQueuedJobs);
    }

    $nAllJobs = 0;
    $nQueuedJobs = 0;

    open LB,"arcstat -a 2>&1 |" or next;
    my @output = <LB>;
    close LB;

    my ($id, $status, @completedJobs);

    foreach my $entry (@output) {
        $entry =~ /^Job: (gsiftp.*)$/ and $id = $1 and next;
        if ($entry=~ /^  *State:\s*\S+\s*\((\S+)\)/){
            $status = $1;
            $id or $self->info("Error: found a status, but there is no id") and next;
            $self->debug(1,"Id $id has status $status");

            # Remove completed jobs, unless we are in debug mode
            if ($status =~ /^((FINISHED)|(FAILED)|(KILLED))$/ and not $self->{LOGGER}->getDebugLevel()){
                push @completedJobs, $id;

                # Removing many jobs at a time is faster than one at a time,
                # but too many may result in "too long argument list" errors.
                if (@completedJobs >= 500) {
                    $self->info($#completedJobs + 1 . " completed jobs will be removed");
                    system("arcclean " . join(" ", @completedJobs));
                    @completedJobs = ();
                }
            }
  
            if ($status !~ /^(CANCELING)|(FINISHED)|(FINISHING)|(DELETED)|(FAILED)|(KILLED)$/){
                $self->debug(1,"The job is queued ($status)");
                $nAllJobs += 1;
                if ($status =~ /^(ACCEPTING)|(ACCEPTED)|(PREPARING)|(PREPARED)|(SUBMITTING)|(INLRMS:Q)$/) {
                    $nQueuedJobs += 1;
                }
            }
            undef $id;
        } elsif ($entry=~ /Job information not found in the information system: (gsiftp.*)/) {
            $id = $1;
            $nAllJobs += 1; # Probably just the info.sys being slow, or something.
            $self->info("Info for job $id not found");
        } elsif ($entry =~ /This job was very recently submitted/) {
            $nQueuedJobs += 1; # Probably queuing
            $self->info("Job $id is probably queuing");
        }
    }

    if (@completedJobs and not $self->{LOGGER}->getDebugLevel()) {
        $self->info($#completedJobs + 1 . " completed jobs will be removed");
        system("arcclean " . join(" ", @completedJobs));
    }

    $getAllBatchIds_timestamp = time();
    $self->info("Found $nAllJobs jobs, of which $nQueuedJobs queueing");
    return ($nAllJobs, $nQueuedJobs);
}

sub getNumberRunning() {
  my $self = shift;
  my @r = $self->getAllBatchIds();
  return $r[0];
}

sub getNumberQueued() {
  my $self = shift;
  my @r = $self->getAllBatchIds();
  return $r[1];
}


sub getContactByQueueID {
    my $self = shift;
    my $queueId = shift;

    my $info = $self->{LOCALJOBDB}->query("SELECT batchId FROM JOBAGENT where jobId=?", undef, {bind_values=>[$queueId]});

    my $batchId = (@$info)[0]->{batchId};

    $self->info("The job $queueId run in the ARC job $batchId");

    return $batchId;
}


#
#---------------------------------------------------------------------
#

sub generateXRSL {
   my $self = shift;
   my $ca = shift;
   my @args =@_;
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

   map { /^\s*\(.*\)\s*$/ or s/^(.*)$/\($1\)/ } @args;
   @args or @args=("");
   my $args=join("\n", @args);
   $args or $args="";

  $self->info("Hello  world with $args");
  my ($ok, $ttl)= $ca->evaluateExpression("Requirements");
  $self->info("Requirements $ttl");
  if ($ttl and $ttl =~ /TTL\s*[=>]*\s*(\d+)/ ) {
     $self->info("Translating \'TTL\' requirement ($1)");
     my $minutes=int($1/60);
     $ttl = "(cpuTime=\"$minutes minutes\")\n";
     #$ttl = "(cpuTime=\"2880 minutes\")\n";
   } else {
     $ttl="";
   }
   my $fullName=AliEn::TMPFile->new({filename=>"arc-submit.$$"})
    or return;
   my $jobscript="$self->{CONFIG}->{TMP_DIR}/agent.startup.$$";
   $self->info("using jobscript $jobscript");
   my $file=$fullName;
   $file=~ s/^.*\/([^\/]*)$/$1/;
   $self->info("File name $file and fullName $fullName and");

   open( BATCH, ">$fullName.xrsl" )
       or print STDERR "Can't open file '$fullName.xrsl': $!"
       and return;
   print BATCH "&
(jobName = \"AliEn-$file\")  
(executable = /usr/bin/time)
(arguments = bash \"$file.sh\")
(stdout = \"std.out\")
$args
$ttl
(stderr = \"std.err\")
(gmlog = gmlog)
(inputFiles = (\"$file.sh\" \"$jobscript\"))
(outputFiles = ( \"std.err\" \"\") (  \"std.out\"  \"\") (  \"gmlog\"  \"\")(\"$file.sh\" \"\"))
(memory=2048)
(*environment = (ALIEN_JOBAGENT_ID $ENV{ALIEN_JOBAGENT_ID})(ALIEN_CM_AS_LDAP_PROXY $ENV{ALIEN_CM_AS_LDAP_PROXY})(ALIEN_SE_MSS $ENV{ALIEN_SE_MSS})(ALIEN_SE_FULLNAME $ENV{ALIEN_SE_FULLNAME})(ALIEN_SE_SAVEDIR $ENV{ALIEN_SE_SAVEDIR})*)
(*environment = (ALIEN_JOBAGENT_ID $ENV{ALIEN_JOBAGENT_ID})(ALIEN_CM_AS_LDAP_PROXY $ENV{ALIEN_CM_AS_LDAP_PROXY})(ALIEN_SE_MSS file)(ALIEN_SE_FULLNAME ALIENARCTEST::ARCTEST::file)(ALIEN_SE_SAVEDIR /disk/global/aliprod/AliEn-ARC_SE,5000000)*)
(environment = (ALIEN_JOBAGENT_ID $ENV{ALIEN_JOBAGENT_ID})(ALIEN_CM_AS_LDAP_PROXY $ENV{ALIEN_CM_AS_LDAP_PROXY}))
"
;
   close BATCH;
   return "$fullName.xrsl";
}

return 1;

