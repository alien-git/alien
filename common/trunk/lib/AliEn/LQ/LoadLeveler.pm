package AliEn::LQ::LoadLeveler;

use strict;
use warnings;

use AliEn::LQ;
use AliEn::Config;
use AliEn::TMPFile;
use vars qw(@ISA);

@ISA = qw( AliEn::LQ );

sub submit {
    my $self       = shift;
    my $classad    = shift;  

    my $jobfile = $self->generateJOB();
    $jobfile or return;

    # Generate commandline to submit job
    my $submit=$self->{CONFIG}->{CE_SUBMITCMD} || "llsubmit";
     
    $self->info("Submitting to LoadLeveler with: $submit $jobfile\n");
     
   open (SUBMIT, "$submit $jobfile 2>&1|") or print "Error  calling '$submit $jobfile'\n" and return;
   my $contact=join("",<SUBMIT>);
   my $done=close SUBMIT or print "Error executing the job: $!\n" and return;
    print "The contact is '$contact'\n";
   if ( $contact) { 
       $contact =~ s/The job "(\S+)" has been submitted/$1/ or print "The ouput was not what we expected\n" and $done=undef;
    } 

    # Check for error on submit and log error info if necessary
    if (! $done) {
      $contact or $contact="";
      $self->{LOGGER}->warning("LoadLeveler","Error submitting the job. Log file '$contact'\n");
      -f $contact and system ('cat', $contact, '>/dev/null 1>&2');
      return -1;
    } else {
      $self->info("LoadLeveler JobID is $contact");
      $self->{LAST_JOB_ID} = $contact;
    }
     
    return 0;
}

sub kill {
    # Initialize variables from parameters
    my $self    = shift;
    my $queueid = shift;

    # Check that $queueid is defined 
    $queueid or return;
    
    # Check that the job exists
    my ($contact )= $self->getContactByQueueID($queueid);
    if (!$contact) {
      #$self->{LOGGER}->error("LCG", "The job $queueid is not here");
      $self->{LOGGER}->error("LoadLeveler", "The job $queueid is not here");
      return 1;
    }

    # Information
    $self->info("Killing job $queueid, JobID is $contact");

    # Kill the job
    #my $error = system( "edg-job-cancel",  "--noint","$contact" );
    my $cancel=$self->{CONFIG}->{CE_KILLCMD} || "llcancel";
    my $error = system( $cancel,"$contact" );
    return $error;
}

sub getBatchId {
    # Initialize variable from parameter
    my $self = shift;
    
    # Return batch id
    return $self->{LAST_JOB_ID};
}

sub getStatus {
    # Initialize variables from parameters
    my $self = shift;
    my $queueid = shift;

    # Check that $queueid is defined 
    $queueid or return;

    # Information
    $self->info("GetStatus: getting status from LoadLeveler for $queueid");
    
    # Check if job is still queued
    my $LLStatus = $self->getJobStatus($queueid);
    $LLStatus or return 'DEQUEUED';
    chomp $LLStatus;
    
    # Show job status and check if job is being dequeued
    $self->debug(1,"LoadLeveler Job $queueid is $LLStatus");
    if ( $LLStatus eq "CA" ||
      $LLStatus eq "RM" ||
      $LLStatus eq "NQ" )  {
      return 'DEQUEUED';
    }
    
    # Check if job is finished 
    if ( $LLStatus eq "C" )  {
      my $outdir = "$self->{CONFIG}->{TMP_DIR}/JobOutput.$queueid";
      my ($contact) = $self->getContactByQueueID($queueid);
      $contact or return;
      $self->info("Will retrieve OutputSandbox for job $queueid, JobID is $contact");
      system("mkdir -p $outdir");
      my $cp="dummycp";
      if ($self->{CONFIG}->{CE_SUBMITCMD}){
         $cp=$self->{CONFIG}->{CE_SUBMITCMD};
         $cp =~ s/llsubmit$/dummycp/;
      }
      system("$cp -dir $outdir $contact");
      return 'DEQUEUED';
    }
    
    # Job is queued
    return 'QUEUED';
}

sub getAllBatchIds {
    # Initialize variables from parameters
    my $self = shift;
    
    # Initialize other variables
    #my $jobIds = $self->{TXT}->queryColumn("SELECT batchId FROM JOBS");
    my @queuedJobs = ();

    #foreach (@$jobIds) {
      #$_ or next;

    # Query for all batch ids - store result in array
    my $user = getpwuid($<);      # always aliprod ???
    #my $user = "aliprod";
    my $status=$self->{CONFIG}->{CE_STATUSCMD} || "llq";
    my $args="";
    if ($self->{CONFIG}->{CE_STATUSARG}){
	$args=join("", @{$self->{CONFIG}->{CE_STATUSARG_LIST}});
    }

	
    open LB,"$status -u $user $args|"; # or next;
    my @output = <LB>;
    close LB;
	  
    my ($id );
    
    # Parse every job line for status information
    foreach my $entry (@output) {
      #print ">> " . $entry;
      $entry =~ /^Id\s+Owner*/ and next;                # Skip header line (id, owner, etc.)
      $entry =~ /^--*/ and next;                        # Skip seperator line (------)
      $entry =~ /^(\S+)\s+\S+\s+\S+\s+\S+\s+(\S+)*/ or next;    # Get first and fifth column (id and status)
      $id = $1;
      $status = $2;
      print "Id $id has status $status\n";
      if ($status !~ /^(NQ)|(RM)|(CA)|(C)$/) {          # NotQueued, Removed, Canceled, Completed
        print "The job is queued ($status)\n";
        push @queuedJobs, $id;
      }
    }
    
    # Possible job status in LoadLeveler
    #
    # E  - Pre-empted
    # H  - User Hold
    # I  - Idle
    # NQ - NotQueued
    # P  - Pending
    # ST - Starting
    # R  - Running
    # RM - Removed
    # S  - System Hold
    # CA - Canceled
    # C  - Completed
    # D  - Deferred
    # HS - System User Hold
    # CK - Checkpointing

    # Information
    print "The queuedJobs are @queuedJobs\n";  
    return @queuedJobs;
}

sub getNumberRunning() {
    # Initialize variables from parameters
    my $self = shift;
    
    # Return result
    return $self->getAllBatchIds();
}

#
#---------------------------------------------------------------------
#

sub generateJOB {
    # Initialize variables from parameters
    my $self = shift;
    $self->{COUNTER} or $self->{COUNTER}=0;
    my $file = "alien-submit.$$.$self->{COUNTER}";
    $self->{COUNTER}++;
    my $tmpdir = "$self->{CONFIG}->{TMP_DIR}";
    
    # If tmpdir does not exist, create it
    if ( !( -d $self->{CONFIG}->{TMP_DIR} ) ) {
      my $dir = "";
      foreach ( split ( "/", $self->{CONFIG}->{TMP_DIR} ) ) {
        $dir .= "/$_";
        mkdir $dir, 0777;
      }
    }

    # If job description file already exists, show error message
    if (-e "$tmpdir/$file.job") {
      $self->debug(1,"File $tmpdir/$file.job exists!\n");
    }
    
    # Create job description file
    open( JOBDESCRIPTION, ">$tmpdir/$file.job" )
      or print STDERR "Can't open file '$tmpdir/$file.job': $!"
      and return;
    my $fullFile=AliEn::TMPFile->new({filename=>$file}); 
    $fullFile or $self->info("Error generating the file name for the outpur") and return; 
    my @list=();
    $self->{CONFIG}->{CE_SUBMITARG} and push @list, @{$self->{CONFIG}->{CE_SUBMITARG_LIST}};
    $self->info("The options are @list ");
    map { s/^(.*)/\# \@ $1\n/} @list;
    print JOBDESCRIPTION
	"#!/bin/sh\n",
        "# @ output = $fullFile.out\n",
	"# @ error = $fullFile.err\n",
	"# @ wall_clock_limit = $self->{CONFIG}->{CE_TTL}\n",
	"# @ job_type = serial\n",
	@list,
	"# @ queue\n",
        "export ALIEN_CM_AS_LDAP_PROXY=$self->{CONFIG}->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}\n",
        "export ALIEN_LOG=AliEn.JobAgent.$$.$self->{COUNTER}\n",
        "export ALIEN_JOBAGENT_ID=${$}_$self->{COUNTER}\n",
	"$ENV{ALIEN_ROOT}/bin/alien RunAgent\n";
    close JOBDESCRIPTION;

    return $tmpdir."/".$file.".job";
}

# End of file
return 1;
