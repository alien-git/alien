package AliEn::UI::Catalogue::LCM::Computer;

use AliEn::CE;
use AliEn::ClientCE;
use AliEn::PackMan;

use AliEn::UI::Catalogue::LCM;

@ISA = qw( AliEn::UI::Catalogue::LCM );

use strict;
use Data::Dumper;
my (%command_list);

%command_list = (
    'jquota'    => ['$self->{QUEUE}->f_jquota', 0],
    'calculateJobQuota'        => ['$self->{QUEUE}->calculateJobQuota',0],
    'top'      => ['$self->{QUEUE}->f_top', 0],
    'ps'       => ['$self->{QUEUE}->f_ps', 0],
    'ps2'      => ['$self->{QUEUE}->f_ps2', 0],
    'system'   => ['$self->{QUEUE}->f_system', 0],
    'jobinfo'  => ['$self->{QUEUE}->f_jobsystem', 0],
    'queueinfo'=> ['$self->{QUEUE}->f_queueinfo', 0],
    'kill'     => ['$self->{QUEUE}->f_kill', 0],
    'status'   => ['$self->{QUEUE}->f_queueStatus', 0],
    'request'  => ['$self->{QUEUE}->offerAgent', 0],
    'submit'   => ['$self->{QUEUE}->submitCommand', 0],
    'resubmit' => ['$self->{QUEUE}->resubmitCommand', 0],
    'validate' => ['$self->{QUEUE}->f_validate', 0],
    'quit'     => ['$self->{QUEUE}->f_quit', 0],
    'exit'     => ['$self->{QUEUE}->f_quit', 0],
    'pgroup'   => ['$self->{QUEUE}->f_pgroup', 0],
    'spy'      => ['$self->{QUEUE}->f_spy', 0],
    'queue'    => ['$self->{QUEUE}->f_queue',0],
 #   'packman'  => ['$self->{PACKMAN}->f_packman',0],
    'masterJob'=> ['$self->{QUEUE}->masterJob',0],
    'checkAgents'=> ['$self->{QUEUE}->checkJobAgents',0],
    'resyncJobAgent'=>['$self->{QUEUE}->resyncJobAgent',0],
    'cleanCache' => ['$self->cleanCache',0],
    'registerOutput' => ['$self->registerOutput',0],
    
#     'jobFindReqMiss'=>['$self->{QUEUE}->jobFindReqMiss',0],
    
    #bank functions
    'gold'      => ['$self->{QUEUE}->f_bank',0],
   
    'jobListMatch'=>['$self->{QUEUE}->f_jobListMatch',67],
		 'killAllAgents'=>['$self->{QUEUE}->f_killAllAgents',0],

);

my %help_list = (
    'help'     => "\tDisplay this message",
    'jquota'     => "\tDisplay job quota information",
    'top'      => "\tDisplay all running and waiting processes",
    'ps'       => "\tDisplays process information",
    'system'   => "\tDisplays system usage information",
    'jobinfo'  => "\tDisplays system usage information for job with tag <jobtag>",
    'kill'     => "\tKill a process",
    'request'  => "Request (and executes) a new command from the server",
    'submit'   => "Submit a command to the server",
    'resubmit' => "Resubmit a command that has failed",
    'validate' => "Submit a job to validate the output",
    'quit'     => "\tExit the program",
    'exit'     => "\tExit the program",
    'pgroup'   => "\tHandling of Processgroups",
    'spy'      => "\tSpy on the output of a process, the workdir or the worker node", 		 
    'queue'    => "\tOpen or close the the queue of a site",
  #  'packman'  => "\tTalks to the Package Manager (PackMan). Use 'packman --help' for more info",
    'masterJob'=> "\tDisplays information about a masterjob and all of its children",
		 'killAllAgents'=>"\tKill all the jobagents in the site. Warning! Do not use it unless you know what you are doing",

     #bank functions
    'gold'=>"\tExecute AliEn bank command",
    

	'jobListMatch' => '\tMatches the jdl of the job with the CE',
		 
);

sub initialize {
  my $self    = shift;
  my $options = shift;

  $self->SUPER::initialize($options) or return;

  $options->{CATALOG} = $self;

#    my $packOptions={PACKMAN_METHOD=> $options->{packman_method}|| "",
#		     CATALOGUE=>$self};

#    $self->{PACKMAN}= AliEn::PackMan->new($packOptions) or return;

  $options->{PACKMAN}=$self->{PACKMAN};

  if($self->checkEnvelopeCreation()) {
    $self->{QUEUE} = AliEn::CE->new($options) or return;
  }
  else {
    $self->{QUEUE} = AliEn::ClientCE->new($options) or return;
  }

  $self->AddCommands(%command_list);
  $self->AddHelp(%help_list);
}

sub cleanCache {
  my $self=shift;
  AliEn::Util::deleteCache($self->{QUEUE});
  return 1;
}

sub registerOutput_HELP{
  return "Registers in the catalogue the output of a job (if the job saved anything in the SE)
Usage:
\t\tregisterOutput <jobId> [-c]
   -c registers also the execution.out ClusterMonitor log over SOAP
";
}

sub registerOutput{
  my $self=shift;
  $self->checkEnvelopeCreation() or return $self->{CATALOG}->callAuthen("registerOutput",@_);
  my $options={};
  @ARGV=@_;
  Getopt::Long::GetOptions($options, 
    "cluster") 
    or $self->info("Error checking the options of add") and return;
  @_=@ARGV;

  my $jobid=(shift || return 0);
  my $service=(shift || 0);
  my $onlycmlog=0;
  my $regok=0;
  my @failedFiles;

  (my $jobinfo) = $self->execute("ps", "jdl", $jobid, "-dir","-status","-silent") or 
    $self->info("Error getting the jdl of the job",2) and return;
  
  $jobinfo->{jdl} or $self->info("Error the jdl is empty",2) and return;


  my $ca;
  eval {$ca=
    Classad::Classad->new($jobinfo->{jdl}) or $self->info("Error parsing the jdl",2) and return;
  };
  if ($@){
    $self->info("Error creating the classad $@",2);
    return;
  }
  my $outputdir= $jobinfo->{path};

  if($jobinfo->{path}){
    if($options->{cluster}) {
       (my $cmlogexists) = $self->{CATALOG}->existsEntry("$jobinfo->{path}execution.out");
       $cmlogexists and $self->info("The files for this job where already registered in $jobinfo->{path}",2) and return $jobinfo->{path};
       $onlycmlog=1; 
       
    } else {
       $self->info("The files for this job where already registered in $jobinfo->{path}",2) and return $jobinfo->{path};
    }
  }

  $jobinfo->{status} or $self->info("Error getting the status of the job",2) and return;

  my ($ok, @pfns)=$ca->evaluateAttributeVectorString("SuccessfullyBookedPFNS");
  if(!$ok) {
    $options->{cluster}  or $self->info("This job didn't register any output",2) and return;
    $onlycmlog=1;
  }

  ($ok, my $user)=$ca->evaluateAttributeVectorString("User");
  (my $currentuser)=$self->execute("whoami", "-silent");
  if($user ne $currentuser) {
      $self->execute("user","-", $user)
        or $self->info("Error, you are not the user the job belongs to. registerOutput can be only called by the job owner '$user' and you are not allowed to become '$user'.",2) and return;
  }

  if(!$onlycmlog) {
    @failedFiles = $self->{CATALOG}->registerOutputForJobPFNS($user,$jobid, @pfns);
    $regok = shift @failedFiles;
    $outputdir = shift @failedFiles;
    $outputdir and $self->info("The output files were registered in $outputdir") or $self->info("Error during output file registration.") and return;
  }


  if($options->{cluster}) {
     ($ok, my @cmlogs)=$ca->evaluateAttributeVectorString("JobLogOnClusterMonitor");
     foreach my $log (@cmlogs) {
       my ($lfn, $guid, $size, $md5, $pfn)=split (/###/, $log);
       $guid or $guid=AliEn::GUID->new()->CreateGuid();
       if(!$outputdir) {
	  $outputdir="~/alien-job-".$jobid;
	  $self->execute("mkdir","-p",$outputdir);
       }
       my $env={lfn=>"$outputdir/$lfn", md5=>$md5, size=>$size, guid=>$guid};
       $self->{CATALOG}->registerPFNInCatalogue($user,$env,$pfn,"no_se");
     }
  }

  ($user ne $currentuser) and $self->execute("user","-", $currentuser);
  
  if(!$onlycmlog) {
    my ($host, $driver, $db) = split("/", $self->{CONFIG}->{"JOB_DATABASE"});
    if (! $self->{TASK_DB}){ 
       $self->{TASK_DB} = AliEn::Database::TaskQueue->new({DB=>$db,HOST=> $host,DRIVER => $driver,
                                                          ROLE=>'admin', SKIP_CHECK_TABLES=> 1});
        AliEn::Util::setupApMon( $self->{TASK_DB});
   }

    $self->{TASK_DB} or $self->info("Error CE: In initialize creating TaskQueue instance failed",2)
        and return;
  
    my $newstatus = 0;
    if($jobinfo->{status} =~ /^SAVED/){ 
       if($regok eq 1) {
           $newstatus = "DONE_WARN";
           ($jobinfo->{status} eq "SAVED") and $newstatus = "DONE";
       } else {
	   $self->info("At least one file could not been registered, setting to job #$jobid to ERROR_RE.");
           $newstatus = "ERROR_RE";
       }
    }
    #($jobinfo->{status} =~ /^ERROR/) and $newstatus = $jobinfo->{status} ;
    if($newstatus) {
      $self->{TASK_DB}->updateStatus($jobid,$jobinfo->{status}, $newstatus, {path=>$outputdir}, $service);
      if(!($jobinfo->{status} =~ /^ERROR/)) {
        $self->info("Job state transition from $jobinfo->{status} to $newstatus");
      }
    }
    $outputdir and $self->info("Registered output files in: $outputdir");
    foreach (@failedFiles) { $self->info("Error registering $_ "); }
  }
  
  return ($outputdir,@failedFiles);
}

return 1;

