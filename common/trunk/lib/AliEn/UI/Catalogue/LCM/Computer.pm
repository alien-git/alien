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
\t\tregisterOutput <jobId>
";
}
sub registerOutput{
  my $self=shift;
  my $jobid=shift;

  my ($jdl)=$self->execute("ps", "jdl", $jobid, "-silent") or 
    $self->info("Error getting the jdl of the job",2) and return;

  $jdl or $self->info("Error the jdl is empty") and return;

  my $ca;
  eval {$ca=
    Classad::Classad->new($jdl) or $self->info("Error parsing the jdl") and return;
  };
  
  if ($@){
    $self->info("Error creating the classad $@");
    return;
  }
  my ($ok, @info)=$ca->evaluateAttributeVectorString("RegisteredOutput");
  $ok or $self->info("This job didn't register any output") and return;
  my $files={};
  my %filesToRegister;

  my $dir="~/recycle/alien-job-$jobid";
  $self->execute("mkdir", "-p", $dir) or $self->info("Error creating $dir") and return; 
  $self->info("The output files will be registered in: $dir");
  
  foreach my $line (@info){
    my ($file, @links)=split (/;;/, $line);
    my ($lfn, $guid, $size, $md5, $pfn)=split (/###/, $file); 
    $guid or $guid=AliEn::GUID->new()->CreateGuid();
    my $fullpath=$lfn;
    $fullpath=~ /^\// or $fullpath="$dir/$lfn";
    #my $info={lfn=>$lfn, md5=>$md5, size=>$size,    guid=>$guid};
    if ($self->execute("add", "-r -size $size $fullpath -md5 $md5 $pfn -guid $guid")){
      $self->info("File $fullpath registered in the catalogue");
    } else{
      $self->info("Error doing ' add -r -size $size $fullpath -md5 $md5 $pfn -silent -guid $guid");
    #  $self->execute("add", "-r -size $size $fullpath -md5 $md5 $pfn -guid $guid");
    }
    foreach my $link (@links){
      my ($l, $s, $m, $g)=split (/###/, $link);
      $self->info("Doing add -r -size $s $dir/$l -md5 $m guid:///$guid?ZIP=$l");
      $self->execute("add", "-r -size $s $dir/$l -md5 $m guid:///$guid?ZIP=$l", )
         and $self->info("File $dir/$l registered in the catalogue");

    }
    
  }
  
  return 1;
}

return 1;

