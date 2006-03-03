package AliEn::Service::Optimizer::Job::Saved;

use strict;

use AliEn::Service::Optimizer::Job;

use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;

  $self->{SLEEP_PERIOD}=10;
  my $method="info";
  $silent and $method="debug";
  $self->{INSERTING_COUNTING} or $self->{INSERTING_COUNTING}=0;
  $self->{INSERTING_COUNTING}++;
  if ($self->{INSERTING_COUNTING}>10){
    $self->{INSERTING_COUNTING}=0;
  }else {
    $method="debug";
  }

  $self->{LOGGER}->$method("Zombies", "The saved optimizer starts");

  my $todo=$self->{DB}->queryValue("SELECT todo from ACTIONS where action='SAVED'");
  $todo or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='SAVED'");

  my $done=$self->checkJobs($silent, "SAVED", "checkSavedJob");

  return;

}

sub checkSavedJob{
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;
  my $now = time;

  $self->info("********************************\n\tWe should do something with job $queueid");
  my $status="DONE";

  my ($ok, $user)=$job_ca->evaluateAttributeString("user");
  my $procDir = AliEn::Util::getProcDir($user, undef, $queueid);


  ($ok, my @info)=$job_ca->evaluateAttributeVectorString("RegisteredOutput");

  my %filesToRegister;

  if ($ok ){
    $self->{CATALOGUE}->execute("mkdir", "-p", "$procDir/job-output");
    my $files={};
    foreach my $line (@info){
      $self->debug(1,"We should do something about $line");
      my ($file, @links)=split (/;;/, $line);
      my ($lfn, $guid, $size, $md5, $selist, @rest)=split (/###/, $file);
      $files->{$lfn}=1;
      my ($seMaster, @seReplicas)=split (/,/, $selist);
      if (!$self->{CATALOGUE}->execute("register","$procDir/job-output/$lfn","/dev/null", $size, $seMaster, $guid, "-force", "-silent")){
	$self->info("Error registering the entry in the catalog");
	$self->putJobLog($queueid,"error", "Error registering the file $lfn in the catalogue");
	next;
      }
      foreach my $replica (@seReplicas){
	$self->{CATALOGUE}->execute("addMirror", "$procDir/job-output/$lfn", $replica);
      }
      $self->info("$lfn registered!!");
      my $newPfn="guid:///$guid";
      foreach my $link (@links) {
	$self->info("Ready to register the link $link" );
	my ($file, $size, $md5)=split (/###/, $link);
	my $pfn="$newPfn?ZIP=$file";
	if ($filesToRegister{$file}) {
	  $self->debug(1,"This is a replica");
	  $filesToRegister{$file}->{selist}.=",$selist";
	}else {
	  $filesToRegister{$file}={lfn=>"$file",
				   pfn=>$pfn,
				   size=>$size,
				   md5=>$md5,
				   selist=>$selist,
				  };
	}
      }
    }
    $self->info("Doing the multiinsert now");
    
    my @filesToRegister=values %filesToRegister;
    $self->{CATALOGUE}->f_bulkRegisterFile("$procDir/job-output", \@filesToRegister);

    ($ok, my $outputDir)=$job_ca->evaluateAttributeString("OutputDir");
    if ($ok) {
      $self->info("The files have to be copied to $outputDir");
      $self->{CATALOGUE}->execute("mkdir", "-p", $outputDir);
      $self->{CATALOGUE}->execute("cp", "-k","$procDir/job-output/", $outputDir, "-user", $user);
    }
  }

  $self->info("Checking the log files");

  ($ok,  @info)=$job_ca->evaluateAttributeVectorString("RegisteredLog");
  if ($ok) {
    $self->{CATALOGUE}->execute("mkdir", "-p", "$procDir/job-log");
    foreach my $line (@info){
      $self->info("We should do something about $line");
      my ($lfn, $guid, $size, $md5, $selist, @rest)=split (/###/, $line);
      $selist or $selist="";
      $size or $size="";
      $md5 or $md5="";
      if (!$self->{CATALOGUE}->execute("register","$procDir/job-log/$lfn","/dev/null", $size, $selist, $guid, "-force", "-silent")){
	$self->info("Error registering the entry in the catalog");
	$self->putJobLog($queueid,"error", "Error registering the file $lfn in the catalogue");
	next;
      }
    }	
  }


  
  $self->{DB}->updateStatus($queueid,"SAVED", $status);
  $self->info("Status updated");
  $self->putJobLog($queueid,"state", "Job state transition from SAVED to $status");

  return 1;
}

1
