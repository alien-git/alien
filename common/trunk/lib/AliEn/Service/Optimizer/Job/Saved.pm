package AliEn::Service::Optimizer::Job::Saved;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::GUID;

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
    my ($olduser)=$self->{CATALOGUE}->execute("whoami");
    $self->{CATALOGUE}->execute("user","-",  $user);

    $self->{CATALOGUE}->execute("mkdir", "-p", "$procDir/job-output");
    foreach my $line (@info){
      $self->registerLine("$procDir/job-output", $line, \%filesToRegister, $queueid);
    }
    $self->info("Doing the multiinsert now");
    
    my @filesToRegister=values %filesToRegister;
    $self->{CATALOGUE}->{CATALOG}->f_bulkRegisterFile("","$procDir/job-output", \@filesToRegister) 
     or $self->putJobLog($queueid,"error", "Error registering the files in the catalogue!!");

    ($ok, my $outputDir)=$job_ca->evaluateAttributeString("OutputDir");
    if ($ok) {
      $self->info("The files have to be copied to $outputDir");
      $self->{CATALOGUE}->execute("mkdir", "-p", $outputDir);
      if (!$self->{CATALOGUE}->execute("cp", "-k","$procDir/job-output/", $outputDir, "-u", $user)) {
	$self->putJobLog($queueid,"error", "Error putting the output in $outputDir");

      }
    }
    $self->{CATALOGUE}->execute("user", "-", $olduser);
  }

  $self->info("Checking the log files");

  ($ok,  @info)=$job_ca->evaluateAttributeVectorString("RegisteredLog");
  if ($ok) {
    $self->{CATALOGUE}->execute("mkdir", "-p", "$procDir/job-log");
    foreach my $line (@info){
      $self->registerLine("$procDir/job-log",$line, \%filesToRegister, $queueid);
    }	
  }


  
  $self->{DB}->updateStatus($queueid,"SAVED", $status);
  $self->info("Status updated");
  $self->putJobLog($queueid,"state", "Job state transition from SAVED to $status");

  return 1;
}


sub registerLine {
  my $self=shift;
  my $dir=shift;
  my $line=shift;
  my $ref=shift;
  my $queueid=shift;
  $self->debug(1,"We should do something about $line");
  my ($file, @links)=split (/;;/, $line);
  my ($lfn, $guid, $size, $md5, @PFN)=split (/###/, $file);
  $guid or $guid=AliEn::GUID->new()->CreateGuid();

  my $info={lfn=>$lfn, md5=>$md5,
	    size=>$size,    guid=>$guid};
  my $selist=",";
  my @list=();
  foreach my $replica (@PFN){
    my ($se, $pfn)=split(/\//, $replica,2);
    $pfn =~ s/\\\?/\?/g;
    push @list, {seName=>$se, pfn=>$pfn};
  }
  @list and $info->{pfns}=\@list;

  if (!$self->{CATALOGUE}->{CATALOG}->f_bulkRegisterFile("i", $dir, [$info])){
    $self->info("Error registering the entry in the catalog");
    $queueid and $self->putJobLog($queueid,"error", "Error registering the file $lfn in the catalogue");
    return;
  }

  $self->info("$lfn registered!!");
  my $newPfn="guid:///$guid";
  foreach my $link (@links) {
    $self->info("Ready to register the link $link" );
    my ($file, $size, $md5, $guid)=split (/###/, $link);
    my $pfn="$newPfn?ZIP=$file";
    if ($ref->{$file}) {
      $self->debug(1,"This is a replica");
      my @list=@{$ref->{$file}->{pfns}};
      push @list, {pfn=>$pfn};
      $ref->{$file}->{pfns}=\@list;
    }else {
      $ref->{$file}={lfn=>"$file", pfns=>[{pfn=>$pfn}],    size=>$size,
		     md5=>$md5,  guid=>$guid,    };
    }
  }
  return 1;
}
1
