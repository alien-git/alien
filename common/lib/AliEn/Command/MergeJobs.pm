#/**************************************************************************
# * Copyright(c) 2001-2002, ALICE Experiment at CERN, All rights reserved. *
# *                                                                        *
# * Author: The ALICE Off-line Project / AliEn Team                        *
# * Contributors are mentioned in the code where appropriate.              *
# *                                                                        *
# * Permission to use, copy, modify and distribute this software and its   *
# * documentation strictly for non-commercial purposes is hereby granted   *
# * without fee, provided that the above copyright notice appears in all   *
# * copies and that both the copyright notice and this permission notice   *
# * appear in the supporting documentation. The authors make no claims     *
# * about the suitability of this software for any purpose. It is          *
# * provided "as is" without express or implied warranty.                  *
# **************************************************************************/

package AliEn::Command::MergeJobs;

select(STDERR);
$| = 1;    # make unbuffered
select(STDOUT);
$| = 1;    # make unbuffered

use AliEn::Command;
@ISA = (AliEn::Command);

use strict;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Logger;
use AliEn::Util;

sub Initialize {
  my $self = shift;


  $self->SUPER::Initialize() or print STDERR "NOP!" and return;

  $self->{ID} or print "NO ID specified\n" and return;
  $self->{UI}=new AliEn::UI::Catalogue::LCM::Computer or return;
  
  return 1;
}

sub Help {
  printf "Usage: MergeJobs.pl [--help]\n";
  printf "                  [--round <name>][--run <#>]\n";
  printf
    "                  [--event <#>][--config <file>][--comment <string>]\n";
  printf "                  [--debug]\n";
  exit(1);
}

sub Execute {
  my $self = shift;

#  my @jobs=$self->waitForJobs;
  print "\n\nMerging jobs\n"; 
  my @jobs=$self->{UI}->execute("top", "-silent", "-split", "$self->{ID}", "-status","DONE");
  
  map { s/^(\d+)\#\#\#.*/$1/} @jobs;
  @jobs or return;
  @jobs=sort @jobs;

  $self->{LOGGER}->info("MergeJobs", ($#jobs+1)." jobs to merge= @jobs");

  $self->{LOGGER}->info("MergeJobs", "\n\nGetting the output of the jobs ($self->{OUTPUT})");

  if (!$self->{MERGE})    {
    return $self->CopyOutput($self->{OUTPUT}, @jobs);
  }

  my $user = AliEn::Util::getJobUserByUI($self->{UI}, $self->{ID});
  my $procDir = AliEn::Util::getProcDir($user, undef, $self->{ID});

  $self->{LOGGER}->info("MergeJob", "Getting the input of all the jobs");
  my @output=split ",", $self->{OUTPUT};

  my @files=();
  foreach my $job (@jobs) {
    foreach my $file (@output) {
      push @files, "$procDir/$file";
    }
  }
  $self->{UI}->execute("preFetch", @files);
  $self->{LOGGER}->debug("MergeJobs","Prefetch done");
      
  mkdir "$self->{WORK_DIRECTORY}/merge";
  chdir "$self->{WORK_DIRECTORY}/merge";
  chmod 0755, "$self->{WORK_DIRECTORY}/$self->{MERGE}";

  foreach my $file (@output) {
    my $i=0;
    $self->{LOGGER}->info("MergeJobs","Merging $file Getting ". ($#jobs+1)." files ");
      
    foreach my $job (@jobs) {
	my $dir="$self->{WORK_DIRECTORY}/merge/$i";
	if (! -d $dir) { 
	    mkdir $dir or print "ERROR creating $dir\n" and return;
	}
	$self->{UI}->execute("get", "-silent", "$procDir/$file", "$dir/$file")
	    or $self->{LOGGER}->info("MergeJobs", "Warning $procDir/$file was not there") and $i--;
	$i++;
    }
    $self->{LOGGER}->info("MergeJobs", "Got $i files to merge");
#    $self->{LOGGER}->info("MergeJobs", "Got $i files to merge");

    $self->{LOGGER}->info("MergeJobs", "Doing $self->{MERGE} $file");
    my $done=system( "$self->{WORK_DIRECTORY}/$self->{MERGE}", $file, $i);
    $self->{LOGGER}->info("MergeJobs", "Done with $done");
    if (!$done) {
      $self->{LOGGER}->info("MergeJobs", "Merging worked!!");
      $self->Register("$self->{WORK_DIRECTORY}/merge/$file", $file);      
    }

  }

  $self->{LOGGER}->info("MergeJobs", "Merging the ouput of the jobs done");
  return 1;
}
sub CopyOutput {
  my $self=shift;
  my $output=shift;
  my @jobs=@_;

  my @output=split (",", $output);

  my $user = AliEn::Util::getJobUserByUI($self->{UI}, $ENV{ALIEN_PROC_ID});
  my $procDir = AliEn::Util::getProcDir($user, undef, $ENV{ALIEN_PROC_ID});

  $self->{LOGGER}->info("MergeJobs", "Copying the output of all the jobs");
  my $i=1;
  foreach my $job (@jobs) {
    my $dir="$procDir/merge/$i";
    $self->{LOGGER}->info("MergeJobs", "Doing $dir");
    $self->{UI}->execute("mkdir","-p",$dir) or return;
    my $jobDir = AliEn::Util::getProcDir($user, undef, $job);

    foreach my $file (@output) {
      $self->{UI}->execute("cp", "-silent", "$jobDir/$file", "$dir/$file")
	or $self->{LOGGER}->warning("MergeJob", "File $jobDir/$file failed") ;
    }
    $i++;
  }
  return 1;
}


return 1;

