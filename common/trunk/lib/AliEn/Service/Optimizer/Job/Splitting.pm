package AliEn::Service::Optimizer::Job::Splitting;

use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

my $self;

my $splitPerDirectory =sub  {
    my $file=shift;
    $file=~ s/\/[^\/]*$//;
    $file=~ s/^lf://i;
    my $arg=$file;

    $arg=~ s/^.*\/([^\/]*)$/-event $1/;

    return $file, $arg;
};
my $splitPerFile =sub  {
    my $file=shift;

    my $arg="";
#    $file=~ s/\/[^\/]*$//;
    return $file, $arg;
};
my $splitPerEvent =sub  {
    my $event=shift;
    $event=~ s/\/[^\/]*$//;
    $event=~ s/^.*\/([^\/]*)$/$1/;

    return $event, "-event $event";
};
my $splitPerSE =sub  {
    my $event=shift;

    $event=~ s/^LF://;
    my @se=$self->{CATALOGUE}->execute("whereis", "-l", "-silent", "$event");

#    @se= grep (/::.*::/, @se);
#    $event=~ s/\/[^\/]*$//;
#    $event=~ s/^.*\/([^\/]*)$/$1/;

    $self->info("Puting it in ". join (",", sort @se));
    return join (",", sort @se), "";
};



sub checkWakesUp {
  $self=shift;
  my $silent=shift;

  my $method="info";
  $silent and $method="debug";

  $self->{LOGGER}->$method("Splitting", "The splitting optimizer starts");
  my $done2=$self->checkJobs($silent, "INSERTING' and jdl like '\% split =\%", 
			     "updateSplitting");

  $self->{LOGGER}->$method("Splitting", "The splitting optimizer finished");
  return;
}

sub updateSplitting {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;
  eval {
    my ($strategy, $jobs)=$self->SplitJob($queueid, $job_ca) or 
      die("The job can't be split\n");

    my ($user)=$self->{DB}->getFieldFromQueue($queueid,"submithost")
      or $self->info("Job $queueid doesn't exist")
	and die ("Error getting the user of $queueid\n");

    #Change the status of the job
    my $job = $self->{DB}->updateStatus($queueid, "INSERTING", "SPLITTING")
      or $self->{LOGGER}->warning("Splitting", "in SubmitSplitJob setting status to 'SPLITTING' failed") 
	and die ("Error setting the job to SPLITTING\n");

    ($job == -1) and $self->info("The job was not waiting any more...") and die ("The job was not waiting any more\n");

    $self->putJobLog($queueid,"state", "Job state transition to SPLITTING");

    if ($strategy !~ /^userDefined/) {
      $self->SubmitSplitJob($job_ca, $queueid, $user, $jobs) or
	die ("Error submitting the subjobs\n");
    } else {
      my ($ok, @def)=$job_ca->evaluateAttributeVectorString("SplitDefinitions");
      foreach my $jdl (@def) {
	$self->_submitJDL($queueid, $user, $jdl) or
	  die("Error submitting one of the splitDefinitions: $jdl\n");
      }
    }
    #    $self->ChangeOriginalJob($job_ca, $queueid, $user);
    $self->info( "Putting the status of $queueid to 'SPLIT'");
    $self->{DB}->updateStatus($queueid,"SPLITTING","SPLIT", {masterjob=>1})
      or $self->info("Error updating status for job $queueid" )
	and die("Error changing the status\n");;
    $self->putJobLog($queueid,"state", "Job state transition from SPLITTING to SPLIT");
  };
  if ($@) {
    $self->info("Error splitting $queueid: $@");
    $self->putJobLog($queueid,"error", "Error splitting: $@");

    $self->{DB}->updateStatus($queueid,"%","ERROR_SPLT")
      or $self->info("Error updating status for job $queueid" );
    $self->putJobLog($queueid,"state","Job state transition to ERROR_SPLT");
    return ;
  }
  return 1 ;
}


sub SplitJob{
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;

  my ($ok, $split)=$job_ca->evaluateAttributeString("Split");
  my ($oksize,     $inputfilesize) = $job_ca->evaluateAttributeString("SplitMaxInputFileSize");
  my ($oknumber, $inputfilenumber) = $job_ca->evaluateAttributeString("SplitMaxInputFileNumber");

  my @inputdataset;
  my $findset="";
  $#inputdataset=-1;

  ( $ok, @inputdataset ) =
    $job_ca->evaluateAttributeVectorString("InputDataSet");

  if ($#inputdataset > -1) {
    $findset = "-i " . join (",",@inputdataset);
  }


  $split or $self->info("This is not a Split Job");
  $split or return;
  $self->info("Trying to split $queueid according to $split");
  my $sort="";
  my $multisplit=0;
  my $run=0;
  my $eventstart=0;
  my $eventstop =0;

  ($split =~ /^directory$/i) and $sort= $splitPerDirectory;
  ($split =~ /^file$/i) and $sort=$splitPerFile;
  ($split =~ /^event$/i) and $sort=$splitPerEvent;
  ($split =~ /^se$/i) and $sort=$splitPerSE;
  ($split =~ /^\-(.*)/) and $sort=1 and $multisplit = $1;
  ($split =~ /^production:(.+)-(.+)/) and $sort=1 and $eventstart = $1 and $eventstop = $2 and $multisplit = ($eventstop-$eventstart+1);

  if ($split =~ /^userdefined$/ ) {
    $self->info("The user gave us the splitting jdls");
    my ($ok, @jdls)=$job_ca->evaluateAttributeVectorString("SplitDefinitions");
    @jdls or $self->info("There are no splitdefinitions") and return;
    return ("userdefined");
  }

  $sort or
    $self->info("Don't know how to sort by $split")
      and return;


  my $jobs;
  if ($multisplit) {
    $jobs=$self->_multiSplit($multisplit, $eventstart, $run)
  } else {
    $jobs=$self->_singleSplit($job_ca, $sort, $inputfilenumber,$inputfilesize,$findset, $queueid);
  }

  $jobs or return;

  my $total=keys %{$jobs};
  print "Splitting: the job can be split in  $total\n";
  ($total>0) or return;
  return ( $split,$jobs );

}
sub _getInputFiles{
  my $self=shift;
  my $job_ca=shift;
  my $findset=shift;
  my $queueId=shift;

  my ($ok, @patterns)=$job_ca->evaluateAttributeVectorString("InputData");
  print "Checking if there is an inputcollection\n";
  $self->copyInputCollection($job_ca, $queueId, \@patterns);
  @patterns or $self->info( "There is no input data")
    and return;
  
  my @files=();

  foreach my $file (@patterns) {
    if ($file=~ /\*/) {
      my ($name, $dir);
      if ($file=~ /^([^\*]*)\*(.*)$/) { $dir=$1; $name=$2};
      $dir=~ s/LF://;
      if ( $name =~ /(.*)\[(\d*)\-(\d*)\]/) {
	$name = $1;
	my $start = $2;
	my $stop  = $3;
	$self->info("Looking for $dir $name");
	my @entries=$self->{CATALOGUE}->execute( "find", "-silent", "-l $stop", "$findset", "$dir", "$name" );
	my $cnt=1;
	foreach (@entries) {
	  if ( ($cnt >= $start) && ($cnt <= $stop) ) {
	    push @files, "LF:$_";
	    $cnt++;
	  }
	}
      } else {
	$self->info("Looking for $dir $name");
	my @entries=$self->{CATALOGUE}->execute( "find", "-silent $findset", "$dir", "$name");
	map {$_="LF:$_";} @entries;
	push  @files, @entries;
      }
    } else {
      $self->info("Inserting $file");
      push @files, $file;
    }
  }
  return @files;
}

sub _singleSplit {
  my $self=shift;
  my $job_ca=shift;
  my $sort=shift;
  my $inputfilenumber=shift;
  my $inputfilesize=shift;
  my $findset=shift;
  my $queueId=shift;

#  my ($ok, @patterns)=$job_ca->evaluateAttributeVectorString("InputData");
#  @patterns or $self->info( "There is no input data")
#	    and return;

  my @files=$self->_getInputFiles($job_ca, $findset, $queueId) or return;

  my $jobs={};
  $self->debug(1, "In SplitJob got @files");
  foreach my $file (@files) {
    $self->debug(1, "In SplitJob sorting $file");
    my ($pos, $arg)=$sort->($file);
    $self->debug(1, "Should go in $pos, $arg");
    my @list=();
    my $subpos=0;
    my $newpos = "$pos;$subpos";
    $jobs->{$newpos} or $jobs->{$newpos}={nfiles=>0, filesize=>0, counter=>"$pos"};
    
    if ($inputfilenumber) {
      # the user requests a maxmimum number of inputfiles per splitjob
      while ( (defined $jobs->{$newpos}->{nfiles}) && ($jobs->{$newpos}->{nfiles} >= $inputfilenumber) ) {
	$subpos++;
	$newpos = "$pos;$subpos";
	$jobs->{$newpos} or $jobs->{$newpos}={nfiles=>0, filesize=>0};
      }
    }
    
    if ($inputfilesize) {
      # the user requests a maximum number of inputfilesize per splitjob
      while ( (defined $jobs->{$newpos}->{filesize}) && ($jobs->{$newpos}->{filesize} >= $inputfilesize) ) {
	$subpos++;
	$newpos = "$pos;$subpos";
	$jobs->{$newpos} or $jobs->{$newpos}={nfiles=>0, filesize=>0};
      }
    }
    $jobs->{$newpos}->{args}=$arg;
    push @{$jobs->{$newpos}->{files}}, "\"$file\"";
    # add the file size
    if ($inputfilesize) {
      my $nolffile = $file;
      $nolffile=~ s/^LF://;
      my @res = $self->{CATALOGUE}->{CATALOG}->f_lsInternal("a", $nolffile);
      shift @res;
      shift @res;
      my $rresult = shift @res;
      if (!defined($rresult)) {
	$self->{LOGGER}->warning("Splitting", "Cannot stat file $nolffile");
      }
      elsif ($#{$rresult} == -1) {
	$self->{LOGGER}->warning("Splitting", "Can not stat file $nolffile");
      }
      else {
	$jobs->{$newpos}->{filesize} += @$rresult[0]->{size};
	$self->info("Adding $nolffile with size @$rresult[0]->{size}");
      }
    } else {
      $self->info("Size does not matter for $file");
    }

    $jobs->{$newpos}->{nfiles}++;
  }
  return $jobs;
}

sub _multiSplit {
  my $self=shift;
  my $multisplit=shift;
  my $eventstart=shift;
  my $run=shift;

  $self->info("In SplitJob got $multisplit subjobs");
  my $jobs={};
  my $event = $eventstart;
  foreach my $subjob (1 .. $multisplit) {
    $self->info("In SplitJob doing subjob $subjob (Run $run Event $event)");
    $jobs->{$subjob}={files=> [],
		      args=> "",
		      counter=>$event};
    if ($run) {
      # don't remove the space after event!
      $jobs->{$subjob}->{args}="--run $run --event $event ";
    }
    $event++;
  }
  return $jobs;
}


sub SubmitSplitJob {
  my $self=shift;
  my $job_ca=shift;
  my $queueid=shift;
  my $user=shift;
  my $jobs=shift;

  if ( !$job_ca->isOK() ) {
    print STDERR "Splitting: in SubmitSplitJob job's jdl is not valid\n";
    return;
  }
  #Removing split from the jdl
  my $text=$job_ca->asJDL();
  $self->debug(1, "Original jdl $text\n");

  #we can't do it with the option 'g', because it doesn't work 
  #if there are two consecutive entries  that have to be deleted
  while(  $text =~ s/([;\[])\s*split[^;]*;/$1/is) {};
  $text=~ s/([;\[])\s*inputdatacollection[^;]*;/$1/i;
  $text =~ s/;\s*email[^;]*;/;/is;

  $self->info("Let's start with $text");
  my ($ok, @splitarguments)=$job_ca->evaluateAttributeVectorString("SplitArguments");
  if (@splitarguments){ 
    $self->info( "SplitArguments defined - OK!");
  } else {
    push @splitarguments, "";
  }

  $job_ca=Classad::Classad->new($text);
  $job_ca->insertAttributeString("MasterJobId", $queueid)
    or $self->info( "Error putting the master job id")
      and return;
  if ( !$job_ca->isOK() ) {
    print STDERR "Splitting: in SubmitSplitJob jdl $text is not valid\n";
    return;
  }

  ($ok, my @inputdataset)=$job_ca->evaluateAttributeVectorString("InputDataSet");
  @inputdataset and $self->info( "InputDataSet defined - OK!");

  ($ok, my $inputdataaction)=$job_ca->evaluateAttributeVectorString("InputDataAction");
  $inputdataaction and $self->info( "InputDataAction is $inputdataaction - OK!");

  #Now, submit a job for each
  ( $ok, my $origreq ) = $job_ca->evaluateExpression("OrigRequirements");
  $origreq or  $origreq="( other.Type == \"machine\" )";
  $self->info("The requirements are $origreq");

  ( $ok, my $origarg ) = $job_ca->evaluateExpression("Arguments");
  $origarg or $origarg="";
  $origarg=~ s/\"//g;
  $self->info("OrigReq $origreq");
  my $i=0;
  foreach my $pos (sort keys %{$jobs}) {
    $i++;
    $self->info("Submitting job $i $pos");

    my $input=$self->_setInputData($jobs->{$pos}, $inputdataaction, \@inputdataset);
    if ($input) {
      $job_ca->set_expression("InputData", $input);
    }

    $job_ca->set_expression("Requirements", $origreq);

    $self->info("Setting Requ. $origreq");

    $self->{CATALOGUE}->{QUEUE}->checkRequirements($job_ca) or next;

    foreach my $splitargs (@splitarguments){
      my $newargs=$self->_checkArgumentsPatterns($splitargs, $jobs->{$pos});
      $job_ca->set_expression("Arguments", "\"$origarg $newargs\"");
      #check also the outputDir
      $self->_checkOutputDir($job_ca, $jobs->{$pos});
      $self->info("Setting Arguments $origarg $newargs");
      if ( !$job_ca->isOK() ) {
	print STDERR "Splitting: in SubmitSplitJob new jdl is not valid\n";
	return;
      }
      $self->_submitJDL($queueid, $user, $job_ca->asJDL, $jobs->{$pos}->{files},$job_ca) or return;
    }
  }
  return 1;
}
sub _checkOutputDir {
  my $self=shift;
  my $job_ca=shift;
  my $jobDesc=shift;
  $self->debug(2, "Checking if we have to redefine the outputDir");
  my ($ok, $jobDir)=$job_ca->evaluateAttributeString("OutputDir");
  $ok or return -1;
  $self->debug(1,"The job is supposed to write in $jobDir");
  #this is for the second time we get the output dir
  #even if we overwrite the value, we keep an old copy
  ( $ok, my $oldJobDir)=$job_ca->evaluateAttributeString("OutputDirOld");
  $oldJobDir and $jobDir=$oldJobDir;
  $jobDir =~ m{#alien_counter([^#]*)#} or return;
  $job_ca->insertAttributeString("OutputDirOld", $jobDir);
  $self->debug(1,"We have to replace counter $1 with $jobDesc->{counter}");
  my $string=$jobDesc->{counter};
  if ($1) {
    my $format=$1;
    $format=~ s{^_}{};
    $self->debug(1,"Using the format $format");
    $string=sprintf("%$format", $jobDesc->{counter});
  }
  $jobDir =~ s{\#alien_counter([^\#]*)\#}{$string};
  $self->info("Putting the outputdir to $jobDir");

  return $job_ca->insertAttributeString("OutputDir", $jobDir)
}

sub _submitJDL {
  my $self=shift;
  my $queueid=shift;
  my $user=shift;
  my $jdlText=shift;
  my $files=shift;
  my $job_ca=shift;

  ( $job_ca) or $job_ca=Classad::Classad->new($jdlText);
  if (!$files) {
    my ($ok, @input)=$job_ca->evaluateAttributeVectorString("InputData");
    $files=\@input;
  }

  $self->debug(1, "JDL $jdlText");

#  my $inputBox=$self->createInputBox($job_ca, $files);
  my $done =$self->{SOAP}->CallSOAP("Manager/Job", "enterCommand",
				    $user, $jdlText,  );
  if ($done) {
    $self->info("Command submitted!! (jobid ". $done->result.")" );
    my $newqueueid = $done->result;
    $self->putJobLog($queueid,"submit","Subjob submitted: $newqueueid");
    $self->{DB}->setSplit($done->result, $queueid)
      or $self->{LOGGER}->warning( "Splitting", "In SubmitSplitJob error setting split for job $queueid" ) and $self->putJobLog($queueid,"error","Subjob submission failed!");
  }
  return 1;
}

sub _checkArgumentsPatterns{
  my $self=shift;
  my $args=shift;
  my $jobDesc=shift;

  my @files=@{$jobDesc->{files}};
  map {s/^\"LF://} @files;
  map {s/\"$//} @files;
  while ($args =~ /\#alien(\S+)\#/i) {
    my $origPattern=$1;
    my $pattern=$1;
    $self->debug(1, "Replacing $pattern");
    my $file="";
    if ($pattern =~ s/^first//i) {
      $file=$files[0];
    }elsif ($pattern =~ s/^last//i){
      $file=$files[$#files];
    }elsif ($pattern =~ s/^all//i){
      $file=join (",", @files);
    }else {
      $self->info("warning: it is not defined if we have to take the first or the last entry. Taking the first");
      $files[0] and $file=$files[0];
    }
    $self->debug(1, "Let's use the file '$file'");
    my $newpattern="";
    if ($pattern =~ /^fulldir$/i) {
      $newpattern=$file;
    }elsif ($pattern=~ /^(dir)+$/i) {
      $self->debug(1, "Taking the directory name");
      while ($pattern=~ s/dir//) {
	$file=~ s/\/[^\/]*$//;
      }
      $file =~ s /^.*\///;
      $self->debug(1, "Using $file");
      $newpattern=$file;
    }elsif ($pattern=~ /^filename$/i) {
      if ($origPattern=~ /^all/) {
	my @basenames;
	foreach my $f (@files){
	  $f=~ s/^.*\///;
	  push @basenames, $f;
	}
	$newpattern=join(",", @basenames);
      }else {
	$file=~ s /^.*\///;
	$newpattern=$file;	
      }
    }elsif ($pattern =~ /^_counter$/i){
      $newpattern=$jobDesc->{counter};
    } else {
      $self->info("Don't know what to do with $pattern");
    }
    $self->info("Let's replace #alien$origPattern# with '$newpattern'");
    $args =~ s/\#alien$origPattern\#/$newpattern/g;
  }
  return $args;
}

# This subroutine checks the input requirements for each subtask.
# It recei
#
sub _setInputData {
  my $self=shift;
  my $jobDesc=shift;
  my $inputdataaction=shift;
  my $ref=shift;
  my @inputdataset=@{$ref};

  ( @{$jobDesc->{files}} )  or return ;

  my $input="";
  my $run=0;
  my $path="";
  my $eventstart=0;
  my $eventstop=0;
  my $first=1;

  my $file;
  $input = "{";

  my @filecopy = @{$jobDesc->{files}};
  for $file (@filecopy) {
    $input .= "$file, ";
    #			$input = "{". join (", ", @{$jobs->{$pos}->{files}}) . "}";

    if ($file=~ /^([^\*]*)\/(.*)\/(.*)\/(.*)$/) { $path=$1; $run=$2; if ($first){$eventstart=$3;$first=0;};$eventstop=$3;};
    if (@inputdataset) {
      my $dir;
      my $name;
      
      if ($file=~ /^([^\*]*)\/(.*)$/) { $dir=$1; $name=$2};
      $dir=~ s/\"//;
      $name=~ s/\"//;
      if ($dir) {
	my $dsitem;
	# add the complete input dataset to the input box
	foreach $dsitem (@inputdataset) {
	  if ($name eq $dsitem) {
	    next;
	  }
	  my $newdsitem = '"'.$dir.'/'.$dsitem . '"';
	  $input .=  $newdsitem . ", ";
	  if ($inputdataaction eq "prestage") {
	      push @{$jobDesc->{files}}, $newdsitem;
	    }
	}
      }
    }
  }
  chop $input;
  chop $input;
  $input .= "}";

  $self->info("Input is $input");

  return $input;
}

sub  ChangeOriginalJob{
  my $self=shift;
  my $job_ca=shift;
  my $queueid=shift;
  my $submitHost =shift;
  $self->info( "Changing the jdl of the original job");

  my $jdl=$job_ca->asJDL;
  my ($ok, $executable)=$job_ca->evaluateAttributeString("Executable");
  ($ok, my $email)=$job_ca->evaluateAttributeString("Email");
  $ok and $email and $email="\nEmail=\"$email\";";
  ($ok, my $req)=$job_ca->evaluateExpression("Requirements");
  my $extra="";
  my $arguments="";
  ($ok, my @output)=$job_ca->evaluateAttributeVectorString("Packages");
  ($ok, my @outputdir)=$job_ca->evaluateAttributeVectorString("OutputDir");

  if (@output)     {
    $self->info( "Putting packages as @output");
    map {$_="\"$_\""} @output;
    $extra.="\nPackages={ ". join (",", @output) ."};";

  }

  if (@outputdir) {
    $self->info( "Putting outputdir as @outputdir");
    map {$_="\"$_\""} @outputdir;
    $extra.="\nOutputDir={ ". join (",", @outputdir) ."};";	
  }
  $jdl=~ s/;\s*InputBox\s*=[^;]*;/;/s;
  $jdl=~ s/\s+\=\s+/Old = /g;
  $jdl=~ s/^\s*\[//sg;
  $jdl=~ s/\s*\]$//sg;


  my $newJdl="[\nExecutable=\"$executable\";$arguments$extra\nType=\"Job\";Requirements=$req;\n$email\n$jdl]";
  $self->info($newJdl);

  my $new_ca=Classad::Classad->new($newJdl);

  if ( !$new_ca->isOK() ) {
    print STDERR "JobOptimizer: in ChangeOriginalJob jdl $newJdl is not valid\n";
    return;
  }
  $jdl=$new_ca->asJDL;
  $self->{DB}->setJdl($queueid, $jdl)
    or $self->info("Error setting jdl $jdl for job $queueid" )
      and return;

  return 1;
}


1
