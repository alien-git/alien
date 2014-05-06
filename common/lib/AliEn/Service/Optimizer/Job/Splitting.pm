package AliEn::Service::Optimizer::Job::Splitting;

use strict;

use AliEn::Service::Optimizer::Job;
use AliEn::Service::Manager::Job;
use Data::Dumper;
use POSIX qw(ceil);
use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Job");

my $self;

my $splitPerDirectory =sub  {
    my $file=shift;
    $file=~ s/\/[^\/]*$//;
    $file=~ s/^lf://i;
    my $arg=$file;

    $arg=~ s/^.*\/([^\/]*)$/-event $1/;

    return $file, $arg, undef;
};

my $splitPerParentDirectory =sub  {
    my $file=shift;
    $file=~ s/\/[^\/]*\/?[^\/]*$//;
    $file=~ s/^lf://i;
    my $arg=$file;

    $arg=~ s/^.*\/([^\/]*)$/-event $1/;

    return $file, $arg, undef;
};
my $splitPerEntries=sub  {
    return 1, "", undef;
};

my $splitPerFile =sub  {
    my $file=shift;

    my $arg="";
#    $file=~ s/\/[^\/]*$//;
    return $file, $arg, undef;
};
my $splitPerEvent =sub  {
    my $event=shift;
    $event=~ s/\/[^\/]*$//;
    $event=~ s/^.*\/([^\/]*)$/$1/;

    return $event, "-event $event", undef;
};
my $splitPerSE =sub  {
    my $event=shift;

    my $sendSize = 1;
    $event=~ s/LF://;
    $event =~ s/,nodownload// and $sendSize = 0;
    my ($seinfo) = $self->{CATALOGUE}->execute("whereis", "-irtc", "-silent", $event);
    $seinfo or ($seinfo) = $self->{CATALOGUE}->execute("whereis", "-irc", "-silent", $event);
    my @se;
    defined $seinfo->{REAL_SE} and $seinfo->{REAL_SE} and @se = @{ $seinfo->{REAL_SE} };
    
    my %foo;
    foreach (@se) { $foo{$_}++ };
    my @uniqueSe = (keys %foo);

    $self->debug(1,"Putting it in ". join (",", sort @uniqueSe));
    return join (",", sort @uniqueSe), "" , ($sendSize ? $seinfo->{size} : 0);
};



sub checkWakesUp {
  $self=shift;

#  $self->{PRIORITY_DB} or 
#    $self->{PRIORITY_DB}=
#      AliEn::Database::TaskPriority->new({ROLE=>'admin'});
#
#  $self->{PRIORITY_DB} or $self->info("Error getting the priority table!!")
#    and exit(-2);

  my $silent=shift;

  my $method="info";
  $silent and $method="debug";
  $self->{LOGGER}->$method("Splitting", "The splitting optimizer starts");
  $self->{SLEEP_PERIOD}=10;
  $self->{DB}->queryValue("SELECT todo from ACTIONS where action='SPLITTING'")
    or return;
  $self->{DB}->update("ACTIONS", {todo=>0}, "action='SPLITTING'");
  $self->info("There are some jobs to split!!");

  my $done2=$self->checkJobs($silent, "1' and upper(CONVERT(uncompress(origJdl) USING latin1)) like '\%SPLIT =\%", "updateSplitting", 4, 15);

  $self->info("Caculate Job Quota");
	$self->{CATALOGUE}->execute("calculateJobQuota", "1");

  $self->{LOGGER}->$method("Splitting", "The splitting optimizer finished");
  return;
}

# Compute the number of sub-jobs for job quota
# This calls SplitJob using DummyQueueID to only know the number of jobs
sub _getNbSubJobs {
	my $self = shift;
	my $job_ca = shift;

	my $nbSubJobs;
  my ($strategy, $jobs)=$self->SplitJob("DummyQueueID", $job_ca) or $self->{LOGGER}->error("The job can't be split") and return;
  if ($strategy !~ /^userDefined/) {
  	$nbSubJobs = scalar(keys %$jobs);
  } else {
  	my ($ok, @def)=$job_ca->evaluateAttributeVectorString("SplitDefinitions");
  	$nbSubJobs = scalar(@def);
  }

	return $nbSubJobs;
}

sub updateSplitting {
  my $self=shift;
  my $queueid=shift;
  my $job_ca=shift;
  eval {
    my ($strategy, $jobs)=$self->SplitJob($queueid, $job_ca) or 
      die("The job can't be split\n");

    my ($user)=$self->{DB}->queryValue("select concat(user,'\@',host) user from QUEUE 
    join QUEUE_USER using (userid) join QUEUE_HOST on (hostid=submithostid) where queueid=?",
                      undef, {bind_values=>[$queueid]})
      or $self->info("Job $queueid doesn't exist")
				and die ("Error getting the user of $queueid\n");

    #Change the status of the job
    my $job = $self->{DB}->updateStatus($queueid, "INSERTING", "SPLITTING", {masterjob=>1})
      or $self->{LOGGER}->warning("Splitting", "in SubmitSplitJob setting status to 'SPLITTING' failed") 
				and die ("Error setting the job to SPLITTING\n");

    ($job == -1) and $self->info("The job was not waiting any more...") and die ("The job was not waiting any more\n");

    $self->putJobLog($queueid,"state", "Job state transition to SPLITTING");
    my $numSubjobs=0;
    if ($strategy !~ /^userDefined/) {
      $numSubjobs=$self->SubmitSplitJob($job_ca, $queueid, $user, $jobs, $strategy);
      defined $numSubjobs  or  die ("Error submitting the subjobs\n");
    } else {
      my ($ok, @def)=$job_ca->evaluateAttributeVectorString("SplitDefinitions");
      foreach my $jdl (@def) {
		$self->_submitJDL($queueid, $user, $jdl) or die("Error submitting one of the splitDefinitions: $jdl\n");
      }
    }
    #    $self->ChangeOriginalJob($job_ca, $queueid, $submitHost);
    $self->info( "Putting the status of $queueid to 'SPLIT' (there were $numSubjobs)");
    $self->{DB}->updateStatus($queueid,"SPLITTING","SPLIT")
      or $self->info("Error updating status for job $queueid" )
	and die("Error changing the status\n");;
    $self->putJobLog($queueid,"state", "Job state transition from SPLITTING to SPLIT");
    
    my ($countsubjobs)=$self->{DB}->queryValue("select count(1) from QUEUE where split=?",
                      undef, {bind_values=>[$queueid]});
    $countsubjobs and $self->info("$queueid has $countsubjobs subjobs" ) or 
      $self->info("Error splitting $queueid: 0 subjobs" ) and 
      die("Job has 0 subjobs after SPLIT ?");
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

sub _splitSEAdvanced {
	my $self=shift;
	my $job_ca=shift;
	my $queueId=shift;
	my $findset=shift;
	
	my @files=$self->_getInputFiles($job_ca, $findset, $queueId) or return;
	my $LIMIT=1000;
	if ($#files > $LIMIT ) {
		$self->putJobLog($queueId, "error", "There are $#files. The limit for brokering per file is $LIMIT");
	  $self->{DB}->updateStatus($queueId,"%","ERROR_SPLT")
      or $self->info("Error updating status for job $queueId" );
    $self->putJobLog($queueId,"state","Job state transition to ERROR_SPLT");
	}
	my $ses={};
	foreach my $file (@files) {
		$self->info("Checking the file $file");
		my $origFile=$file;
		$file=~ s/LF://i;
    $file =~ s/,nodownload//;
    my @se=$self->{CATALOGUE}->execute("whereis", "-lr", "-silent",$file);
    my $done={};
    foreach my $se (@se){
    	
    	$self->info("In $se");
    	$done->{$se} and next;
    	$done->{$se}=1;
    	$ses->{$se} or $ses->{$se}=0;
    	$ses->{$se}++;
    }
		$self->{DB}->insertFileBroker($queueId, $origFile, join(',', @se));
		
	}
	my ($oknumber, $maxInputFileNumber) = $job_ca->evaluateAttributeString("SplitMaxInputFileNumber");
	$maxInputFileNumber or $maxInputFileNumber=@files;
	$self->info("We don't want more than $maxInputFileNumber files per job");

	my $jobs;	
	my ( $ok, $origreq ) = $job_ca->evaluateExpression("Requirements");
	foreach my $se (keys %$ses){
		my $number=ceil($ses->{$se}*1.0/$maxInputFileNumber);
		$self->info("For the se $se, submit $number agents");
		for (my $i=0; $i<$number; $i++){
		 $jobs->{"${se}$i"}={fileBroker=>1,
		 		requirements=> "$origreq && member(other.CloseSE,\"$se\") && this.filebroker==1  ",
		 		 files=>[]};
		}	
	
	}

	$self->info("The se_automatic finished");
	return "se_automatic", $jobs;	
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
  ($split =~ /^parentdirectory$/i) and $sort= $splitPerParentDirectory;
  ($split =~ /^entries$/i) and $sort= $splitPerEntries;
  ($split =~ /^file$/i) and $sort=$splitPerFile;
  ($split =~ /^event$/i) and $sort=$splitPerEvent;
  ($split =~ /^se$/i) and $sort=$splitPerSE;
  ($split =~ /^\-(.*)/) and $sort=1 and $multisplit = $1;
  ($split =~ /^production:(.+)-(.+)/) and $sort=1 and $eventstart = $1 and $eventstop = $2 and $multisplit = ($eventstop-$eventstart+1);
  ($split =~ /^ce$/i) and $sort=1 and $multisplit='ce';

  if ($split =~ /^userdefined$/ ) {
    $self->info("The user gave us the splitting jdls");
    my ($ok, @jdls)=$job_ca->evaluateAttributeVectorString("SplitDefinitions");
    @jdls or $self->info("There are no splitdefinitions") and return;
    return ("userdefined");
  }
  
  if ($split =~ /^se_advanced$/i){
  	$self->info("Splitting by SE, and letting each subjob select files");
  	return $self->_splitSEAdvanced($job_ca, $queueid, $findset);  	
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
  $self->info("Splitting: the job can be split in  $total");
  ($total>0) or return;
  return ( $split,$jobs );

}
sub _getInputFiles{
  my $self=shift;
  my $job_ca=shift;
  my $findset=shift;
  my $queueId=shift;

  my ($ok, @patterns)=$job_ca->evaluateAttributeVectorString("InputData");
  $self->info("Checking if there is an inputcollection");
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
      $self->debug(1,"Inserting $file");
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

  my @files=$self->_getInputFiles($job_ca, $findset, $queueId) or return;

  my $jobs={};
  $self->debug(1, "In SplitJob got @files");
  foreach my $file (@files) {
    $self->debug(1, "In SplitJob sorting $file");
    my ($pos, $arg, $fsize)=$sort->($file);
       
    $self->debug(1, "Should go in $pos, $arg");
    my @list=();
    my $subpos=0;
    my $newpos = "$pos;$subpos";

    if ($pos eq "") {
	$pos = "0";
    }

    $jobs->{$newpos} or $jobs->{$newpos}={nfiles=>0, filesize=>0, counter=>"$pos"};
    
    if ($inputfilenumber) {
      # the user requests a maxmimum number of inputfiles per splitjob
      while ( (defined $jobs->{$newpos}->{nfiles}) && ($jobs->{$newpos}->{nfiles} >= $inputfilenumber) ) {
	    $subpos++;
	    $newpos = "$pos;$subpos";
	    $jobs->{$newpos} or $jobs->{$newpos}={nfiles=>0, filesize=>0, counter=>"${pos}_${subpos}"};
      }
    }
    
    if ($inputfilesize) {
      # the user requests a maximum number of inputfilesize per splitjob
      while ( (defined $jobs->{$newpos}->{filesize}) && ($jobs->{$newpos}->{filesize} >= $inputfilesize) ) {
	    $subpos++;
	    $newpos = "$pos;$subpos";
	    $jobs->{$newpos} or $jobs->{$newpos}={nfiles=>0, filesize=>0, counter=>"${pos}_${subpos}"};
      }
    }
    $jobs->{$newpos}->{args}=$arg;
    push @{$jobs->{$newpos}->{files}}, "\"$file\"";
    # add the file size
	if ($inputfilesize) {
      my $nolffile = $file;
      $nolffile=~ s/^LF://;
      $fsize or 
        my @res = $self->{CATALOGUE}->{CATALOG}->f_lsInternal("a", $nolffile);
      shift @res;
      shift @res;
      my $rresult = shift @res;
      
      if (!$fsize && !defined($rresult)) {
	  	$self->{LOGGER}->warning("Splitting", "Cannot stat file $nolffile");
      }
      elsif (!$fsize && $#{$rresult} == -1) {
		$self->{LOGGER}->warning("Splitting", "Can not stat file $nolffile");
      }
      else {
      	my $sz = ($fsize ? $fsize : @$rresult[0]->{size});
		$jobs->{$newpos}->{filesize} += $sz;
		$self->info("Adding $nolffile with size $sz");
      }
    }
    else {
      $self->debug(1,"Size does not matter for $file");
    }

	defined $jobs->{$newpos}->{filesizedownload} or $jobs->{$newpos}->{filesizedownload} = 0;
    if ($fsize){
    	
	  $jobs->{$newpos}->{filesizedownload} += $fsize;	
	  $self->info("Adding $file with size $fsize");
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
  if ($multisplit=~ /ce/i){
    $self->info("How many different CE do we have??");
    my $ces=$self->{DB}->queryColumn("select site from  SITEQUEUES where blocked='open'");
    my $counter=1;
    foreach my $ce (@$ces){
      $jobs->{$ce}={files=>[], requirements=>"other.ce==\"$ce\""};
      $counter++;
    }
  } else{
    foreach my $subjob (1 .. $multisplit) {
      $self->info("In SplitJob doing subjob $subjob (Run $run Event $event)");
      $jobs->{$subjob}={files=> [],
			args=> "",
			counter=>$event};
	  defined $jobs->{$subjob}->{filesizedownload} or $jobs->{$subjob}->{filesizedownload} = 0;
      if ($run) {
		# don't remove the space after event!
		$jobs->{$subjob}->{args}="--run $run --event $event ";
      }
      $event++;
    }
  }
  return $jobs;
}


sub SubmitSplitJob {
  my $self=shift;
  my $job_ca=shift;
  my $queueid=shift;
  my $user=shift;
  my $jobs=shift;
  my $strategy=shift;

  if ( !$job_ca or !$job_ca->isOK() ) {
    print STDERR "Splitting: in SubmitSplitJob job's jdl is not valid\n";
    return;
  }
  #Removing split from the jdl
  my $text=$job_ca->asJDL();
  $self->debug(1, "Original jdl $text\n");  

  #to make the matching easier, let's put a ; after the last entry
  #and before the first entry;
  $text=~ s/(\s*)$/;$1/s;
  $text=~ s/^(\s*)/$1;/s;

  #this matching can't be done with global in case there are two 
  #consecutive entries that have to be removed
  while(  $text =~ s/;\s*split[^;]*;/;/is) {};
  $text=~ s/;\s*inputdatacollection\s*=[^;]*;/;/i;
  $text=~ s/;\s*inputdata\s*=[^;]*;/;/i;
  $text =~ s/;\s*email\s*=[^;]*;/;/is;
  #$text =~ s/;\s*requirements[^;\]]*;/;/i;

  #$text =~ s/\[;/\[/;
  $self->debug(1, "Let's start with $text");
  my ($ok, @splitarguments)=$job_ca->evaluateAttributeVectorString("SplitArguments");
  if (@splitarguments){ 
    $self->info( "SplitArguments defined - OK! @splitarguments");
  } else {
    push @splitarguments, "";
  }
  
  $text =~ s/^;//g;
  $text =~ s/;;/;/g;
  $job_ca=AliEn::JDL->new($text);
  if ( !$job_ca or !$job_ca->isOK() ) {
    print STDERR "Splitting: in SubmitSplitJob jdl $text is not valid\n";
    return;
  }
  $job_ca->insertAttributeString("MasterJobId", $queueid)
    or $self->info( "Error putting the master job id")
      and return;

  ($ok, my @inputdataset)=$job_ca->evaluateAttributeVectorString("InputDataSet");
  @inputdataset and $self->info( "InputDataSet defined - OK!");

  ($ok, my $inputdataaction)=$job_ca->evaluateAttributeVectorString("InputDataAction");
  $inputdataaction and $self->info( "InputDataAction is $inputdataaction - OK!");


  ( $ok, my $origarg ) = $job_ca->evaluateExpression("Arguments");
  $origarg or $origarg="";
  $origarg=~ s/\"//g;
  
  my $i=0;
  #$job_ca->setExpression("");
  ($ok, my $origOutputDir)=$job_ca->evaluateAttributeString("OutputDir");
  ($ok, my @origOutputFile)=$job_ca->evaluateAttributeVectorString("OutputFile");
  my $origOutputFile=join(" ", @origOutputFile);

  ($ok, my @origOutputArchive)=$job_ca->evaluateAttributeVectorString("OutputArchive");
  my $origOutputArchive=join(" ", @origOutputArchive);
  my $counter=1;
  $self->info("Checking the requirements before setting the inputdata");
  #$self->{CATALOGUE}->{QUEUE}->checkRequirements($job_ca) or next;

  ( $ok, my $origreq ) = $job_ca->evaluateExpression("Requirements");

  $origreq=~ s/other.SPLIT == 1 &&//; 
  $self->info("The requirements are $origreq");

  #Now, submit a job for each

  foreach my $pos (sort keys %{$jobs}) {
    $i++;
    $self->info("Submitting job $i $pos $counter");

    $self->debug(1,"Setting Requ. $origreq");

    my $new_req=$origreq;
    if ($jobs->{$pos}->{requirements}){
      $self->info("This subjob has some requirements!! $jobs->{$pos}->{requirements}");
      $new_req=$jobs->{$pos}->{requirements};  
    }      
    
    my $direct = 0;
    if ($strategy =~ /^se$/i){
    	my ($okst, $stage)=$job_ca->evaluateExpression("Prestage");
    	
	    # ses
	    my ($okd, $directAccess)=$job_ca->evaluateAttributeString("DirectAccess");
	    
	    if (!$okd || !$directAccess){
		    my $sereqs = "(";
		    my @posinfo = split(';', $pos);
		    my @ses = split(',', $posinfo[0]);
		    $new_req .= " && (";
		    foreach (@ses){
		    	 $new_req.=" member(other.CloseSE,\"$_\") ||";
		    }
		    $new_req =~ s/\|\|$//g;
		    $new_req .= " )";
	    }
	        
	    $job_ca->insertAttributeString("Splitted", "$strategy");
	    
	    # size
	    $new_req .= $self->addLocalDiskRequirement($jobs->{$pos}->{filesizedownload}, $job_ca);
	    
	    my ($status) = $self->checkRequirements($new_req, 0, "WAITING");
	    
	    ( ($okst and $stage) or ($status eq "FAILED") ) or $direct = 1; 
    }
    elsif($strategy =~ /^production:(.+)-(.+)/i){
    	# size	
	    $new_req .= $self->addLocalDiskRequirement($jobs->{$pos}->{filesizedownload}, $job_ca);
	    # directly to WAITING
	    $direct = 1;
    }
    
    $job_ca->set_expression("Requirements", $new_req);     
          
    my $input=$self->_setInputData($jobs->{$pos}, $inputdataaction, \@inputdataset);
    if ($input) {
      $job_ca->set_expression("InputData", $input);
      $self->{CATALOGUE}->{QUEUE}->checkRequirements($job_ca) or next;
    } elsif($jobs->{$pos}->{fileBroker}){
      $self->info("Doing the split according to FileBrokering!");
    	$job_ca->set_expression("FileBroker", 1);
       	    	
    }

    foreach my $splitargs (@splitarguments){
      #check also the outputDir
      $self->_checkEntryPattern("OutputDir", "String", $origOutputDir, $job_ca,$jobs->{$pos}, $counter);
      $self->_checkEntryPattern("OutputFile", "Vector", $origOutputFile, $job_ca,$jobs->{$pos}, $counter);
      $self->_checkEntryPattern("OutputArchive", "Vector", $origOutputArchive, $job_ca,$jobs->{$pos}, $counter);
      $self->_checkEntryPattern("Arguments", "Expression", "$origarg $splitargs", $job_ca,$jobs->{$pos}, $counter);

      $counter++;
      if ( !$job_ca->isOK() ) {
	       print STDERR "Splitting: in SubmitSplitJob new jdl is not valid\n";
	       return;
      }
      
      $self->_submitJDL($queueid, $user, $job_ca->asJDL, $jobs->{$pos}->{files}, $job_ca, $direct) or return;
    }
  }
  return 1;
}

sub addLocalDiskRequirement {
	my $self = shift;
	my $size = shift;
	my $job_ca = shift;
	my $req = "";
	
    # size
	if(defined $size){
	   	my $ld = $self->sizeRequirements($size, $job_ca);
	   	$req = " && ( $ld )";
	}
	
	return $req;
}

sub _checkEntryPattern {
  my $self=shift;
  my $entryName=shift;
  my $type=shift;
  my $value=shift;
  my $job_ca=shift;
  my $jobDesc=shift;
  my $counter=shift;
  $value or return;
  $self->debug(1,"The job is supposed to write in $value");
  #this is for the second time we get the output dir
  #even if we overwrite the value, we keep an old copy

  my $newJobDir=$self->_checkArgumentsPatterns($value, $jobDesc, $counter);
  if ($newJobDir and ( ($newJobDir ne "$value") or ($entryName eq "Arguments"))){
    my @set=$newJobDir;
    if ($type eq "Vector"){
      my @f=split(/ /, $newJobDir);
      map {$_="\"$_\"" } @f;
      $newJobDir="{". join("," , @f) ."}";
    }else {
      $newJobDir="\"$newJobDir\"";
    }
    $self->debug(1,"Putting $entryName as $newJobDir ");
    $job_ca->set_expression($entryName, $newJobDir);
  }
  return 1;
}

sub _submitJDL {
  my $self=shift;
  my $queueid=shift;
  my $user=shift;
  my $jdlText=shift;
  my $files=shift;
  my $job_ca=shift;
  my $direct=shift || 0;

  ( $job_ca) or $job_ca=AliEn::JDL->new($jdlText);
  ( $job_ca and $job_ca->isOK() ) 
    or $self->putJobLog($queueid, "error", "Error creating JDL (from $jdlText)") and return;
  if (!$files) {
    my ($ok, @input)=$job_ca->evaluateAttributeVectorString("InputData");
    $files=\@input;
  }

  $self->debug(1, "JDL $jdlText");
  push @ISA, "AliEn::Service::Manager::Job";
  
  my $newqueueid=$self->enterCommand($user, $jdlText, $queueid, undef, 
    {silent=>0,direct=>$direct});
  pop @ISA;
  if(($newqueueid =~ /DENIED:/) or (ref $newqueueid eq "ARRAY") ){
    $self->putJobLog($queueid, "error", "The submission of the subjob failed: $newqueueid");
    return;
  } 
  $newqueueid or return;
  $self->debug(1, "Command submitted!! (jobid $newqueueid)" );
  $self->putJobLog($queueid,"submit","Subjob submitted: $newqueueid");

  return 1;
}

#
#
#
sub _checkArgumentsPatterns{
  my $self=shift;
  my $args=shift;
  my $jobDesc=shift;
  my $counter=shift;

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
      $self->debug(1, "warning: it is not defined if we have to take the first or the last entry. Taking the first");
      $files[0] and $file=$files[0];
    }
    $file =~ s/,nodownload//g;
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
      $file =~ s/,nodownload//g;
      $self->debug(1, "Using $file");
      $newpattern=$file;
    }elsif ($pattern=~ m{^filename(/(.*)/(.*)/)?$}i) {
      my $extra=$1;
      my ($before, $after)=($2, $3);
      my @all=$file;
      if ($origPattern=~ /^all/) {
	my @all=@files;
      }
      my @basenames;
      foreach my $f (@all){
	$f=~ s/^.*\///;
	$f=~ s/,nodownload//g;
	if ($extra){
	  $f =~ s/$before/$after/;
	}
	push @basenames, $f;
      }
      $newpattern=join(",", @basenames);
	
    }elsif ($pattern =~ /^_((counter)|(split))(.*)$/i){
      $self->info("Before replacing, we have $1, $4 and $counter");
      my $format=$4;

      $newpattern=$counter;

      $1 =~ /split/ and $newpattern=$jobDesc->{counter};
      if ($format){
	$format=~ s{^_}{};
	$format=~ s{^\%}{};
	$self->debug(1,"Using the format $format");
	$newpattern=sprintf("%$format", $newpattern);
      }
    } else {
      $self->info("Don't know what to do with $pattern");
    }
    $self->debug(1,"Let's replace #alien$origPattern# with '$newpattern' in '$args'");
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

    if ($file=~ /^([^\*]*)\/(.*)\/(.*)\/(.*)$/) { 
    	 $path=$1; $run=$2; if ($first){$eventstart=$3;$first=0;};$eventstop=$3;
    }
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

  $self->debug(1,"Input is $input");

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


  my $newJdl="Executable=\"$executable\";$arguments$extra\nType=\"Job\";Requirements=$req;\n$email\n$jdl";
  $self->info($newJdl);

  my $new_ca=AliEn::JDL->new($newJdl);

  if ( !$new_ca or !$new_ca->isOK() ) {
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
