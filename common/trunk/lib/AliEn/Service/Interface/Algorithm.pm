package AliEn::Service::Interface::Algorithm;

use strict;
use AliEn::Util;
use Data::Dumper;

my $self = {};

use POSIX qw(strftime);
my @domainAPI=({'xmlns'=>"http://mammogrid.com/portal/api/"});
sub initialize {
  $self     = shift;
  my $options =(shift or {});

  $self->{LOGGER}->info("Algorithm", "Initializing the algorithm module");

  return $self;
}

#############################################################################
#############################################################################
#                 PUBLIC FUNCTIONS
#############################################################################
sub addAlgorithm {
  my $this=shift;
  $self->{LOGGER}->info("Algorithm", "Adding a new algorithm to the catalogue");
  my $xml=shift;

  my ($status, $tree)=$self->checkInput("addAlgorithm", $xml);
  ($status eq "OK") or  return  $self->createResponse(@domainAPI,$status, $tree);

  my $tarPFN=$self->{XML}->getXMLFirstValue("PFN", $tree);

  ($tarPFN ) or  return  $self->createResponse(@domainAPI,"NOK", "There is no PFN in the addAlgorithm call");


  my $error="";
  my $algName=$self->{XML}->getXMLFirstValue("AlgName",$tree);
  $algName or $error="missing the name of the algorithm";
  my $algVersion=$self->{XML}->getXMLFirstValue("AlgVersion",$tree);
  $algName and ($algVersion or $error="missing the version of the algorithm");
  my $algAuthor=$self->{XML}->getXMLFirstValue("AlgAuthor",$tree) ;
  my $algExecutable=$self->{XML}->getXMLFirstValue("AlgExecutable",$tree) ;

  $error and $self->{LOGGER}->info("Algorithm", "Error: $error") and
    return $self->createResponse(@domainAPI,"NOK", "$error");

  $algName or $self->{LOGGER}->infor("Algorithms", "Error: missing the name of the algorithm") and return $self->createResponse(@domainAPI,"NOK", "");

  $self->{LOGGER}->info("Algorithm", "Adding the algorithm $algName (version $algVersion and author $algAuthor)");
  $algName=~ s/\s/_/g;
  $algVersion=~ s/\s/_/g;

  my $LFN="/mammogrid/packages/$algName/Linux.$algVersion";
  my $LFNdir="/mammogrid/packages/$algName/";

  $self->{UI}->execute("whereis", "-silent", $LFN) and $self->{LOGGER}->info("Algorithm", "Version $algVersion of $algName already exists!!") and return $self->createResponse("NOK", "Version $algVersion of $algName already exists!!");
  if (! $self->{UI}->execute("ls", "-silent", $LFNdir)) {
    $self->{LOGGER}->info("Creating the directory");
    $self->{UI}->execute("mkdir", "-p", $LFNdir)or $self->{LOGGER}->info("Algorithm", "Error creating the directory !!") and return $self->createResponse("NOK", "Error creating the directory!!");
    $self->{UI}->execute("chmod", "757", $LFNdir);
    $self->{UI}->execute("addTag", $LFNdir, "Algorithm");
  }
  $self->{UI}->execute("register", $LFN, $tarPFN) or $self->{LOGGER}->info("Algorithm", "Error registering the file in the catalogue") and return $self->createResponse(@domainAPI,"NOK", "Error registering the file in the catalogue");
  my @metadata=();
  $algAuthor and push @metadata, "Author='$algAuthor'";
  $algExecutable and push @metadata, "Executable='$algExecutable'";


  if (@metadata) {
    if (! $self->{UI}->execute("addTagValue", $LFN, "Algorithm", @metadata) )
      {
	$self->{LOGGER}->info("Algorithm", "Error setting @metadata");
	$self->{UI}->execute("rm", $LFN);
	return $self->createResponse(@domainAPI,"NOK", "Error setting @metadata");
      }
  }
  return $self->createResponse(@domainAPI,"OK", "");
}

sub listAlgorithms {
  my $this=shift;
  $self->{LOGGER}->info("Algorithm", "Listing all the algorithms known in the system");
  my @dirs=$self->{UI}->execute("ls", "/mammogrid/packages/");
  $self->debug(1, "Got @dirs");

  my @object=();
  foreach my $alg (@dirs) {
    $self->{LOGGER}->info("Algorithm", "Creating the xml object of $alg");
    my @versions=$self->{UI}->execute("ls", "/mammogrid/packages/$alg");
    $self->debug(1, "Got versions @versions");
    foreach my $version (@versions) {
      push @object, $self->createAlgorithmXML($alg, $version);
    }
  }
  my $r=$self->{XML}->{GENERATOR}
    ->AlgorithmListResponse(@domainAPI,$self->createResponse("OK", "It worked!!", "o"),
			  @object);
  $self->debug(1, "Returning $r");
  $self->{LOGGER}->info("Algorithm", "Query done");
  return  "$r";

}
sub executeAlgorithm {
  my $this=shift;
  $self->{LOGGER}->info("Algorithm", "Executing an algorithm to the catalogue");
  my $xml=shift;
  my $tarFile=shift;
  my ($status, $tree)=$self->checkInput("executeAlgorithm", $xml);
  ($status eq "OK") or  return  $self->createResponse(@domainAPI,$status, $tree);

   ($status, my $jdl)=$self->createExecutionJDL($tree);
  $status eq "OK" or  $self->createResponse($status, $jdl);
  $self->{UI}->execute("user", "alienMaster");
  my ($id)=$self->{UI}->execute("submit", "<$jdl");
  $self->{UI}->execute("user", "admin");
  $id or $self->{LOGGER}->info("Algorithm", "Error submitting the job") and return  $self->createResponse("NOK", "Error submitting the job");
  $self->{LOGGER}->info("Algorithm", "Got $id");
  ($status, my $task)=$self->createTaskXML($id);
  $status eq "OK" or return $self->createResponse(@domainAPI,"NOK", "$task");

#  ($status, $task)=$self->createExecutionXML($id, $task);
#  $status eq "OK" or return $self->createResponse("NOK", "$task");

  my $xmlG=$self->{XML}->{GENERATOR};
  my $r=$xmlG
    ->AlgorithmExecutionResponse(@domainAPI,$self->createResponse("OK", "It worked!!", "o"),
				 $xmlG->AlgorithmExecution(
							  $xmlG->ExecutionId($id),
							  $task)
				 );

  $self->debug(1, "Returning $r");
  $self->{LOGGER}->info("Algorithm", "Execution  done");
  return  "$r";
}


sub getExecutionStatus {
  my $this=shift;
  my $input=shift;

  my ($status, $tree)=$self->checkInput("getExecutionStatus", $input);
  ($status eq "OK") or  return  $self->createResponse(@domainAPI,$status, $tree);

  $self->{LOGGER}->info("Algorithm", "Checking the status of the task");

  my $xmlG=$self->{XML}->{GENERATOR};

  my @tasksId=$self->{XML}->getXMLElement("ExecutionId","-s", $tree);


  if (! @tasksId ) {
    $self->{LOGGER}->info("Algorithm", "There are no tasks with those requirements");
    my $r=$xmlG
    ->AlgorithmExecutionResponse($self->createResponse("NOK", "This execution does not have any tasks", "o"));
    return "$r";
  }

  ($status, my @d)=$self->createExecutionXML($tasksId[0]);
  ($status eq "OK") or return $self->createResponse(@domainAPI,"NOK", $d[0]);
  
  my $r=$xmlG
    ->AlgorithmExecutionResponse(@domainAPI,$self->createResponse("OK", "It worked!!", "o"),
				 @d);
  $self->debug(1, "Returning $r");
  $self->{LOGGER}->info("Algorithm", "GetExecutionStatus done!!");
  return "$r";
}

sub createExecutionXML {
  my $self=shift;
  my $id=shift;
  my $inputData="";

  

  my @tasksID=$self->{UI}->execute("top", "-all", "-split", $id, "-silent");

  if (!@tasksID) {
    $self->debug(1, "There are no subtasks for this task");
    push @tasksID, join ("###", ($id, @_));
  }
  $self->debug(1, "Ready to create the tasks");
  my @tasksXML;
  foreach my $data (@tasksID) {
    my ($taskId, @info)=split ("###", $data);
    my $arg="";
    @info and $arg=$data;
    $self->debug(1, "Creating the task of $taskId ($data)");
    my ($status, $task)=$self->createTaskXML($taskId, $arg );
    $status eq "OK" or return ("NOK", $task);
    push @tasksXML, $task;
  }

  my $xmlG=$self->{XML}->{GENERATOR};
  my $r=$xmlG->AlgorithmExecution(
				  $xmlG->ExecutionId($id),
				  @tasksXML
				 );
  $self->{LOGGER}->info("Algorithm", "CreateExecutionXML done");
  return  ("OK", $r);
}
sub listExecutions {
  my $this=shift;
  my $input=shift;

  my ($status, $tree)=$self->checkInput("listExecution", $input);
  ($status eq "OK") or  return  $self->createResponse($status, $tree);

  my $user=$self->{XML}->getXMLFirstValue("user", $tree);
  $status=$self->{XML}->getXMLFirstValue("TaskStatus", $tree);



  my @top=("top", "-all");
  $user and push @top, ("-user", $user);
  $status and push @top, ("-status", $status);

  $self->{LOGGER}->info("Algorithm", "Checking all the tasks with @top");
  my @tasksId=$self->{UI}->execute(@top);

  $self->{LOGGER}->info("Algorithm", "Got @tasksId");
#  @tasksId or $self->{LOGGER}->info("Algorithm", "There are no tasks with those requirements") and return $self->createResponse("NOK", "This execution does not have any tasks");
  
  my @tasksXML=();
  my $xmlG=$self->{XML}->{GENERATOR};
  foreach my $data (@tasksId) {
      my ($taskId, @moreInfo)=split ("###", $data);
      my ($status, $task)=$self->createExecutionXML($taskId, @moreInfo);
      
      $status eq "OK" or return $self->createResponse("NOK", $task);
      push @tasksXML, $task;
  }

  print "Got @tasksXML\n";

  my $r=$xmlG
    ->AlgorithmExecutionResponse(@domainAPI, $self->createResponse("OK", "It worked!!", "o"),
				  @tasksXML,
				 );
  $self->{LOGGER}->info("Algorithm", "Query done");
  return  "$r";
}
#############################################################################
#############################################################################
#                 PRIVATE FUNCTIONS
#############################################################################
sub createTaskXML {
  my $self=shift;
  my $taskid=shift;
  $self->{LOGGER}->info("Algorithm", "Creating the XML of task $taskid");

  my $info=shift;


  if (!$info){
      $self->{LOGGER}->info("Algorithm", "Asking the top");

      ($info)=$self->{UI}->execute("top", "-id", $taskid, "-all", "-silent");
  }

  $info or $self->{LOGGER}->info("Algorithm", "Task $taskid is not in the system ") and return ("NOK", "Task $taskid is not in the system ");
  $self->{LOGGER}->info("Algorithm", "Got $info");

  my ($id, $status, $command, $exechost, $received, $started, $finished)=
    split ("###", $info);

  my $xml= $self->{XML}->{GENERATOR};
  $status=~ /^INSERTING$/ and $status="WAITING";
  $status=~ /^ERROR_/ and $status="FAILED";
  $status=~ /^KILLED/ and $status="FAILED";

  my @files=();
  if ($status =~ /(DONE)|(FAILED)/ ) {

    my $user = AliEn::Util::getJobUserByUI($self->{UI}, $taskid);
    my $procDir = AliEn::Util::getProcDir($user, undef, $taskid);

    $self->{LOGGER}->info("Algorithm", "Checking the input and output files");
    my @all=$self->{UI}->execute("ls", "-l", "$procDir/", "-silent");
    my @input=();
    my @output=();

    $self->debug(1, "We got @all");
    foreach my $file (@all) {
      my ($perm, $user, $group, $size, $date, $name)= split ("###", $file);
      my $type="TaskOutput";
      if ($user eq "admin") {
	push @input, $xml->LFN("$procDir/$name");
      }else {
	push @output, $xml->LFN("$procDir/$name");
      }
    }
    @input and push @files, $xml->TaskInput(@input);
    @output and push @files, $xml->TaskOutput(@output);
  }
  $received and @files=($xml->TaskReceivedTime(strftime ("%Y-%m-%eT%H:%M:%S", localtime $received)), @files);
#  $started and @files=($xml->TaskStartedTime(strftime( "%Y-%m-%eT%H:%M:%S", localtime $started)), @files);
#  $finished and @files=($xml->TaskFinishedTime(strftime ("%Y-%m-%eT%H:%M:%S", localtime $finished)), @files);

  my $d=$xml->ExecutionTask($xml->TaskId($taskid), 
			    $xml->TaskStatus($status),
			    @files, 

			   );

  $self->debug(1, "XML of the task\n $d");
  $self->{LOGGER}->info("Algorithm", "TaskXML created ");
  return ("OK", $d);
}
sub createAlgorithmXML {
  my $self=shift;
  my $alg=shift;
  my $version=shift || "";
  $self->{LOGGER}->info("Algorithm", "Creating the XML of $alg");

  if (!$version) {
    my @version=$self->{UI}->execute("ls", "/mammogrid/packages/$alg/");
    @version or $self->{LOGGER}->info("Algorithm", "Package $alg does not exist in the catalogue!!") and return;
    $version=$version[$#version];
  }

  my @metadata=$self->{UI}->execute("showTagValue", "-silent","/mammogrid/packages/$alg/$version", "Algorithm");
  my @data=@{$metadata[1]};
  
  $version=~ s/^Linux\.//;
  $alg =~ s/_/ /g;
  my $xml= $self->{XML}->{GENERATOR};
  my $d=$xml->Algorithm($xml->AlgName($alg), 
			$xml->AlgVersion($version),
			$xml->AlgAuthor($data[0]->{Author}), 
			$xml->AlgExecutable($data[0]->{Executable}),);

  $self->debug(1, "Algorithm created $d");
  return $d;
}
#sub saveTarFile {
#  my $self=shift;
#  my $tarFile=shift;#
##
#
#  $tarFile or $self->{LOGGER}->info("Algorithm", "Error: missing the tar file with the binaries of the algorithm") and return ("NOK", "missing the tar file with the binaries of the algorithm");
#  my $fileName="$self->{CONFIG}->{TMP_DIR}/algorithm.$$.".time;#
#
#  open (FILE, ">$fileName")
#    or $self->{LOGGER}->info("Algorithm", "Error: opening the file $fileName") and return ("NOK", "opening the file $fileName");
#  print FILE $tarFile;
#  close FILE;
#  
#  return ("OK", "file://$self->{HOST}$fileName");
#}
sub createExecutionJDL {
  my $self=shift;
  my $tree=shift;

  $self->debug(1, "Starting the creation of the jdl");
  my $error="";
  my $algName=$self->{XML}->getXMLFirstValue("AlgName",$tree);
  $algName or $error="missing the name of the algorithm";
  my $algVersion=$self->{XML}->getXMLFirstValue("AlgVersion",$tree);

  my $algArgs=$self->{XML}->getXMLFirstValue("AlgArgs",$tree);

  $error and $self->{LOGGER}->info("Algorithm", "Error: $error") and
    return ("NOK", $error);

  $algName=~ s/\s/_/g;
  my $LFNdir="/mammogrid/packages/$algName/";
  $self->debug(1, "Checking if the algorithm exists ($LFNdir)");

  my @versions=$self->{UI}->execute("ls", "-silent", $LFNdir);
  @versions or $self->{LOGGER}->info("Algorithm", "The algorithm $algName is not defined in the catalogue") and return ("NOK", "The algorithm $algName is not defined in the catalogue");
  $algVersion or $algVersion=$versions[$#versions];
  my $LFN="$LFNdir$algVersion";
  $algVersion=~ s/^Linux\.//;
  
  $self->{LOGGER}->info("Algorithm", "Executing $algName (version $algVersion)");

  my $fileName="$self->{CONFIG}->{TMP_DIR}/jdl.$$.".time;

  my $inputData="";
  my @LFN=$self->{XML}->getXMLElement("LFN", "-s", $tree);

  if (@LFN) {
    map {$_="\"LF:$_\""} @LFN;
    $inputData="InputData={".join (",", @LFN)."};\n";
  }
  else {
    $self->{LOGGER}->info("Algorithm", "Warning! There is no input data for this execution");
  }
  my $executable=$self->{XML}->getXMLFirstValue("AlgExecutable",$tree);

  if (!$executable) {
    my (@metadata)=$self->{UI}->execute("showTagValue", "-silent",$LFN, "Algorithm");
    my @data=@{$metadata[1]};

    $executable=$data[0]->{Executable};
    $executable or $self->{LOGGER}->info("Algorithm", "Error: the name of the executable is not defined") and return ("NOK", "Error: the name of the executable is not defined");
  }

  my $arg=""; 
  if ($algArgs) {
    $algArgs =~ s/&/\\&/g;
    $algArgs =~ s/\s/&&&/g;
    $arg="ARGUMENTS $algArgs";
  }
  open (FILE, ">$fileName") or 
    $self->{LOGGER}->info("Algorithm", "Error opening the file $fileName")
      and return ("NOK", "Error opening the file $fileName");
  print FILE "Executable=\"RunAlgorithm\";
Arguments=\"PACKAGE ${algName}::$algVersion EXECUTABLE $executable $arg\";
$inputData";
  close FILE;

  return ("OK", $fileName);
}
