package AliEn::CE;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use DBI;
use strict;
use POSIX;
use AliEn::UI::Catalogue::LCM;
use AliEn::Database::TaskQueue;
use AliEn::Database::TaskPriority;
use AliEn::Database::Admin;
use AliEn::Database::CE;
use AliEn::LQ;
use AliEn::Util;

use AliEn::Service::JobAgent;
use AliEn::Classad::Host;
use AliEn::X509;
use Data::Dumper;

use vars qw (@ISA $DEBUG);
push @ISA, 'AliEn::Logger::LogObject';
$SIG{INT} = \&catch_zap;    # best strategy

$DEBUG=0;

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = {};
  
  my $options = shift;
  
  $self->{SOAP}=new AliEn::SOAP;
  #my $user=($options->{user} or getpwuid($<));
  
  #    my $user = "aliprod";
  bless( $self, $class );
  $self->SUPER::new() or return;


  $self->{PASSWD} = ( $options->{passwd} or "" );

  $self->{DEBUG} = ( $options->{debug} or 0 );
  ( $self->{DEBUG} ) and $self->{LOGGER}->debugOn($self->{DEBUG});
  $self->{SILENT} = ( $options->{silent} or 0 );
  $DEBUG and $self->debug(1, "Creating a new RemoteQueue" );
  $self->{CONFIG} = new AliEn::Config() or return;

  my @possible = ();
  $self->{CONFIG}->{CEs} and @possible = @{ $self->{CONFIG}->{CEs} };
  $DEBUG and $self->debug(1,
			  "Config $self->{CONFIG}->{SITE} (Possible queues @possible)" );

  ( $self->{CONFIG} )
    or $self->{LOGGER}->warning( "CE", "Error: Initial configuration not found!!" )
	and return;

  $self->{HOST} = $self->{CONFIG}->{HOST};

  $self->{QUEUEID} = "";
  $self->{COMMAND} = "";

  $self->{WORKINGPGROUP} = 0;


  $self->checkConnection() or return;

  $DEBUG and $self->debug(1, "Connecting to the file catalog..." );

  $self->{CATALOG} =
    ( $options->{CATALOG} or AliEn::UI::Catalogue::LCM->new($options) );
  $self->{CATALOG} or return;

  my $queuename = "AliEn::LQ";
  ( $self->{CONFIG}->{CE} ) 
    and $queuename .= "::$self->{CONFIG}->{CE_TYPE}";

  $DEBUG and $self->debug(1, "Batch sytem: $queuename" );

  eval "require $queuename"
    or print STDERR "Error requiring '$queuename': $@\n"
      and return;
  $options->{DEBUG} = $self->{DEBUG};
  $self->{BATCH}    = $queuename->new($options);

  $self->{BATCH} or $self->info( "Error getting an instance of $queuename") and return;

  $self->{LOGGER}->notice( "CE", "Starting remotequeue..." );


  my $ca=AliEn::Classad::Host->new() or return;
  AliEn::Util::setCacheValue($self, "classad", $ca->asJDL);

  $self->info( $ca->asJDL);
  $self->{X509}=new AliEn::X509 or return;
  $self->{DB}=new AliEn::Database::CE or return;

  my $role = $self->{CATALOG}->{CATALOG}->{ROLE};

  if ($role eq "admin") {
    my ($host, $driver, $db) =
      split ("/", $self->{CONFIG}->{"JOB_DATABASE"});
    $self->{TASK_DB} or 
      $self->{TASK_DB}=
	AliEn::Database::TaskQueue->new({PASSWD=>"$self->{PASSWD}",DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin', SKIP_CHECK_TABLES=> 1});
    $self->{TASK_DB} or $self->{LOGGER}->error( "CE", "In initialize creating TaskQueue instance failed" )
      and return;
    $self->{TASK_DB}->setSiteQueueTable();
  }
  #
  if ($options->{MONITOR}) {
    print "SETTING UP APMON*********************************\n";
    AliEn::Util::setupApMon($self);
    AliEn::Util::setupApMonService($self, "CE_$self->{CONFIG}->{CE_FULLNAME}");
  }


  return $self;
}

#_____________________________________________________________________________
#sub defaultRequirements {
#  
#  my $self=shift;
#  my $req = " (other.Type==\"machine\") ";
#  return $req;
##}

#_____________________________________________________________________________
sub checkRequirements {
  my $self   = shift;
  my $job_ca = shift;
  
  my $default = " (other.Type==\"machine\") ";

  
  foreach my $method ("Input", "Packages", "Memory") {
    $DEBUG and $self->debug(1, "Checking the requirements from $method" );
    my $methodName="requirementsFrom$method";
    my $sereq = $self->$methodName($job_ca);
    ( defined $sereq ) or return;
    $default .= $sereq;
  }

  $self->checkInputDataCollections($job_ca) or return;

  $DEBUG and $self->debug(1, "Checking requirements of the job" );
  my ( $ok, $origreq ) = $job_ca->evaluateExpression("Requirements");

  if ($ok) {
    $DEBUG and $self->debug(1, "Adding requirements from the job" );
    $default .= " && $origreq";
    $job_ca->set_expression( "OrigRequirements", $origreq ) or 
      $self->info( "Error with the original requirements")
	and return 1;
  }

  $DEBUG and $self->debug(1, "Checking if the job is going to be splitted" );
  my $split;
  ( $ok, $split ) = $job_ca->evaluateAttributeString("Split");
  if ($ok) {
    if ($split =~ /^none$/i) {
      
    } elsif (grep ( /^$split/, ("xml","se", "event", "directory", "file","\-1","\-2","\-5","\-10","\-15","\-20","\-50","\-100"))){
      $DEBUG and $self->debug(1, "Job is going to be splitted by $split" );
      $default ="other.SPLIT==1";
    }elsif ( $split =~ /production:(.+)-(.+)/ ) {
      $self->info( "Job is going to be splitted for production, running from $1 to $2" );
    } else {
      $self->{LOGGER}->warning( "CE","I don't know how to split by '$split'" );
      return;
    }
  }

  ($ok, my $ttl)=$job_ca->evaluateExpression("TTL");
  if ($ttl) {
    $default.=" && (other.TTL>$ttl)";
  }

  $DEBUG and $self->debug(1, "All the requirements '$default'" );
  
  $job_ca->set_expression( "Requirements", $default ) and return 1;
  
  $self->{LOGGER}->warning( "CE",
			    "Error setting the requirements for the job: '$default'" );
    return;
}

#_____________________________________________________________________________
sub checkType {

    my $self = shift;
    my $ca   = shift;

    $DEBUG and $self->debug(1, "Checking the type of the job");
    my ( $ok, $type ) = $ca->evaluateAttributeString("Type");
    if ( $ok && lc($type) ne "job" ) {
        print STDERR "JDL is not of type Job !\n";
        return;
    }
    elsif ( !$ok ) {
        $ca->insertAttributeString( "Type", "Job" );
    }

    $DEBUG and $self->debug(1, "Type ok");
    return 1;
}

sub checkInputDataCollections{
  my $self=shift;
  my $ca=shift;

  my ( $ok, @inputdata ) =  $ca->evaluateAttributeVectorString("InputDataCollection");
  $ok or return 1;
  my @newlist;
  my $modified=0;
  my $pwd=$self->{CATALOG}->{CATALOG}->{CURPATH};
  foreach my $file (@inputdata){
    $self->info("Checking the input collection $file");
    $file=~ s/^LF:// or $self->info("Wrong format with $file. It doesn't start with 'LF:'",1) and return;
    
    if ($file=~ m{^/}){
      push @newlist, "\"LF:$file\"";
    }else {
      $self->info("That was relative path. Prepending the current directory");
      $modified=1;
      push @newlist, "\"LF:$pwd$file\"";
    }
  }
  if ($modified){
    $self->info("Updating the inputdatacollection");
    $ca->set_expression("InputDataCollection", "{". join (",", @newlist)."}")
      or $self->info("Error updating the InputDataCollection",1) and return;
  }
  return 1;
}


#_____________________________________________________________________________
# This in fact just expands the pattern matching. It doesn't check requirements
#
sub requirementsFromInput {
  my $self = shift;
  my $ca   = shift;
  # Look for InputData attribute

  my $ok;


  #If there is no input data, everything is fine
  ( $ca->lookupAttribute("InputData") )  or return "";

  ( $ok, my @inputdata ) =  $ca->evaluateAttributeVectorString("InputData");

  $ok or $self->info("Attribute InputData is not a vector of string",1) and 
    return;

  my @inputdataset;
  my $findset="";
  $#inputdataset=-1;

  ( $ok, @inputdataset ) =
    $ca->evaluateAttributeVectorString("InputDataSet");

  if ($#inputdataset > -1) {
    $findset = "-i " . join (",",@inputdataset);
  }

  my @allreq=();
  my @allRequirements=();
  $#allRequirements=-1;

  my $num=$#inputdata+1;
  $self->info("There are $num input files");
  my @flatfiles;

  my $i=0;
  my $pwd=$self->{CATALOG}->{CATALOG}->{CURPATH};
  my $modified=0;
  foreach my $origlfn (@inputdata) {
    my ($file, @options)=split(',', $origlfn);
    $file =~ s/\\//g;
    foreach my $option (@options) {
      $option =~ /^nodownload$/i and next;
      $self->info("Error: options $option not understood in the lfn $origlfn", 1);
      return;
    }
    $i++;
    ($i %100) or  $self->info( "Already checked $i files\n",undef,0);
    $DEBUG and $self->debug(1, "Checking the file $file");
    if ($file=~ /^PF:/) {
      print STDERR "No PF allowed !!! Go to your LF !\n";
      next;
    }
    ($file =~ s/^LF://i) or  
      print STDERR "Malformed InputData -> $file - File Ignored.\n"
	and   next;

    if ( $file !~ m{^/} ) {
      $modified=1;
      $file="$pwd$file";
    }

    if ($file !~ /\*/) {
      push @flatfiles, join(",", "LF:$file", @options);
      next;
    }
    $modified=1;
    $DEBUG and $self->debug(1, "'$file' is a pattern" );
    my $name="";
    my $dir;
    my @list;
    if ($file=~ /^([^\*]*)\*(.*)$/) { $dir=$1; $name=$2};
    if ($name=~ /(.*)\[(\d*)\-(\d*)\]/) {
      $name = $1;
      my $start = $2;
      my $stop  = $3;
      $self->info("Doing: find -silent -l $stop $findset $dir $name");
      @list=$self->{CATALOG}->execute( "find", "-silent", "-l $stop", "$findset", "$dir", "$name");
    } else {
      $name eq "" and $name="*";
      @list=$self->{CATALOG}->execute( "find", "-silent", $findset, $dir, $name);
    }
    @list or $self->info( "Error: there are no files that match $file") and return;
    my $nfiles = $#list+1;
    $self->info( "OK: I found $nfiles files for you!");
    map {$_=join(",", "\"LF:$_\"", @options) } @list;
    push @flatfiles, @list;
    $name =~ s/^.*\/([^\/\*]*)$/$1/;
  }
  if ($modified) {
    $self->info("Putting the inputdata to @flatfiles");
    $ca->set_expression("InputData", "{". join (",", @flatfiles)."}")
      or $self->info("Error updating the InputData",1) and return;
  }
  return "";
}

sub checkInputFiles {
  my $self   = shift;
  my $job_ca = shift;

  $DEBUG and $self->debug(1, "Checking the input box" );

  #Checking the input sandbox
  my ( $ok, @input, @inputName );
  ( $ok, @input )     = $job_ca->evaluateAttributeVectorString("InputFile");
  ( $ok, @inputName ) = $job_ca->evaluateAttributeVectorString("InputName");

  my $input;
  my $name;

  $self->{INPUTBOX} = {};

  foreach $input (@input) {
    $DEBUG and $self->debug(1, "Checking input $input" );
    $name = shift @inputName;
    $name or ( $input =~ /([^\/]*)$/ and $name = $1 );
    if ( $input =~ s/^PF:// ) {
      $self->addPFNtoINPUTBOX($input, $name) or return;
    }
    elsif ( $input =~ s/^LF:// ) {
      $self->addLFNtoINPUTBOX($input, $name) or return;
    }
    else {
      $self->{LOGGER}->warning( "CE",
				"Error with InputFile $input (it's nor LF: neither PF:" );
      return;
    }
  }

  $DEBUG and $self->debug(1, "INPUTFILES ARE OK!!" );

#  ( $ok, @input )     = $job_ca->evaluateAttributeVectorString("InputData");#
#
#  @input or $self->info( "Three is no input data") and return 1;
#  my @newlist=();
#  foreach my $file (@input){
#    $self->info( "Checking $file");
#    if ($file =~ /\*/) {
 #     $self->info( "THIS CORRESPONDS TO SEVERAL FILES!!");
 #     my ($dir, $name);
 #     if ($file=~ /^([^\*]*)\*(.*)$/) { $dir=$1; $name=$2};
 #     $dir=~ s/^LF://;
 #     $DEBUG and $self->debug(1, "Looking in $dir for files like $name");
 #     my @list=$self->{CATALOG}->execute( "find", "-silent", "$dir", "$name" );
 #     @list or $self->info( "Error: there are no files that match $file") and return;
 #     map {$_="LF:$_"} @list;
 #     push @newlist, @list;
 #   }else {
 #     push @newlist, $file;
 #   }
 # }
 # map {$_="\"$_\""} @newlist;#

#  my $list="{". join (",", @newlist)."}";
#  $DEBUG and $self->debug(1, "New list of files: $list");
#  $job_ca->set_expression("InputData", $list );
#  print "TENEMOS ".$job_ca->asJDL;
#  $DEBUG and $self->debug(1, "There are ". ($#newlist +1)." input files");
  return 1;
}

sub addPFNtoINPUTBOX {
  my $self=shift;
  my $input=shift;
  my $name=shift;

#  my $se="Alice::CERN::scratch";
  $self->info( "Copying $input " );
  
  my $data =
    $self->{CATALOG}->{STORAGE}->registerInLCM( $input);
  $self->info( "Register done and $data->{pfn} and $data->{size}" );
  ($data->{pfn} and $data->{size}) or return;
  $self->{INPUTBOX}->{$name} = "$data->{pfn}###$data->{size}###$name###$self->{CONFIG}->{SE_FULLNAME}";
}
sub addLFNtoINPUTBOX{
  my $self=shift;
  my $input=shift;
  my $name=shift;

  $DEBUG and $self->debug(1, "Using LFN $input" );

  $self->{CATALOG}->execute( "ls", "-silent", "$input" )
    or $self->{LOGGER}->warning( "CE",
				 "Error InputFile $input does not exist in the catalogue" )
      and return;
  $self->{INPUTBOX}->{$name} = $input;
}

sub modifyJobCA {

  my $self   = shift;
  my $job_ca = shift;
  
  $DEBUG and $self->debug(1, "Getting the name of the executable" );
  my ( $ok, $command ) = $job_ca->evaluateAttributeString("Executable");
  
  $DEBUG and $self->debug(1, "Getting the attributes" );
  my $arg;
  ( $ok, $arg ) = $job_ca->evaluateAttributeString("Arguments");
  
  $arg and $DEBUG and $self->debug(1, "Got arguments $arg" );

  my $fullPath;
  
  if (!$command){
    $self->info("Error: the executable is missing in the jdl",1);
    $self->info("Usage:  submitCommand <command> [arguments] [--name <commandName>][--validate]"); 
    return;
  }
  my $homedir=$self->{CATALOG}->{CATALOG}->GetHomeDirectory();

  if ($command =~ /\//){
    $DEBUG and $self->debug(1, "Checking if '$command' exists" );
    $self->{CATALOG}->execute( "ls", "-silent", "$command" ) 
      and $fullPath = "$command";
    
    my $org="\L$self->{CONFIG}->{ORG_NAME}\E";
    ($command =~ m{^((/$org)|($homedir))?/bin/[^\/]*$} ) or
      $fullPath="";
  }
  else   {
    my @dirs=($homedir, "/\L$self->{CONFIG}->{ORG_NAME}\E","");
    foreach (@dirs) {
      $DEBUG and $self->debug(1, "Checking if '$command' is in $_" );
      $self->{CATALOG}->execute( "ls", "-silent", "$_/bin/$command" )
	and $fullPath = "$_/bin/$command" and last;
    }
  }

  ($fullPath)
    or $self->info("Error: command $command is not in an executable directory (/bin, /$self->{CONFIG}->{ORG_NAME}/bin, or $homedir/bin)",1    )
      and return;

  # Clear errors probably occured while searching for files
  $self->{LOGGER}->set_error_no();
  $self->{LOGGER}->set_error_msg();

  $self->info( "Submitting job '$fullPath $arg'..." );

  $job_ca->insertAttributeString( "Executable", $fullPath );

  $self->checkInputFiles($job_ca) or return;

  $self->checkType($job_ca) or return;

  $self->checkTimeToLive($job_ca) or return;

  $self->checkRequirements($job_ca) or return;

  $job_ca->insertAttributeString ("User", $self->{CATALOG}->{CATALOG}->{ROLE});
  # Just a safety belt to insure we've not destroyed everything
  if ( !$job_ca->isOK() ) {
    return;
  }
  $DEBUG and $self->debug(1, "Job is OK!!" );

  return 1;
}

sub checkTimeToLive {
  my $self=shift;
  my $job_ca=shift;
  $DEBUG and $self->debug(1, "Checking the time to live of this job");

  my ($ok, $ttl)=$job_ca->evaluateAttributeString("TTL");
  my $realTTL=0;
  if (! $ttl){
    $self->info( "There is no time to live (TTL) defined in the jdl... putting the default '6 hours'");
    $realTTL=6*3600;
  }

  ($ttl=~ s/\s*(\d+)\s*h(our(s)?)?//) and  $realTTL+=$1*3600;
  ($ttl=~ s/\s*(\d+)\s*m(inute(s)?)?//) and  $realTTL+=$1*60;
  ($ttl=~ s/\s*(\d+)\s*s(second(s)?)?//) and  $realTTL+=$1;
  $ttl=~ s/^\s*(\d+)\s*$// and $realTTL+=$1;
  if ( $ttl!~ /^\s*$/){
    $self->info( "Sorry, I don't understand '$ttl' of the time to live. The sintax that I can handle is:\n\t TTL = ' <number> [hours|minutes|seconds]'.\n\tExample: TTL = '5 hours 2 minutes 30 seconds");
    return;
  }
  $job_ca->set_expression( "TTL", $realTTL );
  return 1;
}

sub getJdl {
    my $self = shift;
    my $arg  = join " ", @_;

    ($arg)
      or print STDERR
"Error: Not enough arguments in submit.\n Usage: submit <jdl file in the catalogue>| < <local jdl file> | <<EOF  job decription EOF\n"
      and return;

    my $content;
    if ( $arg =~ /<<\s*EOF/ ) {

      shift =~ /EOF/ or shift;
      #READING FROM THE STDIN
      print STDOUT "Enter the input for the job (end with EOF)\n";
      $content = "";
      my $line = <>;
      while ( $line ne "EOF\n" ) {
	$line !~ /^\#/ and $content .= $line;
	$line = <>;
      }
      print STDOUT "Thanks!!\n";
    }
    elsif ( $arg =~ /</ ) {
      
      #READING FROM A LOCAL FILE
      my $filename;
      $arg =~ /<\s*(\S+)/ and $filename = $1;
      shift =~ /../ or shift;
      $filename or print STDERR "Error: Filename not defined!!\n" and return;
      open FILE, "<$filename"
	or print STDERR "ERROR opening local file $filename\n"
          and return;
      
      my @content = grep ( !/^\#/, <FILE> );
      close FILE;
      
      $content = join "", @content;
    }
    else {
      #File in the catalogue
      $DEBUG and $self->debug(1, "READING A FILE FROM THE CATALOGUE" );
      my $filename = shift;
      my ($file) =
	$self->{CATALOG}->execute( "get", "-silent", "$filename" );
      $file
	or print STDERR
          "Error getting the file $filename from the catalogue\n"
          and return;
      $DEBUG and $self->debug(1, "File $file" );
      open FILE, "<$file"
          or print STDERR "ERROR opening local file $file\n"
	    and return;
      my @content = grep ( !/^\#/, <FILE> );
      close FILE;
      $content = join "", @content;
    }
    
    #Checking for patterns:
    my $template = $content;
    my $i        = 1;
    
    while ( $content =~ /\$$i\D/ ) {
      my $data = shift;
      ( defined $data )
	or $self->{LOGGER}->error( "CE",
				   "Error: jdl requires at least $i arguments\nTemplate :\n$template\n"
				 )
              and return;
      $DEBUG and $self->debug(1, "Using $data for \$$i" );
      $content =~ s/\$$i/$data/g;
      $i++;
    }
    $content =~ /(\$\d)/ and $self->{LOGGER}->warning("CE", "Warning! Argument $i was not in the template, but there is $1\nTemplate:\n$template");

    $content or print STDERR "Error: no description for the job\n" and return;

    return $content;
}

sub submitCommand {
  my $self = shift;
  my @arg = grep ( !/-z/, @_);
  my $content = "";

  my $zoption = grep ( /-z/, @_);


  if ($arg[0] eq "==<") {
    # this is the submission via gShell and GCLIENT_EXTRA_ARG
    shift @arg;
    my @lines = split('\n',shift @arg);
    my @newlines = grep ( !/^\#/, @lines);
    
    $content = (join "\n",@newlines);
    #Checking for patterns:
    my $template = $content;
    my $i        = 1;
    
    while ( $content =~ /\$$i\D/ ) {
      my $data = shift @arg;
      ( defined $data )
	or $self->{LOGGER}->error( "CE",
				   "Error: jdl requires at least $i arguments\nTemplate :\n$template\n"
				 )
	  and return;
      $DEBUG and $self->debug(1, "Using $data for \$$i" );
      $content =~ s/\$$i/$data/g;
      $i++;
    }
    $content =~ /(\$\d)/ and $self->{LOGGER}->warning("CE", "Warning! Argument $i was not in the template, but there is $1\nTemplate:\n$template");
    
    $content or print STDERR "Error: no description for the job\n" and return;
    }
  elsif ($arg[0] eq "=<") {
    shift @arg;
    $content = (join " ",@arg);
  } else {
    $content = $self->getJdl(@arg) or return;
  }

  $DEBUG and $self->debug(1, "Description : \n$content" );

  my $job_ca = Classad::Classad->new("[\n$content\n]");

  my $dumphash;
  $dumphash->{jdl} = $content;

  my $dumper = new Data::Dumper([$dumphash]);

  if ( !$job_ca->isOK() ) {
    print STDERR "=====================================================\n";
    print STDERR $dumper->Dump();
    print STDERR "=====================================================\n";
    print STDERR "Incorrect JDL input\n $content \n";
    return;
    }
  my $jdl=$job_ca->asJDL;
  $DEBUG and $self->debug(1, "Modifying the job description" );
  if ( !$self->modifyJobCA($job_ca) ) {
#    print STDERR $dumper->Dump();
#    print STDERR "Input job suspicious\n$jdl\n";
    return;
  }
  $DEBUG and $self->debug(1, "Job description" . $job_ca->asJDL() );

  if ($self->{INPUTBOX}){
    my $l = $self->{INPUTBOX};

    my @list =sort  keys %$l;
    my @list2=sort values %$l;
    $self->info( "Input Box: {@list}" );
    $DEBUG and $self->debug(1, "Input Box: {@list2}" );
  }

  ( $self->checkConnection() ) or return;
  my $user = $self->{CATALOG}->{CATALOG}->{ROLE};
  $DEBUG and $self->debug(1, "Connecting to $self->{CONNECTION} " );
  my $done =$self->{SOAP}->CallSOAP($self->{CONNECTION},'enterCommand',
				    "$user\@$self->{HOST}", $job_ca->asJDL(), $self->{INPUTBOX} );
  if (! $done) {
      print STDERR "=====================================================\n";
      print STDERR "Cannot enter your job !\n" and return;
      print STDERR "=====================================================\n";
  }
  my $jobId=$done->result;

  if ($self->{WORKINGPGROUP} != 0) {
    $self->f_pgroup("add","$jobId");
    $self->info( "Job $jobId added to process group $self->{WORKINGPGROUP}\n");
  }

  $self->info( "Command submitted (job $jobId)!!" );
  print STDERR "Job ID is $jobId - $zoption\n";
  if ($zoption) {
      my @aresult;
      my $hashresult;
      $hashresult->{"jobId"} = $jobId;
      push @aresult,$hashresult;
      return @aresult;
  }

  return $jobId;
}
sub f_queueStatus {
  my $self=shift;
  $self->info( "Checking the status of the local batch system");
  my $free=$self->getNumberFreeSlots();
  $self->info( "There are $free places to run agents");
 return 1;
}

sub getNumberFreeSlots{
  my $self=shift;

  my $free_slots=$self->{BATCH}->getFreeSlots();
  my $done=$self->{SOAP}->CallSOAP("ClusterMonitor", "getNumberJobs",  $self->{CONFIG}->{CE_FULLNAME}, $free_slots) 
    or return;

  my ($max_queued, $max_running)=$self->{SOAP}->GetOutput($done);

  $self->info( "According to the manager, we can run $max_queued and $max_running");

  my $queued=$self->{BATCH}->getNumberQueued();
  if ($queued) {
    $self->info( "There are $queued jobs");
  }
  my $running=$self->{BATCH}->getNumberRunning();
  if (! defined $running){
    $self->info( "Error getting the number of running jobs");
     $running=$max_running;
  }
  $running eq "" and $running=0;
  ##

  my $free=($max_queued-$queued);


  (  ($max_running - $running)< $free) and $free=($max_running - $running);
  $self->info( "Returning $free slots");

  if ($self->{MONITOR}){
    print "Sending info to monalisa\n";
    $self->{MONITOR}->sendParams({'jobAgents_queued' => $queued, 'jobAgents_running' => $running, 'jobAgents_slots', $free} );
    $self->{MONITOR}->sendBgMonitoring();
  };
  return $free;
}
sub offerAgent {
  my $self   = shift;
  my $silent = ( shift or 0 );
  my $mode="info";
  $silent and $mode="debug";

  ( $self->checkConnection() ) or return;

  ($self->{CONNECTION} eq "ClusterMonitor") or 
    $self->info( "The ClusterMonitor is down. You cannot request jobs" ) and return;

  $DEBUG and $self->debug(1, "Requesting a new command..." );

  my $done;
  my $user = $self->{CATALOG}->{CATALOG}->{DATABASE}->{USER};

  my $free_slots=$self->getNumberFreeSlots();
  ($free_slots and   ($free_slots>0)) or 
    $self->{LOGGER}->$mode("CE", "At the moment we are busy (we can't request new jobs)") and return;
  my $classad="";#AliEn::Util::returnCacheValue($self,"classad");
  if (!$classad){
    my $ca=AliEn::Classad::Host->new() or return;
    $ca=$self->{BATCH}->updateClassAd($ca)
      or $self->info("Error asking the CE to update the classad")
	and return;
    $classad=$ca->asJDL;

    AliEn::Util::setCacheValue($self, "classad", $classad);

  }
  $done = $self->{SOAP}->CallSOAP("ClusterMonitor", "offerAgent",
				  $user,
				  $self->{CONFIG}->{CLUSTERMONITOR_PORT},
				  $self->{CONFIG}->{CE_FULLNAME},
				  $silent, $classad, 
				  $free_slots,
				 );
  $done or return;
  $DEBUG and $self->debug(1, "Got back that we have to start  agents");

  my @jobAgents=$self->{SOAP}->GetOutput($done);
  if (!@jobAgents || ($jobAgents[0] eq "-2")) {
    my $mesage=($done->paramsout || "no more jobs");
    $self->{LOGGER}->$mode("CE", $mesage);
    return -2;
  }
  $DEBUG and $self->debug(1, "Got back that we have to start $#jobAgents +1  agents");
  my $script=$self->createAgentStartup() or return;
  foreach my $agent (@jobAgents) {
    my ($count, $jdl)=@$agent;
    $self->info( "Starting $count agent(s) for $jdl ");
    my $classad= Classad::Classad->new($jdl);
    while ($count --) {
      $self->SetEnvironmentForExecution($jdl);
      my $error = $self->{BATCH}->submit($classad,$script);
      if ($error) {
	$self->info( "Error starting the job agent");
	last;
      } else {
	my $id=$self->{BATCH}->getBatchId();
	if ($id) {
	  $self->info("Inserting $id in the list of agents");
	  $self->{DB}->insertJobAgent({batchId=>$id, 
				       agentId=>$ENV{ALIEN_JOBAGENT_ID}});
	}
      }
    }
  }
#  unlink $script;

  $self->UnsetEnvironmentForExecution();

  $self->info( "All the agents have been started");
  return 1;
}
sub createAgentStartup {
  my $self=shift;
  my $returnContent=(shift ||0 );

#  my $proxy=$self->{X509}->checkProxy();
  my $hours=($self->{CONFIG}->{CE_TTL} ||  12*3600)/3600;
  my $proxy=$self->{X509}->createProxy($hours);

  my $proxyName=($ENV{X509_USER_PROXY} || "/tmp/x509up_u$<");

  my $content="$ENV{ALIEN_ROOT}/bin/alien RunAgent\n";
  if ($proxy) {

    open (PROXY, "<$proxyName") or print "Error opening $proxyName\n" and return;
    my @proxy=<PROXY>;
    close PROXY;
    my $jobProxy="$self->{CONFIG}->{TMP_DIR}/proxy.\$\$.`date +\%s`";
    my $debugTag = $self->{DEBUG} ? "--debug $self->{DEBUG}" : "";
    $content= "echo 'Using the proxy'
mkdir -p $self->{CONFIG}->{TMP_DIR}
cat >$jobProxy <<EOF\n". join("", @proxy)."
EOF
file=$jobProxy
chmod 0400 \$file
export X509_USER_PROXY=\$file;
echo USING \$X509_USER_PROXY
$ENV{ALIEN_ROOT}/bin/alien proxy-info
$ENV{ALIEN_ROOT}/bin/alien RunAgent $debugTag
rm -rf \$file\n";

  }
  if (! $returnContent){
    my $script="$self->{CONFIG}->{TMP_DIR}/agent.startup.$$";

    if (! -d $self->{CONFIG}->{TMP_DIR} ) {
      my $dir="";
      foreach ( split ( "/", $self->{CONFIG}->{TMP_DIR} ) ) {
	$dir .= "/$_";
	mkdir $dir, 0777;
      }
    }

    open (FILE, ">$script") or print "Error opening the file $script\n" and return;
    print FILE "#!/bin/bash\n$content";
    close FILE;
    chmod 0755, $script;
    $content=$script;
  }
  return $content;
}

sub checkQueueStatus() {
    my $self   = shift;
    my $silent = ( shift or 0 );
    my $mode="info";
    $silent and $mode="debug";

    ( $self->checkConnection() ) or return;

    ($self->{CONNECTION} eq "ClusterMonitor") or 
      $self->{LOGGER}->error( "CE", "The ClusterMonitor is down. You cannot request jobs" ) and return;

    $DEBUG and $self->debug(1, "Checking my queue status ..." );

    my $user = $self->{CATALOG}->{CATALOG}->{DATABASE}->{USER};

    my @queueids = $self->{BATCH}->getQueuedJobs();

    if (! @queueids) {
	$self->{LOGGER}->error( "CE","Could not retrieve Queue information!" ) and return;
    } else {
	foreach (@queueids) {
	    $self->info("Found Job-Id $_ in the Queue!");
	}
    }

    my $done = $self->{SOAP}->CallSOAP("ClusterMonitor", "checkQueueStatus", $self->{CONFIG}->{CE_FULLNAME}, @queueids);
    $done or return;

    if ($done->result  eq "0") {
	$self->info("There was a queue inconsistency!");
    } else {
	$self->info("The queue was consistent!");
    }

    $self->info( "Executed checkQueueStatus!!" );

    return 1; 
}



sub SetEnvironmentForExecution{
  my $self=shift;

  my $org=$self->{CONFIG}->{ORG_NAME};
  $self->{COUNTER} or $self->{COUNTER}=0;

# This variable is set so that the LQ and the JobAgent know where the output
# is
  $ENV{ALIEN_LOG}="AliEn.JobAgent.$$.$self->{COUNTER}";
  $ENV{ALIEN_JOBAGENT_ID}="$$.$self->{COUNTER}";
  $self->{COUNTER}++;

  $ENV{"ALIEN_${org}_CM_AS_LDAP_PROXY"}=$ENV{ALIEN_CM_AS_LDAP_PROXY}=
    "$self->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}";

  $ENV{ALIEN_SE_MSS}      = ( $self->{CONFIG}->{SE_MSS}      or "" );
  $ENV{ALIEN_SE_FULLNAME} = ( $self->{CONFIG}->{SE_FULLNAME} or "" );
  $ENV{ALIEN_SE_SAVEDIR}  = ( $self->{CONFIG}->{SE_SAVEDIR}  or "" );

  $ENV{ALIEN_SaveSE} = ( $self->{CONFIG}->{SaveSE_FULLNAME} or "" );

  $ENV{ALIEN_SaveSEs} = "";

  delete $ENV{IFS};
  $ENV{PATH}=~ s{$ENV{ALIEN_ROOT}/bin:+}{};

  ( $self->{CONFIG}->{SaveSEs_FULLNAME} )
    and  $ENV{ALIEN_SaveSEs}= join "###", @{$self->{CONFIG}->{SaveSEs_FULLNAME}};

#  my ($ok, $saveSE ) = $CA->evaluateAttributeString("RemoteSE");#
#
#  if ($saveSE){
#    $self->info( "The job wants to save remotely in $saveSE");
#    my $se=$self->{CONFIG}->CheckService("SE", $saveSE);
#    $se or $self->{LOGGER}->error("CE", "SE $saveSE does not exist") and return;
#    $ENV{ALIEN_SaveSE}=$se->{FULLNAME};
#  }
#  ($ok, $saveSE ) = $CA->evaluateAttributeString("SE");#
#
#  if ($saveSE) {
#    $self->info( "The job wants to save localy in $saveSE");
#    my $se=$self->{CONFIG}->CheckService("SE", $saveSE);
#    $se or $self->{LOGGER}->error("CE", "SE $saveSE does not exist") and return;
#    $ENV{ALIEN_SE_MSS}      = $se->{MSS};
#    $ENV{ALIEN_SE_FULLNAME} = $se->{FULLNAME};
#    $ENV{ALIEN_SE_SAVEDIR}  = $se->{SAVEDIR};
#    
#  }


#  ($ok, my $requirements ) = $CA->evaluateExpression("Requirements");
#  if ($ok ) {
#      $self->info( "Passing the requirements $requirements");
#      $self->{BATCH}->{JDL_REQ}=$requirements;
#  }

#  ($ok, $requirements ) = $CA->evaluateExpression("SpecialRequirements");
#  if ($ok ) {
#      $self->info( "Passing the special requirements $requirements");
#      $self->{BATCH}->{JDL_SPECIAL_REQ}=$requirements;
#  }

#  ($ok, my $inputData) = $CA->evaluateExpression("InputData");
#  if ($ok) {
#      $self->info( "Passing the InputData $inputData");
#      $self->{BATCH}->{JDL_INPUT_DATA}=$inputData;
#  }
  
  return 1;
}
sub UnsetEnvironmentForExecution{
  my $self=shift;

  $DEBUG and $self->debug(1, "Removing the environment variables");

  map {delete $ENV{$_} }  ("ALIEN_CM_AS_LDAP_PROXY", "ALIEN_SE_MSS",
			   "ALIEN_SE_FULLNAME","ALIEN_SE_SAVEDIR", 
			   "ALIEN_SaveSE", "ALIEN_SaveSEs");
  $ENV{PATH}= "$ENV{ALIEN_ROOT}/bin:$ENV{PATH}";


  return 1;
}

sub f_top {
  my $self = shift;
  my @args =@_;

  $DEBUG and $self->debug(1, "In RemoteQueue::top @_" );

  ( $self->checkConnection() ) or return;

  my $done= $self->{SOAP}->CallSOAP($self->{CONNECTION}, "getTop", @_) 
    or return;

  my $result=$done->result;
  $result=~ /^Top: Gets the list of jobs/ and
    return $self->info( $result);

  my @jobs = @$result;
  my $job;
  my $columns="JobId\tStatus\t\tCommand name\t\t\t\t\tExechost";
  my $format="%6s\t%-8s\t%-40s\t%-20s";
  if (grep (/-?-a(ll)?$/, @_) ) {
    $DEBUG and $self->debug(1, "Printing more information");
    $columns.="\t\t\tReceived\t\t\tStarted\t\t\t\tFinished";
    $format.="\t\%s\t\%s\t\%s";
  }
  $self->info( $columns,undef,0);

  foreach $job (@jobs) {
    $DEBUG and $self->debug(3, Dumper($job));
    my (@data ) = ($job->{queueId},
		   $job->{status},
		   $job->{name},
		   $job->{execHost} || "",
		   $job->{received} || "",
		   $job->{started} || "",
		   $job->{finished} || "");

    $data[3] or $data[3]="";
#    #Change the time from int to string
    $data[4] and $data[4]=localtime $data[4];
    $data[5] and   $data[5]=localtime $data[5];
    $data[6] and  $data[6]=localtime $data[6];

    my $string=sprintf "$format", @data;
    $self->info( $string, undef,0);
  }
  return @jobs;
}


sub f_queueinfo {
    my $self = shift;
    my $site = (shift or "%");
    my $done =$self->{SOAP}->CallSOAP($self->{CONNECTION},"queueinfo",$site);
    $done or return;
    $done=$done->result;
    $self->f_queueprint($done,$site);
}

sub f_queueprint {
  my $self = shift;
  my $done = shift;
  my $site = (shift or "");
  my $k;

  my $sum={};

  printf "%-24s%-16s%-18s%-14s", "Site","Blocked","Status","Statustime";

  foreach $k (@{AliEn::Util::JobStatus()}){
    $k=~ s/^ERROR_(..?).*$/ER_$1/ or $k=~ s/^(...).*$/$1/;
    printf "%-5s ", $k;
  }

  print "\n----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";

  foreach (@$done) {
    printf "%-24s%-16s%-18s%-14s", $_->{'site'},($_->{'blocked'} ||"" ), $_->{'status'},$_->{'statustime'};

    foreach $k (@{AliEn::Util::JobStatus()}){
      $_->{$k} or $_->{$k}=0;
      printf "%-5s ",$_->{$k};
      ( defined $sum->{$k}) or $sum->{$k} = 0;

      ( $_->{$k} ) and  $sum->{$k}+=  int($_->{$k});
    }
    $_->{jdl} and print "$_->{jdl}\n";
    print "\n";
  }
  print "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";

  if ( $site eq '%' ) {
    my $sumsite="Sum of all Sites";
    my $empty="----";
    my $zero="0";
    printf "%-24s", $sumsite;
    printf "%-16s", $empty;
    printf "%-18s", $empty;
    printf "%-14s", $empty;

    foreach $k (@{AliEn::Util::JobStatus()}){
      if ( defined $sum->{$k} ) {
	printf "%-5s ",$sum->{$k};
      } else {
	printf "%-5s ",$zero;
      }
    }
    printf "\n";
    print "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
  }
  return $done;
}

sub f_priorityprint() {
    my $self = shift;
    my $done = shift;
    my $lkeys;
    my $firstentry = @$done[0];
    my $out = "user";
    print "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
    printf "%-16s", $out;

    foreach $lkeys (keys %$firstentry) {
	if ($lkeys eq "user"){
	    next;
	}
	printf "%-20s", $lkeys;
    }
    printf "\n";
    print "==================================================================================================================================================================================\n";
    foreach (@$done) {
	printf "%-16s",$_->{"user"};
	foreach $lkeys (keys %$firstentry) {
	    if ($lkeys eq "user") {
		next;
	    }
	    printf "%-20s",$_->{$lkeys};
	}
	printf "\n";
    }
    print "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";
}
sub f_spy_HELP{
  return "spy: check the output of a job while it is still running\nUsage:
\tspy <job id> <filename> [<options>]

If <filename> is 'workdir', spy will return the current directory on the worker node.

Possible options include:
\tgrep <pattern>: Instead of returning all the lines, the worker node will only return the lines that match the pattern.
  head <number>: Return the first <number> lines of the file
  tail <number>: Return the last <number> lines of the file
";
}


sub f_spy {
  my $self = shift;
  my $queueId = shift or print STDERR "You have to specify the job id, you want to spy on\n" and return;
  my $spyfile = shift or print STDERR "You have to specify a filename to spy on, or \n\t'workdir'\t to see the job working directory or\n\t'nodeinfo'\t to see information about the worker node\n" and return;

  $queueId =~ /^[0-9]+$/ or $self->info("The id '$queueId' doesn't look like a job id...\n". f_spy_HELP()) and return;
  my $options={grep=>[]};
  while (@_) {
    my $option=shift;
    if ($option =~/^grep$/) {
      my $pattern=shift or $self->info("Missing pattern") and return;
      push @{$options->{grep}}, "($pattern)";
    } elsif( $option =~ /^tail/) {
      $options->{tail}=shift or $self->info("Missing number of lines") and return;
    } elsif( $option =~ /^head/) {
      $options->{head}=shift or $self->info("Missing number of lines") and return;
    }else {
      $self->info("Unknown option: $option");
      return;
    }
  }

  if (@{$options->{grep}}){
    $options->{grep}=  join ("|", @{$options->{grep}});
  } else {
    $options->{grep}=undef;
  }

  my $done =$self->{SOAP}->CallSOAP($self->{CONNECTION},"spy",$queueId,$spyfile, $options);
  
  $done or return;
  $done=$done->result;
  print $done;
  return 1;
}

sub f_jobsystem {
  my $self = shift;
  my @jobtag = @_;
  my $username=$self->{CATALOG}->{CATALOG}->{ROLE};
  my $callcommand = "getSystem";

  # if we get a job id(s) as arguments, we call the getJobInfo routine
  if (@jobtag) {
    my $jobid;
    eval {$jobid= sprintf "%d", $jobtag[0];};
    if ( $jobid > 0 ) {
      $callcommand = "getJobInfo";
    }
  }

  $DEBUG and $self->debug(1,
			  "Connecting to $self->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}" );

  ( $self->checkConnection() ) or return;
  
  my $done;
  if (@jobtag) {
    $done= $self->{SOAP}->CallSOAP($self->{CONNECTION}, $callcommand,$username,@jobtag );
  } else {
    $done= $self->{SOAP}->CallSOAP($self->{CONNECTION},$callcommand,$username);
  }


  my $error="";
  my $result="";

  $done and $result=$done->result;

  ($done) or $error="Error connecting to the Manager/Job !!";
  ($done) and ( ! $result) and $error="The Manager/Job did not return anything for getSystem";

  $result and ($result eq "-1") and $error = $done->paramsout || "Error reading result of the Manager/Job";

  if ($error){
    $self->info( "The Manager/Job returned error $error" );
    return (-1, $error);
  }

  #    printf("$result\n");
  if ( $callcommand eq "getJobInfo") {
    $self->f_printjobinfo($result);
  }

  if ( $callcommand eq "getSystem") {
    $self->f_printsystem($result);
  }
}

sub f_printjobinfo() {
    my $self = shift;
    my $result = shift;

    printf STDOUT "==========================================================================\n";
    foreach (keys %$result) {
	if (defined $result->{$_}) {
	    printf STDOUT "  %12s :   %6s\n", $_,, $result->{$_};
	}
    } 
    printf STDOUT "==========================================================================\n";
}

sub f_printsystem() {
  my $self = shift;
  my $result = shift;

  my $user=sprintf("%10s", $self->{CATALOG}->{CATALOG}->{ROLE});

  print "==========================================================================
= AliEn Queue                   all          ${user}         [%%]
--------------------------------------------------------------------------\n";

  foreach (@{AliEn::Util::JobStatus()}) {
    my $status=lc($_);
    printf STDOUT "  %12s         %12s      %14s      %6.02f\n","\u$status",($result->{"n$status"} ||0 ), ($result->{"nuser$status"}|| 0), ($result->{"frac$status"} ||0);
  }
  printf "\n==========================================================================
= Job Execution                 all          $user
-------------------------------------------------------------------\n";
  my @list=(['Exec. Efficiency '=>""],['Assign.    Ineff.'=>'assignin'],
	  ['Submission Ineff.','submissionin'],['Execution  Ineff.','executionin'],
	  ['Validation Ineff.','validationin'],['Expiration Ineff.'=>,'expiredin']);
  foreach (@list) {
    my ($title, $var)=@{$_};
    my $total="${var}efficiency";
    my $user="user${var}efficiency";
    printf STDOUT "  $title     %12.02f %%      %12.02f %%\n",$result->{$total},$result->{$user};
  }
  print STDOUT "\n==========================================================================
= Present Resource Usage        all          $user
--------------------------------------------------------------------------\n";
  printf STDOUT "  CPU [GHz]            %12.02f        %12.02f\n",$result->{'totcpu'}/1000.0,$result->{'totusercpu'}/1000.0;
  printf STDOUT "  RSize [Mb]           %12.02f        %12.02f\n",$result->{'totrmem'}/1000,$result->{'totuserrmem'}/1000;
  printf STDOUT "  VSize [Mb]           %12.02f        %12.02f\n",$result->{'totvmem'}/1000,$result->{'totuservmem'}/1000;
  printf STDOUT "\n==========================================================================
= Computing Resource Account    all          $user     \n
--------------------------------------------------------------------------\n";
  printf STDOUT "  CPU Cost [GHz*sec]   %12.02f        %12.02f\n",$result->{'totcost'},$result->{'totusercost'};
  printf STDOUT "==========================================================================
= Site Statistic
--------------------------------------------------------------------------\n";
  my (@allsites) = split '####',$result->{'sitestat'};

  foreach (@allsites) {
    my (@siteinfo) = split '#',$_;
    printf STDOUT "  %-30s %-5s %-5s %-5s %-5s %-5s %-5s %-5s %-5s %-5s\n", $siteinfo[0], $siteinfo[1],$siteinfo[2],$siteinfo[3],$siteinfo[4],$siteinfo[5],$siteinfo[6],$siteinfo[7],$siteinfo[8],$siteinfo[9];
  }
  printf STDOUT "==========================================================================\n";


  return "Done Jobs $result->{'ndone'}";
}

sub f_system {
    my $self = shift;
    my $username=$self->{CATALOG}->{CATALOG}->{ROLE};

    return $self->f_jobsystem();
}
sub f_ps_trace {
  my $self=shift;
  my $id=shift;
  if (!$id ) {
    $self->info( "Usage: ps trace <jobid> [tags]");
    return;
  }
  ( $self->checkConnection() ) or return;
  my $done= $self->{SOAP}->CallSOAP($self->{CONNECTION}, "getTrace", "trace", $id, @_);
  $done or return;
  my @trace;
  my $result=$done->result;
  my $cnt=0;
  my @jobs = split "\n", $result;
#  my @returnjobs;
  foreach (@jobs) {
    $cnt++;
    my $printout = $_;
    $DEBUG and $self->debug(1, "Let's print '$printout'");
    $printout =~ s/\%/\%\%/g;
    if ($printout =~ s/^(\d+)//) {
      $printout=localtime($1) . $printout;
    }
    my $string=sprintf("%03d $printout",$cnt);
    $self->info($string, undef, 0);
    my @elements = split " ", $_;
    my $hashcnt=0;
    my $newhash={};
    foreach my $elem (@elements) {
      $newhash->{$hashcnt} = $elem;
      $hashcnt++;
    }
    $newhash->{trace} = $_;
    push @trace,$newhash;
  }
  return \@trace;
}
sub f_ps2_jdltrace {
    my $self = shift;
    my $command = shift;
    my ($host, $driver, $db) =
	split ("/", $self->{CONFIG}->{"JOB_DATABASE"});
    $self->{TASK_DB} or 
	$self->{TASK_DB}=
	AliEn::Database::TaskQueue->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin', SKIP_CHECK_TABLES=> 1});
    $self->{TASK_DB} or $self->{LOGGER}->error( "CE", "In initialize creating TaskQueue instance failed" )
	and return;
    $self->{TASK_DB}->setSiteQueueTable();
    
    # catch the -trace and -jdl option
    if ($command eq "-trace") {
	my $errorhash;
	$errorhash->{error}    = "GLITE_ERROR_ILLEGAL_INPUTPARAMETERS";
	$errorhash->{errortxt} = "ps2 -trace needs atleast <queueId> as input argument";
	my $queueid = shift or return $errorhash;
	my $trace = $self->f_ps_trace($queueid,@_);	
#	foreach (@$trace) {
#	    foreach my $lkey (keys %$_) {
#		print "$lkey : $_->{$lkey}","\n";
#	    }
#	}
	return @$trace;
    }

    if ($command eq "-jdl") {
	my $errorhash;
	$errorhash->{error}    = "GLITE_ERROR_ILLEGAL_INPUTPARAMETERS";
	$errorhash->{errortxt} = "ps2 -jdl needs <queueId> as input argument";
	my $queueid = shift or return $errorhash;
	my $jdl = $self->{TASK_DB}->getFieldFromQueue($queueid,"jdl");
	my @result=();
	my $rethash={};
	$rethash->{jdl} = $jdl;
	if (defined $jdl) {
	    print "$jdl\n";
	} else {
	    print "Error: Job $queueid is not (anymore) in the task queue!\n";
	    my $errorhash;
	    $errorhash->{error}    = "GLITE_ERROR_ILLEGAL_JOBID";
	    $errorhash->{errortxt} = "Job $queueid is not (anymore) in the task queue!";
	    return $errorhash;
	} 
	push @result, $rethash;
	return @result;
    }
}

sub f_ps2 {

    my $self      = shift;
    ##### usage        #####
    print STDERR "Arguments: @_\n";
    ##### filter -z argument away #####
    my @args;
    foreach (@_) {
	$_ =~/^\-z/ or push @args,$_;
    }

    my $usage = "Usage: ps2 <flags|status> <users> <sites> <nodes> <masterjobs> <order> <jobid> <limit> <sql>\n";
    $usage .= "\t <flags> \t: -a all jobs\n";
    $usage .= "\t         \t: -r all running jobs\n";
    $usage .= "\t         \t: -f all failed/error jobs \n";
    $usage .= "\t         \t: -d all done jobs \n";
    $usage .= "\t         \t: -t all final state jobs (done/error) \n";
    $usage .= "\t         \t: -q all queued jobs (queued/assigned) \n";
    $usage .= "\t         \t: -s all pre-running jobs (inserting/waiting/assigned/queued) \n";
    $usage .= "\t         \t: -arfdtqs combinations\n";
    $usage .= "\t         \t: default '-' = 'all non final-states'\n";
    $usage .= "\n";
    $usage .= "\t <status>\t: <status-1>[,<status-N]*\n";
    $usage .= "\t         \t:  INSERTING,WAITING,ASSIGEND,QUEUED,STARTED,RUNNING,DONE,ERROR_%[A,S,I,IB,E,R,V,VN,VT]\n";
    $usage .= "\t         \t: default '-' = 'as specified by <flags>'\n";
    $usage .= "\n";
    $usage .= "\t <users> \t: <user-1>[,<user-N]*\n";
    $usage .= "\t         \t: % to wildcard all users\n";
    $usage .= "\n";
    $usage .= "\t <sites> \t: <site-1>[,<site-N]*\n";
    $usage .= "\t         \t: default '%' or '-' to all sites\n";
    $usage .= "\n";
    $usage .= "\t <nodes> \t: <node-1>[,<node-N]*\n";
    $usage .= "\t         \t: default '%' or '-' to all nodes\n";
    $usage .= "\n";
    $usage .= "\t <mjobs> \t: <mjob-1>[,<mjob-N]*\n";
    $usage .= "\t         \t: default '%' or '-' to all jobs\n";
    $usage .= "\t         \t: <sort-key>\n";
    $usage .= "\t         \t: default '-' or 'queueId'\n";
    $usage .= "\n";
    $usage .= "\t <jobid> \t: <jobid-1>[,<jobid-N]*\n";
    $usage .= "\t         \t: default '%' or '-' to use the specified <flags>\n";
    $usage .= "\n";
    $usage .= "\t <limit> \t: <n> - maximum number of queried jobs\n";
    $usage .= "\t         \t: regular users: default limit = 2000;\n";
    $usage .= "\t         \t: admin        : default limit = unlimited;\n\n";
    $usage .= "\t <sql>   \t: only for admin role: SQL statement\n";
    $usage .= "Usage: ps2 -trace <jobid> \t: get the job trace\n";
    $usage .= "Usage: ps2 -jdl   <jobid> \t: get the job JDL\n";

    #### implement the -trace and -jdl option 
    if ( ( $args[0] eq "-trace") or ($args[0] eq "-jdl" ) ) {
      return $self->f_ps2_jdltrace(@args);
    }

    my $errorhash;
    $errorhash->{error}    = "GLITE_ERROR_ILLEGAL_INPUTPARAMETERS";
    $errorhash->{errortxt} = "Wrong number of input parameters to function ps2";
    ##### input params ##### 
    my $flags     = shift @args or print STDERR "$usage" and return $errorhash;
    my $users     = shift @args or print STDERR "$usage" and return $errorhash;
    my $sites     = shift @args or print STDERR "$usage" and return $errorhash;
    my $nodes     = shift @args or print STDERR "$usage" and return $errorhash;
    my $masterjobs = shift @args or print STDERR "$usage" and return $errorhash;
    my $order     = shift @args or print STDERR "$usage" and return $errorhash;
    my $ids       = shift @args or print STDERR "$usage" and return $errorhash;
    my $limit     = shift @args or print STDERR "$usage" and return $errorhash;
    my $sql       = join " ",@args or print STDERR "$usage" and return $errorhash;
    ########################
    my $date      = time;

    if ($flags eq "-") {$flags = "";}
    if ($users eq "-") {$users = "$self->{CATALOG}->{CATALOG}->{ROLE}";}
    if ($sites eq "-") {$sites = "";}
    if ($nodes eq "-") {$nodes = "";}
    if ($masterjobs eq "-") {$masterjobs = "";}
    if ($order eq "-") {$order = "queueId";} #default order is by job ID
    if ($ids eq "-") {$ids = "";}
    if ($sql  eq "-") {$sql = "";}
    if ($limit eq "-") {$limit = "";}

    my $sqlstatus = "status='RUNNING' or status='WAITING' or status='ASSIGNED' or status='QUEUED' or status='INSERTING' or status='SPLIT' or status='SPLITTING' or status='STARTED' or status='SAVING'";
    my $sqlusers  = "  ";
    my $sqlsites  = "  ";
    my $sqlnodes  = "  ";
    my $sqlmasterjobs = "  ";
    my $sqlids = "  ";


    my ($host, $driver, $db) =
	split ("/", $self->{CONFIG}->{"JOB_DATABASE"});
    
    $self->{TASK_DB} or 
	$self->{TASK_DB}=
	AliEn::Database::TaskQueue->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin', SKIP_CHECK_TABLES=> 1});
    $self->{TASK_DB} or $self->{LOGGER}->error( "CE", "In initialize creating TaskQueue instance failed" )
	and return;
    $self->{TASK_DB}->setSiteQueueTable();
    

    my $or ="";
#    my $user="";
    # group status selection
    if ($flags =~ /^-/) {
	if ($flags ne "-") {
	    $sqlstatus = "";
	}
	if ($flags =~/r/) {
	    $sqlstatus .= "$or status='RUNNING' or status='STARTED' or status ='SAVING'";
	    $or="or";
	} 
	
	if ($flags =~/q/) {
	    $sqlstatus .= "$or status='QUEUED' or status='ASSIGNED'";
	    $or="or";
	}
	
	if ($flags =~/f/) {
	    $sqlstatus .= "$or status like 'ERROR&' or status='FAILED' or status='EXPIRED'";
	    $or="or";
	}
	
	if ($flags =~/d/) {
	    $sqlstatus .= "$or status='DONE'";
	    $or="or";
	}

	if ($flags =~/t/) {
	    $sqlstatus .= "$or status='DONE' or status='ERROR%'";
	}

	if ($flags =~/s/) {
	    $sqlstatus .= "$or status= 'INSERTING' or status= 'WAITING' or status= 'ASSIGNED' or status= 'QUEUED'";
	}
	
	if ($flags =~/a/) {
	    $sqlstatus = "";
	}
    } else {
	#precise status selection with komma separated list
	$sqlstatus = "  ";
	my @allstatus = split (",",$flags);
	foreach (@allstatus) {
	    $sqlstatus .= "status like '$_' or";
	}
	chop $sqlstatus;
	chop $sqlstatus;
    }

    ##########################################
    # user selection 
    my @allusers = split (",",$users);
    foreach (@allusers) {
	$sqlusers .= " submithost like '$_\@\%' or";
    }

    chop $sqlusers;
    chop $sqlusers;

    ##########################################
    # site selection
    my @allsites = split (",",$sites);
    foreach (@allsites) {
	$sqlsites .= "site like '$_' or";
    }
    
    chop $sqlsites;
    chop $sqlsites;

    ##########################################
    # node selection
    my @allnodes = split (",",$nodes);

    foreach (@allnodes) {
	$sqlnodes .= "node like '$_' or";
    }
    
    chop $sqlnodes;
    chop $sqlnodes;

    ##########################################
    # master job selection
    my @allmasterjobs = split (",",$masterjobs);

    foreach (@allmasterjobs) {
	$sqlmasterjobs .= "split = '$_' or";
    }
    
    chop $sqlmasterjobs;
    chop $sqlmasterjobs;

    ##########################################
    # job id selection
    my @allids = split (",",$ids);

    foreach (@allids) {
	$sqlids .= "queueId = '$_' or";
    }
    
    chop $sqlids;
    chop $sqlids;


    ##########################################
    
    if ($sqlstatus eq "") {$sqlstatus ="1"};
    if ($sqlusers eq "")  {$sqlusers  ="1"};
    if ($sqlsites eq "")  {$sqlsites  ="1"};
    if ($sqlnodes eq "")  {$sqlnodes  ="1"};
    if ($sqlmasterjobs eq "") { $sqlmasterjobs ="1"};
    if ($sqlids eq "") { $sqlids = "1"};
    
    my $where = "";
    my $rresult;
    
    if ($self->{CATALOG}->{CATALOG}->{ROLE} ne "admin") {
	if ($limit eq "") {
	    $limit = " limit 2000 ";
	} else {
	    $limit = " limit $limit ";
	} 
    }else {
	$limit = " limit $limit ";
    }

    if ( ($sql ne "") ) {
	if ($self->{CATALOG}->{CATALOG}->{ROLE} ne "admin") {
	    print STDERR "You are not allowed to execute direct SQL queries!\n";
	    return;
	} else {
	    $where = "$sql";
	    $DEBUG and $self->debug(1, "In psdirect executing sql statuement:\n $where" );
	    $rresult = $self->{TASK_DB}->query("$where $limit")
		or $self->{LOGGER}->error( "CE", "In psdirect error getting data from database" )
		and return ;
	    
	}
    } else {
	$where = "($sqlstatus) and ($sqlusers) and ($sqlsites) and ($sqlnodes) and ($sqlmasterjobs) and ($sqlids) order by $order $limit";
	$DEBUG and $self->debug(1, "In psdirect executing where:\n $where" );
	$rresult = $self->{TASK_DB}->getFieldsFromQueueEx("*","where $where")
	    or $self->{LOGGER}->error( "CE", "In psdirect error getting data from database" )
	    and return ;
    }

    
    $DEBUG and $self->debug(1, "In psdirect done" );
    
    my @jobs;
    for (@$rresult) {
	$_->{submitHost} =~ /(.*)\@(.*)/;
	$_->{user} = $1;
	$_->{submitHost} = $2;
	if ($_->{jdl} =~ /.*Executable\s*=\s*"([^"]*)"/) {
            $_->{executable} = $1;
        } else {
            $_->{executable} = "";
        }
        if ($_->{jdl} =~ /.*Split.*=.*"(.*)".*/) {
            $_->{splitmode}  = $1;
        } else {
            $_->{splitmode}  = "";
        }
        if ((defined $_->{cost}) && ($_->{cost} ne "")) { 
	    $_->{cost} = int ($_->{cost});
        } else {
            $_->{cost} = 0;
        }
    }

   if ($self->{DEBUG} ne "0") {
       foreach (@$rresult) {
           print "---------------------------------\n";
           foreach my $lkeys ( keys %$_ ) {
               printf "%24s = %s\n",$lkeys,$_->{$lkeys};
           }
       }
   }
   return @$rresult;
}

sub f_ps_rc {
  my $self=shift;
  my $id=shift;
  $id or $self->info( "Error: missing the id of the process",11) and return;
  $DEBUG and $self->debug(1, "Checking the return code of $id");
  ( $self->checkConnection() ) or return;
  my $done= $self->{SOAP}->CallSOAP($self->{CONNECTION}, "getJobRC", $id);
  $done or return;
  return $done->result;

}
sub f_ps_HELP {
  return "ps: Retrieves process information \n  Usage: ps [-adflrsxAIX] [-id <jobid>]\n\n  Job Selection:\n    -d  : all <done> jobs\n    -f  : all <failed> jobs\n    -a  : jobs of <all> users [default only my jobs]\n    -r  : all not finished jobs\n    -A  : all kind of job status\n    -I  : all interactive daemon jobs\n    -s  : add splitted jobs to the output\n    -id=<id> : only job <id>\n\n  Output Format:\n    def : <user> <jobId> <status> <runtime> <jobname>\n    -x  : <user> <jobId> <status> <cpu> <mem> <cost> <runtime> <jobname>\n    -l  : <user> <jobId> <status> <maxrsize> <maxvsize> <cpu> <mem> <cost> <runtime> <jobname>\n    -X  : <user> <jobId> <status> <maxrsize> <maxvsize> <cpu> <mem> <cost> <runtime> <ncpu> <cpufamily> <cpuspeed> <jobname>\n   -T  : <user> <jobId> <status> <received> <started> <finished> <jobname>\n\n  Job Status:\n    R  : running\n    W  : waiting for execution\n    A  : assigned to computing element - waiting for queueing\n    Q  : queued at computing element\n     ST : started - processmonitor started\n    I  : inserting - waiting for optimization\n    RS : running split job\n    WS : job is splitted at the moment\n    IS : inserting - waitinf for splitting\n\n  Error Status:\n    EX : job expired (execution too long)\n    K  : job killed by user\n    EA : error during assignment\n    ES : error during submission\n    EE : error during execution\n    EV : -\n\n  Interactiv Job Status:\n    Id : interactive job is idle\n    Ia : interactive job is assigned (in use)\n\n    -h : help text\n\n  Examples:\n    ps -XA             : show all jobs of me in extended format\n    ps -XAs            : show all my jobs and the splitted subjobs in extended format\n    ps -X              : show all active jobs of me in extended format\n    ps -XAs -id 111038 : show the job 111038 and all it's splitted subjobs in extended format\n    ps -rs            : show my running jobs and splitted subjobs";
}

sub f_ps_jdl{
  my $self=shift;
  my $id=shift;
  $id or $self->info( "Usage: ps jdl <jobid> ") and  return;
  ( $self->checkConnection() ) or return;
  my $done= $self->{SOAP}->CallSOAP($self->{CONNECTION}, "GetJobJDL", $id);
  $done or return;
  $self->info("The jdl of $id is ".$done->result() );
  return $done->result;
}

sub f_ps {
  my $self = shift;
  my @args =@_;
  my $verbose = 1;
  my @outputarray;
  my $output;


  my $subcommands={trace=>"f_ps_trace",
		   rc=>"f_ps_rc",
		   jdl=>"f_ps_jdl",};
		
  if ((defined $args[0]) && ($subcommands->{$args[0]})){
    shift;
    my $method=$subcommands->{$args[0]};
    return $self->$method(@_);
  }
		

  my $flags="";
  my $args=join (" ",@_);

  #First, let's take all the flags
  while ($args =~ s{-?-([^i\s]+)}{}){
    $flags.=$1;
  }
  my @id=();
  #The other thing that this command accepts is '-id=<id>'
  while ($args =~ s{-?-id=?\s?(\S+)}{}){
    push @id, "-id=$1";
  }
  #If there is anything else, it is an error:

  if ($args !~ /^\s*$/){
    $self->info( "Error: wrong syntax: don't know what to do with '$args'. Use 'ps -help' for help");
    return;
  }
  my $formatFlags="";

  ( $flags=~ s/q//g) and $verbose = 0;

  while ($flags=~ s/([xljTWX])//g ){
    $formatFlags.=$1;
  }

  $DEBUG and $self->debug(1, "In RemoteQueue::ps @_" );

  $DEBUG and $self->debug(1,
        "Connecting to $self->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}" );

    ( $self->checkConnection() ) or return;

  my $addtags="";

  if ( $flags =~s/a//g) {
    ;
  } else {
    $addtags .= "-u $self->{CATALOG}->{CATALOG}->{ROLE} ";
  }

  grep (((/^-?-id?=?\s?(\d+)/) and ($addtags .= " -id $1 ")), @_);


  #    grep (((/^-?-site?=?\s?(\d+)/) and ($addtags = " -s $1 ")),@_);

#  grep (((/^-?-st?=?\s?(\d+)/) and ($addtags = " -s $1 -z")),@_);

  my $done= $self->{SOAP}->CallSOAP($self->{CONNECTION}, "getPs",  $flags, $addtags, @id);

  #print "ps result:", $done->result, ":\n";

  my $error="";
  my $result="";
  $done and $result=$done->result;

  ($done) or $error="Error connecting to the Manager/Job !!";
  ($done) and ( ! $result) and $error="The Manager/Job did not return anything";

  $result and ($result eq "-1") and $error = $done->paramsout || "Error reading result of the Manager/Job";

  if ($error){
    $self->info( "The Manager/Job returned error $error" );
    return (-1, $error);
  }

  my @jobs = split "\n", $result;
  my $job;
  #    printf STDOUT " JobId\tStatus\t\tCommand name\t\t\t\t\tExechost\n";
  my $username;
  my $now=time;
  foreach $job (@jobs) {
    my ( $queueId, $status, $name, $execHost, $submitHost, $runtime, $cpu, $mem, $cputime, $rsize, $vsize, $ncpu, $cpufamily, $cpuspeed, $cost, $maxrsize, $maxvsize, $site, $node ,$splitjob, $split, $procinfotime,$received, $started,$finished) = ("","","","","","","","","","","","","","","","","","","","","");
    ( $queueId, $status, $name, $execHost, $submitHost, $runtime, $cpu, $mem, $cputime, $rsize, $vsize, $ncpu, $cpufamily, $cpuspeed, $cost, $maxrsize, $maxvsize,$site,$node,$splitjob, $split, $procinfotime , $received, $started, $finished) = split "###", $job;
    
    $site or $site = '';
    $node or $node = '';
    $procinfotime or $procinfotime=$now;
    my $indentor="";
    my $exdentor=" ";
    if ($split) {
      $indentor="-";
      $exdentor="";
    }
    $submitHost =~ /(.*)@.*/ and $username = $1;
    $status =~ s/RUNNING/R/;
    $status =~ s/SAVING/SV/;
    $status =~ s/WAITING/W/;
    $status =~ s/ASSIGNED/A/;
    $status =~ s/QUEUED/Q/;
    $status =~ s/STARTED/ST/;
    if ($splitjob) {
      $status =~ s/DONE/DS/;
      $status =~ s/INSERTING/IS/;
      $site = sprintf "-%-3s-subjobs--",$splitjob;
    } else {
      $status =~ s/DONE/D/;
      $status =~ s/INSERTING/I/;
    }

    $status =~ s/EXPIRED/EX/;
    $status =~ s/KILLED/K/;
    $status =~ s/ERROR_A/EA/;
    $status =~ s/ERROR_S/ES/;
    $status =~ s/ERROR_E/EE/;
    $status =~ s/ERROR_IB/EIB/;
    $status =~ s/ERROR_R/ER/;
    $status =~ s/ERROR_V/EV/;
    $status =~ s/ERROR_VT/EVT/;
    $status =~ s/ERROR_VN/EVN/;
    $status =~ s/ERROR_SV/ESV/;
    $status =~ s/ERROR_P/EP/;
    $status =~ s/FAILED/FF/;
    $status =~ s/IDLE/Id/;
    $status =~ s/INTERACTIV/Ia/;
    $status =~ s/SPLITTING/WS/;
    $status =~ s/SPLIT/RS/;
    $status =~ s/ZOMBIE/Z/;
    $execHost =~ s/(.*)@(.*)/$2/is;

    my $printCpu=$cpu || "-";
    if ( $formatFlags=~ /x/){
      $output = sprintf "%-10s %s%-6s%s %-2s    %-6s  %-5s %-6s  %-6s  %-10s", $username, $indentor, $queueId, $exdentor, $status, $printCpu, $mem, $cost, $runtime, $name;
    } elsif ( $formatFlags=~ /l/){
      $output = sprintf "%-10s %s%-6s%s %-2s    %-8s %-8s %-6s  %-5s %-6s  %-6s  %-10s", $username, $indentor, $queueId, $exdentor, $status, $maxrsize, $maxvsize, $printCpu, $mem, $cost, $runtime, $name;
    } elsif ( $formatFlags=~ /X/){
      $output = sprintf "%-10s %s%-6s%s %-26s %-2s    %-8s %-8s %-6s  %-5s  %-6s  %-8s %-1s %-2s %-4s %-10s", $username, $indentor, $queueId, $exdentor, $site, $status, $maxrsize, $maxvsize, $printCpu, $mem, $cost, $runtime, $ncpu, $cpufamily, $cpuspeed, $name;
    } elsif ($formatFlags =~ /W/ ) {
      $output = sprintf "%s%-6s%s %-29s %-30s %-26s %-10s %-3s %-02s:%-02s:%-02s.%-02s", $indentor, $queueId, $exdentor, $site, $node, $execHost, $name, $status, (gmtime ($now - $procinfotime))[7,2,1,0];
    } elsif ( $formatFlags=~ /T/){
      my $rt= "....";
      my $st= "....";
      my $ft= "....";

      if ($received) {
	$rt = ctime($received);
      }
      if ($started) {
	$st = ctime($started);
      }
      if ($finished) {
	$ft = ctime($finished);
      }
      chomp $rt;
      chomp $st;
      chomp $ft;

      $output = sprintf "%-10s %s%-6s%s %-2s    %-24s  %-24s  %-24s  %-10s", $username, $indentor, $queueId, $exdentor, $status, $rt,$st,$ft , $name;

    } else {
      # no option given
      $output = sprintf "%-10s %s%-6s%s %-2s  %-8s  %-10s", $username, $indentor, $queueId, $exdentor, $status, $runtime, $name;
    }
    push @outputarray,$output;
    $verbose and printf STDOUT "$output\n";
  }

  if ( $formatFlags=~ s/j//g) {
    return @jobs;
  }
  return @outputarray;
}


sub f_kill {
  my $self=shift;

  (@_)
    or $self->{LOGGER}
      ->warning( "CE", "Error: No queueId specified in kill job!" )
	and return;

  ( $self->checkConnection() ) or return;
  my $user=$self->{CATALOG}->{CATALOG}->{ROLE};
  foreach my $queueId (@_) {
    my ($result) = $self->{SOAP}->CallSOAP($self->{CONNECTION}, "killProcess",$queueId, $user) or return;
    print "Process $queueId killed!!\n";
  }
  return 1;
}

sub pgroupmember {
  my $self = shift;
  my $groupid = (shift or 0);
  my $user=$self->{CATALOG}->{CATALOG}->{ROLE};
  return $self->{CATALOG}->execute( "ls", "/proc/groups/$user/$groupid/", "-silent" );
}

sub pgroups {
  my $self = shift;
  my $groupid = (shift or 0);
  my $user=$self->{CATALOG}->{CATALOG}->{ROLE};
  return $self->{CATALOG}->execute( "ls", "/proc/groups/$user/", "-silent" );
}

sub pgroupprocstatus {
  my $self = shift;
  my $pid = (shift or 0);
  my @args = @_;
  return $self->f_ps("-q","-A","-a","-id","$pid",@args);
}

sub f_pgroup {
  my $self    = shift;
  my $command = (shift or "");
  my $user=$self->{CATALOG}->{CATALOG}->{ROLE};
  my $handeled=0;
  
  if ( $command =~ /^new/ ) {
    # new process group
    my @pgroups = $self->{CATALOG}->execute( "ls","-la", "/proc/groups/$user", "-silent" );
    if (! @pgroups){
      print STDERR "Error: You don't have process group support enabled.\nAsk the system administrator to create your /proc/groups/$user/ directory!\n";
      return;
    } else {
      my $highestgroup=0;
      if ( $#pgroups == 1 ) {
	print "Nothing, but a directory\n";
	# the new group index will become 1
      } else {
	# OK, let's see, which is the highest group index
	@pgroups = $self->{CATALOG}->execute( "ls", "/proc/groups/$user", "-silent" );
	foreach (@pgroups) {
	  my $singlegroup = $_;
	  if ( $singlegroup =~ /\d+/ ) {
	    if ( ($_) > $highestgroup ) {
	      $highestgroup = $_;
	    }
	  }
	}	  
      }
      
      my $newgroup = $highestgroup + 1;
      $self->{CATALOG}->execute( "mkdir", "/proc/groups/$user/$newgroup");
      my @checkgroup = $self->{CATALOG}->execute( "ls","-la", "/proc/groups/$user/1", "-silent" );
      if (! @checkgroup) {
	print STDERR "Error: Cannot create new process group!\n";
	return ;
      } else {
	$self->{WORKINGPGROUP} = $newgroup;
	$self->f_pgroup();
      }
    }
    $handeled=1;
  }

  if ( $command =~ /\d+/ ) {
    # set working group to existing process group
    my $groupid = $command;
    my @pgroups = $self->{CATALOG}->execute( "ls","-la", "/proc/groups/$user/$groupid/", "-silent" );
    if (! @pgroups) {
      print STDERR "Error: The group $groupid does not exist!\n";
      $self->f_pgroup();
      return;
    }
    $handeled=1;
    $self->{WORKINGPGROUP} = $groupid;
    $self->f_pgroup();
    if ( @_ ) {
      return $self->f_pgroup(@_);
    }
  } 
  
  if ( $command =~ /^add/ ) {
    # add process to group
    my $take1 = shift;
    my $take2 = shift;
    my $groupid;
    my $procid;

    if ( (defined $take1) && (defined $take2) ) {
      $groupid  = $take1;
      $procid   = $take2;
    } else {
      if ( (defined $take1) && (!defined $take2) && ($self->{WORKINGPGROUP} != 0) ) {

	$groupid = $self->{WORKINGPGROUP};
	$procid  = $take1;
      } else {
	printf STDERR "Error: you have to pass correct arguments!\nUsage: pgroup add [process group] <process ID>\n";
	return;
      }
    }

    if ( $groupid =~ /\d+/ ) {
      my @pgroups = $self->{CATALOG}->execute( "ls","-la", "/proc/groups/$user/$groupid/", "-silent" );
      if (! @pgroups) {
	print STDERR "Error: The group $groupid does not exist!\n";
	return;
      } 
    } else {
      print STDERR "Error: You have to give a valid process group ID!\n";
      return;
    }

    if ( $procid =~ /\d+/ ) {
      my @pgroups = $self->{CATALOG}->execute( "ls","-la", "/proc/groups/$user/$groupid/$procid", "-silent" );
      if (@pgroups) {
	print STDERR "Error: The process $procid already existis in group $groupid!\n";
      } else {
	$self->{CATALOG}->execute("mkdir","/proc/groups/$user/$groupid/$procid" );
	@pgroups = $self->{CATALOG}->execute( "ls","-la", "/proc/groups/$user/$groupid/$procid", "-silent" );
	if (!@pgroups) {
	  print STDERR "Error: Cannot create new process $procid in process group $groupid\n";
	  return;
	}
      }
      $self->f_pgroup("dump");
    } else {
      print STDERR "Error: You have to give a valid process ID!\n";
      return;
    }
    $handeled=1;
  }

  if ( $command =~ /^remove/ ) {
    # remove process from group
    my $procid = shift;
    if ($self->{WORKINGPGROUP} == 0 ) {
      print STDERR "Error: No current working process group selected!\n";
      return;
    }

    my $groupid = $self->{WORKINGPGROUP};
    if ( $procid =~/\d*/ ) {
      
      my @pgroups = $self->{CATALOG}->execute( "ls","-la", "/proc/groups/$user/$groupid/$procid", "-silent" );
      if (@pgroups) {
	# let's remove it
	$self->{CATALOG}->execute("rmdir","-rf","/proc/groups/$user/$groupid/$procid" );
	@pgroups = $self->{CATALOG}->execute( "ls","-la", "/proc/groups/$user/$groupid/$procid", "-silent" );
	if (@pgroups) {
	  print STDERR "Error: Could not remove the process $procid from group $groupid!\n";
	  return;
	} else {
	  $self->f_pgroup("ps");
	}
      } else {
	print STDERR "Error: The process $procid does not exist in group $groupid!\n";
	return;
      }
      
    } else {
      print STDERR "Error: You have to give a valid process ID!\n";
      return;
    }

    $handeled=1;
  }

  if ( $command =~ /^ls/ ) {
    # close existing process group
    my @allgroups = $self->pgroups();
    printf STDOUT "Existing Groups: ";
    foreach (@allgroups) {
      printf STDOUT "$_ ";
    }
    printf STDOUT "\n";
    $handeled=1;
  }

  if ( $command =~ /^close/ ) {
    # close open working process group
    $self->{WORKINGPGROUP} = 0;
    $handeled=1;
    $self->f_pgroup("");
  }

  if ( $command =~ /^status/ ) {
    # print status for all group processes
    $handeled=1;
  }

  if ( $command =~ /^dump/ ) {
    # dumpall group processes
    my $pgroup;
    my @allgprocs;
    $pgroup = $self->{WORKINGPGROUP};
    @allgprocs = $self->pgroupmember("$pgroup");
    my $procnt=0;
    printf STDOUT "===================================================================\n";
    printf STDOUT "Process Group:          $pgroup\n";
    printf STDOUT "-------------------------------------------------------------------\n";
    foreach (@allgprocs) {
      $procnt++;
      printf STDOUT "  |-> PID %4d | \n", $_;
    }
    printf STDOUT "-------------------------------------------------------------------\n";
    $handeled=1;
  }

  if ( $command =~ /^kill/ ) {
    # kill all processes in group
    my $pgroup = $self->{WORKINGPGROUP};
    my @allgprocs = $self->pgroupmember("$pgroup");  
    foreach (@allgprocs) {
	$self->f_kill($_);
    }
    $handeled=1;
  }

  if ( $command =~ /^ps/ ) {
    # dump all processes in group
    my $pgroup;
    my @allgprocs;
    $pgroup = $self->{WORKINGPGROUP};
    @allgprocs = $self->pgroupmember("$pgroup");
    
    printf STDOUT "===================================================================\n";
    printf STDOUT "Process Group:          $pgroup\n";
    printf STDOUT "-------------------------------------------------------------------\n";
    my $procnt=0;
    foreach (@allgprocs) {
      $procnt++;
      printf STDOUT "  |-> PID %4d |   %s\n", $_, $self->pgroupprocstatus("$_",@_);
    }
    printf STDOUT "-------------------------------------------------------------------\n";
    $handeled=1;
  }

  if ( $command eq "" ) {
    if ($self->{WORKINGPGROUP}!=0) {
      print STDOUT "Active Process Group: $self->{WORKINGPGROUP}\n";
    } else {
      print STDOUT "No active Process Group!\n";
    }
    $handeled=1;
  }

  if (!$handeled) {
    print STDERR "Error: Illegal Command $command\n";
  }
}

sub f_validate {
    my $self    = shift;
#    my $queueId = shift;

    (@_)
      or $self->{LOGGER}
      ->warning( "CE", "Error: No queueId specified in validate job!" )
      and return;

    ( $self->checkConnection() ) or return;

    my $queueId;
    foreach $queueId (@_) {
      my ($done) = $self->{SOAP}->CallSOAP($self->{CONNECTION}, "validateProcess",$queueId) or return;
      print "Job to validate  $queueId submitted!\n";
    }
    return 1;
}

sub f_queue_HELP{
  return "'queue': Displays information about all the batch queues defined in the system. Usage:\n
\tqueue info\t\t-\tshow queue information
\tqueue open <queue> \t-\topen a queue on a dedicated site
\tqueue lock <queue> \t-\tlock a queue on a dedicated site
\tqueue list\t\t-\tlist available sites
\tqueue add <queue> \t-\tadd an unknown site to the site queue
\tqueue remove <queue>\t-\tremoves a queue
\tqueue priority \t-\tset of commands for priority scheduling - prints help without any further arguments\n";
}
sub f_queue {
  my $self = shift;
  my $command = shift;

  my ($host, $driver, $db) =
    split ("/", $self->{CONFIG}->{"JOB_DATABASE"});

  if (! defined $command) {
    
    print $self->f_queue_HELP();
    return;
  }

  $self->{TASK_DB} or 
    $self->{TASK_DB}=
      AliEn::Database::TaskQueue->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin', SKIP_CHECK_TABLES=> 1});
  $self->{TASK_DB} or $self->{LOGGER}->error( "Admin-UI", "In initialize creating TaskQueue instance failed" )
    and return;
  $self->{TASK_DB}->setSiteQueueTable();

  $self->{PRIORITY_DB} or 
      $self->{PRIORITY_DB}=
	AliEn::Database::TaskPriority->new({DB=>$db,HOST=> $host,DRIVER => $driver,ROLE=>'admin', SKIP_CHECK_TABLES=> 1});
  $self->{PRIORITY_DB} or $self->{LOGGER}->error( "Admin-UI", "In initialize creating TaskPriority instance failed" );

  $self->{ADMIN_DB} or 
      $self->{ADMIN_DB}=
	  AliEn::Database::Admin->new({SKIP_CHECK_TABLES=> 1});

  $self->{ADMIN_DB} or $self->{LOGGER}->error( "Admin-UI", "In initialize creating Admin instance failed" );

  $DEBUG and $self->debug(1, "Calling f_queue_$command");
  my @return;
  if ( ( $self->{CATALOG}->{CATALOG}->{ROLE} !~ /^admin(ssl)?$/) && ($command ne "list") && ($command ne "info") && ($command ne "priority") ) {
      $self->info( "Error executing queue $command: you are not allowed to execute that!");
      return;
  }

  my $func="f_queue_$command";
  eval {
    @return=$self->$func( @_);
  };
  if ($@) {
    #If the command is not defined, just print the error message
    if ($@ =~ /Can\'t locate object method \"$func\"/) {
      $self->info( "queue doesn't understand '$command'", 111);
      #this is just to print the error message"
      return $self->f_queue();
    }
    $self->info( "Error executing queue $command: $@");
    return;
  }
  return @return;
}
sub f_queue_info {
  my $self=shift;
  my $jdl=grep (/^-jdl$/, @_);
  @_=grep (!/^-jdl$/, @_);
  my $site = (shift or '%') ;
  $jdl and $jdl=",jdl";
  $jdl or $jdl="";
  my $array = $self->{TASK_DB}->getFieldsFromSiteQueueEx("site,blocked, status, statustime$jdl, ". join(", ", @{AliEn::Util::JobStatus()})," where site like '$site' ORDER by site");
  if ($array and @$array) {
    return $self->f_queueprint($array,$site);
  }
  return ;
}

sub f_queue_priority {
  my $self=shift;
  my $subcommand = shift or print STDERR "You have to specify a subcommand to command <priority>:\n" 
    and print " queue priority jobs [user] [max.rows=1000]\t - list the job priority ranking - use <user> = \% to set max. rows for all\n" 
      and print " queue priority list [user]                 \t - list the user priorities\n" and return;

  if ($subcommand eq "jobs" ) {
    my $user = (shift or "%");
    my $limit = (shift or "10000");
    printf "----------------------------------------------------------------------------------------------------\n";
    #	  my $array = $self->{TASK_DB}->getFieldsFromQueueEx("queueId,SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user,priority","where status='WAITING' and submitHost like '$user\@%' ORDER by priority desc limit $limit");      
    my $array = $self->{TASK_DB}->getFieldsFromQueueEx("queueId,SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user,priority","where status='WAITING' ORDER by priority desc limit $limit");      
    my $cnt=0;
    foreach (@$array) {
      $cnt++;
      if (($_->{'user'} eq $user) || ($user eq "%")) {
	printf " [%04d. ]      %-8s %12s %-8s\n",$cnt, $_->{'queueId'},$_->{'user'}, $_->{'priority'};
      }
    }
    printf "----------------------------------------------------------------------------------------------------\n";
    return;
  }
  if ($subcommand eq "list" ) {
    my $user = (shift or "%");
    my $array = $self->{PRIORITY_DB}->getFieldsFromPriorityEx("*","where user like '$user' ORDER BY user");
    if (@$array) {
      $self->f_priorityprint($array);
    }
    return;
  }
  
  if ($subcommand eq "add" ) {
    my $user = shift or print STDERR "You have to specify a username to be added!\n" and return;
    $self->{PRIORITY_DB}->checkPriorityValue($user);
    $self->f_queue_priority("list","$user");
    return;
      }
  
  if ($subcommand eq "set" ) {
    my $user = shift or print STDERR "You have to specify a user to modify!\n" and return;
    my $field = shift or print STDERR "You have to specify a field value to modify!\n" and return;
    my $value = shift or print STDERR "You have to specify a value to set for field $field!\n" and return;
    
    my $array = $self->{PRIORITY_DB}->getFieldsFromPriorityEx("*","where user like '$user' ORDER BY user");
	  if (! $array) {
	    print STDERR "User $user does not have an entry yet - use 'queue priority add <user>' first!\n";
	    return;
	  }
    
    my $lkeys;
    my $reffield = @$array[0];
    my $found = 0;
    foreach $lkeys (%$reffield) {
      if ($lkeys eq "$field") {
	$found =1;
	last;
      }
    }
    if (! $found) {
      print STDERR "There is no priority field named '$field' !\n";
      return;
    }

    my $set={};
    $set->{$field} = $value;
    my $done = $self->{PRIORITY_DB}->updatePrioritySet($user,$set);

    $self->f_queue_priority("list","$user");
  }
}

sub f_queue_ghost{
  my $self=shift;
  my $subcommand = (shift or "list");

  if ($subcommand =~ /list/) {
    my $now = time;
    my $diff = (shift or "600");
    printf "----------------------------------------------------------------------------------------------------\n";
    my $array = $self->{TASK_DB}->getFieldsFromQueueEx("queueId,SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, procinfotime,node, site,status","where ( (status like 'ERROR_%' or status='KILLED' or status='FAILED' or status='ZOMBIE' or status='QUEUED' or status='WAITING') and (procinfotime not like 'NULL') and (procinfotime > 1) and ($now-procinfotime)<$diff) ORDER by site");      
    my $cnt=0;
    foreach (@$array) {
      $cnt++;
      printf " [%04d. ]      %10s %-24s %24s %12s %-12s %-10s\n",$cnt, $_->{'queueId'},$_->{'site'},$_->{'node'},$_->{'user'}, $_->{'status'}, ($now - $_->{'procinfotime'});
    }
    
    printf "----------------------------------------------------------------------------------------------------\n";
    return;
  }
  
  if ($subcommand =~ /change/) {
    my $queueId = shift or print STDERR "You have to specify a queueId for which you want to change the status!\n" and return;
    my $status  = shift or print STDERR "You have to specify the status you want to set!\n" and return;
    
    my $set={};
    $set->{status} = $status;
    
    AliEn::Util::Confirm("Do you want to update $queueId to status $status?") or return;
    
    my $done = $self->{TASK_DB}->updateJob($queueId,$set);
    $done or print STDERR "Could not change job $queueId to status $status!\n" and return;
    return;
  }
}
# This internal subroutine checks if a queue exists or not. 
# By default, it will return true if the table exists.
# It can receive as a second argument a 0 if it has to check the opposite
sub f_queue_exists {
  my $self=shift;
  my $queue=shift;
  #this is to check if the queue has to exist or not;
  my $expectedValue=shift;
  defined $expectedValue or $expectedValue=1;
  my $command=(shift or "exists");
  $queue or $self->info( "Not enough arguments in 'queue $command'\nUsage: \t queue $command <queue name>") and return;
  my $exists=$self->{TASK_DB}->getFieldsFromSiteQueueEx("site","where site='$queue'");
  #If the queue does not exist, but i
  if (@$exists and  ! $expectedValue) {
    $self->info( "Error: the queue $queue already exists!");
    return;
  }
  if (! @$exists and  $expectedValue) {
    $self->info( "Error: the queue $queue does not exist!");
    return;
  }
  return 1;
}
sub f_queue_remove {
  my $self=shift;
  my $queue=shift;
  $self->f_queue_exists($queue, 1, "remove") or return;

  $DEBUG and $self->debug(1, "Let's try to remove the queue $queue");
  return $self->{TASK_DB}->deleteSiteQueue("site='$queue'");
}
sub f_queue_list {
  my $self=shift;
  my $site = (shift or '%') ;
  my $array = $self->{TASK_DB}->getFieldsFromSiteQueueEx("site,blocked,status,maxqueued,maxrunning,queueload,runload,QUEUED, QUEUED  as ALLQUEUED, (RUNNING + STARTED + INTERACTIV + SAVING) as ALLRUNNING","where site like '$site' ORDER by blocked,status,site");
  my $s1 = 0;
  my $s2 = 0;
  my $s3 = 0;
  my $s4 = 0;
  my $s5 = 0;
  my $s6 = 0;
  if (@$array) {
    printf "----------------------------------------------------------------------------------------------------\n";
    printf "%-32s %-12s %-20s %5s %5s %4s/%-4s %4s/%-4s\n","site","open", "status", "load", "runload", "queued","max", "run", "max";
    foreach (@$array) {
      my $allqueued=($_->{'ALLQUEUED'} ||0);
      my $maxqueued=($_->{'maxqueued'} ||0);
      my $allrunning=($_->{'ALLRUNNING'} ||0);
      my $maxrunning=($_->{'maxrunning'} ||0);
      my $blocked=($_->{blocked} || "undef");
      my $queueload=($_->{queueload} ||"undef");
      my $runload=($_->{runload} ||"undef");

      printf "%-32s %-12s %-20s %5s %5s %4s/%-4s %4s/%-4s\n",$_->{'site'},$blocked,$_->{'status'},$queueload,$runload,$allqueued,$maxqueued,$allrunning, $maxrunning;
      $s1+=$allqueued;
      $s2+=$maxqueued;
      $s3+=$allrunning;
      $s4+=$maxrunning;
    }
    $s4 and $s5 = sprintf "%3.02f", 100.0*$s1 / $s4;
    $s4 and $s6 = sprintf "%3.02f", 100.0*$s3 / $s4;
    my $empty="";
    my $sumsite="All";
    printf "----------------------------------------------------------------------------------------------------\n";
    printf "%-32s %-12s %-20s %5s %5s %4s/%-4s %4s/%-4s\n",$sumsite,$empty,$empty,$s5,$s6, $s1,$s2,$s3,$s4;
    printf "----------------------------------------------------------------------------------------------------\n";
  }
  
  return 1;
}

sub f_queue_update {
  my $self=shift;
  my $command=shift;
  my $queue=shift;

  $self->f_queue_exists($queue, 1, $command) or return;

  my $set={};
  $set->{blocked}="open";
  $command =~ /lock/ and $set->{blocked}="locked";
  $self->info( "=> going to $command the queue $queue ...");
  my $update=$self->{TASK_DB}->updateSiteQueue($set,"site='$queue'") or 
    print STDERR "Error opening the site $queue";

  $self->f_queue("info", $queue);
  return 1;
}
sub f_queue_lock{
  my $self=shift;
  $self->f_queue_update("lock",@_);
}
sub f_queue_open{
  my $self=shift;
  $self->f_queue_update("add",@_);
}


sub f_queue_add{
  my $self=shift;
  my $queue=shift;
  $self->f_queue_exists($queue, 0, "add") or return;

  my $set={site=>$queue, blocked=>"locked", status=>"new", statustime=>0};
  foreach (@{AliEn::Util::JobStatus()}){
    $set->{$_}=0;
  }
  my $insert=$self->{TASK_DB}->insertSiteQueue($set) or print STDERR "Error adding the site $queue";
  return $insert;
}

sub f_queue_purge {
  my $self=shift;
  my @killpids=();
  my $topurge=$self->{TASK_DB}->getFieldsFromQueueEx("queueId","where status=''");
  foreach (@$topurge) {
    print "Job $_->{queueId} has empty status field ... will be killed!\n";
    push @killpids,$_->{queueId};
  }
  $self->f_kill(@killpids);
  return;
}

sub f_queue_tokens{
  my $self=shift;
  my $subcommand = (shift or "list");
  if ($subcommand =~ /list/) {
    my $status = (shift or "%");
    printf "Doing listing\n";
    my $tolist=$self->{TASK_DB}->getFieldsFromQueueEx("queueId","where status='$status'");
    foreach (@$tolist) {
      my $token = $self->{ADMIN_DB}->getFieldFromJobToken($_->{queueId},"jobToken");
      printf "Job %04d Token %40s\n",$_->{queueId},$token;
    }
  }
  return;
}

sub f_quit {
  my $self = shift;

  print("bye now!\n");
  exit;
}

sub checkConnection {
  my $self = shift;

  #Checking the CM
  $DEBUG and $self->debug(1, "Checking the connection to the CM");

  if ($self->{SOAP}->checkService("ClusterMonitor")){
    $self->{CONNECTION}="ClusterMonitor" ;
    return 1;
  }
  $self->{CONNECTION}="Manager/Job" ;
  return $self->{SOAP}->checkService("Manager/Job", "JOB_MANAGER", "-retry");
}


sub submitCommands {
    my $self = shift;
    $DEBUG and $self->debug(1, "In submitCommands @_" );
    my $all_commands = shift;

    my @commands = split ";", $all_commands;

    my $command;

    foreach $command (@commands) {
        $DEBUG and $self->debug(1, "Submitting $command" );
        my (@args) = split " ", $command;
        $self->submitCommand(@args);
    }
    $DEBUG and $self->debug(1, "Done  submitCommands!!" );

    return 1;
}

sub resubmitCommand {
  my $self    = shift;
#    my $queueId = shift;

#   check for the -f 'fix' flag, which resubmits all faulty jobs ....

  if ($_[0] eq '-i') {
    if (!defined $_[1]) {
      print STDERR "Error: no queueId specified to <resubmit -i> \n";
      return;
    }
  
    AliEn::Util::Confirm("Do you want to reinsert job $_[1]?") or return;
    ( $self->checkConnection() ) or return;
    my $user = $self->{CATALOG}->{CATALOG}->{ROLE};
    my $done = $self->{SOAP}->CallSOAP($self->{CONNECTION}, "reInsertCommand", $_[1], $user);
    my $result;
    $done or
      $self->info( "Error reinserting $_[1]") and
	return $result;
    $result = $done->result;
    $self->info( "Process $_[1] reinserted!!");
    return $result;
  }


  if ($_[0] eq '-f') {
    if (!defined $_[1]) {
      print STDERR "Error: no queueId specified to <resubmit -f> \n";
      return;
    }
    
    if ((defined $_[2]) && ($_[2] eq '-i')) {
      AliEn::Util::Confirm("Do you really want to resubmit all failed jobs of $_[1] [reinsertion active ]?") or return;
    } else {
      AliEn::Util::Confirm("Do you really want to resubmit all failed jobs of $_[1] [reinsertion unset  ]?") or return;
    }
    
    AliEn::Util::Confirm("Are you really sure ?") or return;
    
    my @allps = $self->f_ps("-q","-Aafs","-id","$_[1]");
    foreach (@allps) {
      my ($user, $id, $status, @rest) = split " ",$_;
      if ( (($status ne 'R') and ($status ne 'ST') and ($status ne 'A') and ($status ne 'I') and ($status ne 'Q') and ($status ne 'W') and ($status ne 'D' ) and ($status ne 'SV') and ($status ne 'Z') ) and ($id =~ /\-.*/)) {
	if ((defined $_[2]) && ($_[2] eq '-i')) {
	  my $id2kill = $id;
	  $id2kill =~ s/\-//g;
	  my @result;
	  my $done = $self->{SOAP}->CallSOAP($self->{CONNECTION}, "reInsertCommand", $id2kill, $user);
	  $self->{SOAP}->checkSOAPreturn($done) or
	    $self->info( "Error reinserting $id2kill") and return $done;
	  push @result, $done->result;
	  $self->info( "Process $id2kill [$status] reinserted!!");
	} else {
	  die;
	  my $id2kill = $id;
	  $id2kill =~ s/\-//g;
	  print("Resubmitting process <$id2kill> [ status |$status| ] \n");
	  # kill first the actual process
	  $self->f_kill($id2kill);
	  # resubmit the same
	  $DEBUG and $self->debug(1, "Resubmitting command $id2kill" );
	  
	  ( $self->checkConnection() ) or return;
	  my $user = $self->{CATALOG}->{CATALOG}->{ROLE};
	  my $done = $self->{SOAP}->CallSOAP($self->{CONNECTION}, "resubmitCommand", $id2kill, $user ,$_[1], $id2kill);
	  my @result;
	  $self->{SOAP}->checkSOAPreturn($done) or 
	    $self->info( "Error resubmitting $id2kill") and 
	      return @result;
	  push @result, $done->result;
	  $self->info( "Process $id2kill resubmitted!!");
	}
      }
    }
    return;
  }
  
  if ($_[0] eq '-k') {
    if (!defined $_[1]) {
      print STDERR "Error: no queueId specified to <resubmit -f> \n";
      return;
    }
    my @allps = $self->f_ps("-q","-Aafs","-id","$_[1]");
    foreach (@allps) {
      my ($user, $id, $status, @rest) = split " ",$_;
      if ( (($status eq 'Z') ) ){
	my $id2kill = $id;
	$id2kill =~ s/\-//g;
	print("Killing Zombie process <$id2kill> [ status |$status| ] \n");
	# kill first the actual process
	$self->f_kill($id2kill);
      }
    }
    return;
  }
  
  if ($_[0] eq '-q') {
    if (!defined $_[1]) {
      print STDERR "Error: no queueId specified to <resubmit -f> \n";
      return;
    }
    my @allps = $self->f_ps("-q","-Aafs","-id","$_[1]");
    foreach (@allps) {
      my ($user, $id, $status, @rest) = split " ",$_;
      if ( (($status eq 'Z') || ($status eq 'W' ) || ($status eq 'EE') || ($status eq 'EA') || ($status eq 'ES') || ($status eq 'ER') || ($status eq 'Q') || ($status eq 'ESV') || ($status eq 'EV') || ($status eq 'EVT') || ($status eq 'EVN') || ($status eq 'EIB')) ){
	my $id2kill = $id;
	$id2kill =~ s/\-//g;
	print("Killing process <$id2kill> [ status |$status| ] \n");
	# kill first the actual process
	$self->f_kill($id2kill);
      }
    }
    return;
  }

  (@_)
    or print STDERR
      "Error: no queueId specified!\nUsage resubmitCommand <queueId>\n"
	and return;
  $DEBUG and $self->debug(1, "Resubmitting command @_" );

  ( $self->checkConnection() ) or return;
  my $user = $self->{CATALOG}->{CATALOG}->{ROLE};
  my @result;
  foreach my $queueId (@_) {
    my $done = $self->{SOAP}->CallSOAP($self->{CONNECTION}, "resubmitCommand", $queueId, $user );
    $done or 
      $self->info( "Error resubmitting $queueId") and 
	return @result;
    push @result, $done->result;
    $self->info( "Process $queueId resubmitted!!");
  }

  return @result;
}


sub DESTROY {
    my $self = shift;
#    ( $self->{LOGGER} )
#      and $DEBUG and $self->debug(1, "Destroying remotequeue" );
    $self->{TASK_DB} and $self->{TASK_DB}->close();
    ( $self->{CATALOG} ) and $self->{CATALOG}->close();
}

sub catch_zap {
    my $signame = shift;
    print STDERR "Somebody sent me a SIG$signame. Arhgggg......\n";
    die;
}

=item masterJob($queueId)

Displays information about a masterJob and all its subjobs

=cut

sub masterJob_HELP{ return "masterJob: prints information about a job that has been split in several subjobs. Usage:
\tmasterJob <jobId> [-status <status>] [-site] [-printid] [-id <id>] [merge|kill|resubmit]

Options:
    -status <status>: display only the subjobs with that status
    -id <id>:   display only the subjobs with that id
    -site <id>: display only the subjobs on that site
    -printid:   print also the id of all the subjobs
    -printsite: split the number of jobs according to the execution site
    merge:      collect the output of all the subjobs that have already finished
    kill:       kill all the subjobs
    resubmit:   resubmit all the subjobs selected
    expunge:    delete completely the subjobs

You can combine kill and resubmit with '-status <status>' and '-id <id>'. For instance, if you do something like 'masterjob <jobId> -status ERROR_IB resubmit', all the subjobs with status ERROR_IB will be resubmitted
";};

sub masterJob {
  my $self=shift;
  my $queueId=shift;
  $queueId or $self->info( "Not enough arguments in 'masterJob': missing queueId\n". $self->masterJob_HELP()) and return;

  $self->info( "Checking the masterjob of $queueId");

  ( $self->checkConnection() ) or return;
  my $user=$self->{CATALOG}->{CATALOG}->{ROLE};

  my $done= $self->{SOAP}->CallSOAP($self->{CONNECTION}, "getMasterJob", $user, $queueId, @_) 
    or return;
  my $info=$done->result();

  my $action=shift @$info;
  my $summary="";
  if ($action eq "info") {
    my $total=0;
    my $jobInfo=shift @$info;
    $summary.="The job $queueId is in status: $jobInfo->{status}\nIt has the following subjobs:\n";
    foreach my $subjob (@$info){
      $subjob or next;
      my $ids="";
      my $site=$subjob->{exechost} ||"";
      $site and $site=" ($site)";
      ($subjob->{ids})
	and $ids="(ids: ".join (", ", @{$subjob->{ids}}) .")";
      $summary.="\t\tSubjobs in $subjob->{status}$site: $subjob->{count} $ids\n";
      $total+=$subjob->{count};
    }

    $summary.="\nIn total, there are $total subjobs";
    if ($jobInfo->{merging}) {
      $summary.="\nThere are some jobs merging the output:";
      foreach my $merge (@{$jobInfo->{merging}}) {
	$summary.="\n\tJob $merge->{queueId} : $merge->{status}";
      }
    }
    
  }else {
    $summary.=join("\n", @$info);
  }
  $self->info($summary);

  return 1;
}

sub checkJobAgents {
  my $self=shift;
  my $inDB=$self->{DB}->queryColumn("SELECT batchId from JOBAGENT")
    or $self->info("Error getting the list of jobagents") and return;
  my @inDB=@$inDB;
  my @inBatch=$self->{BATCH}->getAllBatchIds();
  
  $self->info("According to the db: @inDB. According to the batch system: @inBatch");
  foreach my $job (@inDB) {
    $self->info("Looking for $job");
    grep (/^$job$/, @inBatch) or print "Agent $job is dead!!\n";
    @inBatch=grep (! /^$job$/, @inBatch);
  }
  if (@inBatch){
    $self->info("Jobs @inBatch are in the batch system, but not in the DB");
  }
  return 1;
}
sub requirementsFromPackages {
  my $self=shift;
  my $job_ca=shift;

  $DEBUG and $self->debug(1, "Checking Packages required by the job" );
  my ( $ok, @packages ) = $job_ca->evaluateAttributeVectorString("Packages");
  ($ok) or return "";

  my $installed="";
  #Checking if the packages have to be installed
  ($ok, my $value)=$job_ca->evaluateExpression("PackagesPreInstalled");
  if ($ok and $value) {
    $self->debug( 1, "The packages have to be installed" );
    $installed="Installed";
  }


  $self->info("Checking if the packages @packages are defined in the system");
  my $ref=$self->f_packman("list", "-silent", "-all") or
    $self->info("Error getting the list of packages") and return;
  my $requirements="";
  my @definedPack=@$ref;
  foreach my $package (@packages) {
    $package =~ /@/ or $package=".*\@$package";
    $package =~ /::/ or $package="${package}::.*";
    $self->debug(1,"checking if $package is in @definedPack");
    my @name=grep (/^$package$/, @definedPack);
    if (@name) {
      $requirements.=" && (member(other.${installed}Packages, \"$name[0]\"))";
      next;
    }
    $self->info("The package $package is not defined!!");
    return;
  }

  return $requirements;
}

sub requirementsFromMemory{
  my $self=shift;
  my $job_ca=shift;
  my $requirements="";
  my ( $ok, $memory ) = $job_ca->evaluateExpression("Memory");
  if ($memory) {
    ($memory=~ s/\s+mb?\s*//i) and $memory*=1024;
    ($memory=~ s/\s+gb?\s*//i) and $memory*=1024*1024;
    ($memory=~ s/\s+kb?\s*//i);
    $memory=~ /^\s*\d+\s*$/ or $self->info("Sorry, I don't understand '$memory' as a memory unit. Memory is supposed to be the number of KB that you want in the worker node. You can specify MB or GB if you prefer to specify in those units") and return;
    $requirements=" && (other.FreeMemory>$memory) ";
  }
  ( $ok, $memory ) = $job_ca->evaluateExpression("Swap");
  if ($memory) {
    ($memory=~ s/\s+mb?\s*//i) and $memory*=1024;
    ($memory=~ s/\s+gb?\s*//i) and $memory*=1024*1024;
    ($memory=~ s/\s+kb?\s*//i);
    $memory=~ /^\s*\d+\s*$/ or $self->info("Sorry, I don't understand $memory as a swap unit. Memory is supposed to be the number of KB that you want in the worker node. You can specify MB or GB if you prefer to specify in those units") and return;
    $requirements=" && (other.FreeSwap>$memory) ";
  }

  return $requirements;
}


sub f_packman_HELP {return  "packman: talks to the Package Manager. By default, it talks to the closest PackMan. You can also specify '-name <PackManName>' to talk to a specific instance. Depending on the first argument, it does different tasks:\nUsage: 
\tpackman list:\treturns all the packages defined in the system
\tpackman listInstalled:\treturns all the packages that the service has installed
\tpackman test <package>: tries to configure a package. Returns the metainformation associated with the package, a view of the directory where the package is installed, and an environment that the package would set
\tpackman install <package>: install a package (and all its dependencies) in the local cache of the PackMan
\tpackman installLog <package>: get the installation log of the package
\tpackman remove  <package>: removes a package from the local cache
\tpackman define <name> <version> <tar file> [<package options>]
\tpackman undefine <name> <version>

The format of the string <package> is:
   [<user>\@]<PackageName>[::PackageVersion}
For instance, 'ROOT', 'ROOT::4.1.3', 'psaiz\@ROOT', 'psaiz\@ROOT::4.1.2' comply with the format of <package>
";
}

sub f_packman {
  my $self=shift;
  $self->debug(1, "Talking to the PackMan: @_");
  my $silent     = grep ( /-s/, @_ ); 
  my $returnhash = grep ( /-z/, @_ ); 
  my @arg        = grep ( !/-z/, @_ );
  @arg        = grep ( !/-s/, @arg );

  my $string=join(" ", @arg);
  my $serviceName="PackMan";
  $string =~ s{-?-silent\s+}{} and $silent=1;
  if ( $string =~ s{-?-n(ame)?\s+(\S+)}{} ){
    my $name=$2;
    $self->info( "Talking to the packman $name");

    my $done=$self->{CONFIG}->CheckServiceCache("PACKMAN", $name)
      or $self->info( "Error looking for the packman $name") 
	and return;
    $self->{SOAP}->Connect({address=>"http://$done->{HOST}:$done->{PORT}",
			    uri=>"AliEn/Service/PackMan",
			    name=>"PackMan_$name",
			    options=>[timeout=>5000]}) or return;
    $serviceName="PackMan_$name";
    @arg=split (" ", $string);
  }

  my $operation=shift @arg;
  $operation or 
    $self->info( $self->f_packman_HELP(),0,0) and return;
  my $soapCall;
  my $requiresPackage=0;
  if ($operation =~ /^l(ist)?$/){
    $soapCall="getListPackages";
    $operation="list";
  } elsif  ($operation =~ /^listI(nstalled)?$/){
    $soapCall="getListInstalledPackages";
    $operation="listInstalled";
  } elsif  ($operation =~ /^t(est)?$/){
    $requiresPackage=1;
    $soapCall="testPackage";
    $operation="test";
  } elsif ($operation =~ /^i(nstall)?$/){
    $requiresPackage=1;
    $soapCall="installPackage";
    $operation="install";
  } elsif ($operation =~ /^r(emove|m)?$/){
    $requiresPackage=1;
    $soapCall="removePackage";
    $operation="remove";
  } elsif ($operation =~ /^d(efine)?$/){
    return $self->definePackage(@arg);
  } elsif ($operation =~ /^u(ndefine)?$/){
    return $self->undefinePackage(@arg);
  } elsif ($operation =~ /^installLog?$/){
    $soapCall="getInstallLog";
    $requiresPackage=1;
  } else {
    $self->info( "I'm sorry, but I don't understand $operation");
    $self->info( $self->f_packman_HELP(),0,0);
    return
  }
  if ($requiresPackage) {
    my $package=shift @arg;
    $package or 
      $self->info( "Error not enough arguments in 'packman $operation") 
	and $self->info( $self->f_packman_HELP(),0,0) 
	  and return;
    my $version="";
    my $user=$self->{CATALOG}->{CATALOG}->{ROLE};
    $package =~ s/::([^:]*)$// and $version=$1;
    $package =~ s/^([^\@]*)\@// and $user=$1;
    if  ($operation =~ /^r(emove|m)?$/){
      if ($user ne $self->{CATALOG}->{CATALOG}->{ROLE}) {
	$self->{CATALOG}->{CATALOG}->{ROLE} eq "admin" or 
	  $self->info( "You can't uninstall the package of someone else") and return;
      }
    }
    @arg=($user, $package, $version, @arg);
  }

  $silent or $self->info( "Let's do $operation (@arg)");
  my $result=$self->{SOAP}->CallSOAP($serviceName, $soapCall,@arg)
    or $self->info( "Error talking to the PackMan") and 
      return;

  my ($done, @result)=$self->{SOAP}->GetOutput($result);
  $done or $self->info( "Error asking for the packages")
    and return;
  my $return=1;
  if ($operation =~ /^list(installed)?/i){
    my $message="The PackMan has the following packages";
    $1 and $message.=" $1";
    $silent or $self->info( join("\n\t", "$message:",@result));

    if ($returnhash) {
	my @hashresult;
	map { my $newhash = {}; my ($user, $package) = split '@', $_; $newhash->{user} = $user; $newhash->{package} = $package ; push @hashresult, $newhash;} @result;
	return @hashresult;
    }

    $return=\@result;
  } elsif  ($operation =~ /^t(est)?$/){
    $silent or $self->info( "The package (version $done) has been installed properly\nThe package has the following metainformation\n". Dumper(shift @result));
    my $list=shift @result;
    $silent or $self->info("This is how the directory of the package looks like:\n $list");
    my $env=shift @result;
    $env and $self->info("The package will configure the environment to something similar to:\n$env");
  } elsif ($operation =~ /^r(emove|m)$/){
    $self->info("Package removed!!\n");
  } elsif ($operation =~ /^installLog$/){
    $self->info("The installation log is\n
=========================================================
$done
=========================================================
\n");
  }

  return $return;
}

sub definePackage{
  my $self=shift;
  my $packageName=shift;
  my $version=shift;
  my $tar=shift;
  my $message="";
  $self->info( "Adding a new package");
  $packageName or $message.="missing Package Name";
  $version or $message.="missing version";
  $tar or $message.="missing tarfile";
  (-f $tar) or $message.="the file $tar doesn't exist";

  $message and $self->info( "Error: $message", 100) and return;

  my @args=();
  my $se="";
  my $lfnDir=lc($self->{CATALOG}->{CATALOG}->GetHomeDirectory()."/packages");
  my $sys1 = `uname -s`;
  chomp $sys1;
  my $sys2 = `uname -m`;
  chomp $sys2;
  my $platform="$sys1-$sys2";

  while (my $arg=shift){
    if ($arg=~ /^-?-se$/ ) {
      $se=shift;
      next;
    } 
    if ($arg=~ /^-vo$/) {
      $lfnDir="/$self->{CONFIG}->{ORG_NAME}/packages";
      next;
    }
    if ($arg=~ /^-?-platform$/) {
      $platform=shift;
      next;
    }else {
      push @args, $arg;
    }
  }
  $lfnDir.="/$packageName/$version";

  my $lfn="$lfnDir/$platform";

  $self->{CATALOG}->{CATALOG}->isFile($lfn) and
    $self->info( "The package $lfn already exists") and return;
  $self->{CATALOG}->execute("mkdir", "-p", $lfnDir)
    or $self->info( "Error creating the directory $lfnDir")
      and return;
  $self->{CATALOG}->execute("addTag", $self->{CATALOG}->{CATALOG}->GetHomeDirectory()."/packages/$packageName", "PackageDef")
    or $self->info( "Error creating the tag definition")
      and return;
  $self->{CATALOG}->execute("add", $lfn, $tar, $se) 
    or $self->info( "Error adding the file $lfn from $tar $se")
      and return;
  if (@args) {
    if (!$self->{CATALOG}->execute("addTagValue",$lfnDir, "PackageDef", @args)){
      $self->info( "Error defining the metainformation of the package");
      $self->{CATALOG}->execute("rm", "-rf", $lfn);
      return;
    }
  }
  $self->info( "Package $lfn added!!");
  return 1;
}
sub undefinePackage{
  my $self=shift;
  my $packageName=shift;
  my $version=shift;
  my $message="";
  $self->info( "Undefining a package");
  $packageName or $message.="missing Package Name";
  $version or $message.="missing version";

  $message and $self->info( "Error: $message", 100) and return;

  my $arguments=join (" ", @_);
  my $sys1 = `uname -s`;
  chomp $sys1;
  my $sys2 = `uname -m`;
  chomp $sys2;
  my $platform="$sys1-$sys2";
  if (($arguments=~ s{-?-platform\s+(\S+)}{})){
    $platform=$1;
    @_=split (" ",$arguments);
  }

  my $lfnDir=$self->{CATALOG}->{CATALOG}->GetHomeDirectory()."/packages/$packageName/$version";
  my $lfn="$lfnDir/$platform";

  $self->{CATALOG}->{CATALOG}->isFile($lfn) or
    $self->info( "The package $lfn doesn't exist") and return;
  $self->{CATALOG}->execute("rm", $lfn)
    or $self->info( "Error removing $lfn")
      and return;

  $self->info( "Package $lfn undefined!!");
  return 1;
}

return 1;
