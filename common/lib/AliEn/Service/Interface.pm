package AliEn::Service::Interface;
use Data::Dumper;
use AliEn::XML;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

#use SOAP::Transport::HTTP;
use AliEn::Service;
use strict;
use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Service::Interface::Algorithm;
use POSIX qw(strftime);


use vars qw (@ISA);

@ISA=("AliEn::Service", "AliEn::Service::Interface::Algorithm");
use Classad;

my @domainAPI=({'xmlns'=>"http://mammogrid.com/portal/api/"});
#my @domainPatient=(
#{'xmlns:ns2'=>"http://mammogrid.com/portal/patient/"}
#		  );

sub test {
  my $this = shift;
  print  "BRAVOOOO!!!\n";
  return ("CLARO");
}
sub setAlive{
  my $s=shift;
  return 1;
}

my $self = {};

sub initialize {
    $self     = shift;
    my $options =(shift or {});

    $options->{role}="admin";

    $self->{PORT}=$options->{port};#"7069";#$self->{CONFIG}->{'BROKER_PORT'};
    $self->{SITE}=$self->{CONFIG}->{SITE} || "cern";
    $self->{SITE}=lc($self->{SITE});
    $self->{HOST}=$self->{CONFIG}->{SE_HOST};
    $self->{LOGGER}->info("Interface", "Creating an interface for $self->{SITE}");
    $self->{SERVICE}="Interface";
    $self->{SERVICENAME}="Interface";
    $self->{LISTEN}=10;
    $self->{PREFORK}=1;
#    $self->{SECURE}=1;

    $self->debug(1, "Initializing Interface" );

    
    $self->{XML}= AliEn::XML->new({conformance=>''});

    $self->{XML} or $self->{LOGGER}->info("Error getting the XML parser") 
	and return;

    $self->{UI} = AliEn::UI::Catalogue::LCM::Computer->new($options);

    ( $self->{UI} )
      or $self->{LOGGER}->error( "Broker", "Error creating userinterface" )
      and return;

    $self->{LOGGER}
      ->debug( "Broker", "User Interface created!!\n\tCreating database" );

    $self->SUPER::initialize($options);
    return $self;
}

##############################################################################
# Public functions
##############################################################################

# Method: add
#  Input: Dicom SOP instance Uid identifying a DICOM file
#  Output Document conforming to the Response description
#  Description: Triggers the grid to process a file from the INBOX of the GridBox

sub add {
  my $this=shift;

  my ($status, $tree)=$self->checkInput("add", @_);
  ($status eq "OK") or  return  $self->createResponse($status, $tree);
  $self->{LOGGER}->info("Interface", "The parsing worked...");

  
  #Now, we have to populate the database...
  my $pfn=$self->{XML}->getXMLFirstValue("PFN", $tree);
  $self->{LOGGER}->info("Interface", "Got pfn $pfn");
  $pfn or $self->{LOGGER}->info("Interface", "Error: there is no PFN in the PatientModule") and return $self->createResponse("NOK", "there is no PFN in the PatientModule");
  ($status, my $desc)=$self->populateDatabase($tree, $pfn);
  if ($status ne "OK"){
    $self->{LOGGER}->info("Interface", "The status after populating the database is not ok!!\n$desc\n");
  }
  return $self->createResponse($status, $desc);
}

#
# This is just to check what kind of objects Mirada expect
#

sub printObject {
  my $this=shift;
  $self->{LOGGER}->info("Interface", "Testing the type of the object");
  @_ or  $self->{LOGGER}->info("Interface", "No input!!");
  my $i=1;
  while( @_)
    {
      $self->{LOGGER}->info("Interface", "Argument $i\n");
      print Dumper(shift);
      $i++;
    }
  $self->{LOGGER}->info("Interface", "Everything done!!");

  return $self->createResponse("OK", "");

}

# Method update
# Input: Document conforming the patient schema
# Output: Document conforming the response schema
# Description: It updates the metadata of the specified patient
sub update {
  my $this=shift;

  my ($status, $tree)=$self->checkInput("update", @_);
  ($status eq "OK") or  return  $self->createResponse($status, $tree);

  ($status, my $desc)=$self->executeInDirectory("updateDirectory", $tree);

  return $self->createResponse($status, $desc);

}
# Method simpleQuery
# Input: Document conforming the patient schema, with values in the files to use in the search
# Output: PatientListResponse
# Description: It queries the catalogue for all the entries that satisfy
#              the input requirements, and it sends back the metadata of them
sub simpleQuery {
  my $this=shift;

  my ($status, $tree)=$self->checkInput("simpleQuery", @_);
  my $xml=$self->{XML}->{GENERATOR};
  if ($status ne "OK"){
    my $r=$xml
      ->PatientListResponse(@domainAPI,
			    $self->createResponse($status,$tree,"o"),
			   );
    $self->{LOGGER}->info("Interface", "Query done");
    return  "$r";
  }

  my $desc="";
  foreach my $constraint ($self->{XML}->getXMLElement("Constraint", $tree)) {
    my ($patient)=$self->{XML}->getXMLElement("PatientModule", $constraint);
    ($status, my $desc2)=$self->executeInDirectory("queryDirectory", $patient);
  
    if ($status ne "OK") {
      $self->{LOGGER}->info("Interface", "The query returned an error\n$desc");

      my $r=$xml
	->PatientListResponse(@domainAPI,
			      $self->createResponse($status,$desc,"o"),
			     );
      $self->{LOGGER}->info("Interface", "Query done");
      return  "$r";
    }

    $desc2=$self->modifyComparison($constraint, $desc2);

    my $c=$self->{XML}->getXMLFirstValue("Conjunction", $constraint);
    $c or $c="and";
    $desc or $c="";
 
    $desc.=" $c $desc2";
  }
  #Ok, now we are supposed to do the query in the catalogue
  $self->{LOGGER}->info("Interface", "Ready to do find $desc");
  my @files=$self->{UI}->execute("find", "/mammogrid/", "dcm", split " ", $desc);
  $self->debug(1, "Found @files");

  my $totalFiles=$#files+1;

  if ($self->{XML}->getXMLFirstValue("QueryOffset", $tree)) {
    my $offset=$self->{XML}->getXMLFirstValue("QueryOffset", $tree);
    $self->{LOGGER}->info("Interface", "Skipping the first $offset entries");
    while ($offset and shift @files) {$offset --};
  }

  if ($self->{XML}->getXMLFirstValue("QueryLimit", $tree)) {
    my $limit=$self->{XML}->getXMLFirstValue("QueryLimit", $tree);
    $self->{LOGGER}->info("Interface", "Returning only $limit  entries");
    my @temp=@files;
    @files=();
    while ($limit and @temp) {push (@files, shift @temp); $limit--;};
  }


  my $patientList="";
  my $returned=$#files+1;
  
  if ($self->{XML}->getXMLFirstValue("QueryNoData", $tree)) {
    $self->{LOGGER}->info("Interface", "Skipping the generation of the PatientList");
    $returned=0;
  } else {
    ($status, $patientList)=$self->createPatientListXML(@files);
    
    if ($status ne "OK") {
      my $r=$xml
	->PatientListResponse(@domainAPI,
			      $self->createResponse($status,$patientList,"o"),
			     );
      $self->{LOGGER}->info("Interface", "Query done");
      return  "$r";
    }
  }
  $self->debug(1, "PatientList returned ok");
  
  my $r=$xml
    ->PatientListResponse(@domainAPI,
			  $self->createResponse("OK", "It worked!!", "o"),
			  $patientList,
			  $xml->QueryTotal($totalFiles),
			  $xml->QueryReturned($returned));
  $self->{LOGGER}->info("Interface", "Query done");
  return  "$r";
}
# Method prepare
# Input: Document conforming the patientlist schema, with values in the LFN (the rest are ignored
# Output: PatientListResponse
# Description: It returns the LFN sorted by their availaility
sub prepare {
  my $this=shift;

  my ($status, $tree)=$self->checkInput("prepare", @_);
  ($status eq "OK") or  return  $self->createResponse($status, $tree);

  my @files=$self->{XML}->getXMLElement("LFN","-s", $tree);
  @files or   $self->{LOGGER}->info("Interface", "There were no LFN's in the XML") and return  $self->createResponse("NOK","There were no LFN's in the XML");

  $self->{LOGGER}->info("Interface", "Checking the files @files");

  my $xml=$self->{XML}->{GENERATOR};
  my @list=();
  foreach my $file (@files) {
    push @list, $xml->LFN($file);
  }


  my $r=$xml->FileListResponse(@domainAPI,
			  $self->createResponse("OK", "It worked!!", "o"),
			  $xml->FileList(@list));
  $self->{LOGGER}->info("Interface", "prepare done");
  $self->debug(1, "Returning $r");
  return  "$r";

}

# Method retrieve
# Input: Document conforming the patientlist schema, with only one LFN (the rest are ignored) 
# Output: Response
# Description: It gets the file, from alien, gets the metadata, and calls the pack subroutine. 
sub retrieve {
  my $this=shift;
#  my $file=shift;

  my ($status, $tree)=$self->checkInput("retrieve", @_);
  ($status eq "OK") or  return  $self->createResponse($status, $tree);

  my $file=$self->{XML}->getXMLFirstValue("LFN",$tree);
  my $xml=$self->{XML}->{GENERATOR};
  if (! $file) {
    $self->{LOGGER}->info("Interface", "There were no LFN's in the XML");
    my $r=$xml->URLResponse(@domainAPI,$self->createResponse("NOK", "There were no LFN's in the XML", "o"),) ;
    return  "$r";
  }
  
  $self->{LOGGER}->info("Interface", "Getting the file $file");

  my $date=time;
  my $fileName=$file;
  $fileName =~ s/^.*\///;
  $fileName.=".$$.$date";

  my (@done)=$self->{UI}->execute("get", $file, "/tmp/Mammogrid/OUTBOX/MG/$fileName");
  if (! @done ) {
    $self->{LOGGER}->info("Interface", "Error getting the file");
    my $r=$xml->URLResponse($self->createResponse("NOK", "Error getting the file $file from the catalogue", "o"),) ;
    return  "$r";
  }
  $self->{LOGGER}->info("Interface", "Got the file @done");
  my $pfn="http://$self->{HOST}:6080/MG/$fileName";
  
  my $patch=$self->{XML}->getXMLFirstValue("PATCH",$tree);
  my $r;
  if ($patch) {
    ($status, $r)=$self->createPatientXML( {PFN=> $pfn}, $file);
    if ($status ne "OK") {
      $self->{LOGGER}->info("Interface", "Error creating the patient module");
      $r=$xml->URLResponse($self->createResponse("NOK", "Error creating the patient module", "o"),) ;
      return  "$r";
    }
    $r=$xml->PatientResponse(@domainAPI,$self->createResponse("OK", "It worked!!", "o"),
			 $r); 
  }
  else {
    $r=$xml->URLResponse(@domainAPI,$self->createResponse("OK", "It worked!!", "o"),
			 $xml->PFN($pfn)) ;
  }
  $self->{LOGGER}->info("Interface", "Retrieve done $pfn");
  return  "$r";

}
sub authenticate {
  return $self->createResponse("NOK", "not running in this port!! Please contact http://aliendev.cern.ch:4000/AliEn/Service/SecureFactory!!")
}

##############################################################################
#Private functions
##############################################################################
#sub checkInput
#    input $function: Name of the function that is parsing the input
#          $xml: string containing an xml fil
#    output ($status, $xmlTree)
#          $status is OK if everything is fine. Otherwise, is NOK
#          If status NOK, $xmlTreee contains the error. 
sub checkInput {
  my $self=shift;
  my $function=shift;

  my $xml=shift;
  $xml or $self->{LOGGER}->info("Interface", "Not enough arguments in $function. Missing xml") and return  ("NOK","Not enough arguments");

  $self->{LOGGER}->info("Interface", "\n\t\tStarting a $function");

  $self->debug(1,"Input XML $xml");
  my $tree=$self->{XML}->parse($xml);
  $tree or  $self->{LOGGER}->info("Interface", "Error parsing the input $xml") and return  ("NOK","Error parsing the input");

  return ("OK", $tree);
}
sub getModules {
  my $self=shift;

  my $modules={};
#  $modules->{'ns1:PatientModule'}={ "ID", "PatientId", 
#				    "PATH", "pat",
#				    "TABLE", "PatientsV0",
#				    "DOMAIN", "ns1"};

  $modules->{'PatientModule'}={ "ID", "PatientId", 
				    "PATH", "pat",
				    "TABLE", "PatientsV0",
				    "DOMAIN", "ns2",
			      };

  $modules->{PatientStudyPathology}={
				     "TABLE", "PatientStudyPathologyV0",
				     "CONTAINER", "NO"
				    };

  $modules->{StudyModule}={"ID"=> "StudyId",
			   "PATH" => "std",
			   "TABLE"=> "StudiesV0",
			   "NO_EXPORT_COLUMN"=>"(NumberOfPregnancies)|(.*CancerHistory)|(CancersInFamily)|(OnHrt)|(Previous.*)|(Past.*)|(Reader.*)|(AgeAtStudy)"};

  $modules->{PatientStudyModule}={"TABLE"=>"StudiesV0",
				  "EXPORT_COLUMN"=>"(NumberOfPregnancies)|(.*CancerHistory)|(CancersInFamily)|(OnHrt)|(Previous.*)|(Past.*)|(Reader.*)|(AgeAtStudy)",
				  "NO_EXPORT_COLUMN" => ".*",
				  "CONTAINER" => "ONE",
  
				 };

  $modules->{SeriesModule}={"ID"=> "SeriesInstanceUid",
			    "PATH" => "series",
			    "TABLE"=> "SeriesV0", 
			   "NO_EXPORT_COLUMN"=>"PresentationIntent"};

  $modules->{ImageModule}={"ID"=> "SopInstanceUid",
			   "PATH" => "img",
			   "TABLE"=> "ImagesV0",
			   "NO_EXPORT_COLUMN"=>"(Equipment.*)|(ImageLaterality)|(OrganExposed)|(Exposure.*)|(SamplesPerPixel)|(Photometric.*)|(PixelIntensity.*)",
			   "NO_IMPORT_COLUMN" =>"PFN",
			  };

  $modules->{MammographyImageModule}={#"ID"=> "SopInstanceUID",
				      #			   "PATH" => "img",
				      "TABLE"=> "ImagesV0",
				      "EXPORT_COLUMN", "(ImageLaterality)|(OrganExposed)",
				      "NO_EXPORT_COLUMN"=>".*"};

  $modules->{XrayDoseModule}={"TABLE"=> "ImagesV0",
			      "EXPORT_COLUMN"=> "Exposure",
			      "NO_EXPORT_COLUMN"=> ".*",
			      "NO_IMPORT_COLUMN" =>"BodyPartThickness"};

  $modules->{EquipmentModule}={"TABLE"=> "ImagesV0",
			       "EXPORT_COLUMN"=> "Equipment",
			       "IMPORT_COLUMN" =>"Equipment",
			       "COLUMNPREPEND"=>"Equipment",
			       "CONTAINER","NO"};

  $modules->{RadiologistOpinionOnImage}={"TABLE"=>"AnnotationsV0",
					 "CONTAINER", "NO",
					 "NO_IMPORT_COLUMN" => "WolfePattern"
					};
  return $modules;
}

sub filterColumns {
  my $self=shift;
  my $module=shift;
  my $metadata=shift;

  if (($module->{EXPORT_COLUMN})or($module->{NO_EXPORT_COLUMN})){
    my $pattern=$module->{EXPORT_COLUMN};
    my $no_pattern=$module->{NO_EXPORT_COLUMN};
    
    my @names= keys %{$metadata};
    $self->debug(1, "We only want some columns (had @names)");
    my $newArray={};
    foreach my $name (@names){
      my $nameOrig=$name;
      if ( ($pattern and ($name =~ /^$pattern.*$/)) or
	   ($no_pattern and ($name!~ /^$no_pattern.*$/))){
	$module->{COLUMNPREPEND} and 
	  $name=~ s/^$module->{COLUMNPREPEND}//;
	$newArray->{$name}=$metadata->{$nameOrig};
	#	$metadata[0].="$n###";
	#	$metadata[$#metadata].="$v###";
      }
    }
    $metadata=$newArray;
    @names= keys %{$metadata};
    $self->debug(1, "After we have (had @names)");
  }
  $self->debug(1, "Checking that the entries have values");
  foreach my $name2 (keys %{$metadata}) {
    my $name=$name2;
    $name =~ s/\(.*$//;
    my $value=$metadata->{$name2};
    $self->debug(1, "Checking $name and $value");
    (($name eq "file") or ($name eq "offset") or ($name eq "entryId"))
	    and ($value="");
    if (!($name and $value)) {
      $self->debug(1, "Removing $name2");
      delete $metadata->{$name2};
      #	  $name=~ /Date/ and $value=~ s/-//g;
#      $self->debug(1, "Putting $name and $value");
#      push @attrib, $xml->$name($value);
    }
  }

  $self->debug(1, "Checking that the entries have values done");
  return $metadata;

}
sub createPatientListXML {
  my $self=shift;
  my @files=@_;

  my @patients;
  my $patient={};
  foreach my $file (@files) {
    my $pat;
    $file=~ /^(.*patients\/pat[^\/]*)\// and $pat =$1;
    $pat or $self->{LOGGER}->warning("Interface", "Error finding the patient of $file") and next;
    $self->debug(1, "This is a study of $pat");
    my @list=();
    $patient->{$pat} and @list=@{$patient->{$pat}};
    push @list, $file;
    $patient->{$pat}=\@list;
  }

  foreach my $pat (keys %{$patient}) {
    $self->{LOGGER}->info("Interface", "Patient $pat has ".@{$patient->{$pat}});
    my @studies=@{$patient->{$pat}};

    my ($status, $fileXML)=$self->createPatientXML({},  @studies);
    $self->debug(1, "Creating the patient returned $status");
    ($status eq "OK") or return ($status, $fileXML);
    push @patients, $fileXML;
  }
  $self->{LOGGER}->info("Interface", "Patient list created!!");

  my $list=$self->{XML}->{GENERATOR}->PatientList(@patients);
  return ("OK",$list);
}

sub mergeEntries {
  my $self=shift;
  my $modulePath=shift;
  my %entries=@_;
  my @entries=keys %entries;
  $self->debug(1, "Before mergeEntries we have @entries");

  if ($modulePath) {
    my %temp;
    foreach my $key (@entries) {
      my $t=$key;
      $t=~ s/(${modulePath}[^\/]*)\/[^\/]*$/$1/;
      my @list=();
      $entries{$key} and push (@list, @{$entries{$key}});
      $self->debug(1, "Before merging we had @{$entries{$key}}");
      $temp{$t} and push (@list, @{$temp{$t}}) and
	$self->debug(1, "Merging 2 entries");
      $temp{$t}=\@list;

      $self->debug(1, "After merging we have @{$temp{$t}}");
    }
    %entries=%temp;
  }
  @entries=keys %entries;
  $self->debug(1, "After splitEntries we have @entries");

  return %entries;

}
sub getPatientMetadata{
  my $self=shift;
  my $file=shift;
  my $table=shift;
  my $allMetadata=shift;

  foreach my $entry (@$allMetadata) {
    $self->debug(1, "Looking for $table, and got $entry->{tagName}");
    if ($table eq $entry->{tagName}) {
      $self->debug(1, "Found the $table");
      my $data=$entry->{data};
      foreach my $second (@{$entry->{data}}) {
	$self->debug(1, "Comparing $file and $second->{file}");
	if ($file =~ /^$second->{file}/){
	  $self->debug(1, "Found the right file!!!");
#	  print Dumper ($entry->{data});
#	  print "And the other\n";
#	  print Dumper ($second);
	  my @list=($second);
#	  print Dumper (\@list);
	  return ($entry->{columns}, \@list);
	}
      }
      $self->{LOGGER}->info("Interface", "WARNING!!  I didn't find the metadata $table of $file");
      return ($entry->{columns}, $entry->{data});
    }
  }
  return undef;
##  my @list=$self->{UI}->execute("showTagValue","-silent", $file, $table);
 # return @list;
}

sub getAllMetadata {
  my $self=shift;
  my $patient=shift;
  $self->debug(1, "Getting all the metadata of $patient");

  my $directory=$patient;
  $directory =~ s/(patients\/pat[^\/]*)\/.*$/$1/;
  
  my $table="PatientsV0";
  my @patMetadata=$self->{UI}->execute("showTagValue", "-silent", $directory,$table);
  @patMetadata or $self->{LOGGER}->error("Interface", "Error getting the metadata of $directory") and return;

  my ($metadata)=$self->{UI}->execute("showAllTagValue", "-silent", $directory);
  $metadata or $self->{LOGGER}->error("Interface", "Error getting all the metadata of $directory") and return;
  my @list=@$metadata;
  push @list , {tagName=>$table, columns =>$patMetadata[0], 
			 data=>$patMetadata[1]};
  $metadata=\@list;

  $self->debug(1, "Got all the metadata of the patient!!");
#  use Data::Dumper;
#  print Dumper($metadata);

  return $metadata;
}
sub createPatientXML {
  my $self=shift;
  my $options=(shift or {});
  my @studies=@_;


  my $modules=$self->getModules();

#  my $path=$file;
  my %entries;
  map {$entries{$_}=()} @studies;
  my @object=();
  my $xml= $self->{XML}->{GENERATOR};
  $self->{LOGGER}->info("Interface", "Creating a new PatientXML");
  $self->debug(1, "(for @studies)");

  my $allMetadata=$self->getAllMetadata(@_);

  foreach my $moduleName ("XrayDoseModule","EquipmentModule",
			  "RadiologistOpinionOnImage","MammographyImageModule",
			  "ImageModule", "SeriesModule", "PatientStudyPathology", "PatientStudyModule",
			  "StudyModule", "PatientModule") {
    $self->debug(1, "Creating $moduleName");
    my $module=$modules->{$moduleName};

    %entries=$self->mergeEntries($module->{PATH}, %entries);

    foreach my $file (keys %entries) {
      $self->debug(1, "Working in $file");

#      my @metadata=$self->{UI}->execute("showTagValue","-silent", $file, $module->{TABLE});
      my @metadata=$self->getPatientMetadata( $file, $module->{TABLE}, $allMetadata);

      @metadata or return ("NOK","Error getting the  $module->{TABLE} of $file");
      if ($metadata[1]) {
	$self->debug(1, "Filtering the columns");
	
	my $newArray=$self->filterColumns($module, @{$metadata[1]});
	$self->debug(1, "There is at least one entry in $moduleName");
	my @attrib=();
	foreach my $name2 (keys %{$newArray}) {
	  my $name=$name2;
	  my $value=$newArray->{$name2};
	  $self->debug(1, "Putting $name and $value");
	  push @attrib, $xml->$name($value);
	}

	$self->debug(1, "Checking if we are an image");
	if ($moduleName eq "ImageModule") {
	  $self->debug(1, "Putting the lfn");
	  push @attrib, $xml->LFN("$file/image.dcm");
	  $options->{PFN} and push @attrib, $xml->PFN($options->{PFN});
	  
	}
	
	#      my $moduleName2=$moduleName;
	$self->debug(1, "Checking if we have a domain");
	if ($module->{DOMAIN}) {
	  $self->debug(1, "We have a domain");
	  #	$moduleName2=~ s/^.*\://;
	  @attrib=({'xmlns'=>"http://mammogrid.com/portal/patient/"}, @attrib);
	}
	$self->debug(1, "Checking if we are a container");
	if ($module->{CONTAINER} eq "NO") {
	  $self->debug(1, "This is not a container");
	  my @list=();
	  $entries{$file} and @list=@{$entries{$file}};
	  push @list, $xml->$moduleName( @attrib);
	  $entries{$file}=\@list;
	}elsif ($module->{CONTAINER} eq "ONE") {
	  $self->debug(1, "Containing only one object");
	  my @list=();
	  $entries{$file} and @list=@{$entries{$file}};
	  my $firstElement=pop @list;
	  push @list, $xml->$moduleName($firstElement, @attrib);
	  $entries{$file}=\@list;
	  
	}else {
	  $self->debug(1, "We are a container");
	  $entries{$file} and push @attrib, @{$entries{$file}};
	  my @list=($xml->$moduleName(@attrib));
	  $entries{$file}=\@list;
	}
	$self->debug(1, "Got @{$entries{$file}}");
      }
    }
  }
  my  @values= values %entries;
  
  $self->{LOGGER}->info("Interface", "Patient object created");
  $self->debug(1, "Returning @{$values[0]}");

  return ("OK", @{$values[0]});

}

# Method: packDICOM
#  Input: metadata of the patient
#  Output Document conforming to the PatientResponse description
#  Description: It asks another service to create a DICOM file, with the 
#               metadata that it gets
sub packDICOM {
  my $this=shift;
  my $xml=shift;
  $self->{LOGGER}->info("Interface", "Calling the packing service");
  $self->debug(1, "File\n$xml");

  my $result;
  eval {
    $result= SOAP::Lite->uri("axis/services/PackerService")
      ->proxy($self->{PACKER})
	->servicePack("$xml");
    print "Call done\n";
  };
  if ($@) {
    print "Error doing the soap call\n$@\n";
    return ("NOK", "Error calling pack");
  }
  $self->{SOAP}->checkSOAPreturn($result) or return ("NOK", "Error contacting the packing service");

  my $metaData=$result->result;
  print "The soap call worked!! $result\n$metaData";
  $self->{LOGGER}->info("Interface", "At the moment, we don't check the return value");
#  $metaData=$xml->PatientResponse($metaData,  "$s");
  my $tree=$self->{XML}->parse($metaData);

  if (! $tree) {
    $self->{LOGGER}->info("Interface", "Error parsing the response fron unpack\n $@");
    return ("NOK","The call to pack didnot return an xml");
  }
  my $status=$self->{XML}->getXMLFirstValue("Rc", @{$tree});

  if ($status ne "OK") {
    my $desc=$self->{XML}->getXMLFirstValue("Desc", @{$tree});
    $self->{LOGGER}->info("Interface", "Error the unpack did not return ok\nStatus $status and desc=$desc");
    $status or $status="NOK";
    return ($status, $desc);
  }

  my $pfn=$self->{XML}->getXMLFirstValue("PFN", @{$tree});

  $pfn or $self->{LOGGER}->info("Interface", "Error there is no PFN in the answer from unpack") and    return ("NOK", "there is no PFN in the answer from unpack");

  $self->{LOGGER}->info("Interface", "Returning $pfn");
  return ("OK", $pfn);
}
# Method: unpack
#  Input: UID of the file that has to be added
#  Output Document conforming to the PatientResponse description
#  Description: It asks another service to parse the DICOM file, and get all the metadata
sub unpackDICOM {
  my $this=shift;
  #  my $uid=shift;
  my $tree=shift;
  $self->{LOGGER}->info("Interface", "This method is supposed to call the other SOAP Server\nFor the time being, it returns an static response");

#  my $ASKING_TAMAS=1;
#  $ASKING_TAMAS and $self->{LOGGER}->info("Interface", "At least, we are asking the java soap server");
#  my $metaData;
#  if ($ASKING_TAMAS) {
#    my  $result= SOAP::Lite->uri("axis/services/PackerService")
##      ->proxy($self->{PACKER})
#	->serviceUnpack($uid);
#    print "Call done\n";##

#    $self->{SOAP}->checkSOAPreturn($result) or return ("NOK", "Error contacting the unpack service");
#    print "The soap call worked!! $result\n";
#   $metaData=$result->result;
#  }else {
#    open  (FILE, "</home/alienMaster/xml.txt") or $self->{LOGGER}->info("Interface", "Error reading the file") and return;
#    my @data=<FILE>;
#    close FILE;
#    $metaData=join ("", @data);
#  }
# print "TENGO \n$s\n";
#  my $metaData=$self->createResponse("OK", "Of course it works!!!", "o");
#  $metaData=$xml->PatientResponse($metaData,  "$s");


#  $self->{LOGGER}->info("Interface", "Got $metaData");
  #Now we check that the unpack finished successfully
#  my $tree=$self->{XML}->parse($metaData);

  if (! $tree) {
    $self->{LOGGER}->info("Interface", "Error parsing the response fron unpack\n $@");
    return ("NOK","It  doesn't work!! :(");
  }

  $self->{LOGGER}->info("Interface", "There is an rc. Checking the status");
  my $status=$self->{XML}->getXMLFirstValue("Rc", @{$tree});
  $self->{LOGGER}->info("Interface", "Got status $status");

  my $desc=$self->{XML}->getXMLFirstValue("Desc", @{$tree});
  $self->{LOGGER}->info("Interface", "Got desc $desc");
  my $pfn=$self->{XML}->getXMLFirstValue("PFN", @{$tree});
  $self->{LOGGER}->info("Interface", "Got pfn $pfn");
  
  $pfn or   $self->{LOGGER}->info("Interface", "There is no pfn in the answer from unpack") and  return ("NOK","There is no pfn in the answer from unpack");
#  return ("OK", "http://pcettmg01.cern.ch/inbox/$uid", $tree);
  return ($status, $pfn, $tree);

}
# Methods: populateDatabase
# Input: metadata (XML document conforming a patient element
# Output: (status, description)
# Description: It populates the AliEn catalogue with all the metadata
sub populateDatabase {
  my $self=shift;
  my $xmlTree=shift;
  my $pfn=shift;
  
  $xmlTree or 
    $self->{LOGGER}->info("Interface", 
			  "Not enough arguments in populateDatabase") 
      and return;
  my ($status, $desc)=$self->executeInDirectory("createDirectory", $xmlTree);
  ($status eq "OK") or return ($status, $desc);
  
  #Finally, we have to add the file
  $self->debug(1, "Registering ${desc}image.dcm $pfn");
  $status=$self->{UI}->execute("register", "${desc}image.dcm", "$pfn");
  #"file://pcepaip01.cern.ch/home/alienMaster/xml.dcm"); 
  
  if (!$status) {
    my $message="Error registering the file in the catalogue\n". $self->{LOGGER}->error_msg;
    $self->{LOGGER}->info("Interface", $message);
    return ("NOK", $message);
  }
  
  return ("OK", "Everything worked!!!");
}

sub executeInDirectory{
  my $self=shift;
  my $function=shift;
  my $xmlTree=shift;

  my $modules=$self->getModules();

  my $desc="";
  my $path="/mammogrid/$self->{SITE}/patients";
  foreach my $moduleName ("PatientModule", "StudyModule", "PatientStudyModule",
			  "PatientStudyPathology",
			  "SeriesModule", "ImageModule", "MammographyImageModule",
			  "EquipmentModule", 
			  "XrayDoseModule","RadiologistOpinionOnImage",) {
    $self->debug(1, "Executing in  $moduleName");  
    my $module=$modules->{$moduleName};
    #First, let's get the metadata 
    my ($status, @metaData)=$self->getMetadata($moduleName, $xmlTree);
    
    if ($status ne "OK") {
      $self->{LOGGER}->info("Interface", "Error getting the metadata of the $moduleName, $metaData[0]");
      return ($status, $metaData[0]);
    }
    if ($module->{IMPORT_COLUMN}){
      map {$_="$module->{IMPORT_COLUMN}$_"} @metaData;
    }
    if ($module->{NO_IMPORT_COLUMN}){
      @metaData=grep (! /^$module->{NO_IMPORT_COLUMN}=/, @metaData);
    }
    
    $self->debug(1, "Got the metadata");
    if ($module->{PATH} ) {
      my $id= join ("", grep (/$module->{ID}/i, @metaData));
      $self->debug(1,"The id is $id");
      if ($id) {
	$id=~ s/^.*\=//;
	$id=~ s/[\'\"]//g;
	$id=~ s/\s/_/g;
	#    $modules->{$module}->{PATH} and 
	$path="$path/".$module->{PATH}."$id";
	$self->debug(1, "Modifying the path to $path");
      }else {
	$self->{LOGGER}->info("Interface", "There is no id for $moduleName");
      }
    }
    
    $self->debug(1, "Calling the function in $path");
    
    ($status, $desc)=$self->$function($path, $desc,
				      $module->{TABLE},
				      @metaData);
    if ($status ne "OK") {
      $self->{LOGGER}->info("Interface", "Error creating the directory");
      return ($status, $desc);
    }
    $self->debug(1, "So far so good, and $desc");

    
  }
  $self->debug(1, "ExecuteInDirectory done and $desc");
  return ("OK", $desc);
}
# Method: getMetadata
# Input: XML of PatientResponse and element to get
# Output: status and list of key=value pairs
sub getMetadata {
  my $self=shift;
  my $element=shift;
  my $xml=shift;
  
  my @list=();
  ($xml and $element) or 
    $self->{LOGGER}->info ("Interface", "Not enough arguments in getMetadata") and return ("NOK", "Not enough arguments");
  
  $self->debug(1, "Getting the metadata of $element in $xml");
  
  my (@object)=$self->{XML}->getXMLElement($element, $xml);
  
  @object or $self->debug(1, "There are no $element in the xml") and return "OK";
  $self->debug(1, "Got some metadata");
  #    $self->{XML}->printList("\t", $object[0]);

  #Ok, now that we have the object, we have to get the attributes
  foreach my $obj ($object[0]){
    $self->debug(1, "Checking an object $obj");
    foreach my $att (@{$obj->{content}}) {
#      $self->debug(1, "Checking $att");
      my $name="";   
      my $content="";
      if  (UNIVERSAL::isa( $att, "HASH" ) ){
	$name=$att->{name};
	$name=~ s/^(.*\:)//;
	if (UNIVERSAL::isa($att->{content}, "ARRAY")) {
	  #		    print "We got an array!!\n";
	  my @list=@{$att->{content}};
	  if (UNIVERSAL::isa($list[0], "HASH")) {
	    #			print "We got an array!!\n";
	    $content=$list[0]->{content};
	    UNIVERSAL::isa($content, "ARRAY") and $content="";
	    $content=~ s/^\s*$//s;
	  }
	}
	($content eq "") and $content=undef;
	$content =~ /\s/ and $content="'$content'";
	
      }
      $self->debug(1, "Got $name and $content");
      if ($name and defined $content) {
	@list=(@list, "$name=$content");
      }
    }
  }
  $self->debug(1, "Returning @list");
  return ("OK", @list);

}
# Method: createResponse
#  Input: status and optional description
#           $options=> o->return the object (not the string)
#  Output Document conforming to the Response description
#  Description: It creates an xml document conforming the Response
sub createResponse {
  my $this=shift;
  my $status=shift;
  my $desc=(shift or "");
  my $options=(shift or "");
  my $date = strftime "%Y-%m-%eT%H:%M:%S", localtime;

  $self->{LOGGER}->info("Interface", "Returning $status and $desc");
  my $xml = $self->{XML}->{GENERATOR};

  my $a=$xml->Response(@domainAPI,$xml->Rc($status), 
		       $xml->Desc($desc),
		       $xml->Requestor(),
		       $xml->RqstDateTime($date));
  $options=~ /o/ and return $a;
  return "$a";

}

#Method: MetaDataDirectory
#Input:
#       directory:Name of the directory in the AliEn Catalogue
#       desc: description returned so far by the previous calls
#       tagName (optional): Name of th tag to associate to this directory
#       vars: list of key=value pairs specifying the metadata 
#Output:
#       status: OK, NOK
#       desc: description if something fails 
#
#Description: 
sub MetaDataDirectory {
  my $self=shift;
  my $directory=shift;
  my $desc=shift;
  my $tagName=(shift or "");
  my @vars=@_;

  $directory =~ s/\/?$/\//;
  $self->{LOGGER}->info("Interface","Querying the metadata $tagName");

#  my $exists=$self->{UI}->execute("ls", "-silent", $directory);

  my $error="";
#  ($exists) or $error= "The directory does not exist";
  $tagName  or $error="No tag specified";

  $error and $self->{LOGGER}->info("Interface", $error) 
      and return ("NOK", $error);

#  map {$_=~ s/\=[\'\"](.*)[\'\"]$/=\'$1\'/} @vars;
  map {$_=~ s/\=([^\'\"]*)$/=\'$1\'/} @vars;
  map {$_=~ s/\=[\"](.*)[\"]$/=\'$1\'/} @vars;
  map {$_="$tagName:$_"} @vars;
  $desc and @vars=($desc, @vars);
  $desc=join (" and ", @vars);

  $self->{LOGGER}->info("Interface", "Returning $desc");
  return ("OK", $desc);
}
#Method: queryDirectory
#Input:
#       directory:Name of the directory in the AliEn Catalogue
#       desc: description returned so far by the previous calls
#       tagName (optional): Name of th tag to associate to this directory
#       vars: list of key=value pairs specifying the metadata 
#Output:
#       status: OK, NOK
#       desc: description if something fails 
#
#Description: It constructs the 'where' statement for the query to the catalogue
sub queryDirectory {
  my $self=shift;
  my $directory=shift;
  my $desc=shift;
  my $tagName=(shift or "");
  my @vars=@_;

  $directory =~ s/\/?$/\//;
  $self->debug(1,"Quering the metadata $tagName");

#  my $exists=$self->{UI}->execute("ls", "-silent", $directory);

  my $error="";
#  ($exists) or $error= "The directory does not exist";
  $tagName  or $error="No tag specified";

  $error and $self->{LOGGER}->info("Interface", $error) 
      and return ("NOK", $error);

#  map {$_=~ s/\=[\'\"](.*)[\'\"]$/=\'$1\'/} @vars;
  map {$_=~ s/\=([^\'\"]*)$/=\'$1\'/} @vars;
  map {$_=~ s/\=[\"](.*)[\"]$/=\'$1\'/} @vars;
  map {$_="$tagName:$_"} @vars;
  $desc and @vars=($desc, @vars);
  $desc=join (" and ", @vars);

  $self->debug(1, "Returning $desc");
  return ("OK", $desc);
}
#Method: updateDirectory
#Input:
#       directory:Name of the directory in the AliEn Catalogue
#       desc: description returned so far by the previous calls
#       tagName (optional): Name of th tag to associate to this directory
#       vars: list of key=value pairs specifying the metadata 
#Output:
#       status: OK, NOK
#       desc: description if something fails 
#
#Description: It changes the metadata of the given directory
#             If the directory does not exists, it returns error
sub updateDirectory {
  my $self=shift;
  my $directory=shift;
  my $desc=shift;
  my $tagName=(shift or "");
  my @vars=@_;

  $directory =~ s/\/?$/\//;
  $self->{LOGGER}->info("Interface","Updating the directory $directory");

  my $exists=$self->{UI}->execute("ls", "-silent", $directory);

  my $error="";
  ($exists) or $error= "The directory does not exist";
  $tagName  or $error="No tag specified";

  $error and $self->{LOGGER}->info("Interface", $error) 
      and return ("NOK", $error);

#  my $father=$directory;
#  $father =~ s/\/[^\/]*\/$//;

  $self->{LOGGER}->info("Interface", "Preparing to do addTagValue $directory, $tagName, @vars");
  my @done=$self->{UI}->execute("addTagValue",$directory, $tagName, @vars);

  if (!@done) {
    $error="The update did not work\n". $self->{LOGGER}->error_msg;
    $self->{LOGGER}->info("Interface", $error);
    return ("NOK", $error);
  } 
  $self->{LOGGER}->info("Interface", "The update worked");
  return "OK";
}

#Method: createDirectory
#Input:
#       directory:Name of the directory in the AliEn Catalogue
#       desc: Description returned so far by the previous calls
#       tagName (optional): Name of th tag to associate to this directory
#       vars: list of key=value pairs specifying the metadata 
#Output:
#       status: OK, NOK
#       desc: description if something fails 
#
#Description: If the directory already exists, it doesn't do anything
#             If it doesn't, it creates it.
#             If a tagName is passed, it will create the tag in the upper directory
#             If the metadata is passed, it will insert it in the tag specified
sub createDirectory {

  my $this   = shift;
  my $directory =shift;
  my $desc      =shift;
  my $tagName= shift;
  my @vars=@_;

  $directory =~ s/\/?$/\//;

  $self->debug(1,"Checking if the directory exists");

  my $exists=$self->{UI}->execute("ls", $directory);

  $self->{LOGGER}->info("Interface","Creating a new directory $directory");
  my $done=$self->{UI}->execute("mkdir", "-p", $directory);
  $done or  $self->{LOGGER}->error("Interface","Error creating the directory $directory") and return ("NOK", $self->{LOGGER}->error_msg);

  if ($tagName) {
    my $father=$directory;
    $father =~ s/\/[^\/]*\/$//;
    $self->{LOGGER}->info("Interface","Adding the metadata $tagName to $father");    
    $done=$self->{UI}->execute("addTag","$father", "$tagName" );
    if (!$done) {
      $self->{LOGGER}->error("Interface","Error creating the metadata $tagName");
      $self->{UI}->execute("rmdir",$directory );
      return ("NOK", $self->{LOGGER}->error_msg);
    }
    ($exists) and return ("OK", $directory);

    $self->{LOGGER}->info("Interface","Got $directory $tagName, @vars");
    
    if (@_) {
      $self->{LOGGER}->info("Interface","Adding the metadata $tagName info @vars");
      #	$self->{UI}->execute("debug", 5);
      
      $done=$self->{UI}->execute("updateTagValue", $directory, $tagName, @vars);
      #	$self->{UI}->execute("debug", 0);
      
      if (!$done) {
	$self->{LOGGER}->error("Interface","Error inserting the  metadata $tagName");
	$self->{UI}->execute("rmdir",$directory );
	return ("NOK", $self->{LOGGER}->error_msg);
      }
    }
  }
  $self->{LOGGER}->info("Interface", "Create directory worked and $directory!!");
  return ("OK", $directory);
}
sub modifyComparison{
  my $self=shift;
  my $constraint=shift;
  my $desc2=shift;

  my $compText=$self->{XML}->getXMLFirstValue("Comparison", $constraint);
  if ($compText) {
    my $comp=$compText;
    $compText eq "EQUAL" and $comp="=";
    $compText eq "LIKE" and $comp="===";
    $compText eq "GREATER" and $comp=">";
    $compText eq "GEATER_OR_EQUAL" and $comp=">=";
    $compText eq "SMALLER" and $comp="<";
    $compText eq "SMALLER_OR_EQUAL" and $comp="<=";
    $compText eq "NOT_EQUAL" and $comp="!=";
    $self->{LOGGER}->info("Interface", "Got $desc2 and $comp");

    $desc2 =~ s/=/$comp/g;
  } 

  return $desc2;
}

#sub registerFile {
#    my $this        = shift;
#    my $host    = shift;
#    my $user    = shift;#

#    $self->{LOGGER}->info("Interface","Registering a new file");
#    $self->{LOGGER}->info("Interface","Got $host, $user and  @_");
#
#    return 1;

#}


return 1;

