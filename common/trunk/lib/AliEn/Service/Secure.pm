package AliEn::Service::Secure;

use AliEn::XML;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

#use SOAP::Transport::HTTP;
use AliEn::Service;
use SOAP::Lite;
use strict;
use AliEn::UI::Catalogue::LCM;
use AliEn::Service::Interface::Algorithm;
 use POSIX qw(strftime);
use vars qw (@ISA);

sub test {
  my $this = shift;
  print  "BRAVOOOO!!!\n";
  return ("CLARO");
}

@ISA=("AliEn::Service", "AliEn::Service::Interface::Algorithm");
use Classad;

my $self = {};
sub setAlive{
  my $s=shift;
  return 1;
}

sub initialize {
    $self     = shift;
    my $options =(shift or {});
    $options->{user}=~ s/\*\*/ /g;
    $self->{SSL_client_cert}=$options->{user};

    $options->{role}="admin";

    $self->debug(1, "Creatting a Secure object" );
    my $port=$options->{port};
    $self->{LOGGER}->info("Secure", "Port $port");
    $self->{PORT}=$options->{port};

    $self->{HOST}=$ENV{'ALIEN_HOSTNAME'}.".".$ENV{'ALIEN_DOMAIN'};
    chomp $self->{HOST};

    $self->{SERVICE}="Secure";
    $self->{SERVICENAME}="Secure";
    $self->{LISTEN}=10;
    $self->{PREFORK}=1;

    $self->{SECURE}=0;

    $self->debug(1, "Initializing Secure service" );

    $self->{XML}=new AliEn::XML;

    $self->{XML} or $self->{LOGGER}->info("Error getting the XML parser") 
	and return;

    $self->{UI} = AliEn::UI::Catalogue::LCM->new($options);

    ( $self->{UI} )
      or $self->{LOGGER}->error( "Broker", "Error creating userinterface" )
      and return;

    $self->{LOGGER}
      ->debug( "Broker", "User Interface created!!\n\tCreating database" );

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
  my $UID=(shift or "");
  $self->{LOGGER}->info("Interface", "Adding a new file");

  $UID or $self->{LOGGER}->info("Interface","Error: no UID received!!!")
    and return  $self->createResponse("NOK","no UID received!");

  #First, get the metadata from the file
  my ($status, $desc, $metaData)=$self->unpackDICOM($UID);
  $self->debug(1, "Got $status, $desc and $metaData");
  if ($status ne "OK"){
    $self->{LOGGER}->info("Interface", "The status after the unpack is not ok!!\n$desc\n");
    return $self->createResponse($status, $desc);;
  }
  $self->{LOGGER}->info("Interface", "The parsing worked...");
  
  #Now, we have to populate the database...
  ($status, $desc)=$self->populateDatabase($metaData, $desc);
  if ($status ne "OK"){
    $self->{LOGGER}->info("Interface", "The status after populating the database is not ok!!\n$desc\n");
  }
  return $self->createResponse($status, $desc);;

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
  ($status eq "OK") or  return  $self->createResponse($status, $tree);

  ($status, my $desc)=$self->executeInDirectory("queryDirectory", $tree);

  if ($status ne "OK") {
    $self->{LOGGER}->info("Interface", "The query returned an error\n$desc");
    return $self->createResponse($status, $desc);
  }
  #Ok, now we are supposed to do the query in the catalogue
  $self->{LOGGER}->info("Interface", "Ready to do find $desc");
  my @files=$self->{UI}->execute("find", "/", "dcm", split " ", $desc);
  $self->{LOGGER}->info("Interface", "Found @files");

  my @list=();
  foreach my $file (@files) {
    my ($status, $fileXML)=$self->createPatientXML($file);
    $status eq "OK" or $self->createResponse($status, $fileXML);
    push @list, $fileXML;
  }


  my $r=$self->{XML}->{GENERATOR}
    ->PatientListResponse($self->createResponse("OK", "It worked!!", "o"),
			  @list);
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

  my @LFN=$self->{XML}->getXMLElement("LFN",$tree);
  @LFN or   $self->{LOGGER}->info("Interface", "There were no LFN's in the XML") and return  $self->createResponse("NOK","There were no LFN's in the XML");
  my @files=();
#  print "GOT @LFN\n";
  foreach my $lfn (@LFN) {
#    $self->{XML}->printList("\t", $lfn);
    my @list=@{$lfn->{content}};
    push @files, $list[0]->{content};
  }
  $self->{LOGGER}->info("Interface", "Checking the files @files");

  my @list=();
  foreach my $file (@files) {
    ($status, my $fileXML)=$self->createPatientXML($file);
    $status eq "OK" or $self->createResponse($status, $fileXML);
    push @list, $fileXML;
  }


  my $r=$self->{XML}->{GENERATOR}
    ->PatientListResponse($self->createResponse("OK", "It worked!!", "o"),
			  @list);
  $self->{LOGGER}->info("Interface", "prepare done");
  return  "$r";

}
# Method retrieve
# Input: Document conforming the patientlist schema, with only one LFN (the rest are ignored) 
# Output: Response
# Description: It gets the file, from alien, gets the metadata, and calls the pack subroutine. 
sub retrieve {
  my $this=shift;

  my ($status, $tree)=$self->checkInput("retrieve", @_);
  ($status eq "OK") or  return  $self->createResponse($status, $tree);

  my @LFN=$self->{XML}->getXMLElement("LFN",$tree);
  @LFN or   $self->{LOGGER}->info("Interface", "There were no LFN's in the XML") and return  $self->createResponse("NOK","There were no LFN's in the XML");

  if ($#LFN>0) {
    $self->{LOGGER}->info("Interface", "You can only retrieve one file at a time");
    return ("NOK", "You can only retrieve one file at a time");
  }
  my $file="";
  my @list=$LFN[0]->{content};
  $list[0] and $file=$list[0]->{content};
  $file or  $self->{LOGGER}->info("Interface", "No LFN specified") and
    return ("NOK", "No LFN specified");

  $self->{LOGGER}->info("Interface", "Getting the file $file");

  my (@done)=$self->{UI}->execute("get", $file);
  @done or $self->{LOGGER}->info("Interface", "Error getting the file")
    and return ("NOK", "Error getting the file");
  $self->{LOGGER}->info("Interface", "Got @done");

  $self->{LOGGER}->info("Interface", "At this point we are supposed to do the packing");
  return $self->createResponse("OK", $done[0]);

}
sub authenticate {
  return $self->createResponse("NOK", "not implemented yet!!")
}

##############################################################################
#Private functions
##############################################################################
sub checkInput {
  my $self=shift;
  my $function=shift;

  my $xml=shift;
  $xml or $self->{LOGGER}->info("Interface", "Not enough arguments in $function. Missing xml") and return  ("NOK","Not enough arguments");

  $self->{LOGGER}->info("Interface", "\n\t\tStarting a $function");

  my $tree=$self->{XML}->parse($xml);
  $tree or  $self->{LOGGER}->info("Interface", "Error parsing the input $xml") and return  ("NOK","Error parsing the input");

  return ("OK", $tree);
}

sub createPatientXML {
  my $self=shift;
  my $file=shift;

  my $modules={};
  $modules->{PatientModule}={ "ID", "PatientId", 
			      "PATH", "pat",
			      "TABLE", "PatientsV0"};

  $modules->{StudyModule}={"ID"=> "StudyId",
			   "PATH" => "std",
			   "TABLE"=> "StudiesV0"};

  $modules->{SeriesModule}={"ID"=> "SeriesInstanceUID",
			    "PATH" => "series",
			    "TABLE"=> "SeriesV0"};

  $modules->{ImageModule}={"ID"=> "SopInstanceUID",
			   "PATH" => "img",
			   "TABLE"=> "ImagesV0",
			   "NO_COLUMN"=>"(Equipment.*)|(ImageLaterality)|(OrganExposed)|(Exposure.*)"};

  $modules->{MammographyImageModule}={#"ID"=> "SopInstanceUID",
				      #			   "PATH" => "img",
				      "TABLE"=> "ImagesV0",
				      "COLUMN", "(ImageLaterality)|(OrganExposed)",
				      "NO_COLUMN"=>".*"};

  $modules->{XrayDoseModule}={"TABLE"=> "ImagesV0",
			      "COLUMN"=> "Exposure",
			      "NO_COLUMN"=> ".*",
			      "COLUMN" =>"Exposure.*"};

  $modules->{EquipmentModule}={"TABLE"=> "ImagesV0",
			       "COLUMN"=> "Equipment",
			       "COLUMNPREPEND"=>"Equipment",
			       "CONTAINER","NO"};

  $modules->{RadiologistOpinionOnImage}={"TABLE"=>"AnnotationsV0",
					 "CONTAINER", "NO"
					};

  my $path=$file;
  my @object=();
  my $xml= $self->{XML}->{GENERATOR};

  foreach my $module ("XrayDoseModule","EquipmentModule",
		      "RadiologistOpinionOnImage","MammographyImageModule", 
		      "ImageModule", "SeriesModule", 
		      "StudyModule", "PatientModule") {
    $self->{LOGGER}->info("Interface", "Creating $module");
    my @metadata=$self->{UI}->execute("showTagValue","-silent", $file, $modules->{$module}->{TABLE});
    @metadata or return ("NOK","Error getting the  $modules->{$module}->{TABLE} of $file");
    if ($#metadata>0) {
      if (($modules->{$module}->{COLUMN})or($modules->{$module}->{NO_COLUMN})){
	my $pattern=$modules->{$module}->{COLUMN};
	my $no_pattern=$modules->{$module}->{NO_COLUMN};
	$self->{LOGGER}->info("Interface", "We only want some columns");
	$self->debug(1, "BEFORE  $metadata[0]\n $metadata[$#metadata]");
	my @names= split ("###", $metadata[0]);
	my @values=split ("###", $metadata[$#metadata]);
	$metadata[0]="";
	$metadata[$#metadata]="";
	while (@names){
	  my $n=shift @names;
	  my $v=shift @values;
	  if ( ($pattern and ($n =~ /^$pattern.*$/)) or
	       ($no_pattern and ($n!~ /^$no_pattern.*$/))){
	    $modules->{EquipmentModule}->{COLUMNPREPEND} and 
	      $n=~ s/^$modules->{EquipmentModule}->{COLUMNPREPEND}//;
	    $metadata[0].="$n###";
	    $metadata[$#metadata].="$v###";
	  }
	}
	$self->debug(1, "AFTER $metadata[0]\n $metadata[$#metadata]");
      }

      $self->debug(1, "There is at least one entry in $module");
      my @names=split ("###", $metadata[0]);
      my @values=split ("###", $metadata[$#metadata]);
      my @attrib=();
      while (@names) {
	my $name=shift @names;
	$name =~ s/\(.*$//;
	my $value=shift @values;
	(($name eq "file") or ($name eq "offset") or ($name eq "entryId"))
	  and ($value="");
	($name and $value) and
	  push @attrib, $xml->$name($value);
      }
      $module eq "ImageModule" and push @attrib, $xml->LFN($file);
      if ($modules->{$module}->{CONTAINER} eq "NO") {
	push @object,$xml->$module(@attrib);
      }else {
	@object and push @attrib, @object;
	@object=$xml->$module(@attrib);
      }
      $self->debug(1, "Got @object");
    }
  }
  $self->{LOGGER}->info("Interface", "Patient object created for $file");
#  my $fileTree=$self->{XML}->parse($xml);

  #  if ($status ne "OK") {
  #  $self->{LOGGER}->info("Interface", "The query returned an error\n$desc");
  return ("OK", $object[0]);
  #  }
}

# Method: unpack
#  Input: UID of the file that has to be added
#  Output Document conforming to the PatientResponse description
#  Description: It asks another service to parse the DICOM file, and get all the metadata
sub unpackDICOM {
  my $this=shift;
  my $uid=shift;
  $self->{LOGGER}->info("Interface", "This method is supposed to call the other SOAP Server\nFor the time being, it returns an static response");

  my $ASKING_TAMAS=1;
  $ASKING_TAMAS and $self->{LOGGER}->info("Interface", "At least, we are asking the java soap server");
  my $metaData;
  if ($ASKING_TAMAS) {
    my  $result= SOAP::Lite->uri("axis/services/PackerService")
      ->proxy("http://pcettmg01.cern.ch:8090/axis/services/PackerService")
	->serviceUnpack($uid);
    print "Call done\n";

    $self->{SOAP}->checkSOAPreturn($result) or return ("NOK", "Error contacting the unpack service");
    print "The soap call worked!! $result\n";
   $metaData=$result->result;
  }else {
    open  (FILE, "</home/alienMaster/xml.txt") or $self->{LOGGER}->info("Interface", "Error reading the file") and return;
    my @data=<FILE>;
    close FILE;
    $metaData=join ("", @data);
  }
# print "TENGO \n$s\n";
#  my $metaData=$self->createResponse("OK", "Of course it works!!!", "o");
#  $metaData=$xml->PatientResponse($metaData,  "$s");


  $self->{LOGGER}->info("Interface", "Got $metaData");
  #Now we check that the unpack finished successfully
  my $tree=$self->{XML}->parse($metaData);

  if (! $tree) {
    $self->{LOGGER}->info("Interface", "Error parsing the response fron unpack\n $@");
    return $self->createResponse("NOK","It  doesn't work!! :(");
  }
  my @response=$self->{XML}->getXMLElement("Rc", @{$tree});

  @response or $self->{LOGGER}->info("Interface", "There is no response in the answer...") and return ("NOK", "No response in the answer from unpack");
  $self->{LOGGER}->info("Interface", "There is an rc. Checking the status");
  my $status="";
  ($response[0]) and ($response[0]->{content}) and 
    (@{$response[0]->{content}}[0]) and 
    $status=@{$response[0]->{content}}[0]->{content};

  $self->{LOGGER}->info("Interface", "Got status $status");
  
  @response=$self->{XML}->getXMLElement("Desc", @{$tree}); 
  my $desc="";

  ($response[0]) and ($response[0]->{content}) and 
    (@{$response[0]->{content}}[0]) and 
    $desc=@{$response[0]->{content}}[0]->{content};

  $self->{LOGGER}->info("Interface", "Got desc $desc");
  return ("OK", "http://pcettmg01.cern.ch/inbox/$uid", $tree);
  return ($status, $desc, $tree);

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
    $status=$self->{UI}->execute("add", "${desc}image.dcm", "$pfn");
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

  my $modules={};
  $modules->{PatientModule}={ "ID", "PatientId",
			      "PATH", "pat",
			      "TABLE", "PatientsV0"};

  $modules->{StudyModule}={"ID"=> "StudyId",
			   "PATH" => "std",
			   "TABLE"=> "StudiesV0"};

  $modules->{PatientStudyModule}={"TABLE"=>"StudiesV0",
				 };

  $modules->{SeriesModule}={"ID"=> "SeriesInstanceUID",
			    "PATH" => "series",
			    "TABLE"=> "SeriesV0"};

  $modules->{ImageModule}={"ID"=> "SopInstanceUID",
			   "PATH" => "img",
			   "TABLE"=> "ImagesV0"};

  $modules->{MammographyImageModule}={#"ID"=> "SopInstanceUID",
#			   "PATH" => "img",
			   "TABLE"=> "ImagesV0"};

  $modules->{EquipmentModule}={"TABLE"=>"ImagesV0",
			       "COLUMN"=>"Equipment"};

  $modules->{XrayDoseModule}={"TABLE"=>"ImagesV0",
			      "NO_COLUMN" =>"BodyPartThickness"
			      };
  $modules->{RadiologistOpinionOnImage}={"TABLE"=>"AnnotationsV0",
					 "NO_COLUMN" => "WolfePattern"
					 };
  my $desc="";
  my $path="/mammogrid/cern/patients";
  foreach my $module ("PatientModule", "StudyModule", "PatientStudyModule",
		      "SeriesModule", "ImageModule", "MammographyImageModule",
		      "EquipmentModule", 
		      "XrayDoseModule","RadiologistOpinionOnImage",) {
    $self->{LOGGER}->info("Interface", "Creating $module");  
    
    #First, let's get the metadata 
    my ($status, @metaData)=$self->getMetadata($module, $xmlTree);
    
    if ($status ne "OK") {
      $self->{LOGGER}->info("Interface", "Error getting the metadata of the $module, $metaData[0]");
      return ($status, $metaData[0]);
    }
    if ($modules->{$module}->{COLUMN}){
      map {$_="$modules->{$module}->{COLUMN}$_"} @metaData;
    }
    if ($modules->{$module}->{NO_COLUMN}){
      @metaData=grep (! /^$modules->{$module}->{NO_COLUMN}=/, @metaData);
    }

    $self->debug(1, "Got the metadata");
    if ($modules->{$module}->{PATH} ) {
      my $id= join ("", grep (/$modules->{$module}->{ID}/i, @metaData));
      $id=~ s/^.*\=//;
      $id=~ s/[\'\"]//g;
      $id=~ s/\s/_/g;
      #    $modules->{$module}->{PATH} and 
      $path="$path/".$modules->{$module}->{PATH}."$id";
    }

    $self->debug(1, "Calling the function in $path");

    ($status, $desc)=$self->$function($path, $desc,
				      $modules->{$module}->{TABLE},
				      @metaData);
    if ($status ne "OK") {
      $self->{LOGGER}->info("Interface", "Error creating the directory");
      return ($status, $desc);
    }

  }
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
  
  @object or $self->{LOGGER}->info ("Interface", "There are no $element in the xml") and return "OK";
  $self->debug(1, "Got @object");
  #    $self->{XML}->printList("\t", $object[0]);

  #Ok, now that we have the object, we have to get the attributes
  foreach my $obj ($object[0]){
    $self->debug(1, "Checking an object $obj");
    foreach my $att (@{$obj->{content}}) {
      $self->debug(1, "Checking $att");
      my $name="";   
      my $content="";
      if  (UNIVERSAL::isa( $att, "HASH" ) ){
	$name.=$att->{name};
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
      ($name and defined $content) and @list=(@list, "$name=$content");
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
  my $date =localtime;
  $self->{LOGGER}->info("Interface", "Returning $status and $desc");
  my $xml = $self->{XML}->{GENERATOR};

  my $a=$xml->Response($xml->Rc($status), 
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
  $self->{LOGGER}->info("Interface","Quering the metadata $tagName");

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

  $self->{LOGGER}->info("Interface","Checking if the directory exists");

  my $exists=$self->{UI}->execute("ls", $directory);

  $self->{LOGGER}->info("Interface","Creating a new directory $directory");
  my $done=$self->{UI}->execute("mkdir", "-p", $directory);
  $done or  $self->{LOGGER}->error("Interface","Error creating the directory $directory") and return ("NOK", $self->{LOGGER}->error_msg);

  if ($tagName) {
    my $father=$directory;
    $father =~ s/\/[^\/]*\/$//;
    $self->{LOGGER}->info("Interface","Adding the metadata to $father");    
    $done=$self->{UI}->execute("addTag","$father", "$tagName" );
    if (!$done) {
      $self->{LOGGER}->error("Interface","Error creating the metadata $tagName");
      $self->{UI}->execute("rmdir",$directory );
      return ("NOK", $self->{LOGGER}->error_msg);
    }
    ($exists) and return "OK";

    $self->{LOGGER}->info("Interface","Got $directory $tagName, @vars");
    
    if (@_) {
      $self->{LOGGER}->info("Interface","Adding the metadata info @vars");
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
  $self->{LOGGER}->info("Interface", "Everything worked!!");
  return ("OK", $directory);
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

