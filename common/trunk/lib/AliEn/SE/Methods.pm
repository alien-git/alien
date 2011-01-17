package AliEn::SE::Methods;

use strict;

use vars qw(@ISA $DEBUG);

use AliEn::Config;
use AliEn::Logger::LogObject;

$DEBUG=0;

push @ISA, 'AliEn::Logger::LogObject';


sub help {
  my $self=shift;
  my $pfn=($self->{PFN} or "");
  $self->info( "The syntax of the pfn ('$pfn') is not right,\n\tPossible pfns are:\n\tfile://pcegee02.cern.ch/tmp/foo\n\tsrm://lxb2024.cern.ch/data/myfile1");
  return ;
}
sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = ( shift or {} );
  ( UNIVERSAL::isa( $self, "HASH" ))
    or $self={"PFN", $self};
  bless( $self, $class );
  $self->SUPER::new() or return;
  $self->{PFN} or print STDERR "Error: no file specified\n" and return;

  $self->{DEBUG} or $self->{DEBUG} = 0;
  $self->{CONFIG} = new AliEn::Config();
  ( $self->{CONFIG} )
    or print STDERR "Error getting the configuration\n"
      and return;

  $self->{LOGGER} or $self->{LOGGER}= new AliEn::Logger;
  $self->{DEBUG} and $self->{LOGGER}->debugOn();

  $self->parsePFN();


  my @possibleMSS=("AliEn::MSS::\L$self->{PARSED}->{METHOD}\E", 
		   "AliEn::MSS::grid::\U$self->{PARSED}->{METHOD}\E");
  
  my $test;

  while ($test=shift @possibleMSS)      {
    $DEBUG and $self->debug(1, "TRYING $test");
    if (eval "require $test"){
      $self->{PARSED}->{NO_CREATE_DIR}=1;
      $self->{PARSED}->{DATABASE}=$self->{DATABASE};
      $self->{MSS}=$test->new({},$self->{PARSED});
      $self->{MSS} or 
	$self->{LOGGER}->error("Methods", "Error creating a new $test\n $@") 
	  and return $self->help();
      
      @possibleMSS=();
    }
    $DEBUG and $self->debug(5, "Got $! and @! and $@\n");
  }
  
  if (! $self->{MSS}) {	
    my $name = "AliEn::SE::Methods::$self->{PARSED}->{METHOD}";
    eval "require $name"
      or $self->debug(1, "Error requiring the package $name\n$@\nDoes the method $self->{PARSED}->{METHOD} exist?")
	and return $self->help();
    @ISA = ( $name, @ISA );
  }
  
  $DEBUG and $self->debug(1, "Checking the local file");
  my $name;
  $self->{PARSED}->{PATH} =~ /\/([^\/]*)$/ and $name = $1;
  chomp $name;
  $self->{LOCALFILE}
    or $self->{LOCALFILE} = "$self->{CONFIG}->{CACHE_DIR}/$name.$$".time;
  $self->{LOCALFILE} =~ s/\n//; 
  $DEBUG and $self->debug(1, "Initializing");
  $self->initialize() or return $self->help();
  
  my $tempdir = $self->{LOCALFILE};
  $tempdir =~ s/\/[^\/]*$//;
  
  $self->{DEBUG} and $self->debug(3,"Creating directory $tempdir");
  if ( !( -d $tempdir ) ) {
    my $dir = "";
    foreach ( split ( "/", $tempdir ) ) {
      $dir .= "/$_";
      mkdir $dir, 0777;
    }
  }

  return $self;
}


sub parsePFN {
  my $self = shift;

  $DEBUG and $self->debug(1, "Getting method of $self->{PFN}...");

  $self->{PARSED}={};
  $self->{PARSED}->{ORIG_PFN}=$self->{ORIG_PFN}=$self->{PFN};
  $self->{PFN} =~ s/^([^:]*):\/([^\/])/$1:\/\/\/$2/;
  $self->{PFN} =~ /^([^:]*):\/\/([^\/]*)(\/[^?]*)\??(.*)$/;
  $self->{PARSED}->{METHOD} = ( $1 or "" );
  $self->{PARSED}->{HOST}   = ( $2 or "" );
  $self->{PARSED}->{PATH}   = ( $3 or "" );
  $self->{PARSED}->{VARS}   = ( $4 or "" );

  $self->{PARSED}->{PORT} = "";

  #    ($self->{HOST}=~ s/($[^:]*):(.*)^/$1/) and ($self->{PORT}=$2);
  ( $self->{PARSED}->{HOST} =~ s/\:(.*)// ) and ( $self->{PARSED}->{PORT} = $1 );

  $DEBUG and $self->debug(1, "the list includes $self->{PARSED}->{VARS}");
  my @list=split ( /[=\?\&]/, $self->{PARSED}->{VARS} );

  while (@list){
    my ($key, $value)= (shift @list, shift @list);

    ($key and $value) or last;
    $key="\U$key\E";
    $DEBUG and $self->debug(2, "Putting variable VARS_$key as $value");
    $self->{PARSED}->{"VARS_$key"}=$value;
  }
  
  $self->{ORIG_PFN}=~ m{^root://.*\#(.*)$} and $self->{PARSED}->{VARS_ZIP}=$1;
  
  $self->{PARSED}->{METHOD} =~ /^file$/ and $self->{PARSED}->{PATH}=~ s{^(.*)\#(.*)$}{$1} and $self->{PARSED}->{VARS_ZIP}=$2;
  
  $DEBUG and $self->debug(1, "Parsed info: ".$self->method." ".$self->host." ".$self->port." ".$self->path." $self->{PARSED}->{VARS}");
  
 

}

sub method {
  my $self=shift;
  return $self->{PARSED}->{METHOD};
}
sub host{
    my $self=shift;
    return $self->{PARSED}->{HOST};
}
sub path{
    my $self=shift;
    return $self->{PARSED}->{PATH};
}
sub port{
    my $self=shift;
    return $self->{PARSED}->{PORT};
}
sub scheme {
    my $self=shift;
    return $self->{PARSED}->{METHOD};
}
sub string {
  my $self=shift;
  return  $self->{ORIG_PFN};
}


sub realGet {
  my $self=shift;
  my $tempFile=shift;
  
  if ($self->{MSS})   {
    $DEBUG and $self->debug(1, "Getting the file through MSS");
    $self->{MSS}->setEnvironment($self->{PARSED});
    if ($self->{MSS}->get( $self->path,  $self->{LOCALFILE})) {
      $self->{MSS}->unsetEnvironment();
      $self->{SILENT} or 
	$self->info("Error: not possible to copy file $self->{PARSED}->{PATH}!!");
      return;
    }
    $self->{MSS}->unsetEnvironment();

    $self->debug(1, "The call to MSS finished");
    ( -f $self->{LOCALFILE} )
      or $self->info("Error: file not copied!!")
	and return;
  }  else     {
    $DEBUG and $self->debug(1, "Getting the file through SE/Methods");
    $tempFile=$self->SUPER::get(@_) or return;
  }
  return 1;
  
}
sub get {
  my $self=shift;
  my $tempFile=$self->{LOCALFILE};
  
  my $zipGUID="";
  my $zipArchive="";

  if ($self->{ENVELOPE}) {
    $zipGUID=AliEn::Util::getValFromEnvelope($self->{ENVELOPE},'zguid');
    if ($zipGUID){
      $self->info("We are in fact extracting a file from the archive $zipArchive");
      $zipArchive="$self->{CONFIG}->{CACHE_DIR}/$zipGUID";  
    } 
  }
  
  if ($zipGUID and -f $zipArchive ) {
    $self->info("We already have the archive locally") 
  } else { 
    $self->realGet($tempFile) or return;
  } 
  
  my $zip=$self->{PARSED}->{VARS_ZIP};
  if ($zip and $zipArchive ) {
    $self->info("This is in fact a zip file. Extracting $zip from $zipArchive");
    (-f $zipArchive) or $self->info("Moving $tempFile to $zipArchive") and rename $tempFile, $zipArchive;

     eval "require Archive::Zip"
       or $self->info("ERROR REQUIRING Archive::Zip $@") and return;
     my $zipFile = Archive::Zip->new( $zipArchive )
       or  $self->info("Error opening  $zipArchive") and return;
     $zipFile->extractMember($zip, $self->{LOCALFILE}) and
      $self->info("Error extracting $zip  from $zipArchive") and return;
  }

  return $self->{LOCALFILE};
}

sub put {
  my $self=shift;
  if ($self->{MSS}) {
    $self->debug(1, "Putting the file through MSS");
    if ($self->{MSS}->put($self->{LOCALFILE}, $self->path)) {
      $self->{SILENT} or 
	$self->info("Error: not possible to copy file to $self->{PARSED}->{PATH}!! ($!)");
      return;
    }
    $self->debug(1, "The call to MSS finished");
    ( -f $self->{LOCALFILE} )
      or $self->info("Error: file not copied!!")
	and return;
    return $self->{LOCALFILE};
  }

  $self->debug(1, "Putting the file through SE/Methods");
  return $self->SUPER::put(@_);

}
sub getSize {
  my $self=shift;

  my $size;
  if ($self->{MSS})    {
    my $path=$self->path;
    $self->debug(1, "Getting the size of $path");
    $size=$self->{MSS}->sizeof( $path, $self->string());
  }else {
    $size=$self->SUPER::getSize(@_);
  }

  my $zip=$self->{PARSED}->{VARS_ZIP};
  if ($zip) {
    $self->info("This is in fact a zip file. Extracting $zip");
    my $done=$self->get() or return;
    $self->info("Got the file $done");
    $size = -s $done;

  }
  $self->debug(1, "Size is " . (defined $size ? $size : "undef") );

  return $size;
}

sub getStat {
  my $self=shift;

  return 1;
}



sub initialize{ 
  my $self=shift;
  
  $self->{MSS} and return 1;
   return $self->SUPER::initialize(@_);
}
sub getLink {
  my $self = shift;

  if ($self->{MSS})   {
    $self->info( "Getting link of $self->path, $self->{LOCALFILE}");
    my $error=$self->{MSS}->link($self->path, $self->{LOCALFILE});
    ($error eq 0 )
      and  $self->info( "Done!")
	and return $self->{LOCALFILE};
  }
  
  $self->debug(1,"There is no getLink for $self");
  return $self->get(@_);
    
}
sub remove {
  my $self=shift;

  if ($self->{MSS}){
    $self->debug(1, "Removing ".$self->path);
    return $self->{MSS}->rm($self->path);	
  } else {
    $self->debug(1, "Removing the file through SE/Methods");
    return $self->SUPER::remove(@_);
  }
}
sub getFTPCopy {
  my $self = shift;
  if ($self->{MSS})   {
    $self->info( "Getting FTPCopy");
    return $self->{MSS}->getFTPCopy($self->path,  $self->{PARSED}->{ORIG_PFN});
  
  }
  my $return;
  eval {
    $return=$self->SUPER::getFTPCopy(@_);
  };

  if (! $@){
   $self->info("The FTPCopy of the method worked!!");
    return $return;
  }

  $self->info( "Returning file and $self->{PARSED}->{ORIG_PFN}");
  return $self->{PARSED}->{ORIG_PFN};
}

sub stage {
  my $self=shift;
  $self->tryMethod("stage", @_);
}

sub isStaged {
  my $self=shift;
  $self->tryMethod("isStaged", @_);
}

sub tryMethod{
  my $self=shift;
  my $method=shift;

  if ($self->{MSS})   {
    $self->info( "Telling the MSS to $method the file");
    return $self->{MSS}->$method($self->path);
  }
  my $done;
  eval { 
    $self->info("Let's try also in the methods...");
    my $n="SUPER::$method";
    $done=$self->$n($self->{PARSED}->{ORIG_PFN});
  }; 
  if ($@){
    $self->info("NOPE: $@\n");
  }
  $done and return 1;

  $self->info("The file doesn't have to be staged");
  return 1;
}
return 1;
