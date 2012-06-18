package AliEn::MSS::rpc;

use strict;
use AliEn::MSS;
use AliEn::RPC;
use vars qw(@ISA);

@ISA = ( "AliEn::MSS" );

sub new {
    my $self = shift;
    $self= $self->SUPER::new(@_);


    $self->debug(1, "Creating a new MSS/RPC");
    $self->{RPC}=new AliEn::RPC;


    return $self;
}


sub get {
  my $self = shift;
  my $file = shift;
  my $localfile= shift;

#  print "TRYING TO GET A FILE $file from $self->{HOST}:$self->{PORT}\n";
  $self->debug(1,"Getting $file using RPC");
  
  if ($self->{HOST} eq $self->{CONFIG}->{HOST}) {
    $self->debug(1, "This is the same host");
    return AliEn::MSS::file->get($file, $localfile);
  }
  my $name="$self->{HOST}:$self->{PORT}";
  $self->{RPC}->Connect($name,"http://$self->{HOST}:$self->{PORT}") or return 1;

  my $response=$self->{RPC}->CallRPC($name,  "getFileSOAP",$file) or return 1;

  open( FILE, ">$localfile" )
    or print STDERR "Error opening the file $localfile\n"
      and return 1;
  syswrite( FILE, $response->result );
  close(FILE);
  undef $response;
  
  $self->debug(1,"File saves as '$localfile'");
  
  return 0;
}

sub lslist {
  my $self = shift;
  my $path = shift;
  my @fileInSE;
 
  if ($self->{HOST} eq $self->{CONFIG}->{HOST}){
    return AliEn::MSS::file->lslist($path);
  } else {
    return \@fileInSE;
  }
}

sub sizeof {
  my $self = shift;
  my $file=shift;
  
   $self->info("Getting the size of $self->{PATH}...Contacting $self->{URI} at $self->{HOST}:$self->{PORT}");

#  if ($self->{HOST} eq "$host"){
    $self->debug(1, "This is the same host");
    return AliEn::MSS::file->sizeof($file);
#  }
#  my $response =
#      SOAP::Lite->uri("AliEn/Service/$self->{URI}")
#      ->proxy("http://$self->{HOST}:$self->{PORT}")
#      ->checkFileSize("$file");
#
#    ($response) or return;

#    #    print STDERR ("Checking $arg has size ".$response->result."...\n");#

#     $self->debug(1, "Checking has size ". $response->result );
#
#  return $response->result;
}

sub url {
    my $self = shift;
    my $file = shift;

    return "soap://$self->{HOST}:$self->{PORT}$file?URI->$self->{VARS_URI}";
}

sub initialize {
  my $self=shift;
  $self->{FTP_LOCALCOPIES}=1;
  return 1;
}

return 1;

