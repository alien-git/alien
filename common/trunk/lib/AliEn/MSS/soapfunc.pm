package AliEn::MSS::soapfunc;

use strict;
use AliEn::MSS;
use SOAP::Lite on_fault => sub { return; };
use vars qw(@ISA);

@ISA = ( "AliEn::MSS" );

sub new {
    my $self = shift;
    $self= $self->SUPER::new(@_);


    $self->debug(1, "Creating a new MSS/soapfunc");

    $self->{URI}  = "";
    $self->{CALL} = "";
#    $self->{VARS} =~ /.+URI=(.*)?+.+/ and $self->{URI} = $1;
#    $self->{VARS} =~ /.+CALL=(.*)?+.+/ and $self->{CALL} = $1;
    $self->{VARS_URI} or print "Error URI not defined!\n" and return;
    $self->{VARS_CALL} or print "Error CALL not defined!\n" and return;
    return $self;
}


sub get {
  
  my $self = shift;
  my $file = shift;
  my $localfile= shift;
  
  $file =~ /\/(.*)/;
  $file = $1;

  $self->debug(1,"Getting from http://$self->{HOST}:$self->{PORT} $file as 'AliEn/Service/$self->{VARS_URI} calling $self->{VARS_CALL}'");

  my $response ;

  my $subroutine= $self->{VARS_CALL};
  if ($self->{VARS_ARGS}) {
    my @args = split ",",$self->{VARS_ARGS};
    $response =
      SOAP::Lite->uri("AliEn/Service/$self->{VARS_URI}")
	->proxy("http://$self->{HOST}:$self->{PORT}")
	  ->$subroutine(@args);
  } else {
    $response =
      SOAP::Lite->uri("AliEn/Service/$self->{VARS_URI}")
	->proxy("http://$self->{HOST}:$self->{PORT}")
	  ->$subroutine("$file");
  }
		       
   $self->debug(1,"In $self->{METHOD} getting ...");
  
  ($response)
    or print STDERR
      "Error contacting the $self->{VARS_URI} at $self->{HOST}:$self->{PORT}\n"
	and return 1;
  
  ( defined( $response = $response->result ) )
    or print STDERR "Something wrong??\n"
      and return 1 ;
  $self->debug(1, "Got the answer!!'");
  
  my $maxlength = 1024 * 10000;
  
  open( FILE, ">$localfile" )
    or print STDERR "Error opening the file $localfile\n"
      and return 1;
  
  syswrite( FILE, $response, $maxlength, 0 );
  close(FILE);
  undef $response;
  
  $self->debug(1,"File saves as '$localfile'");
  
  return 0;
}


sub sizeof {
  my $self = shift;
  my $file=shift;
  $self->debug(1,"Getting the size of $self->{PATH}...Contacting $self->{VARS_URI} at $self->{HOST}:$self->{PORT}");
  return 0;
}

sub url {
    my $self = shift;
    my $file = shift;

    return "soapfunc://$self->{HOST}:$self->{PORT}$file?URI->$self->{VARS_URI}";
}

return 1;

