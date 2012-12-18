package AliEn::MSS::http;

use strict;
use AliEn::MSS;
use LWP;
use vars qw(@ISA);

@ISA = ( "AliEn::MSS" );

sub get {
  my $self = shift;
  my $file = shift;
  my $localfile= shift;


  my $pfn=$self->{ORIG_PFN};

  $self->debug(1, "In HTTP  getting $pfn");

  $self->getWithWGET($pfn, $localfile) or return 0;

  $self->debug(1, "Getting the file with LWP");
  my $ua = LWP::UserAgent->new;
  ($ua) or print STDERR "Error getting the user agent\n" and return 1;

  my $request =  HTTP::Request->new( 'GET', $pfn );
  
  ($request) or print STDERR "Error gettting the request\n" and return 1;
  
  my $response = $ua->request($request);    # or

  ($response->{_msg} eq "OK")
    or $self->{LOGGER}->info("HTTP", "Error getting $pfn: $response->{_msg}") 
      and return 1;

 $self->debug(1, "GOT ".$response->content);

  open( FILE, ">$localfile" )
    or print STDERR "Error opening local file $localfile\n"
      and return 1;
  print FILE $response->content;
  close FILE;
  
  $self->debug(1,"File saved as '$localfile'");
  
  return 0;
}


sub getWithWGET{
  my  $self = shift;
  my $file = shift;
  my $localfile= shift;

  $self->debug(1, "Checking if wget exitst");
  open (OUTPUT, "which wget >& /dev/null|");
  my $done=close(OUTPUT);

  $done or return 1;

  $self->{LOGGER}->info("HTTP", "wget exists . Getting the file  $file $localfile");

  open (OUTPUT, "wget $file -q -O $localfile |");
  $done=close(OUTPUT);
  
  $self->debug(1, "Got and $done ");
  $done and return 0;
  $self->debug(1, "Didn't get the file :(");
   return 1;
}

sub lslist {
  my $self=shift;
  my @fileInSE;
  return \@fileInSE;
}

sub sizeof {
  my $self = shift;
  my $file=shift;
  
  my $pfn=$self->{ORIG_PFN};
  
  $self->debug(1,"Getting the size of $self->{PATH}...");

  my $ua = LWP::UserAgent->new;
  ($ua) or print STDERR "Error getting the user agent\n" and return;

  my $request1 = HTTP::Request->new( HEAD => $pfn );
  my $response1    = $ua->request( $request1 );
  my $size1        = $response1->content_length;

  $size1 and return $size1;
  $self->debug(1, "The size wasn't in the head :(");
  
  my $request = HTTP::Request->new( 'GET', $pfn );
  
  ($request) or print STDERR "Error gettting the request\n" and return;
  
  my $response = $ua->request($request);
  
  return length $response->content;
}

sub url {
    my $self = shift;
    my $file = shift;
    my $pfn="http://$self->{HOST}";
    $self->{PORT} and $pfn.=":$self-{PORT}";
    $pfn.=$file;
    return $pfn;
}
sub mkdir {
  return 0;
}
return 1;

