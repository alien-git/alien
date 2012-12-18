package AliEn::SE::Methods::guid;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA $DEBUG );
@ISA = ("AliEn::SE::Methods::Basic");

$DEBUG=0;

use AliEn::UI::Catalogue::LCM;
use AliEn::MSS::file;
use AliEn::CE;
sub initialize {
  my $self = shift;

  $self->{CATALOGUE}=AliEn::UI::Catalogue::LCM->new() or
    $self->info("Error getting an instance of the catalogue in lfn") and
      return;

  return $self;

}

sub get {
  my $self = shift;
  $DEBUG and $self->debug(1, "Getting a file from a 'guid' pfn");

  my $guid=$self->{PARSED}->{PATH};
  $guid =~ s{^/}{};
  $self->info ("In virtual copying $guid to $self->{LOCALFILE}");

  my @options=$self->{LOCALFILE};
  ( $self->{PARSED}->{VARS_ZIP}) and @options=();

  #if we are getting a zip archive, put it under another name, so that
  #we insert the entry in the local cache
  my ($file)=$self->{CATALOGUE}->execute("get", "-silent","-g", $guid, @options)
    or print "Error getting $guid\n" and return;


  return $file;
}

sub getSize {
  my $self = shift;
  my $guid=$self->{PARSED}->{PATH};

  my $info=$self->{CATALOGUE}->{CATALOG}->getInfoFromGUID( $guid) or
    $self->info("Error getting the lfns of $guid") and return;
  $DEBUG and $self->debug(1,"The size of $guid is $info->{size}");
  return $info->{size};
}
sub getFTPCopy{
  my $self=shift;
  $DEBUG and $self->debug(1,"This is an ftp copy of a link...."); 

  my $file=$self->get() or print "Error copying the file\n" and return;
  $DEBUG and $self->debug(1, "YUHUUU!!!!! $file");
  return $file;
}
return 1;

