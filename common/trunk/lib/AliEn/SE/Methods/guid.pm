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
  my $lfn=$self->_getLFN($guid) or return;

  my @options=$self->{LOCALFILE};
  ( $self->{PARSED}->{VARS_ZIP}) and @options=();

  #if we are getting a zip archive, put it under another name, so that
  #we insert the entry in the local cache
  my ($file)=$self->{CATALOGUE}->execute("get", "-silent",$lfn, @options)
    or print "Error getting $guid\n" and return;


  return $file;
}
sub _getLFN {
  my $self=shift;
  my $guid=shift;
  my $lfn;

  if ($self->{PARSED}->{DB}) {
    $self->info("looking only in db $self->{PARSED}->{DB}");
    my @options=( "-db$self->{PARSED}->{DB}", "-silent", $guid);
    $self->{PARSED}->{TABLE} and 
      push @options, "-table$self->{PARSED}->{TABLE}";
    $lfn=$self->{CATALOGUE}->execute("guid2lfn", @options) or
      $self->info("The guid is not in that database!!\n");
  }
  if (!$lfn) {
    my @lfn=$self->{CATALOGUE}->execute("guid2lfn", "-silent", $guid) or
      $self->info("Error getting the lfns of $guid") and return;
    $lfn=$lfn[0];
  }

  return $lfn;
}
sub getSize {
  my $self = shift;
  my $guid=$self->{PARSED}->{PATH};

  my @lfn=$self->{CATALOGUE}->execute("guid2lfn", $guid) or
    $self->info("Error getting the lfns of $guid") and return;

  my ($info)=$self->{CATALOGUE}->execute("ls", "-silent","-la", $lfn[0]) 
    or return;
  my ($perm, $user, $group, $size,$other)=split(/###/,$info);
  $DEBUG and $self->debug(1,"The size of $guid is $size");
  return $size;
}
return 1;

