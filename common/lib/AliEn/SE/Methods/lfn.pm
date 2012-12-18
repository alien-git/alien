package AliEn::SE::Methods::lfn;

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
  $DEBUG and $self->debug(1, "Getting a file from an 'lfn' pfn");

  my $lfn=$self->{PARSED}->{PATH};
  $self->info ("In virtual copying $lfn to $self->{LOCALFILE}");


  my ($file)=$self->{CATALOGUE}->execute("get",  $lfn)
    or print "Error getting $lfn\n$AliEn::Logger::ERROR_MSG\n" and return;

  my $zip=$self->{PARSED}->{VARS_ZIP};
  if ($zip) {
    $self->info("This is in fact a zip file. Extracting $zip");
    eval "require Archive::Zip"
      or $self->info("ERROR REQUIRING Archive::Zip $@") and return;
    my $zipFile = Archive::Zip->new( $file )
      or  $self->info("Error creating the zip archive $file") and return;
    $zipFile->extractMember($zip, $self->{LOCALFILE}) and
       $self->info("Error extracting $zip  from $file") and return;
  } else {
    AliEn::MSS::file::cp({},$file, , $self->{LOCALFILE})
	and $self->info("Error copying the file $file to $self->{LOCALFILE}")
	  and return;;
  }
  $self->info( "Now, I should update the entry in the database");
  return $self->{LOCALFILE};
}

sub getSize {
  my $self = shift;
  my $lfn=$self->{PARSED}->{PATH};

  my ($info)=$self->{CATALOGUE}->execute("ls", "-silent","-la", $lfn) or return;
  my ($perm, $user, $group, $size,$other)=split(/###/,$info);
  use Data::Dumper;
  print Dumper($info);
  my $zip=$self->{PARSED}->{VARS_ZIP};
  if ($zip) {
    $self->info("This is in fact a zip file. Extracting $zip");
    my $done=$self->get() or return;
    $self->info("Got the file $done");
    $size = -s $done;

  }
  print "RETURNING $size\n";;
  return $size;
}
return 1;

