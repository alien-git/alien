package AliEn::MSS::db;

use strict;
use AliEn::MSS;

use vars qw(@ISA);

@ISA = ( "AliEn::MSS" );
use AliEn::Database;

sub new {
    my $self = shift;
    $self= $self->SUPER::new(@_);


    $self->debug(1, "Creating a new MSS/DB");
    $self->{PROXY_HOST} = $self->{CONFIG}->{'PROXY_HOST'};
    $self->{PROXY_PORT} = $self->{CONFIG}->{'PROXY_PORT'};

    return $self;
}


sub get {
  
  my $self = shift;
  my $file = shift;
  my $localfile= shift;
  $self->debug(1, "Getting $file");
  
  my $dbh=$self->Connect($file);
  $dbh or return 1;

  $self->debug(1, "Saving the file as $localfile");
  my ( $searchcolumn, $index ) = split ( '=', $self->{VARS} );

  my ( $nothing, $driver, $database, $table, $column ) =
    split ( "\/", $self->{PATH} );

  my $value = $dbh->queryValue("SELECT $column from $table where $searchcolumn='$index'");

  open( LOCALFILE, ">$localfile" )
      or print STDERR "Error opening the file $localfile"
      and return 1;
    print LOCALFILE $value;
    close(LOCALFILE);
  
  $self->debug(1, "Method db done!!");

  $dbh->destroy;
  undef $dbh;

  
  return 0;
}

sub Connect{
  my $self=shift;
  my $file=shift;

  $self->debug(1, "In DB with  $file");
  
  my ($nothing,  $driver, $database, $table, $column ) =
    split ( "\/", $file );

   $self->debug(1, "Connecting to $database, $self->{HOST}:$self->{PORT} as $self->{DATABASE}->{USER}");
  
  
  my $dbh = AliEn::Database->new(
        {
            "DB", $database,
            "HOST"   => "$self->{HOST}:$self->{PORT}",
            "DRIVER" => $driver,
            "SILENT" => "1",
            "USER"   => $self->{DATABASE}->{USER},
            "ROLE"   => $self->{DATABASE}->{ROLE},
            "TOKEN"  => $self->{DATABASE}->{TOKEN}
        }
				);
  
  ($dbh)
    or print STDERR "Error: not possible to open the connection\n"
      and return;
  #$dbh->validate or return;
  return $dbh;
}

sub lslist {
  my $self=shift;
  my @fileInSE;
  return \@fileInSE;
}

sub sizeof {
  my $self = shift;
  my $file=shift;
  
  my ( $searchcolumn, $index ) = split ( '=', $self->{VARS} );
  my $dbh=$self->Connect($file);

  my ( $nothing, $driver, $database, $table, $column ) =
    split ( "\/", $self->{PATH} );

  $dbh or return;
  my $SQL = "SELECT LENGTH($column) from $table where $searchcolumn='$index'";
  $self->debug(1, "Executing $SQL");
  
  my $size = $dbh->queryValue($SQL);
  $dbh->destroy;
  
  return $size;
}

sub url {
  my $self = shift;
  my $file = shift;
  
  return "db://$self->{HOST}:$self->{PORT}$file?$self->{VARS}";
}

return 1;

