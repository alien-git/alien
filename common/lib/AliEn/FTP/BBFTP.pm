package AliEn::FTP::BBFTP;

use strict;
use vars qw(@ISA);
use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

sub initialize {
  my $self   = shift;
  my $options = shift;

  $self->{DESTHOST} = $options->{HOST};
  return $self;
}

sub dirandfile {
    my $fullname = shift;
    $fullname =~ /(.*)\/(.*)/;
    my @retval = ( $1, $2 );
    return @retval;
}

sub put {
    my $self       = shift;
    my $localfile  = shift;
    my $remotefile = shift;
    my $options    = ( shift or "" );

    my $command = "$options; put $localfile $remotefile";
    return $self->transfer($command, @_);
}

sub get {
    my $self       = shift;
    my $localfile  = shift;
    my $remotefile = shift;
    my $options    = ( shift or "" );

    my $command = "$options; get $remotefile $localfile";
    return $self->transfer($command, @_);
}

sub transfer {
    my $self    = shift;
    my $command = shift;
    my $sourceCertificate =shift || "";

    #my ($file,$remotefile, $direction) = @_;
    #my ($dir,$filename) = dirandfile($file);
    #$rdir =~ s/\/?$//;

    $command =~ s/\/\//\//g;
    my $BBFTPcommand = "setoption createdir; setbuffersize 1024; " . $command;

	$BBFTPcommand =~ s/\/\//\//g;
    my @args = (
        "$ENV{ALIEN_ROOT}/bin/bbftp", "-e",
        "$BBFTPcommand",              "-p",
        "5",                          "-w",
        "10025",                      "-V",
        $self->{DESTHOST}
    );
    if ($sourceCertificate) {
      $sourceCertificate =~ s{^.*/CN=}{}g;
      push @args, "-g", "$sourceCertificate";

    }


    print "DOING @args\n";
    my $error = system(@args);
    $error = $error / 256;
    return ($error);
}
sub startListening {
  my $self=shift;
  my $s     = shift;
  system("env|grep X509");
  my $error = system("$ENV{ALIEN_ROOT}/bin/bbftpd -b -w 10025 -l DEBUG");

  if ( !($error) ) {
    $self->info("BBFTP started" );
  }
  else {
    my $err = $error / 256;
    $self->info(  "BBFTPD started with error  $err(Maybe it is already running?)" );
  }
  return 1;
}

sub getURL{
  my $self=shift;
  my $pfn=shift;
  $pfn=~ s{^file://[^/]*}{bbftp://$self->{DESTHOST}:10025};
  $self->info("In the BBFTP method, returning $pfn");
  return $pfn;
}

return 1;

