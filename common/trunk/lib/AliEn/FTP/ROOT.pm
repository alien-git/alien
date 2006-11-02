package AliEn::FTP::ROOT;

use strict;
use vars qw(@ISA);
use AliEn::FTP;
@ISA = ( "AliEn::FTP" );
use AliEn::SE::Methods::root;
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
    print "Let's copy the file from $localfile to $remotefile\n";
    return ;AliEn::SE::Methods::root::put($self,$localfile, $remotefile);
#    my $command = "$options; put $localfile $remotefile";
#    return $self->transfer($command, @_);
}

sub get {
    my $self       = shift;
    my $localfile  = shift;
    my $remotefile = shift;
    my $options    = ( shift or "" );
my $certificate = ( shift or "" );
my $fromhost  = ( shift or "" );
my $tohost  = ( shift or "" );

print "we arrived here!!\n";
my $command = "xrdcp root://$fromhost/$remotefile $localfile -OD\\&authz=alien -OS\\&authz=alien -DIFirstConnectMaxCnt 1";
print $command."\n";
return system($command);
#    my $command = "$options; get $remotefile $localfile";
#    return $self->transfer($command, @_);
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
  $self->info("WE DON'T START THE XROOTD SERVER (hope that it started with the SE)");
  return 1;
}

sub getURL{
  my $self=shift;
  my $pfn=shift;
  $self->{CONFIG} or $self->{CONFIG}=AliEn::Config->new();
  my $io=$self->{CONFIG}->{SE_IODAEMONS} || "";
  $self->info("Starting with io $io");
  $io =~ s/^xrootd:// or $self->info("Error the iodaemon is not defined") and return;
  my %options=split (/[=:]/, $io);
  $options{port} or $self->info("Error getting the port where the xrootd is running") and return;
  $pfn=~ s{^((file)|(castor))://[^/]*}{root://$self->{DESTHOST}:$options{port}} or $self->info("The file $pfn can't be accessed through xrootd") and return;
  $self->info("In the XROOTD method, returning $pfn");
  return $pfn;
}

return 1;

