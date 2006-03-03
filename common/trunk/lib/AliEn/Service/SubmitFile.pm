package AliEn::Service::SubmitFile;

#select(STDERR); $|=1;
#select(STDOUT); $|=1;

use strict;
use AliEn::Service;
use Compress::Zlib;
use AliEn::SE::Methods;
use AliEn::X509;

my $self = {};

$SIG{INT} = \&catch_zap;

use vars qw(@ISA);

@ISA=qw(AliEn::Service);

sub initialize {
  $self=shift;
  my $options=(shift or {});

  (UNIVERSAL::isa($options, "HASH") )
    or print STDERR "ERROR: SubmitFile didn't get an array as arguments!!\n"
      and return;

  map {$self->{$_}=$options->{$_}} (keys %$options);
  defined $self->{USE_CERT} or $self->{USE_CERT}=1;

#  print "Creating a Submit File Daemon\n";
  $self->{STARTDAEMON}=0;

  $self->{X509}=AliEn::X509->new();
  
  $self->{HOST}=$self->{CONFIG}->{HOST};

  $self->{options} and $self->{options} =~ /n/ or 
    $self->checkPFN or return;
  $self->{SERVICE}="SubmitFile";
  $self->{SERVICENAME}="SubmitFile";
  if (! $self->{STARTDAEMON})    {
    $self->{PORT}="noport";
    return $self;
  }
  #First of all, let's make sure that the file exists...
  $self->debug(1, "Checking if $self->{file} exists");
  -f $self->{file}
    or $self->info( "The file $self->{file} does not exist in your local machine",10002) and return;

  $self->info( "Starting a service to transfer the file from the local machine");

  $self->{PORT}=$self->getPort();
  $self->{PORT} or return;
  $self->{pfn_soap} =~ s/<PORT>/$self->{PORT}/;
  $self->{pfn} =~ s/<PORT>/$self->{PORT}/;

  my $done=0;
  if ($self->{pfn} =~ /^((bb)|(grid))ftp/) {
    my $method="start\U$1FTP\EServer";
    $self->{GRIDMAP}=$self->{X509}->createGridmap($options->{SEcert});
    if ($self->{GRIDMAP}) {
      $done=$self->$method();
    }
  }
  ($done) or $done=$self->startSOAPServer();
  if (!$done) {
    unlink "$self->{CONFIG}->{TMP_DIR}/PORTS/lockFile.$self->{PORT}";
    return;
  }
  $self->debug(1, "Waiting for connections to get the file");
  return $self;
}


sub startSOAPServer{
  my $self=shift;
  
  $self->info( "Starting a soap server...");
  $self->{pfn}=$self->{pfn_soap};
  my $error = fork(); 	# STILL OLD FASHIONED FORK

  ( defined $error )
    or  $self->{LOGGER}->error("SubmitFile" , "Error forking the process\n") and return;
  
  
  #  waitpid( $error, &WNOHANG );
  
  if ($error){
    #This is the parent
    $self->debug(1 ,"Child is $error. I'm $$");
    $self->{CHILDPID} = $error;
    return 1;
  }
  my $daemon =  AliEn::Server::SOAP::Transport::HTTP->new({
							   LocalAddr => $self->{HOST},
							   LocalPort => $self->{PORT},
							   Prefork   => 1}
							 )->dispatch_and_handle("AliEn::Service::SubmitFile");
  print STDERR "Daemon stopped";
  exit();
}
sub startGRIDFTPServer{
  my $self=shift;
  my $port=(shift or $self->{PORT});

  my $oldMap=($ENV{GRIDMAP} or "");
  $ENV{GRIDMAP}=$self->{GRIDMAP};
  my $daemon="$ENV{GLOBUS_LOCATION}/sbin/in.ftpd";
  (-f $daemon) or
    $self->info("The file $daemon doesn't exist..") and return;

  my $error = system("$daemon -S -p$port");
  $ENV{GRIDMAP}=$oldMap;
  if ( $error) {
    $self->info( "Error starting gridftp");
    return;
  }
  open (FILE, "ps  -eo 'pid cmd' |") or $self->info( "Error trying to get the pid of gridftp") and return;
  my @lines=<FILE>;
  close FILE;
  @lines =grep (s/^\s*(\d+)\s*ftpd: accepting connections on port $self->{PORT}.*$/$1/s, @lines);
  
  $self->{CHILDPID}=shift @lines;
  $self->debug(1, "GRIDFTP STARTED WITH $error (pid $self->{CHILDPID})");
  return 1;
} 


sub startBBFTPServer{
  my $self=shift;
  my $port=(shift or $self->{PORT});


  $ENV{X509_CERT_DIR}="$ENV{ALIEN_ROOT}/etc/alien-certs/certificates";
  my $oldMap=($ENV{GRIDMAP} or "");
  $ENV{GRIDMAP}=$self->{GRIDMAP};
  my ($oldCert, $oldKey)=($ENV{X509_USER_CERT}, $ENV{X509_USER_KEY});

  ($ENV{X509_USER_CERT}, $ENV{X509_USER_KEY})=("","");

  my $error = system("$ENV{ALIEN_ROOT}/bin/bbftpd -w $port -b -lDEBUG");
  $ENV{GRIDMAP}=$oldMap;
  $oldCert and $ENV{X509_USER_CERT}=$oldCert;
  $oldKey and $ENV{X509_USER_KEY}=$oldKey;

  if ( $error) {
    $self->info( "Error starting bbftp");
    return;
  }
  open (FILE, "ps  -eo 'pid cmd' |") or $self->info( "Error trying to get the pid of gridftp") and return;
  my @lines=<FILE>;
  close FILE;

  @lines =grep (s/^\s*(\d+)\s*\S+bbftpd -w $port.*$/$1/s, @lines);
  
  $self->{CHILDPID}=shift @lines;
  $self->debug(1, "BBFTP STARTED WITH $error (pid $self->{CHILDPID})");
  return $self->{CHILDPID};
} 

sub setAlive{
  return;
}
sub stopTransferDaemon {
  my $u=shift;

  ( $self->{CHILDPID}) or return;
  $self->debug(1, "Trying to kill the Daemon $self->{CHILDPID} ( and I am $$)");

  $self->stopService($self->{CHILDPID});
  ($self->{GRIDMAP}) and (-f $self->{GRIDMAP}) and unlink $self->{GRIDMAP};
  -f "$self->{CONFIG}->{TMP_DIR}/PORTS/lockFile.$self->{PORT}" and 
    unlink "$self->{CONFIG}->{TMP_DIR}/PORTS/lockFile.$self->{PORT}";
  return 1;
}

sub checkPFN{
  my $d=shift;

  $self->{pfn} or
    print STDERR "Error no file specified for transfer in SubmitFile\n" and return;

  $self->debug(1, "Checking the pfn  $self->{pfn}");

  my $URL = new AliEn::SE::Methods( $self->{pfn});
  $URL or return;

  my $host=$URL->host;
  $self->{file}=$URL->path;
  my $method=$URL->scheme;

  $self->debug(1 ,"Before changing: $host, $self->{file} y $method");

  if ($method eq "file" ){
    if( ($host eq $self->{HOST}) or ("$host.$self->{CONFIG}->{DOMAIN}" eq $self->{HOST})){
      # In this case, we overwrite tbe method
      $self->{STARTDAEMON}=1;
      $self->debug(1 ,"Changing the pfn");

      my $subject=$self->{X509}->checkProxySubject();
      $self->{pfn_soap}="soap://$self->{HOST}:<PORT>$self->{file}?URI=SubmitFile";	
      if ($subject  &&  $self->{USE_CERT}) {
	$subject =~ s/\/CN=proxy//g;
	$subject =~ s/=/\/\//g;

	$self->{pfn}="gridftp://$self->{HOST}:<PORT>$self->{file}?SUBJECT=$subject";
#	$self->{pfn}="bbftp://$self->{HOST}:<PORT>$self->{file}?SUBJECT=$subject";
      } else {
	$self->{pfn}=$self->{pfn_soap};
      }
    }
  }
  $self->debug(1 ,"Now  $self->{pfn}");
  return 1;
}

sub getFileSOAP {

    #This just opens the file from whereever and returns it

    my $this = shift;
    my $file = shift;
    my $dir  = ( shift or undef );
    my $buffer;
    my $maxlength = 1024 * 1024 *10;
    $self->debug(1, "In getFileSOAP" );


    ( $file eq $self->{file})
      or $self->{LOGGER}->error( "SubmitFile",
        "Error: trying to get the wrong file: asking for $file and $self->{pfn}" )
      and return;

    if ( open( FILE, "$file" ) ) {
        my $aread = read( FILE, $buffer, $maxlength, 0 );
        close(FILE);
        ( $aread < $maxlength ) or
	  $self->info( "Error: trying to transfer a file bigger than $maxlength") and  return;
        $self->info("Transfering file $file" );
    }
    else {
        $self->{LOGGER}->warning( "SE", "$file does not exist" );
        return;
    }

    $buffer or return "";
    my $var = SOAP::Data->type( base64 => $buffer );

    return $var;

}
sub checkFileSize {
    my $this = shift;
    my $file = shift;

    $self->info("Getting the size of $file" );

    ( -f $file )
      or $self->{LOGGER}->warning( "ClusterMonitor", "$file does not exist" )
      and return;

   ( $file eq $self->{file})
      or $self->{LOGGER}->error( "SubmitFile",
        "Error: trying to get the wrong file: asking for $file and $self->{pfn}" )
      and return;


    my (
        $dev,  $ino,   $mode,  $nlink, $uid,     $gid,  $rdev,
        $size, $atime, $mtime, $ctime, $blksize, $blocks
      )
      = stat($file);
    $self->debug(1, "Size of $file is $size" );
    return $size;
}



return 1;

