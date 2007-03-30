package AliEn::Service::SE;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use Socket;
use Carp;
use AliEn::Service;
use AliEn::Database::SE;

use vars qw(@ISA $DEBUG);

@ISA=qw(AliEn::Service);
$DEBUG=0;
use strict;

use AliEn::SE::Methods;

use AliEn::MSS::file;
use AliEn::LVM;
use AliEn::X509;
use AliEn::Service::SubmitFile;
use POSIX ":sys_wait_h";
use AliEn::GUID;
use AliEn::Util;

my $DAEMONS={xrootd=>{startup=>"startXROOTD"},
	     srm=>{startup=>"startSRM"},
	     fdt=>{startup=>"startFDT"}
	    };
	    
# Use this a global reference.

my $self = {};

sub initialize {
  $self = shift;
  my $options =(shift or {});

  $self->debug(1, "Creatting a SE" );

  $self->{PORT}=$self->{CONFIG}->{'SE_PORT'};
  $self->{HOST}=$self->{CONFIG}->{'SE_HOST'};
  $self->{SERVICE}="SE";
  $self->{SERVICENAME}=$self->{CONFIG}->{'SE_FULLNAME'};
  $self->{LISTEN}=1;
  $self->{PREFORK}=10;
#  $self->{IOttl}     = 3600;
  $self->{CONFIG}->{SE}
    or $self->{LOGGER}->warning( "SE", "Error: no SE to manage" )
      and return;
  $self->{X509}=new AliEn::X509;
  my $name = $self->{CONFIG}->{SE_MSS};
  $name
    or $self->{LOGGER}->warning( "SE", "Error: no mass storage system" )
      and return;
  $name = "AliEn::MSS::\L$name\E";
  eval "require $name"
    or $self->{LOGGER}->warning( "SE", "Error: $name does not exist $! and $@" )
      and return;
  $self->{FORKCHECKPROCESS}=1;

  $self->{MSS} = $name->new($self);
  $self->{MSS}
    or $self->{LOGGER}->warning( "SE", "Error: getting an instance of $name" )
      and return;
  foreach my $subSE (grep (s/^SE_(VIRTUAL_)/$1/ , keys %{$self->{CONFIG}})) {
    $self->info("Configuring the virtual SE $subSE");
    my $name = ($self->{CONFIG}->{"SE_$subSE"}->{MSS} || $self->{CONFIG}->{SE_MSS});
    $name or $self->{LOGGER}->warning( "SE", "Error: no mass storage system" )
	and return;
    $name = "AliEn::MSS::\L$name\E";
    eval "require $name"
      or $self->{LOGGER}->warning( "SE", "Error: $name does not exist $! and $@" )
      and return;
    $self->{$subSE}={};
    $self->{$subSE}->{MSS}=$name->new({VIRTUAL=>$subSE}) or return;
    $self->{$subSE}->{LVM}=$self->ConfigureLVM($subSE,$subSE)
      or return;
  }


  $self->info( "Managing SE $self->{CONFIG}->{SE}" );
  $self->{CACHE_DIR} = "$self->{CONFIG}->{CACHE_DIR}/FTD";
  if ( !( -d $self->{CACHE_DIR} ) ) {
    mkdir $self->{CACHE_DIR}, 0777;
  }
  
#  $self->{IS} = SOAP::Lite->uri("AliEn/Service/IS")
#    ->proxy("http://$self->{CONFIG}->{IS_HOST}:$self->{CONFIG}->{IS_PORT}");

 # $self->{IS}
 #   or $self->{LOGGER}->warning( "SE",
#				 "IS at $self->{CONFIG}->{IS_HOST}:$self->{CONFIG}->{IS_PORT} is not up"
#      )
#      and return;
  $self->{SOAP}->checkService("IS") or
    $self->info( "Error contacting the IS");

  $self->{SOAP}->{MANAGER} = SOAP::Lite->uri("AliEn/Service/Manager/Transfer")
    ->proxy("http://$self->{CONFIG}->{TRANSFER_MANAGER_ADDRESS}");


  $self->debug(1,
"Contacting Transfer Manager at $self->{CONFIG}->{TRANSFER_MANAGER_ADDRESS}");

  $self->{DATABASE}=AliEn::Database::SE->new() or return;
#   configure options
  $self->{LVM}=$self->ConfigureLVM($name) or return;
  $self->{CERTIFICATE}=$self->{CONFIG}->{SE_CERTSUBJECT};

  $self->startIOServers() or return;
  $ENV{'IO_AUTHZ'}="alien";
  $self->{GUID}=AliEn::GUID->new();
  my $QoS=$self->{CONFIG}->{SE_QOS};
  if (!$QoS){
    $self->info("Warning!! The retention policy for this SE is not defined. Assuming it is 'replica'");
    $QoS='replica';
  } 
  $self->{PROTOCOLS}=$QoS;

  
  return $self;
}


sub startIOServers{
  my $self=shift;

  $self->info("Let's start the io servers");
  my $ioDef={};

  if ($self->{CONFIG}->{SE_IODAEMONS}) {
    $ioDef->{default}={name=>$self->{CONFIG}->{SE_IODAEMONS_LIST},
		       mss=>$self->{MSS}};
  }else{
    $self->info("Warning! there are no io_daemos defined for this SE");
  }

  foreach my $virt (grep (s/^SE_(VIRTUAL_)/$1/ , keys %{$self->{CONFIG}})) {
    $self->{CONFIG}->{"SE_$virt"}->{IODAEMONS_LIST} or next;
    $ioDef->{$virt}={name=>$self->{CONFIG}->{"SE_$virt"}->{IODAEMONS_LIST},

		     mss=>$self->{$virt}->{MSS}};
  }
  $self->{IODDAEMONS}={};
  foreach my $def (keys %$ioDef){
    my @servers=@{$ioDef->{$def}->{name}};
    my @daemons;
    $self->setMSSEnvironment($ioDef->{$def}->{mss});
    foreach my $entry (@servers) {
      my ($daemon, @options)=split(":", $entry);
      $daemon="\L$daemon\E";
      $DEBUG and $self->debug(1, "Starting a $daemon");
      $DAEMONS->{$daemon}
	or $self->{LOGGER}->error("SE", "Error: don't know how to start a $daemon") and return;
      my $func=$DAEMONS->{$daemon}->{startup};
      my %options=$self->$func($def, @options) or 
	$self->{LOGGER}->error("SE", "Error starting a $daemon") and return;

      push @daemons, {name=>$daemon, %options};

    }
    $self->{IODAEMONS}->{$def}=\@daemons;

  }
  $DEBUG and $self->debug(1, "All the daemons have been started");
  return 1;
}

###############################################################################
##############################################################################
#  IO DAEMONS
###############################################################################
##############################################################################

sub createXROOTConfFile{
  my $self=shift;
  my $name=shift;
  my $optionsFile="$self->{CONFIG}->{LOG_DIR}/xrootd.$name.conf";
  my $port;
  my %options;
  my $extra="";
  my $olb_port=51002;
  my $host=$self->{HOST};
  my $fsLib = AliEn::Util::isMac() ? "libXrdTokenAuthzOfs.dylib" : "libXrdTokenAuthzOfs.so";
  foreach my $option (@_){
    my ($key, $value)=split("=", $option);
    if ($key=~ /^port$/){
      $port=$value;
      $self->{SUBPORT} = $port;
      $self->{SUBURI} = "root://$host:$port/";
      $options{port}=$port;
      next;
    }
    if ($key =~ /^(stage_)|(sealed_envelope)|(RFIO_USE_CASTOR_V2)/i) {
      $key=uc($key);
      $self->info("Defining the Env variable $key=$value");
      $ENV{$key}=$value;
      next;
    }
    if ($key =~ /^olb_host$/i) {
      $self->info("Allowing host $value");
      $extra.="olb.allow host $value\n";
      next;
    }
    if ($key =~ /^olb_port$/i) {
      $olb_port=$value;
      next;
    }
    if ($key =~ /^host$/i){
      $host=$options{host}=$value;
      $self->{SUBURI}="root://$host";
      $self->{SUBPORT} and $self->{SUBURI}.=":$self->{SUBPORT}";
      next;
    }
    $self->info("Don't know what to do with the option $option!");
    return;
  }
  $options{protocol}="root";

  $port or $self->info("Using the default port 1094") and $port=1094;
#  $self->{CONFIG}->{SE_SAVEDIR} or 
#    $self->info("Error: there are no directories to export")
#      and return;
#  my @dirs=@{$self->{CONFIG}->{SE_SAVEDIR_LIST}};

  #export the / since it is the root of our lfns
  my @dirs;
  push @dirs, "/";

  map {$_="xrootd.export $_"} @dirs;

  my $fsLibdir = "$ENV{'ALIEN_ROOT'}/api/lib";

  if (! -e "$fsLibdir") {
      $fsLibdir = "$ENV{'ALIEN_ROOT'}/lib";
      if (! -e "$fsLibdir" ) {
	  $self->info("Error - $fsLib is not existing under $ENV{'ALIEN_ROOT'}/api/lib or $ENV{'ALIEN_ROOT'}/lib ",12);
	  return;
      }
  }

  my $async="xrootd.async off\n";  
  open (FILE, ">$optionsFile") or 
    $self->info("Error opening $optionsFile", 12) and return;
  print FILE "# Start the server on port $port
xrd.port $port
# Use the authorization library
xrootd.fslib $fsLibdir/$fsLib
# Export files
@dirs
xrd.sched mint 16 maxt 16 avlt 4
";

  if ( $self->{CONFIG}->{SE_MSS}=~ /castor/i) {
    $self->info("This xrootd is running on castor");
    $async="";
    print FILE "odc.manager $self->{HOST} $olb_port
#odc.trace redirect
odc.trace all debug
ofs.trace all debug

ofs.redirect remote

# forwarding never worked when testing via the perl admin client:
#ofs.forward rm



# olbd
olb.port $olb_port
olb.space linger 0 min 20g 1g
olb.allow host $self->{HOST}
$extra
olb.trace all debug
olb.delay startup 15 drop 30s
";
    $options{olbd}=1;
  }

  $async and print FILE $async;

  if($self->{CONFIG}->{MONALISA_HOST}){
  	my $mon_conf = "xrootd.monitor all flush 15s window 15s dest files info user $self->{CONFIG}->{MONALISA_HOST}:9930\n";
  	print FILE $mon_conf;
  }
  close FILE;

  return ($optionsFile, %options);
}

sub startSRM {
  my $self=shift;
  my %options=("protocol", "srm");
  $self->info("This SE can be accessed through SRM to $self->{MSS}->{URI} $self->{MSS}->{MOUNTPOINT} ");
  return %options;
}
sub startXROOTD {
  my $self=shift;
  my $name=shift;

  my ($optionFile, %options)=$self->createXROOTConfFile($name,@_) or return;

  if ($options{host}) {
    $self->info("We don't have to start the xrootd (it runs on $options{host})");
    return %options;
  }
  $self->info("Starting the xrootd daemon");
  my $pid=fork();
  defined $pid or $self->info("Error doing the fork") and return;

  my $log="$self->{CONFIG}->{LOG_DIR}/xrootd.$name.log";
  my $source="";
  (-f "$ENV{'HOME'}/.alien/Environment.xrootd") and 
    $source="source $ENV{'HOME'}/.alien/Environment.xrootd;";

  if (! $pid) {
    exec("$source xrootd -c  $optionFile > $log 2>\&1");
  }
  my $pidf=$log;
  $pidf =~ s/\.log/\.pid/;
  open (FILE, ">$pidf" ) or $self->info("error opening $pidf") and return;
  print FILE "$pid\n";
  close FILE;
  my $pid2="";
  if ($options{olbd}) {
    $pid2=fork();
    ($pid2) or exec("$source olbd -m -c $optionFile > $log.olbd 2>\&1  ");
    $self->info("Starting olbd (pid $pid2)");
  }

  sleep(2);
  
  if (!(kill(0,$pid))) {
    $self->info("The process $pid died");
    return;
  }
  $self->info("Putting the pid $pid $pid2 into the file");
  open (FILE, ">>$self->{CONFIG}->{LOG_DIR}/SE.pid") or $self->info("Error opening the file") and return;
  print FILE " $pid $pid2 ";
close FILE;

  return %options;
}

sub startFDT {
  my $self = shift;
  my $name = shift;
  my $options = shift || '';
  
  $self->info("Starting the FDT daemon...");
  my $port = ($options =~ /-p\s+(\d+)/ ? $1 : 54321);
  my $pid=fork();
  defined $pid or $self->info("Error doing the fork") and return;
  my $log="$self->{CONFIG}->{LOG_DIR}/fdt.$name.log";
  if (! $pid) {
    exec("$ENV{ALIEN_ROOT}/java/MonaLisa/java/bin/java -jar $ENV{ALIEN_ROOT}/java/MonaLisa/Service/lib/fdt.jar $options > $log 2>\&1");
  }
  sleep(2);
  if(! kill(0, $pid)) {
    $self->info("The FDT process $pid died.");
    return;
  }
  $self->info("Putting the pid $pid into the SE pid file. FDT log is in $log");
  open (FILE, ">>$self->{CONFIG}->{LOG_DIR}/SE.pid") or $self->info("Error opening the file") and return;
  print FILE " $pid ";
  close FILE;
  return (protocol => 'fdt', port => $port);
}

###############################################################################
##############################################################################

sub ConfigureLVM {
  my $self=shift;
  my $name=shift;
  my $virtual=shift;

  my $db=$self->{DATABASE};
  my @singlevolume=@{$self->{CONFIG}->{'SE_SAVEDIR_LIST'}};
  my $mss=$self->{MSS};
  my $mss_type=$self->{CONFIG}->{SE_MSS};
  if ($virtual){
    $db=AliEn::Database::SE->new({VIRTUAL=>$virtual}) or return;
    @singlevolume=@{$self->{CONFIG}->{"SE_$virtual"}->{SAVEDIR_LIST}};
    $mss=$self->{$virtual}->{MSS};
    $mss_type=($self->{CONFIG}->{"SE_$virtual"}->{MSS} || $mss_type);
  }

  my $lvm=AliEn::LVM->new({DB=>$db}) or return;


  #  startIOdaemon('testhost.test.ch','peters');


  #########################################################################
  # Logical Volume Manager LVM
  # initilize the database, if file database is empty, set the rebuild flag

  my $rebuild = $lvm->initialiseDatabase();

  # read the volumes from the SAVEDIR configuration list and create them

  foreach (@singlevolume) {
    my ($mountpoint,$size) = split (",", $_);
    (defined $size) or  $size = -1;
#    $mss->{MOUNTPOINT} and $mountpoint="$mss->{MOUNTPOINT}$mountpoint";
    my $host=lc($self->{CONFIG}->{HOST});
    ($mss->{URI} and $mss->{URI}=~ m{^[^/]*/[^/]*/([^/]*)/}) and $host=$1;

    my $volume = {
		  'volume'      => $mountpoint,
		  'mountpoint'  => $mountpoint,
		  'size'        => $size,
		  'freespace'   => $size,
		 };

    $self->info( "LVM Adding Volume name=$name path=$mountpoint size=$size");
    $lvm->addVolume($volume, $mss,$rebuild, "\L$mss_type://$host\E");

  }

  # crosscheck DB file entries against files on disk and recalculate the volume space
  $self->info( "LVM Syncing the database");
  $lvm->syncDatabase();
  my $info=$lvm->{DB}->retrieveAllVolumesUsage();

  my $totblocks =  $info->{size};
  my $freeblocks = $info->{freespace};
  my $usedblocks = $info->{usedspace};
  my $nfiles     = $lvm->{DB}->getNumberOfFiles();
  $self->sendApMonInfo($info, $nfiles);

  my $usedtb = sprintf "%04.02f",$usedblocks*1.0/(1024.0*1024.0*1024.0);

  if ($totblocks == -1) {
    $self->info( "LVM Volume Space  :infinite");
  } else {
    $self->info( "LVM Volume Space  :$totblocks \t [1k]");
  }
  $self->info( "LVM #of Files     :$nfiles\n
LVM Free   Blocks :$freeblocks \t [1k]
LVM Used   Blocks :$usedblocks \t [1k] \t $usedtb TB");


  return $lvm;
}


sub quit {
    my $self = shift;
    $self->debug(1, "Killing SE\n" );

}
#This function gets a pfn into the SE. If it doesn't manage, it starts a transfer
# By default, it will copy the file into the MSS of the SE. 
# It will update the LVM accordingly
# $options: Hash containing: source, (optional) target, oldSE,lfn  and options
#           It can also contain 'retrieve_subject', and then the SE
#           will start an ftp server so that that certificate can retrieve the file
#         Possible options: 
#                         f-> force to get the file again (delete cache copies)
#                         c-> cache: do not copy the file into the MSS
#                         d-> delete: delete the original copy after the file
#                                has been copied (if possible)
#                         m-> mirror: after getting the file, there is no need
#                               to start a service to transfer it to the client
#
#COMMENTS: This function is call from 'register', and also from 'get', if
#         the user does not manage to get the file himself.

sub copyFile {
  my $this    = shift;
  my $options = shift;

  $self->setAlive();
  $options
    or $self->{LOGGER}
      ->warning( "SE", "Error: not enough arguments registering a file" )
	and return ( -1, "Not enough arguments" );
  if ( !UNIVERSAL::isa( $options, "HASH" ) ) {
    $self->{LOGGER}
      ->warning( "SE", "Error: argument $options is not an array!" );
    return ( -1, "Argument is not an array" );
  }

  my $pfn    = $options->{source};
  my $target = ( $options->{target} or "" );

  my $oldSE = ( $options->{oldSE} or "" );
  my $opt=($options->{OPTIONS} or "");
  my $lfn=($options->{lfn} or "");
  my $guid=($options->{guid} or "");
  my $se=($options->{se} or "");

  ($se, my $seInfo)=$self->checkVirtualSEName($se);

  my $mss=$seInfo->{mss};

  $options->{options} and $opt="$opt$options->{options}";
  $self->info( "Getting the file $pfn from $oldSE (options $opt ) ");
  if ($target) {
    #Creating the local directory
    my $dir = $target;
    $dir =~ s/\/?[^\/]*//;

    if ($dir) {
      $self->info( "Creating directory $dir" );
      $mss->createdir($dir)
	or return ( -1, "Creating directory $dir" );
    }
  }

  ($pfn)
    or $self->{LOGGER}
      ->warning( "SE", "Error: not enough arguments copying a file" )
	and return ( -1, "Not enough arguments (pfn)" );
  
  #checking if we have to clean the cache
  if ($opt=~ /f/) {
    #the local copy is not valid anymore. Delete all of them.
    $self->{DATABASE}->deleteLocalCopies($pfn);
  }

  #We try to get the local copy
  $self->info( "Checking local copies");
  my ($name, $size)=$self->checkLocalCopy($pfn, $target); 
  if (!$name){
    if ($opt=~ /t/){
      $self->info("Skipping trying to get the file (a transfer will be issued)");
    } else {
      $self->info("Let's try to do the url");
      my $file =AliEn::SE::Methods->new({ "DEBUG", $self->{DEBUG}, "PFN", $pfn,
					  "DATABASE", $self->{DATABASE},
					});
      $self->info("URL created");
      if ($file){
	$self->_checkCacheSpace();
	$self->info( "trying to get the file");
	#    local $SIG{PIPE} =sub {
	#      print "ERROR: I GOT A SIG PIPE!!\n";
	#      die("got a sig pipe...");
	#    };
	
	$self->{LOGGER}->set_error_msg("");
	eval {
	  $name = $file->get();
	  if ($name) {
	    $self->info( "Got the file. Asking for the size");
	    $size = -s $name;
	    ($size ) or $self->debug(1, "Asking the size") and  
	      $size=$file->getSize();
	    $size or $self->debug(1, "Size wasn't defined!!") 
	      and $size=0; 
	  }
	};
	if ($@) {
	  $self->info( "The call to get the file died with $@");
	}
	$self->info( "Got $name and $size");
      } else {
	$self->{LOGGER}->warning( "SE", "Error parsing $pfn" );
      }
    }
    if ((! $name) and (! $oldSE)){
      my $message=($self->{LOGGER}->error_msg() || "Not possible to get the file $pfn");

      $self->info( "Not possible to get the file, and there is no oldSE\n$message");

      return (-1, $message);
    }

    $name or return $self->SetTransfer($pfn, $target, $oldSE, $options, $size);
    
    $self->{DATABASE}->insertLocalCopy({pfn=>$pfn,
					localCopy=>"file://$self->{HOST}$name", 
					size=>$size});
  }
  $self->info("Ok, we got the file. What to do with it?");

  my $md5=AliEn::MD5->new($name);
  if ($opt=~ /c/) {
    $self->info( "We only need a cache copy. Do not register it in the MSS");
#    if ($options->{retrieve_subject}) {
#      $self->info( "Let's start a service to pick up the file");
#      return $self->_startFTPServer($name, $size, $options->{retrieve_subject}, $options->{OPTIONS});
#    }
    return  ( {pfn=> "file://$self->{HOST}$name", size => $size, md5=>$md5 } );
  }
  return $self->registerInMSS($size, $guid, $name, $lfn, $options, $se, $md5);
}
sub registerInMSS {
  my $self=shift;
  my ($size, $target, $name, $lfn, $options, $seName, $md5)=@_;

  my ($lvm, $mss)=($self->{LVM},$self->{MSS});

  if ($seName){
    $mss=$self->{$seName}->{MSS};
    $lvm=$self->{$seName}->{LVM};
  }

  ($target, my @rest)=$self->getFileName($seName, $size, {guid=>$target,
							  md5=>$md5});
  $target or $self->info( "Error getting a new file name")
    and return (-2,  "No space left on device in $self->{CONFIG}->{'SE_FULLNAME'}  $target");
  #    }

  $self->info("Saving the file $name (as $target)" );
  $self->setMSSEnvironment($mss);
  my $save = $mss->save( $name, $target);

  my $info = $lvm->{DB}->retrieveAllVolumesUsage();
  my $freeblocks=$info->{freespace};
  $self->info( "LVM Free   Blocks :$freeblocks \t [1k]");
  if($self->{MONITOR}){
    my $nfiles = $lvm->{DB}->getNumberOfFiles();
    $self->sendApMonInfo($info, $nfiles);
  }
  
  if (!$save) {
    $self->{LOGGER}->warning( "SE", "Error saving $name (and $target)" );
    $lvm->removeFile({size=>$size, file=>$target});
    return ( -1, "Error copying the file to the MSS" );
  }

  my $guid=$save;
  $guid =~ s{^.*/([^/\.]*)\.\d+$}{$1};

  $self->info( "File saved in $save (guid $guid)" );
  if ($options=~ /d/){ 
    $self->info( "Deleting the original file ($name)" );
    unlink $name;
  }
  return ( { "pfn" => $save, "size" => $size, guid=>$guid, md5=>$md5 } );
}

#This subroutine will try to start an ftp server so that the user can fetch 
#a file
#
#sub _startFTPServer{
#  my $self=shift;
#  my $file=shift;
#  my $size=shift;
#  my $subject=shift;
#  my $options=shift;
#  $options=~ /m/ and $self->info("No need to start a daemon")
#    and return ({"pfn" => "file://$self->{HOST}$file", "size" => $size});
#  $self->info( "Let's start for user '$subject' to fetch $file");
#  my $port=$self->getPort();
#  $self->{GRIDMAP}=$self->{X509}->createGridmap($subject);
#  my $childpid= AliEn::Service::SubmitFile::startBBFTPServer($self, $port);
#
#  if (! $childpid) {
#    $self->info( "Error starting gridftp server");
#    return ({"pfn" => "file://$self->{HOST}$file", "size" => $size});
#  }
#  $self->info( "The bbftp started successfully!!");
#
#  $self->{DATABASE}->insert("FTPSERVERS", {pid=>$childpid,
#					port=>$port,
#					pfn=>$file,
#					time=>,time,
#					user=>$subject});
#  my $cert=$self->{CONFIG}->{SE_CERTSUBJECT};
#
#  $cert =~ s/=/\/\//g;
#
#  return ({"pfn" => "bbftp://$self->{HOST}:$port$file?SUBJECT=$cert", 
#	   "size" => $size});
#}

sub stopFTPServer{
  my $this=shift;
  my $port=shift;
  $self->info( "Stopping the service in port $port");
  my $pid=$self->{DATABASE}->queryColumn("SELECT pid FROM FTPSERVERS where port=$port");
  $self->{DATABASE}->delete("FTPSERVERS", "port=$port");
  unlink "$self->{CONFIG}->{TMP_DIR}/PORTS/lockFile.$port";
  map {$self->stopService($_)} @$pid;
  return 1;
}

sub SetTransfer {
  my $self=shift;
  my $pfn=shift;
  my $target=shift;
  my $oldSE=shift;
  my $options=shift;
  my $size=shift;

  my $errorMessage="";
  $self->info("Setting a transfer with $pfn, $target, $oldSE, $options and $size");
  ( !$oldSE ) and $errorMessage ="Error: There is no old SE for $pfn";
  ($oldSE) and ( $oldSE =~ /^$self->{CONFIG}->{SE_FULLNAME}$/i ) and
    $errorMessage="we are supposed to transfer from $oldSE, but that's me!!!";

  if ($errorMessage) {
    $self->info($errorMessage);
    return (-1, $errorMessage);
  }
  $self->info( "We can't get the file... but we can start a transfer!");
  my $save="";
  ($target) and  $save = $self->{MSS}->getURL($target);	  


  $self->info( "Saving in $save" );
  $options->{TOPFN}=$save;
  my $result=$self->{SOAP}->CallSOAP("MANAGER","enterTransfer",$options);
  $result  or return (-1, $self->{LOGGER}->error_msg());
  #Make the remote SE get the file
  
  my $transferid=$result->result;
  $self->info( "New transfer $transferid");
  $self->{DATABASE}->insertLocalCopy({pfn=>$pfn, transferid=>$transferid});

  return (-2, $transferid);
}


sub checkLocalCopy {
  my $self      = shift;
  my $pfn       = shift;
  my $localFile = shift;
#  my $client_cert  = shift;
#  my $opt       =shift;

  $self->info( "Looking for local copies");   
  my ($data) =$self->{DATABASE}->checkLocalCopies($pfn);
  my $size=$data->{size};
  if ($data and exists $data->{localCopy}) {
    $self->info( "Got $data->{localCopy}");
    my $URL=AliEn::SE::Methods->new({"PFN", $data->{localCopy},
				     "LOCALFILE", $localFile,
				     "DEBUG", 0});
    if ($data->{localCopy}=~ s{^file://[^/]*/}{/} ) {
      if (-s  $data->{localCopy} eq $size) {
	$self->info("Returning the file $data->{localCopy} without getting it again!!");
	return ($data->{localCopy}, $size);
      }
      $self->info("The file $data->{localCopy} doesn't have size $size");
    } else{
      $self->info("We are doing a get of $data->{localCopy}");
      my $exists="";
      $URL and ($exists)=$URL->get("-s");
      if ( $exists ) {
	return ($exists, $size);
      }
#      $self->info( "Giving back to $client_cert the local copy $data->{localCopy} ($exists)" );
#      return $self->_startFTPServer($exists, $size, $client_cert, $opt);
    }
  }
  $self->info( "File $data->{localCopy} does no longer exist"); 
  $self->{DATABASE}->deleteLocalCopies($pfn);

  $self->info( "There are no local copies"); 
  return;
}

#sub removeFileFromLVM{
#  my $this=shift;
#  my $size=shift;
#  my $file=shift;
#  $self->info( "Removing the file $file with $size");
#  my $newFile = {
#		     'file'          => $file,
#		     'size'          => $size,
#		 };
#  $self->{LVM}->removeFile($newFile);
#  return 1;
#}
sub alive {
    $self->{ALIVE_COUNTS}++;

    if ( ( $self->{ALIVE_COUNTS} == 12 ) ) {
        $self->info( "SE contacted" );
        $self->{ALIVE_COUNTS} = 0;
    }
    return { "VERSION" => $self->{CONFIG}->{VERSION} };
}
sub checkTransfer {
  my $this=shift;
  my $id=shift;
  my @status=$self->CallTransferManager("checkTransfer", $id);
  return @status;

} 

sub updateLocalCache {
  my $this=shift;
  my $id=shift;
  my $pfn=shift;

  $self->info( "The transfer is done ($pfn)\n. Inserting it in the table");
  my $lURL = new AliEn::SE::Methods( $pfn );
  my $size=$lURL->getSize;
  $self->{DATABASE}->updateLocalCopy($pfn, $size,$id);

  return 1;
}

sub restartTransfer {
  my $this=shift;
  my $id=shift;

  return $self->CallTransferManager("changeStatusTransfer", $id, "INSERTING");

}
sub deleteTransfer {
  my $this=shift;
  my $id=shift;
  return $self->CallTransferManager("deleteTransfer", $id);
}
# CallTransferManager
#  input  $funtion=name of the function to call
#         @_ rest of the arguments to pass to the function
#  output result and paramsout of the call
#
# THIS IS SUPPOSED TO BE AN INTERNAL FUNCTION
sub CallTransferManager {
  my $self=shift;
  my $function=shift;

  $self->setAlive();

  $self->info( "Calling $function in the Manager (@_)");

  my $result=$self->{SOAP}->CallSOAP("MANAGER",$function,@_);

  $self->info( "We got an answer back $result");
  $result   or  return (-1, $self->{LOGGER}->error_msg());
  my $status=$result->result;
  my @extra=$result->paramsout;
  $self->info( "Status $status");
  return $status, @extra;

}


sub getFileSOAP {

    #This just opens the file from whereever and returns it

    my $this = shift;
    my $file = shift;
    my $dir  = ( shift or undef );

    $self->setAlive();
    my $buffer;
    my $maxlength = 1024 * 10000;
    $self->info( "In getFileSOAP with $file" );
    if ($dir) {
        $file = $self->{CONFIG}->{$dir} . "/" . $file;
    }

    #    if( ($file =~ /^$self->{CONFIG}->{CACHE_DIR}.*/) or ($file =~ /^$self->{CONFIG}->{LOG_DIR}.*/) or ($file =~ /^$self->{CONFIG}->{TMP_DIR}.*/) or ($file =~ /^$ENV{ALIEN_ROOT}.*/))  {

    ( $file =~ /\.\./ )
      and $self->{LOGGER}->warning( "SE",
        "User requests a file from a non authorized directory. File $file" )
      and return;
    if ( open( FILE, "$file" ) ) {
        my $length = sysseek( FILE, 0,2 );
	sysseek( FILE,0,0);
        my $aread = sysread( FILE, $buffer, $length, 0 );
        close(FILE);
        ( $aread < $maxlength ) or 
	    $self->{LOGGER}->warning("SE", "Trying to get a big file by SOAP")
		and die("Trying to transfer by soap a file bigger than $maxlength\n");
        $self->info( "Transfering file $file" );
    }
    else {
        $self->{LOGGER}->warning( "SE", "$file does not exist" );
        return;
    }

    $buffer or return "";
    my $var = SOAP::Data->type( base64 => $buffer );

    return $var;

    #   }
    #   else {
    #	# The directory we wish to get from is now autirized
    #	$self->{LOGGER}->warning("SE","User requests a file from a non authorized directory");
    #	return;
    #    }
}

sub getURL {
    my $this = shift;
    my $file = shift;

    $self->info( "Giving back the url of $file" );

    return $self->{MSS}->getURL($file);
}

#function bringFileToSE
#input: $se         Se Name (Alice::CERN::Castor)
#       $options   hash including source, target, USER, DESTINATIOn, LFN, TYPE
#
# output
#       undef if error
#       -2 if transfer has been schechuled
#       {"pfn"=>""} if everything worked
#
# It calls copyFile in the right SE
#
# Called from LCM->bringFileToSE
# 

sub getFile {
  my $this=shift;

  $self->info( "In getFile with @_");

  my $se=shift; 
  my $options=shift;

  $options->{se}=$se;
  return $self->copyFile($options);
}

sub getFileChunkSOAP {

    #This just opens the file from whereever and returns a file chunk with size and offset

    my $this = shift;
    my $file = shift;
    my $dir  = shift;
    my $offset = shift;
    my $size = shift;
    my $buffer;
    my $maxlength = 1024 * 1000000;
    $self->info( "In getFileChunkSOAP with $file (size:$size/offset:$offset" );
    if ($dir) {
        $file = $self->{CONFIG}->{$dir} . "/" . $file;
    }

    #    if( ($file =~ /^$self->{CONFIG}->{CACHE_DIR}.*/) or ($file =~ /^$self->{CONFIG}->{LOG_DIR}.*/) or ($file =~ /^$self->{CONFIG}->{TMP_DIR}.*/) or ($file =~ /^$ENV{ALIEN_ROOT}.*/))  {

    ( $file =~ /\.\./ )
      and $self->{LOGGER}->warning( "SE",
        "User requests a file from a non authorized directory. File $file" )
      and return;
    if ( open( FILE, "$file" ) ) {
	my $length = sysseek( FILE, 0,2 );
	if ($offset > $length) {
	    $self->{LOGGER}->warning("SE","Requesting non existant file chunk");
	    $size=0;
	}

        my $aseek = sysseek( FILE, $offset, 0) or $self->{LOGGER}->warning("SE","Requesting non existant file chunk");
        my $aread = sysread( FILE, $buffer, $size, 0 );
	$self->info("Seek result is $aseek Read result is $aread");
        close(FILE);
        ( $aread < $maxlength ) or
            $self->{LOGGER}->warning("SE", "Trying to get a big file")
                and return;
        $self->info( "Transfering file chunk $file offset $offset size $size" );
    }
    else {
        $self->{LOGGER}->warning( "SE", "$file does not exist" );
        return;
    }

    $buffer or return "";
    my $var = SOAP::Data->type( base64 => $buffer );

    return $var;
}

sub startIOdaemon {

  my (@singleoption)=();
  
  $self->{CONFIG}->{'SE_OPTIONS_LIST'} and 
    @singleoption=$self->{CONFIG}->{'SE_OPTIONS_LIST'};;
  
  my @substring;
  
  # set default IOport range 8850 - 8950
  $self->{minIOport} = 8850;
  $self->{maxIOport} = 8950;
  
  foreach (@singleoption) {
    my ($identifier,$value) = split ("=", $_);
    my $cidentifier = $identifier;
    
    if ($cidentifier=~/ioport/) {
      my ($minport,$maxport)  = split("-", $value);
      $self->{minIOport}   = $minport;
      $self->{maxIOport}   = $maxport;
    }

    $cidentifier = $identifier;
    if ($cidentifier=~/iottl/) {
      $self->{IOttl} = $value;
    }
  }

  $self->info( "IO ports $self->{minIOport} - $self->{maxIOport}" );
  $self->info( "IO TTL   $self->{IOttl}" );

    #This creates a pair of key, starts an IO daemon with the given port and the key and
    #returns a private key via soap for access !

    $self->info( "In startIOdaemon with @_"); 
    my $this       = shift;
    my $clienthost = shift;
    my $username   = shift;
    my $buffer     = "";

    my $keydirectory = "$ENV{'HOME'}/.alien/transferkeys";
    my $keyfilename = "$keydirectory/".int(rand(10000000));
    mkdir ($keydirectory, 0700);
    my $done = system("ssh-keygen -f $keyfilename -b512 -tdsa -N \"\"");
#    my $done = system("$ENV{'ALIEN_ROOT'}/AliEn/OpenSSH/alien_sshkey_gen.sh","$keyfilename");


    ######################################################################
    # find an open port first ....
    my $proto = getprotobyname('tcp');

    my $minport = $self->{minIOport};
    my $maxport = $self->{maxIOport};
    my $port    = $minport;
    $port--;
    while ( $port < $maxport) {
	$port++;
	socket(Server, PF_INET, SOCK_STREAM, $proto) || next;
	setsockopt(Server, SOL_SOCKET, SO_REUSEADDR, pack("l",1)) || next;
	bind(Server, sockaddr_in($port, INADDR_ANY))              || next;
	last;
    }

    if ($port == $maxport) {
	$self->{LOGGER}->warning( "SE", "There is no free port!!!" );
	return;
    }
    
    $self->info( "Assigned Port $port to $clienthost");
    
    if ( open ( KEYFILE, "$keyfilename.pub" ) ) {
        my $aread = sysread( KEYFILE, $buffer, 10000000, 0 );
        ( $aread > 0 ) or
            $self->{LOGGER}->error("SE", "Can not read keyfile $keyfilename");
        close (KEYFILE);

    }

#    $self->info( "Replace hostname - $ENV{'HOSTNAME'} - in keyfile with -$clienthost-\n");
    print ">",$buffer,"\n";
     $buffer =~ s/(\@\s*)\S+([^\@])*$/$1$clienthost $2/;
#    $buffer =~ s/$ENV{'USER'}/$username/;
#    $buffer =~ s/(=\s*)\S+([^\=])*$/$1 $clienthost $2/;
#    $buffer =~ s/'$ENV{'HOSTNAME'}'/'$clienthost'/;

    print "<",$buffer,"\n";

    if ( open ( KEYFILE, "> $keyfilename.pub" ) ) {
        my $awrite = syswrite( KEYFILE, $buffer, 10000000, 0 );
        ( $awrite > 0 ) or
            $self->{LOGGER}->error("SE", "Can not write keyfile $keyfilename");
        close (KEYFILE);
    } else { $self->{LOGGER}->error("SE","Can open to write keyfile $keyfilename")};

    if ( open ( KEYFILE, "$keyfilename" ) ) {
	my $aread = sysread( KEYFILE, $buffer, 10000000, 0 );
	( $aread > 0 ) or
	    $self->{LOGGER}->error("SE", "Can not read keyfile $keyfilename");
	close (KEYFILE);
    }

    $done = system("$ENV{'ALIEN_ROOT'}/AliEn/OpenSSH/SSH-Starter.sh","$clienthost","$keyfilename","$port");
    
    # append the port in the buffer
    $buffer.="\n Port: $port\n";
    $buffer.="\n User: $ENV{'USER'}\n";
    my $var = SOAP::Data->type( base64 => $buffer );
#    $self->info("Returning SOAP");
    return $var;
}

sub  checkWakesUp {
  my $self = shift;
  my $silent =shift;
  
  if (!$self->{FIRST_EXEC} ){
    $self->{FIRST_EXEC}=1;
    $self->{LOGGER}->redirect("$self->{CONFIG}->{LOG_DIR}/SE_remove.log");
  }
  my $method="info";
  my @methodData=();
  $silent and $method="debug" and push @methodData, 1;
  $self->$method(@methodData, "READY TO DELETE THE ENTRIES");
  my $time=time();
  $self->{PROXY_CHECKED} or $self->{PROXY_CHECKED}=1;
  if ($time-$self->{PROXY_CREATED}>3600){
    $self->{X509}->checkProxy();
    $self->{PROXY_CHECKED}=$time;
  }
 foreach my $subSE (grep (s/^SE_(VIRTUAL_)/$1/ , keys %{$self->{CONFIG}}), "") {
    my ($seName, $seInfo)=$self->checkVirtualSEName($subSE);
    
    my $entries=$seInfo->{lvm}->{DB}->query("SELECT binary2string(guid) as guid, pfn from TODELETE limit 1000");
    foreach my $entry (@$entries){
      $self->info("Ready to do something with");
      use Data::Dumper;
      print Dumper ($entry);
      my $pfn=AliEn::SE::Methods->new($entry->{pfn}) or next;
      my $path=$pfn->path();
      if (-f $path){
	$self->info("The path $path exists. Let's move it to the olddirectory");
	my @info=stat($path);
	my $now=time;
	if ($info[9]+36000<$now){
	  $self->info("The file is more than ten hours old. Let's delete it");

	  $seInfo->{lvm}->removeFile({file=>$entry->{pfn},
				      guid=>$entry->{guid}}) 
	    or 	$self->info("Error deleting the entry from the LVM") and next;
	  $self->info("And removing the file");
	  system("mv", $path, "$ENV{HOME}/OLDFILES");

	  $seInfo->{lvm}->{DB}->do("DELETE FROM TODELETE where pfn='$entry->{pfn}'");
	}
      }
    }

}
  return;
}
sub setMSSEnvironment{
  my $self=shift;
  my $mss=shift;
  $self->info("Checking if we have to set the environment");
#  $self->{CURRENT_MSS} eq $mss and return 1;
  $mss->{ENVIRONMENT} or return 1;
  foreach my $key (keys %{$mss->{ENVIRONMENT}}){
    print "Setting $key to $mss->{ENVIRONMENT}->{$key}\n";
    if ($mss->{ENVIRONMENT}->{$key}) {
      $ENV{$key}=$mss->{ENVIRONMENT}->{$key};
    } else {
      delete $ENV{$key};
    }
  }
#  $self->{CURRENT_MSS}=$mss;
  return 1;
}

sub checkVirtualSEName{
  my $self=shift;

  my $seInfo={mss=>$self->{MSS},lvm=>$self->{LVM},
	      fullname=>$self->{CONFIG}->{SE_FULLNAME}};
  my $name=shift
    or return ("", $seInfo);

  $self->debug(1,"Checking if the name '$name' is a valid name");
  $name=~ s{^VIRTUAL_}{};
  $name=~ s{^SE_}{};
  (($name =~ /^$self->{SE_NAME}$/i) or 
   ($name =~ /^$self->{CONFIG}->{SE_FULLNAME}$/i))
    and $self->debug(1,"Default SE") and return  ("", $seInfo);
  $self->info("Using the virtual SE $name");
  if ($name =~ s{^([^:]*)::([^:]*)::}{}){
    my ($vo, $site)=($1, $2);
    my $error;
    $self->info("Specifying the fullname");
    $site =~ /$self->{CONFIG}->{SITE}/i or $error="The SE is in another site ($site)";
    $vo=~/$self->{CONFIG}->{ORG_NAME}/i or $error="The SE is from another VO ($vo)";
    $error and $self->info("$error") and die ($error);
  }
  $seInfo->{fullname}="$self->{CONFIG}->{ORG_NAME}::$self->{CONFIG}->{SITE}::$name";

  $name="VIRTUAL_".uc($name);
  $self->{$name} or
    $self->info("We can't create a name for the SE $name")
      and die ("We can't create a name for the SE $name");
  $seInfo->{mss}= $self->{$name}->{MSS};
  $seInfo->{lvm}=$self->{$name}->{LVM};

  $self->setMSSEnvironment($seInfo->{mss});

  return  ($name, $seInfo);
}


# This method registers a new file in the SE.
# It receives the size of the file
#
sub getFileName{
  my $this=shift;

  #First, let's check if the first argument is the SEName or the size;
  my $seName=shift;
  my $size=shift;
  my $options=shift;


  my $guid=($options->{guid} or "");
  my $ioMethods=($options->{iomethods} or "");
  my $md5=($options->{md5} or "");

  ($seName, my $info) =$self->checkVirtualSEName($seName);
  my $lvm=$info->{lvm};
  my $mss=$info->{mss};
  (my $name, $guid)= $mss->newFileName($guid);

  $self->info("In getFileName -> new Name is $name -> guid is $guid"); 
  $name or return;

  my $newFile = {
		 'file'          => $name,
		 'ttl'           => $self->{MSS}->{'LVMTTL'},
		 'size'          => int($size/1024),
		 'guid'          => $guid,
		 'md5'           => $md5,
		 'sizeBytes'     => $size,
		};

  $self->debug(1, "Adding the file");

  $name = $lvm->addFile($newFile) or 
    $self->info("$$ Error adding the file to the LVM". $self->{LOGGER}->error_msg()) 
      and die("Error adding the file to the LVM". $self->{LOGGER}->error_msg()."\n");
  $self->info("$$ Successfully added the file " . $newFile->{guid});
  my $newdir;
  if ($name =~/(.*)\/(.*)$/) {
    $newdir = $1 .'/';
  }	

  $mss->mkdir($newdir);

  my $infoSE=$lvm->{DB}->retrieveAllVolumesUsage();
  my $freeblocks = $infoSE->{freespace};
  my $usedblocks = $infoSE->{usedspace};
  my $usedtb = sprintf "%04.02f",$usedblocks*1.0/(1024.0*1024.0*1024.0);
  if($self->{MONITOR}){
    my $nfiles     = $lvm->{DB}->getNumberOfFiles();
    $self->sendApMonInfo($infoSE, $nfiles);
  }

  $self->info( "LVM Free   Blocks :$freeblocks \t [1k]");
  $self->info( "LVM Used   Blocks :$usedblocks \t [1k] \t $usedtb TB");
  my $url=$mss->url($name);
  $self->debug(1, "getFileName returns $name, $self->{CONFIG}->{FTD_REMOTEOPTIONS}, $url");
  my $iourl=$self->checkIOmethod($url, $ioMethods, $seName);
  print "***************  $iourl $url $ioMethods $seName\n";
  return ($name, "$self->{CONFIG}->{FTD_REMOTEOPTIONS}", $url, $iourl, $guid);

}

sub getVolumePath{
  my $this=shift;
  my $seName=shift;    
  my $size=shift;
  my $ioMethods=shift;

  ($seName, my $info) =$self->checkVirtualSEName($seName);

  my $lvm=$info->{lvm};
  my $mss=$info->{mss};

  my $volume = $lvm->{DB}->chooseVolume($size);

  my $guid=$self->{GUID}->CreateGuid();
  $guid and $self->debug(1,"Got $guid");

  if ($volume) {
    my $url=$mss->url($volume->{mountpoint});
    my $iourl=$self->checkIOmethod($url, $ioMethods, $seName);
    $self->info( "Returning vol=$volume->{mountpoint} guid=$guid 
iourl=$iourl url=$url");
    return ($volume->{mountpoint},$guid,$iourl,$url);
  }

  return;
}

sub verifyFileName {
  my $this=shift;
  my $name=shift;
  my $size=shift;

  $name =~ s/\/\//\//g;
	
  if ( (!$name) or (!$size) ) {
    return (-1, "Not enough arguments in verifyFileName");
  }

  my $newFile = {
		 'file'          => $name,
		};

  my $sesize   = $self->{MSS}->sizeof($name);
  if ($sesize != $size) {
    $self->{MSS}->rm($name);
    $self->info( "In veryFileName -> $name has size $sesize instead of $size -> removed from LVM + SE!");
    my $result = $self->{LVM}->removeFile($newFile);
    return 0;
  } else {
    $self->info( "In veryFileName -> $name has been stored with the correct size $size!");
  }
  return 1;
}

sub getUrlFileName{
  my $this=shift;

  my ($target, $name, $url2)=$self->getFileName("SE", @_);

  $self->debug(1, "In getUrlFileName, returning the url of $target") ;

  my $url = "";

  if ($target) {
      $url = $self->{MSS}->getURL($target);
   }

  return {"URL",$url,"PATH",$target};
}

sub getFileSize {
    my $this=shift;
    my $file=shift;

    my $size = $self->{MSS}->sizeof($file);
    my $buffer;
    $self->info( "In sizeof-> returning 'int($size)'"); 
    $buffer =  ( int($size) );
    my $var = SOAP::Data->type( base64 =>$buffer );
    return $var;
}

sub getLVMDF {
  my $this=shift;
  my $seName=shift;
  ($seName, my $seInfo) =$self->checkVirtualSEName($seName);

  my $lvm=$seInfo->{lvm};
  my $serviceName=$seInfo->{fullname};

  my $nfiles  = $lvm->{DB}->getNumberOfFiles();
  my $info=$lvm->{DB}->retrieveAllVolumesUsage();
  my $ublocks = $info->{usedspace};
  my $tblocks = $info->{size};
  my $fblocks = $tblocks - $ublocks;
  $self->sendApMonInfo($info, $nfiles);
  
  my $use     = 0;

  if ($tblocks > 0) {
      $use = sprintf "%d",(100 * $ublocks)/$tblocks;
  } else {
      $use = "100";
  }

  if ($fblocks <0) {
      $fblocks = "-1";
      $use = "0";
  }

  my $buffer  = sprintf "%-20s %+12s %+12s %+12s %+3s%% %+9s %s",$serviceName,$tblocks,$ublocks,$fblocks,$use,$nfiles,$self->{CONFIG}->{'SE_MSS'};
#  my $buffer  = "$self->{SERVICENAME}\t$tblocks\t$ublocks\t$fblocks\t$use\t$self->{CONFIG}->{'SE_MSS'}";
  $self->info("Returning the space: $buffer");
  return $buffer;
}
				   
sub _checkCacheSpace {
  my $self=shift;
  $self->info( "Checking the space in the cache ($self->{CONFIG}->{CACHE_DIR})");

  open (FILE, "df -h $self->{CONFIG}->{CACHE_DIR} |") or 
    $self->info( "Error checking the space") and return;
  my @data=<FILE>;
  close FILE;

  my $line=join ("", grep (/%/, @data));
  $line or return;
  my $percent;
  $line =~ /\s(\d+)%/ and $percent=$1;
  $percent or return;
  $self->info( "At the moment, $percent % of the disk is used");
  ($percent>90) or return 1;
  $self->info( "Let's delete some files");
  system ("rm -rf $self->{CONFIG}->{CACHE_DIR}/*.*.*");
  return 1;

}
#
#
#
sub unregisterFile {
  shift;
  my $seName=shift;
  ($seName, my $seInfo) = $self->checkVirtualSEName($seName);
  my $guid=shift;
  $self->info("***Ready to delete the entries of '$guid'");
  if (!$seInfo->{lvm}->removeFile({file=>$guid,guid=>$guid})){
    $self->info("Error removing the entry '$guid'");
    die ("Error removing the entry '$guid' from the LVM\n");
  }
  
  $self->info("***File deleted!!!");
  return 1;
}
sub registerFile {
  shift;

  my $seName=shift;

  ($seName, my $seInfo) = $self->checkVirtualSEName($seName);

  my $pfn=shift;
  my $size=shift;
  my $guid=shift || "";
  my $options=(shift || {});

  my $lvm=$seInfo->{lvm};
  my $seFullName=$seInfo->{fullname};
  $self->info("\n\nTrying to register a file in the SE (guid $guid)");
  if (! $guid){
    $self->debug(1, "Getting a new GUID for this file");
    $guid=$self->{GUID}->CreateGuid();
    $guid and $self->debug(1,"Got $guid");
  }
  $guid or $self->info( "ERROR CREATING THE GUID")
    and die("Error creating the guid");
  my $newFile={guid=>$guid,
	       size=>$size,
	       pfn=>$pfn,
	      };
  $options->{md5} and $newFile->{md5}=$options->{md5};
  $newFile->{md5} or $newFile->{md5}=AliEn::MD5->new($pfn);

  $lvm->addFile($newFile, {volumeId=>"", })
    or $self->info("Error registering the file in the LVM")
      and die ("Error registering the file in the LVM: ". $self->{LOGGER}->error_msg()."\n");
  #  $self->{DATABASE}->insert("FILES", {size=>$size,
  #				      pfn=>$pfn, guid=>$guid}) 
  #    or $self->die("Error inserting the entry in the DB");
  $self->info( "File $guid added");
  return {guid=>$guid, se=>$seFullName};
}
#sub getPFNFromLFN{
#  my $this=shift;
#  my $lfn=shift;
#  $self->info( "Getting the pfn of $lfn");
#  my $pfn=$self->{DATABASE}->queryValue("SELECT pfn from FILES where lfn='$lfn'");
#  $self->info( "Got $pfn");
#  return $pfn;
#}
sub getPFNFromGUID{
  my $this=shift;
  my $seName=shift;
  my $guid=shift;
  my $ioMethod=shift;
  my $options=shift ||{};

  ($seName, my $seInfo)=$self->checkVirtualSEName($seName);

  $self->info("Getting the pfn of $guid");

  my $db=$seInfo->{lvm}->{DB};
  my $pfn=$db->getPFNFromGUID($guid)
    or return (-1, "Error doing the query");
  my @pfns=@$pfn;
  @pfns or return (-1, "guid $guid is not registered in this SE ($seName)");
  if ($options->{stage}) {
    foreach my $p (@pfns) {
      $self->info("Staging the file $p");
      eval{
	my $url=AliEn::SE::Methods->new($p);
	$url->stage();
      };
      if ($@){
	$self->info("Error staging the file $p");
      }
    }
  }

  if (! $options->{noiomethod}){
    $self->info( "Got the pfns:  @pfns");
    my @newPfns;
    foreach my $p (@pfns) {
      my $newPfn=$self->checkIOmethod($p, $ioMethod, $seName) 
	or $self->info("Error checking the pfn $p") and next;
      push @newPfns, $newPfn;
    }
    @newPfns or $self->info("Error translating the pfns") and return (-1, "Error checking the iomethods");
    @pfns=@newPfns;

  }

  $self->info("Giving back @pfns");
  return @pfns;
}

sub checkIOmethod {
  my $self=shift;
  my $pfn=shift;
  my $clientMethods=shift || [];
  my $seName=shift;
  $self->debug(1,"Checking if we have to return $pfn");
  my @methods;
  if ($seName){
    $self->info("We are doing it for the se $seName");
    $self->{IODAEMONS}->{$seName} and 
      @methods=@{$self->{IODAEMONS}->{$seName}};
  }
  if (!@methods){
    $self->{IODAEMONS}->{default} or $self->info("Warning: we don't have any iomethods") and return $pfn;
    @methods=@{$self->{IODAEMONS}->{default}};
  }

  if (@$clientMethods) {
    $self->info("The client only supports @$clientMethods (comparing with @methods)");
    my @found;
    foreach my $method (@$clientMethods){
      $DEBUG and $self->debug("Checking $method in @methods");
      foreach (@methods){
	$_->{name} eq $method and push @found, $method;
      }
    }
    @methods=@found;
  }
  
  @methods  or $self->info("There are no shared io methods between the client and the server!! (this server understands: @{$self->{CONFIG}->{SE_IODAEMONS_LIST}}) Let's hope it knows how to get $pfn") and return $pfn;
  $self->debug(1,"Trying to convert $pfn to $methods[0]->{name}");
  $pfn=~ /^(file)|(castor)/ or $self->info("The pfn doesn't look like a local file... lets' return it the way it is") and return $pfn;
  my $method=$methods[0]->{protocol};
  my $port=$methods[0]->{port};
  my $host=$methods[0]->{host} || $self->{CONFIG}->{HOST};
  defined $port and $port=":$port";
  ($pfn !~ /^srm/ || $method eq "root") and 
    $pfn=~ s{^[^:]*://[^/]*}{$method://$host$port/};
  $self->debug(1,"Let's return $pfn");
  return $pfn;
}

# If Monitoring is enabled, send the SE info (size [MB], used [MB], free [MB], usage [%], nfiles [#])
sub sendApMonInfo {
  my $infoSE = shift;
  my $nfiles = shift;
  
  if($self->{MONITOR}){
    my $free = $infoSE->{freespace} / 1024.0;
    my $used = $infoSE->{usedspace} / 1024.0;
    my $size = $infoSE->{size} / 1024.0;
    my $usage = ($size > 0) ? (100 * $used / $size) : 100;
    # if nfiles is a hash ref, instead of a single number, just take the first value from it
    my @tmpNfiles = values(%$nfiles) if ref($nfiles) eq "HASH";
    $nfiles = $tmpNfiles[0] if @tmpNfiles;
    $self->{MONITOR}->sendParams("se_freespace", $free, "se_usedspace", $used, "se_totalsize", $size, "se_nfiles", $nfiles, "se_usage", $usage);
  }
}

return 1;

__END__

=head1 NAME

SE::SE - AliEn Storage Element

=head1 SYNOPSIS

=over 4


=item new()

=item startListening()

=item quit()

=item copyFile($pfn, $oldSE, $user)

=item startChecking()

=item getRoute ($source,$destination,$size)

=item doTransfer($file,$nexthost,$method,$rdir)

=item askForTransfer($size)

=item requestTransfer($file,$destURL,$priority)

=item getInfo()

=item verifyTransfer($ID,$filename,$size,$remotedir,$sourceURL,$endstation_flag,$destURL)

=item failTransfer($ID,$size)

=item checkTransfers()

=back


=head1 DESCRIPTION

The File Transfer Daemon (FTD) is the part of AliEn that can transport potentially very large files from one site to another. To the user the tranportation method is unknown, and the actual method used for the tranfser is determined on the fly depending on where the file is tranported from and to and also how big the file is. Currently only two methods are implemented, namely bbftp and gsiftp, but one could imagine several other methods like GridFTP, SOAP-Transport or even mailing the file. To transfer a file from A to B, two FTDs are needed, one at A and another at  B. Note that the FTD at B could possibly be located at a different host than the actual destination as long as they share filesystem (To check that the file arrived). For example to transport a file to wacdr001d.cern.ch, a FTD could be positioned at lxplus.cern.ch. The only modification is that a route should be inserted into the routing table, to specify that lxplus.cern.ch controls transfers to wacdr001d.cern.ch. This could be usefull if the actual ftp-damond (in case of ftp tranfer method) is running on a machine where a FTD can not be started.

The Daemon consists of several methods, some of them can be accesed with Remote Procedure Call via SOAP. The user-application, will usually only need one method, namely requestTransfer (see Requesting filetransfers).

When a user requests a transfer of a file via requestTransfer, the FTD checks that the file actually exists. If this is the case, an entry is made into a local database specifying information abaout the file; where its going, from where it originally came, filesize and various other information. The FTD will then return that the file is transfered. Every minute the FTD wakes up and checks if it has any filetransfer waiting. If no it simply sleeps again. If a file is in WAITING state, it calls the Information Service (IS), to request a route to the final destination. This is done via a SOAP call to the IS. In most cases the transfer will go directly to the destination, but if IS determines which host to do the transfer to. The FTD then calls the FTD located at the next host (as determined by IS). It asks if it can do a transfer of the given size. The remote FTD will then decide based on local disk space and  number of running transfers if a transfer is granted at the momment. If the transfer is not granted, the FTD at the source will try again the next time it wakes up. If the transfer is granted, the Remote FTD will also return which directory it uses for temporary files. This is used if this host is not the final destination. Then a call to doTransfer is made and the tranfers is done. When asking the IS for a route, also the method of transportation is determined. If the IS can not be contacted, direct route with method BBFTP is attempted. The transfer is now done, and afterwards a call to the RemoteFTD is made to verify the transfer. If the RemoteFTD was not the final destination, this FTD will call requestTransfer, to schedule the transfer to the final destination. If it happended to be the final destination, a SOAP-call is made to the original FTD, to say that the file has reached its final destination. This FTD, then marks the file as DONE, and send an email to the user that requested the transfer (if specified).

=head1 USAGE


=head2 Starting FTD

To start the File Transfer Daemon, execute 'alien StartFTD'. This will create an instance of FTD and then fork. The father will call FTD::startListening, which will start listening for incoming SOAP requests. The child process will call FTD::startChecking. This method will simply sleep for 60 seconds and then do FTD::checkTransfers(). It will loop forever.

When a site starts a clustermonitor L<DBQueue::ClusterMonitor> an FTD is automatically created.

=head2 Requesting filetransfers

If a file needs tranport from one site to another, the application or user simply asks the FTD where the file is located to send the file to its destination. This is done with method requestTransfer($file,$desturl,$priority,$email). At the momment $file is the absolute path (Where the FTD is running) of the file to transfer. The $desturl is the destination to put the file in. An example would be //wacdr001d.cern.ch/alice/simulation/2001-01/00001/. This will put $file (only the file itself not the path) to directory (alice/simulation/2001-01/00001/ on host wacdr001d.cern.ch. The priority can be used to specify how important this transfer is, use values between 0 and 10 where 10 is least important. 0 Means that the transfer will be done interactively and the FTD will block until the file is transferedsuccesfully. *REMARK* This is not implented yes, so $priority will have not meaning. The paramter $email is optional. If an email-adress is specified, an email is sent to this adress upon succesfull transfer. The email will contain information about  the transfer (file, destination, speed etc.).x

=head1 TODO

=head2 Mass storage support

At the momment when transfering to and into CASTOR at cern method BBFTP is used since it can handle rfio. Intead the call to request transfer should not be with a filename, but with a file url like castor:/PATH_TO_FILE, HPSS://hpssalice.ccin2p3.fr/PATH_TO_FILE_ON_HPSS. Then the FTD should figure out how to retrieve the file from Mass storage before transporting the file. Also at the final destination, we would like to be able to transfer to mass storage.

=head2 Priority

The notion of priority, and interactive transfer should be implemented. 



=head2 

=cut

