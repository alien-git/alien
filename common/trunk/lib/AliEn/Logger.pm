package AliEn::Logger;

use strict;
select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;
use vars qw(@ISA $ERROR_NO $ERROR_MSG $DEBUG_LEVEL);
use AliEn::MSS::file;

use Fcntl;

my $self;
$ERROR_MSG = "";
$DEBUG_LEVEL=0;
$ERROR_NO=0;
my $TRACELOG=0;

my $INFO_LEVELS={debug=>0,info=>1, notice=>2,warning=>3,error=>4,
		 critical=>5,alert=>6, emergency=>7};
sub new {
  my $proto = shift;
  ($self) and return $self;


  $self=(shift or {});
  $self->{LEVEL}=1;

  $self->{LEVELNAME}="info";
  defined $self->{logagent} or $self->{logagent}=1;
  # Let's initialize
  if ( $self->{logagent}){
    $proto->_initializeAgent($self) or return;
  }else {
    $proto->_initializeDispatch($self) or return;
  }
  if ($self->{logfile}){
    $self->redirect($self->{logfile}) or return;
  }
  open SAVEOUT,  ">&STDOUT";
  open SAVEOUT2, ">&STDERR";

#to avoid typo warning
  open SAVEOUT,  ">&STDOUT";
  open SAVEOUT2, ">&STDERR";

  $self->{KEEP_MESSAGES}=0;
  $self->{MESSAGES}=[];

  return $self;
}
sub _initializeAgent{
  my $proto=shift;
  $self=shift;
  require Log::Agent;
  bless($self, (ref($proto) || $proto));
  return 1;
}
sub _initializeDispatch{
  my $proto=shift;
  my $options=shift;

  require Log::Dispatch;
  require Log::Dispatch::Screen;
#  require Log::Dispatch::File;
  require AliEn::Logger::SoapLog;
  require AliEn::Logger::Local;
  require AliEn::Logger::Error;
  @ISA=qw(Log::Dispatch);
  $self = $proto->SUPER::new;
  foreach (keys %$options){
    $self->{$_}=$options->{$_};
  }
#  my $soapObj = AliEn::Logger::SoapLog->new(
#					    name      => 'Remote',
#					    min_level => 'critical'
#					   );
#  ($soapObj) or return;
#  
#  $self->add($soapObj);
  $self->add(
	     AliEn::Logger::Local->new(
				       name      => 'Local',
				       min_level => 'debug'
				      )
	    )
    or return;

  return 1;
}
# This subroutine redirects all the output of the messages sent to the logger, STDOUT and
# STDERR to another file. If the file is empty, it redirects them to the original STDOUT
#
#
sub redirect{
  my $self=shift;
  my $file=shift;
  if ($file) {
    #Redirecting to a file
    my $dir=$self->{logfile}=$file;
    $dir=~ s/\/[^\/]*$//;
    if (! -d $dir)  {
      AliEn::MSS::file::mkdir($self, $dir) 
	  and print "Error creating the directory $dir\n" and return;
    }
    if ( $self->{logagent}){
      $self->_initializeAgentRotate() or return;
    } else{
      $self->_initializeDispatchRotate() or return;
    }
    if ( !sysopen STDOUT, "$self->{logfile}", O_SYNC|O_APPEND|O_WRONLY |O_NONBLOCK  | O_CREAT ) {
#      open STDOUT, ">&SAVEOUT";
      die "stdout $self->{logfile} not opened!!";
    }
    if ( !open( STDERR, ">&STDOUT" ) ) {
#      open STDOUT, ">&SAVEOUT";
#      open STDERR, ">&SAVEOUT2";
      print STDERR "Could not open stderr file\n";
      die;
    }
    $self->{logfile_size}=-1;
  }else {
    $self->{logfile}="";
    #Redirecting to the original STDOUT
    open STDOUT, ">&SAVEOUT";
    open STDERR, ">&SAVEOUT2";
    if ($self->{logagent}) {
      require Log::Agent::Rotate;
      require Log::Agent::Driver::File;
      require Log::Agent::Driver::Default;

      my $driver=Log::Agent::Driver::Default->make();
      Log::Agent::logconfig(-driver => $driver);

    }else {
      $self->remove('Local');
      $self->add( AliEn::Logger::Local->new(name      => 'Local',
					    min_level => 'debug' ));
    }
  }
  return 1;
}
 

sub keepAllMessages{
  my $self=shift;
  $self->{KEEP_MESSAGES}=1;
  $self->{MESSAGES}=[];
}

sub displayMessages{
  my $self=shift;
  $self->{KEEP_MESSAGES} = 0;
  $self->{MESSAGES} = [];
}

sub _initializeAgentRotate{
  my $self=shift;
  require Log::Agent::Rotate;
  require Log::Agent::Driver::File;

  my $rotate_dflt = Log::Agent::Rotate->make(
					     -backlog     => 7,
					     -unzipped    => 2,
					     -is_alone    => 0,
					     -max_size    => 1000000,
					    );

  my $driver = Log::Agent::Driver::File->make(
					      -channels => {
							    'error'  => "$self->{logfile}_err",
							    'output' => ["$self->{logfile}", $rotate_dflt],
							   },
					      -stampfmt=>"none", 
					     );
  Log::Agent::logconfig(-driver => $driver);
  return 1;
}
sub _initializeDispatchRotate{
  my $self=shift;
  require  Log::Dispatch::FileRotate;
  $self->remove('Local');
  $self->add( Log::Dispatch::FileRotate->new( name      => 'Local',
                                              min_level => 'debug',
                                              filename  => $self->{logfile},
                                              mode      => 'append' ,
					      size      => 1000000,
                                              max       => 6,
					    ));
  return 1;
}

sub removeSOAPLogger{
  my $self=shift;
#  $self->remove('Remote');
}
sub setMinimum {
  my $self=shift;
  my $level=shift;

  if ($level and defined $INFO_LEVELS->{$level}) {
    $self->{LEVEL}=$INFO_LEVELS->{$level};
    $self->{LEVELNAME}=$level;
  }

  return 1;
}
sub infoToSTDERR {
  my $self=shift;
  ( $self->{logagent}) and return 1;

  $self->remove('Local');
  $self->remove('Error');

  my $soapObj = AliEn::Logger::Error->new(
					  name      => 'Error',
					  min_level => 'info'
					 );
  ($soapObj) or return;

  $self->add($soapObj);
  return 1;
}
sub debugToSTDERR {
  my $self=shift;
  ( $self->{logagent}) and return 1;

  $self->remove('Local');
  $self->remove('Error');

  my $soapObj = AliEn::Logger::Error->new(
					  name      => 'Error',
					  min_level => 'debug'
					 );
  ($soapObj) or return;

  $self->add($soapObj);  return 1;
}

my %external=(SSL => '$IO::Socket::SSL::DEBUG',
#	      Authen=>'$Authen::AliEnSASL::Perl::Baseclass::DEBUG'
#	      Verifier =>'$AliEn::Authen::Verifier::DEBUG',
#	     'ClientVerifier' => '$AliEn::Authen::ClientVerifier::DEBUG',
#	      'Database'=>'$AliEn::Database::DEBUG',
#	      'Catalogue'=>'$AliEn::Catalogue::DEBUG'
);


sub getDebugLevel {
  my $self=shift;

  return $DEBUG_LEVEL;
}

sub debugOn() {
  my $self = shift;

  my @modules=@_;
  my ($level)=grep (/^[0-9]+$/, @_);
  defined $level or $level=5;

  @modules and @modules= grep (!/^\d+$/, @modules);
  $modules[0] and  $modules[0]=~ /,/ and @modules=split (",", $modules[0]);
  $DEBUG_LEVEL=$level;
  foreach my $m (@modules) {
    foreach my $object (grep (/^AliEn::${m}(::)?/, 
			      keys %{$self->{LOG_OBJECTS}})) {
      $self->{LOG_OBJECTS}->{$object}=$level;
      eval "\$$object\::DEBUG=$level";
      $@ and print "ERROR $@\n";
    }
  }
  if (! @modules) {
    foreach my $object (keys %{$self->{LOG_OBJECTS}}) {
      $self->{LOG_OBJECTS}->{$object}=$level;
      eval "\$$object\::DEBUG=$level";
    }
  }
  foreach my $key (keys %external ){
    if (grep (/^$key$/, @modules) ) {
      $self->info("Logger", "Debugging $key");
      my $module=$external{$key};
      $module=~ s/^\$(.*)::[^:]*$/$1/;
      eval "require $module";
      if ($@) {
	$self->info("Logger", "Error requiring $module: $@");
      } else {
	eval "$external{$key}=4";
      }
    }
  }

  grep (/^SOAP::Lite$/, @modules) and SOAP::Lite->import('trace' );

  $self->setMinimum("debug");
}
sub silentOn() {
  my $self=shift;

  $self->{SILENT_PREVIOUS}=$self->{LEVELNAME};
  $self->setMinimum("emergency");
}
sub silentOff() {
  my $self=shift;
  my $mode= ($self->{SILENT_PREVIOUS} or "info");
  $self->setMinimum( $mode );
}

sub tracelogOn{
  my $self=shift;
  $TRACELOG=1;
  return 1;
}

sub tracelogOff{
  my $self=shift;
  $TRACELOG=0;
  return 1;
}

sub getTracelog{
  my $self=shift;
  return $TRACELOG;
}




sub debugOff {
  my $self = shift;

  $DEBUG_LEVEL=0;

  foreach (keys %external ){
    eval "$external{$_}=0";
  }

  foreach my $object (keys %{$self->{LOG_OBJECTS}}) {
    $self->{LOG_OBJECTS}->{$object}=0;
    eval "\$$object\::DEBUG=0";
  }

  $self->setMinimum( "info");
}
sub debug() {
  my $self=shift;
  $self->display("debug", @_);
  return 1;
}

sub info() {
  my $self   = shift;
  $self->display("info", @_);
  return 1;
}
sub notice() {
  my $self   = shift;
  $self->display("notice", @_);
  return 1;
}
sub warning() {
  my $self   = shift;
  $self->display("warning", @_);
  return 1;
}
sub error() {
  my $self   = shift;
  my $target=shift;
  my $message =shift;
  my $error = (shift or 1);
  $self->display("error", $target,$message, $error, @_);
  return 1;
}
sub critical() {
  my $self   = shift;
  my $target=shift;
  my $message =shift;
  my $error = (shift or 6);
  $self->display("critical", $target,$message, $error, @_);
  return 1;
}
sub alert() {
  my $self   = shift;
  my $target=shift;
  my $message =shift;
  my $error = (shift or 66);
  $self->display("alert", $target,$message, $error, @_);
  return 1;
}
sub emergency() {
  my $self   = shift;
  my $target=shift;
  my $message =shift;
  my $error = (shift or 666);
  $self->display("emergency", $target,$message, $error, @_);
  return 1;
}

sub getMessages {
  my $self=shift;
  my $dlevel = ($TRACELOG ? "notice" : "error");
  my $level = (shift || $dlevel);
  (($DEBUG_LEVEL) or ($level eq "debug")) and return \@{$self->{MESSAGES}};

  $level =~ s/error/(error)/;
  $level =~ s/notice/(notice)|(error)/;
  $level =~ s/\(?error\)?/(error)|(critical)/;
  my @list = grep (/^((\S+\s+){3})$level\s+/, @{$self->{MESSAGES}});
  return \@list;
}

sub display {
  my $self=shift;
  my $level=shift;

  my $target = shift;
  my $msg    = shift;
  my $errno   = (shift or "");
  my $format = shift;
  defined $format or $format= 1;

  if ($errno) {
#    $self->set_error_msg( $msg);
    $ERROR_NO=$errno;
    $ERROR_MSG = $msg;
    $ERROR_MSG =~ s/^\s*error\s*:?//i;
    chomp ($ERROR_MSG);
  }

  $INFO_LEVELS->{$level}<$self->{LEVEL} and return 1;


  if ( $format) {
    my $date =localtime;
    $date =~ s/^\S+\s((\S+\s+){3}).*$/$1/	;
    $msg="$date $level\t$msg";
 }
  if ($self->{KEEP_MESSAGES}){
    $format and $msg.="\n";
    push @{$self->{MESSAGES}}, $msg;
    return 1;
  }
  if ($self->{logagent}){
    $msg=~ s{\%}{\%\%}g;
    Log::Agent::logsay(  $msg);
    ($INFO_LEVELS->{$level}>$INFO_LEVELS->{notice})
      and Log::Agent::logwarn($msg);
  }else {

    my $d="SUPER::$level";
    $self->$d("$msg\n");
  }
  if ($self->{logfile}){
    my $size= -s $self->{logfile};
    if ($size <$self->{logfile_size}) {
      if ( !sysopen STDOUT, "$self->{logfile}", O_SYNC|O_APPEND|O_WRONLY |O_NONBLOCK  | O_CREAT ) {
#	open STDOUT, ">&SAVEOUT";
	die "stdout in $self->{logfile} not opened!!";
      }
      
      if ( !open( STDERR, ">&STDOUT" ) ) {
#	open STDOUT, ">&SAVEOUT";
#	open STDERR, ">&SAVEOUT2";
#	print STDERR "Could not open stderr file\n";
	die "Could not open stderr file";;
      }
    }
    $self->{logfile_size}=$size;
  }
  return 1;
}

sub error_msg {
  return $ERROR_MSG;
}
sub getMode {
  return $self->{LEVELNAME};
}
sub set_error_msg{
  my $self=shift;
  my $message=(shift or "");
  $ERROR_MSG=$message;
}
sub error_no{
  return $ERROR_NO;
}

sub set_error_no{
  my $self=shift;
  $ERROR_NO=(shift or 0);
}

sub reset_error_msg{
  my $self=shift;
  $ERROR_MSG="";
  $ERROR_NO=0;
  return 1;
}

1;

__END__

=head1 NAME

Logger - The AliEn logger module

=head1 SYNOPSIS

=over 4

=item Logger->new();

=item Logger->add(Log::Dispatch::* OBJECT);

=item Logger->debug("Source","Message");

=item Logger->info("Source","Message", [<error number>]);

=item Logger->notice("Source","Message", [<error number>]);

=item Logger->warning("Source","Message", [<error number>]);

=item Logger->error("Source","Message", [<error number>]);

=item Logger->critical("Source","Message", [<error number>]);

=item Logger->alert("Source","Message", [<error number>]);

=item Logger->emergency("Source","Message", [<error number>]);

=item Logger->debugOn();

=item Logger->debugOff();

=back

=head1 DESCRIPTION

This is the main logger module in AliEn. It will redirect all messages of level 'warning' and above to central SOAP server, which will collect all messages to a central logging facillity, and take further action. Logger->debugOn and Logger->debugOff will turn debuggin to screen on or off respectively. By default its off.

The source parameter specifies from which service the message is from. This could be ProxyServer, AuthenDaemon, TFN, ClusterMonitor, or any service. The loggin will itself figure out from which host the message came from.

Use debug only for real debugging messages (like "In Verifier with @_") and that sort. Use info or notice for higher level messages. Only use emergency if this error is fatal for the whole alien system (erasing databases, or something like that). Use critical if the error is critical for the given service. 

This class is singleton, meaning that there will only be one instance. If there is alredy one instance, then a reference to that will be returned.

=cut


