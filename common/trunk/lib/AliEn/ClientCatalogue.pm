package AliEn::ClientCatalogue;

use AliEn::Catalogue;
use strict;

use vars qw(@ISA);

@ISA = ('AliEn::Catalogue',@ISA);

sub new {
  my $proto   = shift;
  my $class   = ref($proto) || $proto;
  my $self    = {};
  my $options = shift;
  bless( $self, $class );
  $self->{LOGGER} = new AliEn::Logger;
  $self->{CONFIG} = new AliEn::Config($options);
  $self->{SOAP} = new AliEn::SOAP
    or print "Error creating AliEn::SOAP $! $?" and return;
  $self->{UMASK} = 0755;
  $self->{DISPPATH} = "$self->{CONFIG}->{USER_DIR}/".substr($self->{CONFIG}->{ROLE},0,1)."/$self->{CONFIG}->{ROLE}";
  $self->f_cd("$self->{DISPPATH}")
    or $self->info("Home directory for $self->{CONFIG}->{ROLE} does not exist")
    and return;
  $self->{GLOB}=1;
  return $self;
}

sub getHost{
   my $self=shift;
   return $self->{CONFIG}->{CATALOG_HOST};
}

sub callAuthen {
  my $self = shift;
  $self->info("Calling Authen over SOAP");
  my $user=$self->{CONFIG}->{ROLE};
  if($_[0] =~ /^-user=([\w]+)$/)  {
    $user = shift;
    $user =~ s/^-user=([\w]+)$/$1/;
  }
  my $info = 0;
  
  for (my $tries = 0; $tries < 5; $tries++) { # try five times
    $info=$self->{SOAP}->CallSOAP("Authen", "doOperation", $user, @_) and last;
    $self->info("Sleeping for a while before retrying...");
    sleep(5);
  }
   ($info,my  @out)=$self->{SOAP}->GetOutput($info);
  
  $info->{message} and $self->info($info->{message},undef, 0);
  if ($info->{ok}){
    print "The call worked!\n";
  } 
  return @out;
 # $info or $self->info("Connecting to the [Authen] service failed!") 
 #  and return ({error=>"Connecting to the [Authen] service failed!"}); 
}

sub f_mkdir {
  my $self = shift;
  my ($options,$path) = @_;
  $options and $options=" -$options";
  $path = $self->GetAbsolutePath($path);
  $self->info("Making directory '$path'");
  return $self->callAuthen("mkdir","$path$options" );
}
sub f_getTabCompletion {
  my $self=shift;
  $self->info("WE ARE CHECKING THE TAB WITHOUT DATABASE");
  my @e=$self->callAuthen("tabCompletion", @_);
  $self->info("Hello world, we got @e");
  return @e;
}
sub f_removeFile {
  my $self = shift;
  my ($options,$path) = @_;
  $path = $self->GetAbsolutePath($path);
  $self->info("Removing $path");
  my $env = $self->callAuthen("rm","$options","$path");
  return $env;
}

sub f_rmdir {
  my $self = shift;
  my ($options,$path) = @_;
  $path = $self->GetAbsolutePath($path);
  $self->info("Removing directory $path");
  my $env = $self->callAuthen("rmdir","$options","$path");
  $self->info("From Authen: $env");
  return $env;
}

sub f_ls {
  my $self=shift;
  my $options = shift;
  my $path    = ( shift or $self->{DISPPATH} ); 
  $options and $options=" -$options";
 
  return  $self->callAuthen("ls", "$path$options");
}

sub f_mv {
  my $self = shift;
  my ($options,$source,$target) = @_;
  $target = $self->GetAbsolutePath($target);
  $source = $self->GetAbsolutePath($source);
  $self->info("Moving $source to $target");
  my $env = $self->callAuthen("mv","$options","$source","$target");
  $self->info("From Authen: $env");
  return $env;
}

sub f_touch {
  my $self = shift;
  my ($options,$path) = @_;
  $path = $self->GetAbsolutePath($path);
  $self->info("Creating file $path");
  my $env = $self->callAuthen("touch","$options","$path");
  $self->info("From Authen: $env");
  return $env;
}

sub f_ln {
  my $self = shift;
  my ($options,$source,$target) = @_;
  $target = $self->GetAbsolutePath($target);
  $source = $self->GetAbsolutePath($source);
  $self->info("Moving $source to $target");
  my $env = $self->callAuthen("ln","$options","$source","$target");
  $self->info("From Authen: $env");
  return $env;
}

sub f_addTag {
  my $self = shift;
  $_[0] = $self->GetAbsolutePath($_[0]);
  my $env = $self->callAuthen("removeTag",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_addTagValue {
  my $self = shift;
  $_[1] = $self->GetAbsolutePath($_[1]);
  my $env = $self->callAuthen("addTagValue",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_updateTagValue {
  my $self = shift;
  $_[1] = $self->GetAbsolutePath($_[1]);
  my $env = $self->callAuthen("updateTagValue",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_removeTag {
  my $self = shift;
  $_[0] = $self->GetAbsolutePath($_[0]);
  my $env = $self->callAuthen("removeTag",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_removeTagValue {
  my $self = shift;
  $_[0] = $self->GetAbsolutePath($_[0]);
  my $env = $self->callAuthen("removeTagValue",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_cleanupTagValue {
  my $self = shift;
  $_[0] = $self->GetAbsolutePath($_[0]);
  my $env = $self->callAuthen("cleanupTagValue",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_cd {
  my $self = shift;
  my $path = shift;
  (defined $path) or ($path = $self->GetHomeDirectory());
  $path = $self->GetAbsolutePath($path);
  my $env = $self->callAuthen("checkPermissionOnDirectory",$path);
  $env 
    or $self->info("You do not have permissions in $path")
    and return;
  $self->{DISPPATH} = $path;
  return 1;
}

sub authorize{
  my $self = shift;

  #
  # Start of the Client side code
  my $user=$self->{CONFIG}->{ROLE};
  $self->{CATALOG} and $self->{CATALOG}->{ROLE} and $user=$self->{CATALOG}->{ROLE};

  if($_[0] =~ /^-user=([\w]+)$/)  {
    $user = shift;
    $user =~ s/^-user=([\w]+)$/$1/;
  }
  #gron: isn't the following working and better:
  #($_[0] =~ /^-user=([\w]+)$/) and $user=$1 and shift;


  $self->info("Connecting to Authen...");
  my $info=0;
  for (my $tries = 0; $tries < 5; $tries++) { # try five times 
    $info=$self->{SOAP}->CallSOAP("Authen", "consultAuthenService", $user, @_) and last;
    $self->info("Connecting to the [Authen] service was not seccussful, trying ".(4 - $tries)." more times till giving up.");
    sleep(5);
  }
  $info or $self->info("Connecting to the [Authen] service failed!")
     and return ({error=>"Connecting to the [Authen] service failed!"});
  #my @authorize=$self->{SOAP}->GetOutput($info);
  #my $hash = shift @authorize; 

#  my @hasha =$self->{SOAP}->GetOutput($info);
#  my $hash=$hasha[0];
  my ($hash) =$self->{SOAP}->GetOutput($info);
  $self->info("gron: rc is $hash->{ok} and we got $hash->{envelopecount} envelopes...");
#  my $hash = shift @authorize; 

  my $messtype="info";
  my $message="Authorize replied ";
  if($hash->{ok}) {
    $message .= "[OK]. ";
  } else {
    $messtype="error";
    $message .= "[ERROR]. ";
  }
  $hash->{interrupt} and $message .= "Thrown [INTERRUPT]! " ;
  $hash->{message} and  $message .= "Message: $hash->{message} ";
#  $self->tracePlusLogError($messtype,$message);

 return $hash;
}


return 1;
__END__
