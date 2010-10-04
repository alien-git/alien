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
  return $self;
}

sub getHost{
   my $self=shift;
   return $self->{CONFIG}->{CATALOGHOST};
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
  eval {
    for (my $tries = 0; $tries < 5; $tries++) { # try five times
      $info=$self->{SOAP}->CallSOAP("Authen", "doOperation", $user, @_) and last;
      $self->info("Sleeping for a while before retrying...");
      sleep(5);
    }
  };
  if (@_){
   print "ERROR @_\n";
   return;
  }
 # $info or $self->info("Connecting to the [Authen] service failed!") 
 #  and return ({error=>"Connecting to the [Authen] service failed!"}); 
  my $newhash=$self->{SOAP}->GetOutput($info);
  return $newhash;
}

sub f_mkdir {
  my $self = shift;
  my ($options,$path) = @_;
  $path = $self->GetAbsolutePath($path);
  $self->info("Making directory $path");
  my $env = $self->callAuthen("mkdir","$options","$path");
  return $env
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

return 1;
__END__
