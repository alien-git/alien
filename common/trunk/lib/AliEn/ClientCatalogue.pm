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
  $self->f_cd("$self->{DISPPATH}");
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

  my $user=$self->{CONFIG}->{ROLE};
  if($_[0] =~ /^-user=([\w]+)$/)  {
    $user = shift;
    $user =~ s/^-user=([\w]+)$/$1/;
  }
  return $self->{SOAP}->CallAndGetOverSOAP("Authen", "doOperation", $user, @_);
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

sub f_groups {
  my $self = shift;
  my $env = $self->callAuthen("groups",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_chgroup {
  my $self = shift;
  my $env = $self->callAuthen("chgroups",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_chmod {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  my $env = $self->callAuthen("chmod",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_chown {
  my $self = shift;
  my @args = @_;
  $args[2] = $self->GetAbsolutePath($args[2]);
  my $env = $self->callAuthen("chown",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_addTag {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("removeTag",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_addTagValue {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  my $env = $self->callAuthen("addTagValue",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_updateTagValue {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  my $env = $self->callAuthen("updateTagValue",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_removeTag {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("removeTag",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_removeTagValue {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("removeTagValue",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_cleanupTagValue {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("cleanupTagValue",@args);
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

sub f_mkremdir {
  my $self = shift;
  my @args = @_;
  if(defined $args[3]) {
    $args[3] = $self->GetAbsolutePath($args[3]);
  }
  else {
    $self->info("File not specified");
    return;
  }
  my $env = $self->callAuthen("mkremdir",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_zoom {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("zoom",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_tree {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("tree",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_type {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("type",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_du {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  my $env = $self->callAuthen("du",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_stat {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("stat",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_guid2lfn {
  my $self = shift;
  my $env = $self->callAuthen("guid2lfn",@_);
  $self->info("From Authen: $env");
  return $env;
}

sub f_lfn2guid {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  my $env = $self->callAuthen("lfn2guid",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub expungeTables {
  my $self = shift;
  my $env = $self->callAuthen("expungeTables");
  $self->info("From Authen: $env");
  return $env;
}

sub f_showTrigger {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  my $env = $self->callAuthen("showTrigger",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_removeTrigger {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[1]);
  my $env = $self->callAuthen("removeTrigger",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_setExpired {
  my $self = shift;
  my ($sec,@files) = @_;
  map{$_ = $self->GetAbsolutePath($_)}@files;
  my $env = $self->callAuthen("setExpired",$sec,@files);
  $self->info("From Authen: $env");
  return $env;
}

sub f_showStructure {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  my $env = $self->callAuthen("showStructure",@args);
  $self->info("From Authen: $env");
  return $env;
}

sub f_renumber {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  my $env = $self->callAuthen("renumberDirectory",@args);
  $self->info("From Authen: $env");
  return $env;
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


  return $self->{SOAP}->CallAndGetOverSOAP("Authen", "consultAuthenService", $user, @_);
}


return 1;
__END__
