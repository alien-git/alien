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
  if((defined $options->{user}) and !(defined $options->{role})) {
    $options->{role} = $options->{user};
  }
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

  my $user=$self->{CONFIG}->{ROLE};
  if($_[0] =~ /^-user=([\w]+)$/)  {
    $user = shift;
    $user =~ s/^-user=([\w]+)$/$1/;
  }
  
  $self->{LOGGER}->getDebugLevel() and push @_, "-debug=".$self->{LOGGER}->getDebugLevel();
  $self->{LOGGER}->getTracelog() and push @_, "-tracelog";

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
  my $e=$self->callAuthen("tabCompletion", @_);
  $self->info("Hello world, we got $e");
  return $e;
}

sub f_removeFile {
  my $self = shift;
  my ($options,$path) = @_;
  $path = $self->GetAbsolutePath($path);
  $self->info("Removing $path");
  return $self->callAuthen("rm","$options","$path");
}

sub f_rmdir {
  my $self = shift;
  my ($options,$path) = @_;
  $path = $self->GetAbsolutePath($path);
  $self->info("Removing directory $path");
  return $self->callAuthen("rmdir","$options","$path");
}

sub f_ls {
  my $self=shift;
  my $options = shift;
  my $path    = ( shift or $self->{DISPPATH} ); 
  $options and $options=" -$options";
 
  my @list = $self->callAuthen("ls", "$path$options");
  if($options =~ /l/) {
    map { $_ =~ s/###/\t/g} @list;
  }
  map {print STDOUT $_."\n"} @list;
  return @list;
}

sub f_whereis {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  return $self->callAuthen("whereis",@_);
}

sub f_mv {
  my $self = shift;
  my ($options,$source,$target) = @_;
  $target = $self->GetAbsolutePath($target);
  $source = $self->GetAbsolutePath($source);
  $self->info("Moving $source to $target");
  return $self->callAuthen("mv","$options","$source","$target");
}

sub f_touch {
  my $self = shift;
  my ($options,$path) = @_;
  $path = $self->GetAbsolutePath($path);
  $self->info("Creating file $path");
  return $self->callAuthen("touch","$options","$path");
}

sub f_ln {
  my $self = shift;
  my ($options,$source,$target) = @_;
  $target = $self->GetAbsolutePath($target);
  $source = $self->GetAbsolutePath($source);
  $self->info("Moving $source to $target");
  return $self->callAuthen("ln","$options","$source","$target");
}

sub f_groups {
  my $self = shift;
  return $self->callAuthen("groups",@_);
}

sub f_chgroup {
  my $self = shift;
  return $self->callAuthen("chgroups",@_);
}

sub f_chmod {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  return $self->callAuthen("chmod",@args);
}

sub f_chown {
  my $self = shift;
  my @args = @_;
  $args[2] = $self->GetAbsolutePath($args[2]);
  return $self->callAuthen("chown",@args);
}

sub f_addTag {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("removeTag",@args);
}

sub f_addTagValue {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  return $self->callAuthen("addTagValue",@args);
}

sub f_updateTagValue {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  return $self->callAuthen("updateTagValue",@args);
}

sub f_removeTag {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("removeTag",@args);
}

sub f_removeTagValue {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("removeTagValue",@args);
}

sub f_cleanupTagValue {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("cleanupTagValue",@args);
}

sub f_cd {
  my $self = shift;
  my $path = shift;
  (defined $path) 
    or ($path = "$self->{CONFIG}->{USER_DIR}/".substr($self->{CONFIG}->{ROLE},0,1)."/$self->{CONFIG}->{ROLE}");
  #$path =~ s/\/$//;
  $path = $self->GetAbsolutePath($path);
  my $env = $self->callAuthen("checkPermissionOnDirectory",$path);
  $env 
    or $self->info("You do not have permissions in $path")
    and return;
  $self->{DISPPATH} = "$path";
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
  return $self->callAuthen("mkremdir",@args);
}

sub f_zoom {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("zoom",@args);
}

sub f_tree {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("tree",@args);
}

sub f_type {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("type",@args);
}

sub f_du {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  return $self->callAuthen("du",@args);
}

sub f_stat {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("stat",@args);
}

sub f_guid2lfn {
  my $self = shift;
  return $self->callAuthen("guid2lfn",@_);
}

sub f_lfn2guid {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  return $self->callAuthen("lfn2guid",@args);
}

sub expungeTables {
  my $self = shift;
  return $self->callAuthen("expungeTables");
}

sub f_showTrigger {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  return $self->callAuthen("showTrigger",@args);
}

sub f_removeTrigger {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[1]);
  return $self->callAuthen("removeTrigger",@args);
}

sub f_setExpired {
  my $self = shift;
  my ($sec,@files) = @_;
  map{$_ = $self->GetAbsolutePath($_)}@files;
  return $self->callAuthen("setExpired",$sec,@files);
}

sub f_showStructure {
  my $self = shift;
  my @args = @_;
  $args[1] = $self->GetAbsolutePath($args[1]);
  return  $self->callAuthen("showStructure",@args);
}

sub f_renumber {
  my $self = shift;
  my @args = @_;
  $args[0] = $self->GetAbsolutePath($args[0]);
  return $self->callAuthen("renumberDirectory",@args);
}

sub removeExpiredFiles {
  my $self = shift;
  return $self->callAuthen("removeExpiredFiles");
}

return 1;
__END__
