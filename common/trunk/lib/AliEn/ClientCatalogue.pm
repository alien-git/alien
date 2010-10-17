package AliEn::ClientCatalogue;

use AliEn::Catalogue;
use strict;

use vars qw(@ISA);

#@ISA = ('AliEn::Catalogue',@ISA);

sub new {
  my $proto   = shift;
  my $class   = ref($proto) || $proto;
  my $self    = {};
  my $options = shift;
  bless( $self, $class );
  if((defined $options->{user}) and !(defined $options->{role})) {
    $options->{role} = $options->{user};
  }
  $self->{ROLE} = $options->{role};
  $self->{LOGGER} = new AliEn::Logger;
  $self->{CONFIG} = new AliEn::Config($options);
  $self->{SOAP} = new AliEn::SOAP
    or print "Error creating AliEn::SOAP $! $?" and return;
  $self->{UMASK} = 0755;
  $self->{DISPPATH} = "$self->{CONFIG}->{USER_DIR}/".substr($self->{CONFIG}->{ROLE},0,1)."/$self->{CONFIG}->{ROLE}/";
  $self->f_cd("$self->{DISPPATH}")
    or $self->info("Home directory for $self->{CONFIG}->{ROLE} does not exist or you do not have permissions")
    and return;
  $self->{GLOB}=1;
  $self->{ROLE}=$options->{role} || $options->{ROLE} || $self->{CONFIG}->{ROLE};
  return $self;
}

sub getHost{
   my $self=shift;
   return $self->{CONFIG}->{CATALOG_HOST};
}


sub callAuthen {
  my $self = shift;

  my $user=$self->{ROLE};
  if($_[0] =~ /^-user=([\w]+)$/)  {
    $user = shift;
    $user =~ s/^-user=([\w]+)$/$1/;
  }
  
  $self->{LOGGER}->getDebugLevel() and push @_, "-debug=".$self->{LOGGER}->getDebugLevel();
  $self->{LOGGER}->getTracelog() and push @_, "-tracelog";
  return $self->{SOAP}->CallAndGetOverSOAP("Authen", "doOperation", $user, $self->{DISPPATH},  @_);
}

sub f_cd {
  my $self = shift;
  my $path = shift;
  (defined $path) 
    or ($path = "$self->{CONFIG}->{USER_DIR}/".substr($self->{CONFIG}->{ROLE},0,1)."/$self->{CONFIG}->{ROLE}");
  #$path =~ s/\/$//;
  $path = AliEn::Catalogue::Basic::GetAbsolutePath($self, $path);
  my $env = $self->callAuthen("cd",$path);
  $env 
    or $self->info("You do not have permissions in $path")
    and return;
  $self->{DISPPATH} = "$path";
  return 1;
}

sub cleanArguments {
  my $self = shift;
 
  my @reply = ();
  foreach (@_) {$_ ne "" and push @reply, $_ ;}
  return @reply;
}

sub AUTOLOAD {
  my $name = our $AUTOLOAD;
  $name =~ s/.*::(f_)?//; 
   
  my $ops={"ls"=>"ls","isFile"=>"isFile", "isDirectory"=>"isDirectory", "getLFNlike"=>"getLFNlike",
  authorize=>"authorize", checkPermission=>"checkGUIDPermissions", checkPermissions=>"checkLFNPermissions",
  removeExpiredFiles=>"removeExpiredFiles",renumber=>"renumberDirectory",showStructure=>"showStructure",
  setExpired=>"setExpired",removeTrigger=>"removeTrigger", du=>"du", stat=>"stat", 
  guid2lfn=>"guid2lfn",lfn2guid=>"lfn2guid",expungeTables=>"expungeTables",showTrigger=>"showTrigger",
  mkremdir=>"mkremdir",zoom=>"zoom",tree=>"tree",type=>"type",  
  mkdir=>"mkdir",find=>"find",checkLFN=>"checkLFN",getTabCompletion=>"tabCompletion",
  removeFile=>"rm", rmdir=>"rmdir", whereis=>"whereis", mv=>"mv", touch=>"touch",
ln=>"ln", groups=>"groups", chgroup=>"chgroups", chmod=>"chmod",
chown=>"chown", addTag=>"addTag", addTagValue=>"addTagValue",updateTagValue=>"updateTagValue",
showTagValue=>"showTagValue", removeTag=>"removeTag", removeTagValue=>"removeTagValue",
cleanupTagValue=>"cleanupTagValue", showTags=>"showTags",
  };
  if ($ops->{$name}){
    return shift->callAuthen($ops->{$name},@_);
  } elsif ($name =~ /(ExpandWildcards)/){
    return AliEn::Catalogue::ExpandWildcards(@_);
  } elsif ($name =~ /(GetAbsolutePath)/){
    return AliEn::Catalogue::Basic::GetAbsolutePath(@_);
  } elsif ($name =~/DESTROY/){
    return;
  }
  print STDERR "The function $name is not defined in ClientCatalog!!\n";
}

sub getDispPath {
  my $self = shift;
  return $self->{DISPPATH};
}

return 1;
__END__
