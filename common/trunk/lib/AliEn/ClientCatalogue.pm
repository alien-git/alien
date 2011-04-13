package AliEn::ClientCatalogue;

use AliEn::Catalogue;
use strict;

use vars qw(@ISA);

@ISA = ('AliEn::Logger::LogObject', @ISA);

sub new {
  my $proto   = shift;
  my $class   = ref($proto) || $proto;
  my $self    = {};
  my $options = shift;
  bless($self, $class);
  if ((defined $options->{user}) and !(defined $options->{role})) {
    $options->{role} = $options->{user};
  }
  $options->{DEBUG}  = $self->{DEBUG}  = ($options->{debug}  or 0);
  $options->{SILENT} = $self->{SILENT} = ($options->{silent} or 0);
  $self->{LOGGER}    = new AliEn::Logger;
  $self->{CONFIG}    = new AliEn::Config($options);
  $self->{ROLE} = $options->{role} || $options->{ROLE} || $self->{CONFIG}->{ROLE};
  $self->{SOAP} = new AliEn::SOAP
    or print "Error creating AliEn::SOAP $! $?" and return;
  $self->{UMASK} = 0755;

  if ($ENV{ALIEN_PROC_ID} and $ENV{ALIEN_JOB_TOKEN}) {
    $self->info("We are authenticating with the job token...");
    my ($user) = $self->callAuthen("whoami", "-silent") or return;
    $self->{ROLE} = $self->{CONFIG}->{ROLE} = $user;
    $self->info("We are supposed to be $user");
  }
  $self->{DISPPATH} = $self->GetHomeDirectory();
  $self->f_cd("$self->{DISPPATH}")
    or $self->info("Home directory for $self->{CONFIG}->{ROLE} does not exist or you do not have permissions")
    and return;
  $self->{GLOB} = 1;

  $self->{SILENT} = $options->{silent} || 0;
  return $self;
}

sub getHost {
  my $self = shift;
  return $self->{CONFIG}->{CATALOG_HOST};
}

sub callAuthen {
  my $self = shift;
  my $user = $self->{ROLE};
  if ($ENV{ALIEN_PROC_ID} and $ENV{ALIEN_JOB_TOKEN}) {
    $user = "alienid:$ENV{ALIEN_PROC_ID} $ENV{ALIEN_JOB_TOKEN}";
  }

  if ($_[0] =~ /^-user=([\w]+)$/) {
    shift;
    $user = $1;
  }
  $self->{LOGGER}->getDebugLevel() and push @_, "-debug=" . $self->{LOGGER}->getDebugLevel();
  return $self->{SOAP}->CallAndGetOverSOAP($self->{SILENT}, "Authen", "doOperation", $user, $self->{DISPPATH}, @_);
}

sub f_cd {
  my $self = shift;
  my $path = shift;
  (defined $path)
    or ($path = $self->GetHomeDirectory());
  $path = AliEn::Catalogue::Basic::GetAbsolutePath($self, $path);
  my $env = $self->callAuthen("cd", $path);
  $env
    or $self->info("You do not have permissions in $path")
    and return;
  $self->{DISPPATH} = "$path";
  return 1;
}

sub f_pwd {
  return AliEn::Catalogue::f_pwd(@_);
}

#sub cleanArguments {
#  my $self = shift;
#
#  my @reply = ();
#  foreach (@_) {$_ ne "" and push @reply, $_ ;}
#  return @reply;
#}

sub f_quit {
  my $self = shift;
  $self->info("bye now");
  exit;
}

sub AUTOLOAD {
  my $name = our $AUTOLOAD;
  $name =~ s/.*::(f_)?//;


  my $ops = {
    addFileToCollection      => "addFileToCollection",
    addMirror                => "addMirror",
    addTag                   => "addTag",
    addTagValue              => "addTagValue",
    addUser                  => "addUser",
    authorize                => "authorize",
    checkFileQuota           => "checkFileQuota",
    checkLFN                 => "checkLFN",
    checkPermission          => "checkGUIDPermissions",
    checkPermissions         => "checkLFNPermissions",
    chgroup                  => "chgroups",
    chmod                    => "chmod",
    chown                    => "chown",
    cpMetaData               => "cpMetaData",
    createCollection         => "createCollectionCatalogue",
    cleanupTagValue          => "cleanupTagValue",
    du                       => "du",
    deleteMirror             => "deleteMirror",
    df                       => "df",
    existsEntry              => "existsEntry",
    expungeTables            => "expungeTables",
    find                     => "find",
    fquota_list              => "fquota_list",
    fquota_set               => "fquota_set",
    getLFNlike               => "getLFNlike",
    getTabCompletion         => "tabCompletion",
    getCPMetadata            => "cpMetaData",
    groups                   => "groups",
    guid2lfn                 => "guid2lfn",
    isFile                   => "isFile",
    isDirectory              => "isDirectory",
    ls                       => "ls",
    lfn2guid                 => "lfn2guid",
    ln                       => "ln",
    listFilesFromCollection  => "listFilesFromCollection",
    moveDirectory            => 'moveDirectory',
    mkdir                    => "mkdir",
    mkremdir                 => "mkremdir",
    mv                       => "mv",
    pwd                      => "pwd",
    rmdir                    => "rmdir",
    removeExpiredFiles       => "removeExpiredFiles",
    removeFile               => "rm",
    removeTag                => "removeTag",
    removeTagValue           => "removeTagValue",
    removeTrigger            => "removeTrigger",
    renumber                 => "renumberDirectory",
    refreshSERankCache       => "refreshSERankCache",
    resyncLDAP               => "resyncLDAP",
    removeFileFromCollection => "removeFileFromCollection",
    registerFile             => 'register',
    showMirror               => "showMirror",
    setExpired               => "setExpired",
    showStructure            => "showStructure",
    showTagValue             => "showTagValue",
    showTags                 => "showTags",
    stat                     => "stat",
    showTrigger              => "showTrigger",
    touch                    => "touch",
    tree                     => "tree",
    type                     => "type",
    updateTagValue           => "updateTagValue",
    updateCollection         => "updateCollection",
    verifyToken              => "verifyToken",
    whereis                  => "whereis",
    zoom                     => "zoom",


  };
  if ($ops->{$name}) {
    return shift->callAuthen($ops->{$name}, @_);
  } elsif ($name =~ /(user)/) {
    my $self = shift;
    my $tmp = $self->callAuthen("user", @_);
    $tmp
      and $self->{ROLE} =
      $_[1]

      #and $self->_setUserGroups() <<<----- Required on client?
      and return 1;
    return 0;
  } elsif ($name =~ /(ExpandWildcards)/) {
    return AliEn::Catalogue::ExpandWildcards(@_);
  } elsif ($name =~ /(whoami)/) {
    return AliEn::Catalogue::f_whoami(@_);
  } elsif ($name =~ /(dirname)/) {
    return AliEn::Catalogue::f_dirname(@_);
  } elsif ($name =~ /(basename)/) {
    return AliEn::Catalogue::f_basename(@_);
  } elsif ($name =~ /(GetAbsolutePath)/) {
    return AliEn::Catalogue::Basic::GetAbsolutePath(@_);
  } elsif ($name =~ /(complete_path)/) {
    return AliEn::Catalogue::Basic::f_complete_path(@_);
  } elsif ($name =~ /(GetHomeDirectory)/) {
    return AliEn::Catalogue::Basic::GetHomeDirectory(@_);
  } elsif ($name =~ /(echo)/) {
    return AliEn::Catalogue::f_echo(@_);
  } elsif ($name =~ /DESTROY/) {
    return;
  }
  die("The function $AUTOLOAD is not defined in ClientCatalog!!\n");
}

sub f_disconnect {
  return;
}

sub getDispPath {
  my $self = shift;
  return $self->{DISPPATH};
}

return 1;
__END__
