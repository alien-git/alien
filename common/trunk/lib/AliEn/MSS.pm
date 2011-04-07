package AliEn::MSS;

use AliEn::Database::TXT::MSS;
use strict;
use AliEn::Config;
use AliEn::GUID;
use File::Basename;
use AliEn::Util;

use vars qw (@ISA);

push @ISA, 'AliEn::Logger::LogObject';

use strict;

=head1 NAME

AliEn::MSS

=head1 DESCRIPTION

AliEn::MSS module contains the functions related with Mass Storage System.

=head1 SYNOPSIS

getFTPCopy()

=head1 METHODS

=over

=item C<getFTPCopy>

getFTPCopy returns the protocol name $ftd->{PROTOCOLS} and path $path to the calling function makeLocalCopy of AliEn::Service::FTD module. Inside the getFTPCopy the function getFTDbyHost of AliEn::Service::IS is called and this function fetches the protocol value for $host from INFORMATIONSERVICE database.  

=cut

sub new {
  my ($this) = shift;
  my $class   = ref($this) || $this;
  my $options = shift;
  my $self    = (shift or {});

  bless $self, $class;

  $self->SUPER::new() or return;

  $self->{SOAP} = new AliEn::SOAP;
  $self->{SOAP} or return;

  $self->{LOGGER} = ($options->{LOGGER} or AliEn::Logger->new());
  $self->{LOGGER}
    or print STDERR "Error setting the MSS: no Logger specified\n" and return;

  $self->debug(1, "CREATING A NEW FILE SYSTEM\n");

  $self->{CONFIG} = ($options->{CONFIG} or AliEn::Config->new());
  ($self->{CONFIG}) or return;

  if ($self->{NO_CREATE_DIR}) {
    $self->debug(1, "Skipping making the directories");
    return $self;
  }

  my $savedir = $self->{CONFIG}->{SE_SAVEDIR};
  my @environment;
  $self->{CONFIG}->{SE_ENVIRONMENT_LIST} and push @environment, @{$self->{CONFIG}->{SE_ENVIRONMENT_LIST}};

  if ($options->{VIRTUAL}) {
    $self->info("Managing the MSS for the virtual SE $options->{VIRTUAL}");
    $self->{CONFIG}->{"SE_$options->{VIRTUAL}"}
      or $self->info("Error in MSS: the virtual SE $options->{VIRTUAL} doesn't exist")
      and return;
    $savedir     = $self->{CONFIG}->{"SE_$options->{VIRTUAL}"}->{SAVEDIR};
    @environment = ();
    $self->info("Checking the environment for a virtual SE");
    $self->{CONFIG}->{"SE_$options->{VIRTUAL}"}->{ENVIRONMENT_LIST}
      and push @environment, @{$self->{CONFIG}->{"SE_$options->{VIRTUAL}"}->{ENVIRONMENT_LIST}};
  }

  $self->debug(1, "Got environment @environment");
  $self->{ENVIRONMENT} = {};
  foreach my $entry (@environment) {
    my ($key, $value) = split(/=/, $entry, 2);
    $key = uc($key);
    $value or $value = "";
    $self->{ENVIRONMENT}->{$key} = $value;
  }

  (defined $savedir)
    or $self->{LOGGER}->warning("MSS", "Error setting the MSS: no SAVEDIR specified")
    and return;

  if ($savedir) {
    my ($mountpoint, $name, $size) = split(",", $savedir);
    $self->{SAVEDIR} = $mountpoint;
    $self->mkdir($self->{SAVEDIR})
      and $self->{LOGGER}->warning("MSS", "Error creating the save directory $self->{SAVEDIR}\n")
      and return;
  }
  $self->{LOGDIR} = "$self->{CONFIG}->{LOG_DIR}/MSS";
  $self->{LOGDIR}
    or $self->{LOGGER}->warning("MSS", "Error setting the MSS: no LOGDIR specified")
    and return;

  AliEn::Util::mkdir($self->{LOGDIR})
    or $self->{LOGGER}->info("Error creating the log directory $self->{LOGDIR}\n")
    and return;

  $self->{HOST} = $self->{CONFIG}->{HOST};

  $self->{TXTDB} = AliEn::Database::TXT::MSS->new({"LOGDIR", $self->{LOGDIR}});
  $self->{TXTDB} or return;

  $self->{GUID} = new AliEn::GUID or return;
  $self->initialize() or return;
  return $self;
}

sub initialize {
  my $self = shift;
  return 1;
}

sub configure {
  my $self = shift;
  print STDERR "PLEASE, IMPLEMENT ME ($self)!!!\n";
}

sub newFileName {
  my $self = shift;
  my $guid = shift;
  $self->debug(1, "Getting a new file name");

  if (!$guid) {
    $self->debug(1, "Creating GUID");
    $guid = $self->{GUID}->CreateGuid();
    if (!$guid) {
      $self->{LOGGER}->error("MSS", "Cannot create new guid");
      return;
    }
  }

  $self->{SAVEDIR}
    or $self->{LOGGER}->error("MSS", "SAVEDIR is not defined")
    and return;

  #    $self->get("$self->{SAVEDIR}/LASTDIR","$self->{LOGDIR}/MSS.db/LASTDIR");

  #    my ($data) = $self->{TXTDB}->query("SELECT dir_id, file_id FROM LASTDIR");

  #    my ($alienDir)  = 1;
  #    my ($alienFile) = 1;

  #    ($data) and ( ( $alienDir, $alienFile ) = split "###", $data );

  $self->debug(1, "Getting Hash GUID");
  my $dir = sprintf "%02.2d/%05.5d", $self->{GUID}->GetCHash($guid), $self->{GUID}->GetHash($guid);

  my $saveFile = sprintf "$dir/$guid";

  $saveFile =~ s/$self->{SAVEDIR}\///;
  $self->debug(1, "Returning '$saveFile'");
  return ($saveFile, $guid);
}

# Copies a file in the MSS. It needs to receive the target.
# $source -> Original file
# $target -> Full path name to the destination

sub save {
  my $self   = shift;
  my $source = shift;
  my $target = shift;
  my $newFile;

  $self->{SAVEDIR}
    or $self->{LOGGER}->error("MSS", "SAVEDIR is not defined")
    and return;
  ($source and $target)
    or $self->{LOGGER}->warning("MSS", "Error: not enough arguments to save a file")
    and return;

  my $dir = dirname $target;
  $self->debug(1, "Creating directory $dir");

  if ($self->mkdir($dir)) {
    $self->{LOGGER}->warning("MSS:file", "Error creating $dir");
    return;
  }

  $self->{LOGGER}->info("MSS", "Saving the file $source to $target");

  if ($self->put($source, $target)) {
    print STDERR "Error copying $source, $target\n";
    return;
  }

  $self->{LOGGER}->info("MSS", "Done and " . $self->url($target));

  return $self->url($target);
}

sub parse {
  my $self = shift;
  my $lfn  = shift;

  my $method = "file";
  my $path   = "$lfn";

  $path =~ s/^([^:]*):// and $method = "$1";

  return ($method, $path);
}

sub createdir {
  my $self = shift;
  my $dir  = shift;
  $self->{SAVEDIR}
    or $self->{LOGGER}->error("MSS", "SAVEDIR is not defined")
    and return;
  $self->mkdir("$self->{SAVEDIR}/$dir")
    and $self->{LOGGER}->warning("MSS:file", "Error creating $self->{SAVEDIR}/$dir")
    and return;
  return 1;
}

sub getURL {
  my $self = shift;
  my $dir  = shift;
  my $path = "";

  $self->{SAVEDIR}
    or $self->{LOGGER}->error("MSS", "SAVEDIR is not defined")
    and return;

  # this path has to be checked, if it creates problesm ...
  $path = "$dir";

  $path =~ s/\/\//\//;
  return $self->url($path);
}

sub get {
  my $self = shift;
  return $self->cp(@_);
}

sub getchunk {
  my $self = shift;
  return $self->read(@_);
}

sub put {
  my $self = shift;
  return $self->cp(@_);
}

#sub cp{
#  print "NOT IMPLEMENTED!!\n";
#  return 1;
#}
sub link {
  my $self = shift;
  $self->debug(1, "Link not implemented");
  return 1;
}

sub getFTPCopy {
  my $self     = shift;
  my $path     = shift;
  my $orig_pfn = shift;

  $self->info("Checking the FTP (path:$path, and orig $orig_pfn)");

  #Some MSS can't be access directly, and they have to make a local copy of
  # the file
  if ($self->{FTP_LOCALCOPIES}) {
    my $name;
    $path =~ /\/([^\/]*)$/ and $name = $1;
    my $localpath = "$self->{CONFIG}->{CACHE_DIR}/$name.$$" . time;
    $self->get($path, $localpath)
      and $self->{LOGGER}->error("MSS", "Error getting the local copy of $path")
      and return;
    $path = $localpath;
    return "file://$self->{HOST}$localpath";
  }
  return $orig_pfn;
}

sub lslist {

}

=item C<setEnvironment($variables)>


=cut

sub setEnvironment {

}

=item C<unsetEnvironment()>

These methods are called before retrieving a file, with all the variables that 
are defined in the PFN. By default, they are just ignored. However, any
MSS can override the method and do something with the variables

=cut

sub unsetEnvironment {

}

sub stage {
  my $self = shift;
  $self->info("This MSS doesn't require staging");
  return 1;
}

return 1;
