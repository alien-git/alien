package AliEn::Classad::Host;

use AliEn::Config;
use Classad;
use strict;
use vars qw(@ISA);
use Filesys::DiskFree;
use AliEn::PackMan;
#use AliEn::ClientPackMan;
use AliEn::Util;

push @ISA,'AliEn::Logger::LogObject';

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = ( shift or {} );
  bless( $self, $class );
  $self->SUPER::new() or return;
  $self->{CONFIG} or $self->{CONFIG} = AliEn::Config->new();
  $self->{CONFIG} or return;

  my $config  =  $self->{CONFIG};

  $self->debug(1, "Creating the ClassAd" );

  my $otherReq="";

  $self->{CONFIG}->{CE_CEREQUIREMENTS} and 
    $otherReq=" && ($self->{CONFIG}->{CE_CEREQUIREMENTS})";
  $self->{PACKMAN} or $self->{PACKMAN}=AliEn::ClientPackMan->new();

  $self->{PACKMAN} or $self->info("Error getting the list of ClientPakcMan") and return;

  my $ca =
    Classad::Classad->new(
 "[ Type=\"machine\"; Requirements=(other.Type==\"Job\" $otherReq); WNHost = \"$self->{CONFIG}->{HOST}\"; CEName= \"$self->{CONFIG}->{CE_NAME}\";]" );
 ( $ca and $ca->isOK()) 
   or $self->info("Error creating the Classads.Check if the requirements ($otherReq) have the right format") and return;
  $self->setCloseSE($ca) or return;
    
  $self->setPackages($ca) or return;
  
  
  $self->setCE($ca) or return;
  $self->setGridPartitions($ca) or return;
  $self->setLocalInfo($ca) or return;
  $self->setTTL($ca) or return;
  $self->setSystemInfo($ca) or return;
  $self->setVersion($ca) or return;
  $self->setPrice($ca) or return;
  
  if ( !$ca->isOK() ) {
    print STDERR "CE::new : classad not correct ???!!!\n";
    return;
  }

  $self->debug(1, "Returning ". $ca->asJDL());
  return $ca;
}

#
# Private functions
#

sub setPrice {
  my $self=shift;
  my $ca=shift;
	
  # Get Price from LDAP
  my $price = "";
     $price = $self->{CONFIG}->{CE_SI2KPRICE};
  my $name  = $self->{CONFIG}->{CE_FULLNAME};
     $price or ( $self->info("Warning! No price set in LDAP for CE. Setting price to '1'")
	         and ($price=1) );
  my $p = sprintf ("%.2f",$price);
  return $ca->set_expression("Price", $p);
}


sub setVersion {
  my $self=shift;
  my $ca=shift;
  
  return $ca->set_expression("Version", "\"$self->{CONFIG}->{VERSION}\"");

}


sub setSystemInfo {
  my $self=shift;
  my $ca=shift;
  
  my $platform=AliEn::Util::getPlatform($self);
  
  $platform and ($ca->set_expression("Platform", "\"$platform\"") or return);
  open (FILE,"</proc/meminfo") or 
    print "Error checking /proc/meminfo\n" and return 1;
  my ($free, $swapfree, $total, $swap);

  foreach (<FILE>) {
    if (/^Swap:\s*(\d+)\s+\d+\s+(\d+).*$/) {
      $swap=int($1/1024);
      $swapfree=int($2/1024);
      next;
    }
    if (/^Mem:\s*(\d+)\s+\d+\s+(\d+).*$/) {
      $total=int($1/1024);
      $free=int($2/1024);
      next;
    }
  }
  close FILE;

  $swap and (  $ca->set_expression("Swap", $swap) or return);
  $total and (  $ca->set_expression("Memory", $total) or return);
  $free and ($ca->set_expression("FreeMemory", $free) or return);
  $swapfree and ($ca->set_expression("FreeSwap", $swapfree) or return);
  return 1;
}
sub setLocalInfo{
  my $self=shift;
  my $ca=shift;
  my $dir=  $ENV{ALIEN_HOME};
  $ENV{ALIEN_WORKDIR} and (-d $ENV{ALIEN_WORKDIR}) and 
    $dir=$ENV{ALIEN_WORKDIR};
  $self->debug(1,"Looking for the free space in $dir");
  my $handle=Filesys::DiskFree->new();
  $handle->df_dir($dir);
  my $space=$handle->avail($dir);
#  if (open (FILE, "df -P $dir | tail -1 | awk \'{print \$4}\' |")) {
#    my $space=join("", <FILE>);
#    close FILE;
#    chomp $space;
#    $space =~ s/^(\d+)\s.*$/$1/;
  if (! $space){
     $self->info("Probably '$dir' is a link... getting the size in a different way");
     $handle->df();
     $space=$handle->avail($dir);
  }

  if ($space){
    $ca->set_expression( "LocalDiskSpace", $space/1024 );
  }else {
    $self->info("Error getting the diskspace") and return 0;
  }
  if (open (FILE, "uname -r |")) {
    my $uname=join("", <FILE>);
    close FILE;
    chomp $uname;
    $ca->set_expression( "Uname", "\"$uname\"" );
  }else {
    $self->info("Error getting uname") and return 1;
  }
  return 1;
}
sub setCloseSE{
  my $self=shift;
  my $ca=shift;
  my @closeSE = ();
  $self->{CONFIG}->{SEs_FULLNAME}
    and @closeSE = @{ $self->{CONFIG}->{SEs_FULLNAME} };

  return $self->setItem($ca, "CloseSE", @closeSE);
}

sub setPackages {
  my $self=shift;
  my $ca=shift;


  #my ($status, @packages)=$self->{PACKMAN}->f_packman ("list", "-s", "ALIEN_SOAP_SILENT");
  my ($status, @packages)=$self->{PACKMAN}->f_packman ("list", "-silent");
  if (@packages) {
    $self->debug(1, "Setting the list of packages to @packages");
    $self->setItem($ca, "Packages", @packages) or return;
  }
  $self->debug(1,"Asking for the installed packages");
  #($status, @packages)=$self->{PACKMAN}->getListInstalledPackages( "-s", "ALIEN_SOAP_SILENT");
  ($status, @packages)=$self->{PACKMAN}->f_packman ("listInstalled", "-silent");
  if (@packages){
    $self->debug(1, "Setting the installed packages");
    $self->setItem($ca, "InstalledPackages", @packages);
  }
  return 1;
}


sub setCE {
  my $self=shift;
  my $ca=shift;
  if ($self->{CONFIG}->{CE_FULLNAME}) {
    $ca->set_expression( "CE", "\"$self->{CONFIG}->{CE_FULLNAME}\"" )
      or $self->{LOGGER}->warning("CE", 
				  "Error setting the CE ($self->{CONFIG}->{CE_FULLNAME})" )
	and return;
  }
  if ($self->{CONFIG}->{CE_HOST}){
    $ca->set_expression( "Host", "\"$self->{CONFIG}->{CE_HOST}\"" )
      or $self->{LOGGER}->warning("CE", 
				  "Error setting the CE ($self->{CONFIG}->{CE_HOST})" )
     and return;
  }
  return 1;
}

sub setGridPartitions {
  my $self=shift;
  my $ca=shift;
  ( $self->{CONFIG}->{GRID_PARTITION} ) or return 1;
  my  @partitions=@{ $self->{CONFIG}->{GRID_PARTITION_LIST} };
  return $self->setItem($ca, "GridPartitions", @partitions);
}
sub setTTL {
  my $self=shift;
  my $ca=shift;
  my $ttl=($self->{CONFIG}->{CE_TTL} or  "");
  ( $ttl) or 
    $self->info( "Using default TTL value: 12 hours") 
      and $ttl=12*3600;
  return $ca->set_expression('TTL', $ttl);

}

sub setItem{
  my $self=shift;
  my $ca=shift;
  my $item=shift;
  my @elements=@_;

  ($#elements>-1) or return 1;

  map { s/^(.*)$/\"$1\"/ } @elements;
  my $string = "{" . join ( ", ", @elements ) . "}";
  $self->debug(1, "Setting $item $string" );
  $ca->set_expression( $item, $string )
    or $self->{LOGGER}->warning( "CE", "Error setting the $item (@elements" )
      and return;
  return 1;
}
1;
