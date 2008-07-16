package AliEn::Service::PackManMaster;

=head1 B<NAME>

AliEn::Service::PackMan

=head1 B<SYNOPSIS>

  my $packman=AliEn::Service::PackMan->new();
  $packman->startListening()

=head1 B<DESCRIPTION>

This is the Service that implements the Package Manager. It inherits from AliEn::Service

The public methods that it includes are:

=over

=cut

use AliEn::Service;
use AliEn::Util;
use AliEn::UI::Catalogue;
use Cwd;

use vars qw(@ISA $DEBUG);

@ISA=qw(AliEn::Service);


$DEBUG=0;

use strict;


# Use this a global reference.

my $self = {};

sub initialize {
  $self=shift;
  $self->info("Creating a PackManMaster");

  $self->{UI}=AliEn::UI::Catalogue->new({role=>'admin'}) or return;

  $self->{DB}=$self->{UI}->{CATALOG}->{DATABASE}->{LFN_DB};
  $self->{DB} or $self->info("Error getting the database") and return;
  ($self->{HOST}, $self->{PORT})=
    split (":", $self->{CONFIG}->{"PACKMANMASTER_ADDRESS"});
  $self->{SERVICE}="PackManMaster";
  $self->{SERVICENAME}="PackManMaster";
  return $self;
}

sub findPackageLFN {
  my $this=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
  my $platform=shift;
  $self->info("We should check in the database for the package");

  my $vo_user=uc("VO_$self->{CONFIG}->{VO_NAME}");
  my $query="SELECT lfn from PACKAGES where packageName=? and (platform=? or platform='source') and (username=? or username=?)";
  my @bind=($package, $platform, $user, $vo_user);
  my @bind_source=($package, $platform, $user, $vo_user);

  if ($version) {
    $query.=" and packageVersion=? ";
    push @bind, $version;
    push @bind_source, $version
  }
  my $result=$self->{DB}->queryColumn($query, undef, {bind_values=>\@bind})
    or die ("Error doing the query $query");


  if (! @$result){
    $self->info("The package doesn't exist for that platform. Let's look for source");
    $result=$self->{DB}->queryColumn($query, undef, {bind_values=>\@bind_source})
      or die ("Error doing the query $query");
  }
  $self->info("We got $#$result and @$result");
  if ($#$result <0 ){
    $self->info("The package $user, $package, $version, $platform doesn't exist");
    return -2;
  }

  my $lfn=$$result[0];
  $self->info("The package '$lfn' exists!!!");

  my (@dependencies)=$self->{UI}->execute("showTagValue", "-silent",$lfn, "PackageDef");
  my $item={};
  @dependencies and $dependencies[1]  and $item=shift @{$dependencies[1]};

  $self->info( "$$ Metadata of this item");
  use Data::Dumper;
  print Dumper($item);

  return $lfn, $item;
}


sub recomputeListPackage {
  my $this=shift;
  $self->info("Recomputing the list of packages");
  $self->{DB}->do("update ACTIONS set todo=1 where action='PACKAGES'") or return;
  $self->info("The information will be updated in 10 seconds");

  return 1;
}


sub getListPackages{
  my $this=shift;
  $self->info("Retrieving the list of Packages (@_)");
  my @args= grep (! /^-/, @_);
  my $platform=shift @args;

  my $silent="";
  $self->{DEBUG} or $silent="-silent";

  my $query="SELECT distinct fullPackageName from PACKAGES";
  my $bind=[];

  if( $platform ne  "all") {
    $self->info("Returning the info of the platform $platform");
    $query.=" where  (platform=?  or platform='source')";
    $bind=[$platform];
  }
  $self->info("Let's do $query");
  my $packages=$self->{DB}->queryColumn($query,undef, {bind_values=>$bind}) 
    or $self->info("Error doing the query") and return;

  use Data::Dumper;
  print Dumper($packages);
  return (1, @$packages);

}

return 1;


