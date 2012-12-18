package AliEn::Service::FC;

use AliEn::Service;
use AliEn::UI::Catalogue;

use vars qw(@ISA);

@ISA=qw(AliEn::Service);
use strict;

# Use this a global reference.

my $self = {};

sub initialize {
  $self = shift;
  my $options =(shift or {});

  $self->debug(1, "Creatting a FC" );

  $self->{PORT}="9992";
  $self->{HOST}=$self->{CONFIG}->{HOST};
  $self->{SERVICE}="FC";
  $self->{SERVICENAME}="FC";
  $self->{LISTEN}=1;
  $self->{PREFORK}=10;
  $self->{CATALOG}=AliEn::UI::Catalogue->new() or return;
  $self->{DISPATCH_WITH}={'http://glite.org/wsdl/services/org.glite.data.catalog.service.storageindex' => 'AliEn::Service::FC'};

  return $self;

}
sub listSEbyLFN {
  shift;
  my $lfn=(shift or "");
  $self->{LOGGER}->info("FC", "Converting the lfn $lfn into a list of SE");
  my @list=$self->{CATALOG}->execute("whereis", $lfn);
  @list or $self->die('NotExistsException', 'that lfn doesn\'t exist');
  my @se;
  while (@list){
    #Let's put the se in the list
    push @se, shift @list;
    #Let's skip the pfn
    shift @list;
  }
  return \@se;
}

sub listSEbyGUID {
  shift;
  my $guid=(shift or "");
  $self->{LOGGER}->info("FC", "Converting the guid $guid into a list of SE");
  my $lfn=$self->getLFNofGUID($guid);
  $lfn or $self->die('NotExistsException', 'that guid doesn\'t map to any lfn');

  return $self->listSEbyLFN($lfn);
}
#Internal functions
sub getLFNofGUID {
  my $self=shift;
  my $guid=shift;
  $self->{LOGGER}->info("FC", "Let's call query");
  my $db=$self->{CATALOG}->{CATALOG}->{DATABASE};
  my $hosts=$db->query("SELECT address, db,driver from HOSTS");
  $hosts or $self->{LOGGER}->info("FC", "Error getting the list of databases") and return;
  my $lfn;
  foreach my $entry (@$hosts) {
    $self->{LOGGER}->info("FC", "Trying with $entry->{address}, $entry->{db}, $entry->{driver}");
    $db->reconnect($entry->{address}, $entry->{db},
		   $entry->{driver});
    $lfn=$db->queryValue("SELECT path from D0 where guid='$guid' and path not like '%/'");
    $lfn and last;
  }
  $lfn or $self->{LOGGER}->info("FC", "The guid $guid doesn't have any lfn") and return;
  
  $self->{LOGGER}->info("FC", "The lfn is $lfn");
  return $lfn;
}
