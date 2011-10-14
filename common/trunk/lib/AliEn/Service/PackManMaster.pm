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

  $self->{DB}=$self->{UI}->{CATALOG}->{DATABASE};
  $self->{DB} or $self->info("Error getting the database") and return;
  ($self->{HOST}, $self->{PORT})=
    split (":", $self->{CONFIG}->{"PACKMANMASTER_ADDRESS"});
  $self->{SERVICE}="PackManMaster";
  $self->{SERVICENAME}="PackManMaster";
  return $self;
}

sub recomputeListPackages {
  my $this=shift;
  $self->info("Recomputing the list of packages");
  $self->{DB}->do("update ACTIONS set todo=1 where action='PACKAGES'") or return;
  $self->info("The information will be updated in 10 seconds");

  return 1;
}

return 1;
