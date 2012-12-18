package AliEn::FTP::xrm;

use strict;

use strict;
use vars qw(@ISA $DEBUG);
use AliEn::FTP;
push @ISA, "AliEn::FTP" ;

use Net::LDAP;
use AliEn::Util;
use AliEn::SE::Methods::root;

sub initialize {
  my $self=shift;

  $self->{MSS}=AliEn::SE::Methods->new('root://host/path') 
    or return;

  return $self->SUPER::initialize(@_);

}

sub delete {
  my $self=shift;
  my $pfn=shift;

  $pfn=~ s{^file://[^/]*}{};
  $self->info("Ready to delete $pfn with rm");
  $self->{MSS}->{PFN}=$pfn;
  $self->{MSS}->parsePFN();
  my $done=$self->{MSS}->remove($pfn);
  $done eq 0 and return 1;
  $self->info("Error deleting $pfn",1);
  return;
}

return 1;

