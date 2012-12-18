package AliEn::FTP::rm;

use strict;

use strict;
use vars qw(@ISA $DEBUG);
use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

use Net::LDAP;
use AliEn::Util;
use AliEn::MSS::file;
use AliEn::SE::Methods;

sub initialize {
  my $self=shift;
  $self->{MSS}=AliEn::MSS::file->new({NO_CREATE_DIR=>1});
  return $self->SUPER::initialize(@_);

}

sub delete {
  my $self=shift;
  my $pfn=shift;

  if ($pfn =~ /^root:/){
    $self->info("THIS FILE IS NOT LOCAL");
    my $pfn=AliEn::SE::Methods->new($pfn) or return;
    $self->info("Ready to delete the file");
    $pfn->remove();
    $self->info("File removed (?)");
    return;
  }

  $pfn=~ s{^file://[^/]*}{};
  $self->info("Ready to delete $pfn with rm");
  my $done=$self->{MSS}->rm($pfn);
  $done eq 0 and return 1;
  $self->info("Error deleting $pfn",1);
  return;
}

return 1;

