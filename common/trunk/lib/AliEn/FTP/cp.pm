package AliEn::FTP::cp;

use strict;

use strict;
use vars qw(@ISA $DEBUG);
use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

use Net::LDAP;
use AliEn::Util;
use AliEn::MSS::file;

sub initialize {
  my $self=shift;
  $self->{MSS}=AliEn::MSS::file->new({NO_CREATE_DIR=>1});
  return $self->SUPER::initialize(@_);

}

sub copy {
  my $self=shift;
  my $source=shift;
  my $target=shift;
  my $line=shift;

  $self->info("Ready to copy $source into $target with cp");
  my $done=$self->{MSS}->cp($source->{pfn}, $target->{pfn});
  $done eq 0 and return 1;
  $self->info("Error copying $source->{pfn} into $target->{pfn}",1);
  return;
}

return 1;

