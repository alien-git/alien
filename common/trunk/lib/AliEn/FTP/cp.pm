package AliEn::FTP::cp;

use strict;

use strict;
use vars qw(@ISA $DEBUG);
use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

use Net::LDAP;
use AliEn::Util;


sub copy {
  my $self=shift;
  my $source=shift;
  my $target=shift;
  my $line=shift;

  $self->info("Ready to copy $source into $target with cp");

  my ($protocol, $se, $sourceHost, $targetHost)=split(',', $line);

  use Data::Dumper;
  print Dumper($source,$target, $line);
  return;
}

return 1;

