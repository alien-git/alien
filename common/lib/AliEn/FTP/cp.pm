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

  $self->info("Ready to copy $source->{turl} into $target->{turl} with cp");

  my $from=$source->{turl};
  my $to=$target->{turl};

  my @splitturl = split (/\/\//, $source->{turl},3);
  $splitturl[2] and  $from="/".$splitturl[2];

  @splitturl = split (/\/\//, $target->{turl},3);
  $splitturl[2] and  $to="/".$splitturl[2];

  $self->info("submitting command: cp $from $to ...");

  my $done=$self->{MSS}->cp($from,$to);
  $done eq 0 and return 1;
  $self->info("Error copying $source->{turl} into $target->{turl}",1);
  return;
}

return 1;

