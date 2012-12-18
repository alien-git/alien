package AliEn::SE::Methods::bbftp;

use AliEn::SE::Methods::Basic;
use AliEn::X509;

use strict;
use vars qw( @ISA );
@ISA = ("AliEn::SE::Methods::Basic");

use strict;

sub initialize {
    my $self = shift;
    $self->{X509}=new AliEn::X509;
}
sub get {
  my $self=shift;

  $self->{X509} or $self->{X509}=new AliEn::X509;
  $self->{X509}->checkProxy();

  $self->{LOGGER}->debug("BBFTP", "In bbftp, getting the file $self->{PARSED}->{ORIG_PFN}");
  my $remoteSubject=($self->{PARSED}->{VARS_SUBJECT} or "");
  $remoteSubject =~ s{//}{=}g;
  $remoteSubject =~ s{/CN=proxy}{}g;
  $remoteSubject =~ s{^.*/CN=}{}g;

  my $command="$ENV{ALIEN_ROOT}/bin/bbftp  -p 5 -w $self->{PARSED}->{PORT} -e \"setoption createdir;get $self->{PARSED}->{PATH} $self->{LOCALFILE}\" $self->{PARSED}->{HOST} -g \"$remoteSubject\"";
  $ENV{X509_CERT_DIR} or
    $ENV{X509_CERT_DIR}="$ENV{ALIEN_ROOT}/etc/alien-certs/certificates";

  $self->{SILENT} and $command.=">/dev/null 2>&1";

  $self->{LOGGER}->debug("BBFTP", "Doing $command");
  my ($oldCert, $oldKey)=($ENV{X509_USER_CERT}, $ENV{X509_USER_KEY});

  ($ENV{X509_USER_CERT}, $ENV{X509_USER_KEY})=("","");
  my $error = system($command);
  $oldCert and $ENV{X509_USER_CERT}=$oldCert;
  $oldKey and $ENV{X509_USER_KEY}=$oldKey;

  $error and return;
  return $self->{LOCALFILE};
}
return 1;
