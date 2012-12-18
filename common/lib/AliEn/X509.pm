package AliEn::X509;

use strict;
use warnings;
use AliEn::Config;
use AliEn::Logger::LogObject;
use vars qw (@ISA $DEBUG);

push @ISA, 'AliEn::Logger::LogObject';
$DEBUG = 0;

sub new {
  my $class = shift;
  my $self  = {};

  #    $self->{OPENSSL_PATH} = shift;
  $self->{IS_LOADED} = 0;
  bless $self, $class;
  $self->SUPER::new() or return;

  $self->{CONFIG} = new AliEn::Config();

  #  $ENV{X509_USER_CERT} or $ENV{X509_USER_CERT}="$ENV{ALIEN_HOME}/globus/usercert.pem";
  #  $ENV{X509_USER_KEY} or $ENV{X509_USER_KEY}="$ENV{ALIEN_HOME}/globus/userkey.pem";
  system("which openssl > /dev/null 2>&1")
    and $self->info("Error: couldn't find openssl in the path")
    and return;

  return $self;
}

sub load {
  my $self     = shift;
  my $filename = shift;
  if (!(-e $filename)) {
    print "$filename not found\n";
    return;
  }
  $self->{FILENAME}  = $filename;
  $self->{IS_LOADED} = 1;
}

sub getSubject {
  my $self = shift;
  if (!($self->{IS_LOADED})) {
    print "The certificate is not loaded. call X509->load(filename) first\n";
    return;
  }

  my $pid = open(my $TEMP, "-|", "openssl x509 -noout -in $self->{FILENAME} -subject");

  if ($pid < 1) {
    print "Could not execute openssl command\n";
  }
  my $temp;
  $temp = <$TEMP>;
  close($TEMP);
  !$temp and return;
  chop($temp);
  $temp =~ s/^subject=\s*//;
  return $temp;
}

sub checkProxy {
  my $self = shift;
  $self->debug(1, "Checking if we have a proxy certificate");
  my $proxy = ($ENV{X509_USER_PROXY} || "/tmp/x509up_u$<");
  $self->{PROXY_TIME} or $self->{PROXY_TIME} = $self->getProxyTime();
  my $time = time;
  if ((-f $proxy) && ($time < $self->{PROXY_TIME})) {
    $self->debug(1, "There is a proxy, and it is valid");
    return 1;
  }
  $self->info( "We have to create a new proxy");
  return $self->createProxy();
}

sub getRemainingProxyTime {
  my $self = shift;

  $self->debug(1, "getRemainingProxyTime called");

  open(my $FILE, "-|", "$ENV{GLOBUS_LOCATION}/bin/grid-proxy-info 2>&1")
    or $self->debug(1, "Error doing $ENV{GLOBUS_LOCATION}/bin/proxy-info")
    and return 0;
  my @data = <$FILE>;
  close $FILE;

  $self->debug(1, join " ", @data);
  @data = grep (s/^timeleft\s*:\s*(\S+)\s+.*$/$1/s, @data);
  my $time = 0;
  @data and $data[0] or return 0;
  $self->debug(1, "Time left $data[0]");
  my (@time) = split(/:/, $data[0]);
  my $expireTime = ($time[0] * 60 + $time[1]) * 60 + $time[2];

  return $expireTime;
}

sub getProxyTime {
  my $self       = shift;
  my $expireTime = $self->getRemainingProxyTime();
  my $now        = time;

  #Let's return the time when the proxy is going to expire (with a 5 minutes
  #security margin
  return $now + $expireTime - 300;
}

sub checkProxySubject {
  my $self = shift;
  my $proxy = ($ENV{X509_USER_PROXY} || "/tmp/x509up_u$<");
  $self->checkProxy() or return;
  $self->load($proxy) or return;
  return $self->getSubject($proxy);
}

sub createGridmap {
  my $self = shift;
  my $subject = (shift or $self->{CONFIG}->{SE_CERTSUBJECT});

  my $subject2 = $subject;
  $subject2 =~ s/[\/\s]/_/g;
  my $dir = "$ENV{ALIEN_HOME}/identities.\L$self->{CONFIG}->{ORG_NAME}\E/$subject2";
  AliEn::MSS::file::mkdir($self, $dir);
  my $gridmap = "$dir/gridmap";
  $self->debug(1, "Creating the gridmap file $gridmap with \n\t\"$subject\" " . getpwuid($<));

  open(my $FILE, ">","$gridmap")
    or $self->info("Error opening $gridmap")
    and return;
  print $FILE "\"$subject\" " . getpwuid($<) . "\n";
  close $FILE;
  return $gridmap;
}

sub createProxy {
  my $self    = shift;
  my $hours   = (shift || "");
  my $options = shift || {};
  if ($hours) {
    my $minutes = ($hours * 60) % 60;
    $hours = "-valid " . int($hours) . ":$minutes";
  }
  my $silent = "&> /dev/null";
  $DEBUG and $silent = "";

  my $error  = system("$ENV{GLOBUS_LOCATION}/bin/grid-proxy-init  -pwstdin $hours </dev/null $silent");
  my $method = "info";
  my @extra;
  $options->{silent} and $method = "debug" and push @extra, 2;
  $error and $self->$method(@extra, "Error doing $ENV{GLOBUS_LOCATION}/bin/grid-proxy-init $!") and return;
  return 1;
}

sub extendProxyTime {
  my $self = shift;
  $self->info("Trying to extend the time of the proxy");

  return;
}

1;

__END__

=pod

=head1 NAME 

AliEn::X509 - A perl wrapper for X509 certificates (In PEM format)

=head1 DESCRIPTON

A small perlwrapper for the openssl command to get basic values in a X509 certificate.

=cut



