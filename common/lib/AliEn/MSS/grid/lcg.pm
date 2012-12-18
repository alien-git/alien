package AliEn::MSS::grid::lcg;

@ISA = qw (AliEn::MSS::grid);

use AliEn::MSS::grid;

use strict;
#
#  edg://<REPLICA CATALOG><EDG lfn>
#
# 

sub new {
    my $self = shift;#
    my $options2= shift;
    my $options =(shift or {});

    $self->GetEDGSE($options);
    $ENV{GLOBUS_LOCATION}="/opt/globus";
    delete $ENV{X509_CERT_DIR};
#    $options->{NO_CREATE_DIR} = 1;
    $self = $self->SUPER::new($options2, $options);
    $self or return;
    # Check whether LCG is working...
    $self->{LOGGER}->info("LCG","Command is edg-rm --vo $self->{CONFIG}->{ORG_NAME} printInfo" );
    my $result = system("edg-rm --vo $self->{CONFIG}->{ORG_NAME} printInfo");
    if ($result) {
      $self->{LOGGER}->error("LCG","Could not contact LCG services ($result), aborting the SE.");
#      return;
    }
    $self->{LOGGER}->info("LCG","This is the dimwit version of the LCG SE interface!");
    return $self;
}

sub GetEDGSE {
  my $self = shift;
  my $options = shift;
  my $config = AliEn::Config->new();
  $config->{SE_SAVEDIR}="/castor/cern.ch/grid/alice";
  return 1;
}

sub mkdir {
    return 0;
}

sub GetPhysicalFileName {
    my $self = shift;
    $self->{LOGGER}->info("LCG","I am dimwit, I do no registration on RLS." );
    return;
}

sub url {
    my $self = shift;
    my $file = shift;
    $file = "\L$file\E";
    my $url = "castor://wacdr001d.cern.ch/castor/cern.ch/grid/alice/$file";
    $self->{LOGGER}->info("LCG","AliEn URL is $url\n");
#    return "castor://$self->{HOST}$file";
    print "Returning $url\n";
    return $url;
}

sub sizeof {
  my $self = shift;
  my $file = shift;

  $self->{LOGGER}->info("LCG","I am dimwit, no sizeof() here.");
  print "I am dimwit, no sizeof() here.\n";
  return;
}

sub cp {
  my $self=shift;
  my (@args) = @_;
  $self->{LOGGER}->info("LCG", "Putting a file into CERN CASTOR with @args");
  print "LCG::put =======> $args[0] ----> $args[1] ($args[2])\n";
  print "This will actually do nothing for the time being (TEST VERSION!!!)\n";
  return 1;
}

return 1;
