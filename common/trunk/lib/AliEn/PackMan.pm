package AliEn::PackMan;

use AliEn::Config;
use strict;
use vars qw(@ISA);


push @ISA, 'AliEn::Logger::LogObject';

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = ( shift or {} );
  bless( $self, $class );
  $self->SUPER::new();
  $self->{CONFIG} or $self->{CONFIG}=AliEn::Config->new();
  if ($self->{PACKMAN_METHOD}){
    $self->info("This packman uses the method $self->{PACKMAN_METHOD}");
    my $name="AliEn::PackMan::$self->{PACKMAN_METHOD}";
    eval "require $name";
    if ($@){
      $self->info("Error requiring $name: $@");
      return;
    }
    bless($self, $name);


  }else {
    $self->{SOAP}=AliEn::SOAP->new() or return;
  }
  $self->initialize() or return;
  return $self;
}
sub initialize{
  return 1;
}
sub getListInstalled {
  my $self=shift;
  $self->info("Asking the PackMan for the packages that it has installed");
  my ($done)=$self->{SOAP}->CallSOAP("PackMan","getListInstalledPackages","ALIEN_SOAP_SILENT") or return;

  my @list=$done->paramsout;
  return  @list;

}

sub getListPackages {
  my $self=shift;
  $self->info("Asking the PackMan for the packages that it has installed");
  my ($done)=$self->{SOAP}->CallSOAP("PackMan","getListPackages") or return;

  my @list=$done->paramsout;
  $self->info("Returning @list");
  return @list;
}

sub installPackage{
  my $self=shift;
  my $user=shift;
  my $package=shift;
  my $version=shift;
 

  $self->info("Asking the PackMan to install");
  
  my $result;
  my $retry=5;
  while (1) {
    $self->info("Asking the package manager to install $package as $user");
      
    $result=$self->{SOAP}->CallSOAP("PackMan", "installPackage", $user, $package, $version) and last;
    my $message=$AliEn::Logger::ERROR_MSG;
    $self->info("The reason it wasn't installed was $message");
    $message =~ /Package is being installed/ or $retry--;
    $retry or last;
    $self->info("Let's sleep for some time and try again");
    sleep (30);
  }
  if (! $result){
    $self->info("The package has not been instaled!!");
    return;
  }
  my ($ok, $source)=$self->{SOAP}->GetOutput($result);
  $self->info("The PackMan returned '$ok' and '$source'");
  return ($ok, $source);

}



return 1;

