package AliEn::Service::Config;

use AliEn::Service;

$ENV{ALIEN_CONFIG_FROM_LDAP}=1;

use vars qw(@ISA $DEBUG);

@ISA=qw(AliEn::Service);

use strict;
my $self = {};
$DEBUG=0;

sub initialize {
  $self = shift;  my $options =(shift or {});

  $self->{LOGGER}->debug( "SE", "Creatting a Config");

  if ($self->{CONFIG}->{CONFIG_ADDRESS}){
    my $address=$self->{CONFIG}->{CONFIG_ADDRESS};
    $address =~ s/^(https?):\/\/// and ($1 eq "https") and $self->{SECURE}=1;
    ($self->{HOST}, $self->{PORT})=split (/:/, $address);
  } else {
    $self->{HOST}=$self->{CONFIG}->{HOST};
    $self->{PORT}=$self->getPort();
  }
  $self->{SERVICE}="Config";
  $self->{SERVICENAME}="Config";
  #Let's get the address of the ldap server of our VO
  $self->{LDAPSERVERS}={};
#  $self->_GetLDAPAddress($self->{CONFIG}->{ORG_NAME}) or return;
  $self->{cache}={};
  return $self;

}

sub GetConfiguration {
  my $this=shift;
  my $vo=(shift ||"");
  my $domain=(shift || "");
  my $hostname=(shift || "");

  $self->{LOGGER}->info("Config", "Someone from hostname asked the configuration of $vo");
  if ($self->{cache}->{$domain}){
    $self->info("There is something in the cache!!");
    my $time=time();
    if ($self->{cache}->{$domain}->{expired}>$time) {
      $self->info("Returning a config from the cache");
      return $self->{cache}->{$domain}->{value};
    }
    
  }
  my $config;
  eval {
    $config=AliEn::Config->Initialize({organisation=>$vo,
				       domain=>$domain,
				       skip_localfiles=>1,
				       force=>1,
				      });
    delete $config->{LOGGER};
  };

  if ($@) {
    $self->die('NOCONFIG', "ERROR: $@");
  }
  my $time=time;
  $config=SOAP::Data->type("HASH"=>$config);
  $self->{cache}->{$domain}={value=>$config,
			     expired=>$time+60};;
  $self->info("Returning a configuration");
  return $config;
}


1
