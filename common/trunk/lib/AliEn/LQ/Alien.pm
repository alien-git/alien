package AliEn::LQ::Alien;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use AliEn::CE;
use strict;

sub initialize {
  my $self=shift;  


  $self->{CONFIG}->{CE_SUBMITARG} or $self->info("Error: we are missing the name of the VO to send the info to. Please, put it in the SUBMITARG") and return;
  my @list=@{$self->{CONFIG}->{CE_SUBMITARG_LIST}};
  $self->{ENV_TO_CHANGE}={};
  foreach (@list){
    my ($key, $value)=split(/=/,$_,2);
    $value or $self->info("Skiping the argument '$_' (not in <key>=<value> format") and next;
    $self->{ENV_TO_CHANGE}->{$key}={old=>$ENV{$key}, new=>$value};
    my $message='undef';
    $ENV{$key} and $message=$ENV{$key};
    $self->info("We will set $key to $value (instead of $message)");
  }
  $self->{ENV_TO_CHANGE}->{ALIEN_ORGANISATION} or $self->info("Error: missing the name of the organisation in the arguments") and return;

  $self->setEnv();
  $self->{CE}=AliEn::CE->new();
  $self->unsetEnv();
  $self->{CE} or $self->info("Error creating the CE") and return;
  $self->info("All the initialization worked!!!");
  $self->info("This CE submits to a different VO ($self->{ENV_TO_CHANGE}->{ALIEN_ORGANISATION})");
  return $self;
}

sub setEnv{
  my $self=shift;
  foreach my $key (keys %{$self->{ENV_TO_CHANGE}}){
    $ENV{$key}=$self->{ENV_TO_CHANGE}->{$key}->{new};
  }
  $self->{CE} and $self->{CE}->{SOAP}->deleteCache();

  $self->{CONFIG}=$self->{CONFIG}->Reload({force=>1});
}

sub unsetEnv{
  my $self=shift;
  foreach my $key (keys %{$self->{ENV_TO_CHANGE}}){
    delete $ENV{$key};
    $self->{ENV_TO_CHANGE}->{$key}->{old} or next;
    $ENV{$key}=$self->{ENV_TO_CHANGE}->{$key}->{old};
  }
  $self->{CE}->{SOAP}->deleteCache();
  $self->{CONFIG}=$self->{CONFIG}->Reload({force=>1});

}

sub submit {
  my $self = shift;
  my $classad=shift;
  my ( $command, @args ) = @_;
  $self->info("Submitting the job to another V.O ($self->{ENV_TO_CHANGE}->{ALIEN_ORGANISATION}->{new})");
  my $env="ALIEN_CM_AS_LDAP_PROXY=$ENV{ALIEN_CM_AS_LDAP_PROXY}";
  foreach my $key (keys %{$self->{ENV_TO_CHANGE}}){
    $self->{ENV_TO_CHANGE}->{$key}->{old} or next;
    $env.=" $key=$self->{ENV_TO_CHANGE}->{$key}->{old}";
  }
  my $jdl="executable=\"runOtherVOJobAgent\";
arguments=\"$env\";
requirements=other.CE==\"pcegee02::CERN::pcegee02\"";
  my $old= $ENV{ALIEN_CM_AS_LDAP_PROXY};
  delete  $ENV{ALIEN_CM_AS_LDAP_PROXY};

  $self->debug(1, "In submit, sending $jdl ");
  $self->setEnv();
  my @done=$self->{CE}->submitCommand("=<","$jdl");
  $self->unsetEnv();
  $self->debug(1, "In submit got @done");

  $ENV{ALIEN_CM_AS_LDAP_PROXY}=$old;
  if (! @done) {
    $self->info("Error getting the file");
    return -1;
  }
  $self->debug(1, "Everything worked!!");
  return 0 ;
}

sub getQueueStatus{
  my $self=shift;
  $self->info("Trying to get the number of jobs waiting (from $self->{CONFIG}->{HOST}");
  $self->setEnv();
  my @done=$self->{CE}->f_top(@_, "-submithost", $self->{CONFIG}->{HOST});
  $self->unsetEnv(); 
  my @ids;
  foreach my $job (@done){
    push @ids, "$job->{queueId} agent startup";
  }
  return @ids;
}

sub getNumberQueued{
  my $self=shift;
  return $self->getQueueStatus("-status", "WAITING");
}

return 1;
