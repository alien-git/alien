package AliEn::SE::Methods::root;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA $DEBUG);
@ISA = ("AliEn::SE::Methods::Basic");

$DEBUG=0;

use strict;

sub initialize {
    my $self = shift;
    $self->{SILENT}=1;
    $self->{CLASS}= ref $self;

}
sub _execute {
  my $self=shift;
  my $command=shift;

  $self->{LOGGER}->{LOG_OBJECTS}->{$self->{CLASS}}
    or $command.="> /dev/null 2>&1";

  $self->debug(1, "Doing $command");
  return system ($command);
}

sub get {
  my $self = shift;

  $self->debug(1,"Trying to get the file $self->{PARSED}->{ORIG_PFN} (to $self->{LOCALFILE})");
  $self->{PARSED}->{PATH}=~ s{^//}{/};
  my $command="xrdcp root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH} $self->{LOCALFILE} -OD\\&authz=alien -OS\\&authz=alien -DIFirstConnectMaxCnt 1";

  # At the moment, xrdcp doesn't return properly. Let's check if the file exists
  $self->_execute($command);

  (-f $self->{LOCALFILE}) or return;

  $self->debug(1,"YUUHUUUUU!!\n");
  return $self->{LOCALFILE};
}

sub put {
  my $self=shift;
  $self->debug(1,"Trying to put the file $self->{PARSED}->{ORIG_PFN} (from $self->{LOCALFILE})");

  my $command="xrdcp $self->{LOCALFILE} root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH} -DIFirstConnectMaxCnt 1";

  my $error = $self->_execute($command);

  $error and return;
  $self->debug(1,"YUUHUUUUU!!\n");
  return "root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH} -DIFirstConnectMaxCnt";
}

sub remove {
  my $self=shift;
  $self->debug(1,"Trying to remove the file $self->{PARSED}->{ORIG_PFN}");

  my $command="xrm root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
  my $error=$self->_execute($command);

  ($error<0) and return;
  $self->debug(1,"YUUHUUUUU!!\n");
  return "root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";

}

sub getSize {
    return 1;
}
return 1;
