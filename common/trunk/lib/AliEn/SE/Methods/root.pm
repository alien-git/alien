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
  my $command="xrdcp root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH} $self->{LOCALFILE} -DIFirstConnectMaxCnt 1";

  # At the moment, xrdcp doesn't return properly. Let's check if the file exists
  $self->_execute($command);

  (-f $self->{LOCALFILE}) or return;

  $self->debug(1,"YUUHUUUUU!!\n");
  return $self->{LOCALFILE};
}

sub put {
  my $self=shift;
  $self->debug(1,"Trying to put the file $self->{PARSED}->{ORIG_PFN} (from $self->{LOCALFILE})");

  $self->{PARSED}->{PATH}=~ s{^//}{/};
  my $command="xrdcp -np -v $self->{LOCALFILE} root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH} -DIFirstConnectMaxCnt 1";

#  my $error = $self->_execute($command);
  open (OUTPUT, "$command 2> /dev/null |") or $self->info("Error: xrdcp is not in the path") and return;
  my @output=<OUTPUT>;
  close OUTPUT;
  $self->debug(2, "Got the output: @output");
  my ($line)=grep (/Data Copied \[bytes\]\s*:\s*(\d+)/, @output);
  if ($line){
    $line=~ /Data Copied \[bytes\]\s*:\s*(\d+)/;
    $self->info("Transfered  $1  bytes");
    my $size=-s $self->{LOCALFILE};
    if ($size eq $1){
      $self->info("YUHUUU!!");
      return "root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH} -DIFirstConnectMaxCnt";
    } 
    $self->info("The file has not been completely transfered");
    return;
  }

#  $error and return;
  $self->info("Something went wrong with xrdcp!!\n @output\n");
  return;
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
