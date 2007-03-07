package AliEn::SE::Methods::fdt;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA $DEBUG);
@ISA = ("AliEn::SE::Methods::Basic");

$DEBUG=0;

use strict;

sub initialize {
    my $self = shift;
    $self->{FDT} = "$ENV{ALIEN_ROOT}/java/MonaLisa/java/bin/java -jar $ENV{ALIEN_ROOT}/java/MonaLisa/Service/lib/fdt.jar";
    $self->info("Initializing AliEn::SE::Methods::fdt");
    $self->{SILENT}=1;
    $self->{CLASS}= ref $self;
    return $self->{CLASS};
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
  my $localDir = ($self->{LOCALFILE} =~ /^(.*)\// ? $1 : "");
  my $localTarget = ($self->{LOCALFILE} =~ /([^\/]*)$/ ? $1 : "");
  my $localFile = ($self->{PARSED}->{PATH} =~ /([^\/]*)$/ ? $1 : "");
  my $command="$self->{FDT} -c $self->{PARSED}->{HOST} ";
  $command .= "-p $self->{PARSED}->{PORT} " if $self->{PARSED}->{PORT};
  $command .= "-pull -d $localDir $self->{PARSED}->{PATH}";

  # At the moment, xrdcp doesn't return properly. Let's check if the file exists
  $self->_execute($command);
 
  (-f "$localDir/$localFile" && rename("$localDir/$localFile", $self->{LOCALFILE})) or return;
  
  $self->debug(1,"YuHuHUUUU!!\n");
  return $self->{LOCALFILE};
}

sub put {
  my $self=shift;
  $self->debug(1,"Trying to put the file $self->{PARSED}->{ORIG_PFN} (from $self->{LOCALFILE})");

  my $rmtDir = ($self->{PARSED}->{PATH} =~ /^(.*)\// ? $1 : "");
  my $rmtFile= ($self->{PARSED}->{PATH} =~ /([^\/]*)$/ ? $1 : "");
  my $localDir = ($self->{LOCALFILE} =~ /^(.*)\// ? $1 : "");
  my $localFile = ($self->{LOCALFILE} =~ /([^\/]*)$/ ? $1 : "");
  my $command="(cd $localDir && ln -sf $localFile $rmtFile && $self->{FDT} -c $self->{PARSED}->{HOST} ";
  $command .= "-p $self->{PARSED}->{PORT} " if $self->{PARSED}->{PORT};
  $command .= "-d $rmtDir $rmtFile && rm $rmtFile)";
  if(open(OUT, "$command 2>/dev/null |")){
    my $transfSize = -1;
    my $fileSize = -s $self->{LOCALFILE};
    while(<OUT>){
      chomp;
      $transfSize = $1 if $_ =~ /finishes\s+TotalBytes:\s+(\d+)/;
    }
    close OUT;
    if($transfSize eq $fileSize){
    	$self->info("YuHuHUUUU!");
	return "fdt://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
    }else{
    	$self->info("The file has not been completely transfered. Command was '$command'");
	return;
    }
  }else{
    $self->info("Something went wrong. Command was '$command'");
  }
  return;
}

sub remove {
  my $self=shift;
  $self->debug(1,"Trying to remove the file $self->{PARSED}->{ORIG_PFN}");

  #my $command="xrm root://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
  #my $error=$self->_execute($command);

  #($error<0) and return;
  $self->debug(1,"Not implemented!!\n");
  return "fdt://$self->{PARSED}->{HOST}:$self->{PARSED}->{PORT}/$self->{PARSED}->{PATH}";
}

sub getSize {
    return 1;
}

return 1;

