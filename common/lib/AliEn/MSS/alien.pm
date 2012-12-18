package AliEn::MSS::alien;

use strict;
use AliEn::MSS;

use vars qw(@ISA);

@ISA = ( "AliEn::MSS" );


sub mkdir {
}
sub get {
  my $self=shift;
  my ($from, $to)=@_;
  $self->debug(1, "Trying to get a file @_");
  my $done=$self->executeCommand("get", @_);
  $done or return -1;
  return 0;
}

sub executeCommand{
  my $self=shift;

  my $org=$self->{HOST};
  my $oldOrg=$self->{CONFIG}->{ORG_NAME};
  $self->debug(1, "Organisation -> $org");
  my $tmp=$self->{CONFIG}->Reload({"organisation", $org});
  if (!$tmp) {
    $self->info("Error getting the config of $org");
    return;
  }
  $self->{CONFIG}=$tmp;

  my $cat=AliEn::UI::Catalogue::LCM->new({silent=>1} );
  if (!$cat){
    $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});
    $self->{LOGGER}->info("MSS/Alien", "Error getting the authentication for $org");
    return;
  }
  my @done=$cat->execute(@_);

  $cat->close();
  $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});

  if (! @done) {
    $self->info("Error getting the file");
    return;
  }
  $self->debug(1, "Everything worked!!");
  return @done;
}

sub put {

}

sub rm {
}

sub sizeof {
  my $self=shift;
  my $file = shift;

  $self->debug(1, "Trying to get a file @_");
  my $baseName=$file;
  $baseName=~ s/^.*\/([^\/]*)$/$1/;
  my (@size)=$self->executeCommand("ls", "-la", $file);
  (@size) or return -1;
  $self->debug(1, "Got @size and $baseName");
  @size = grep (s /^([^\#]*\#{3}){3}(\d+)\#{3}[^\#]*\#{3}$baseName\#+/$2/, @size);
  $self->debug(1, "After selecting @size");
  @size or $self->info("Error that file does not exist");

  return $size[0];
}


sub url {
    my $self = shift;
    my $file = shift;

    return "alien://$self->{ORGANISATION}$file";
}


return 1;

