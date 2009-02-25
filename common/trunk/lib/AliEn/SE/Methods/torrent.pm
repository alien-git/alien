package AliEn::SE::Methods::torrent;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA );
@ISA = ("AliEn::SE::Methods::Basic");


use AliEn::CE;
sub initialize {
    my $self = shift;
}

sub get {
  my $self = shift;

  $self->info ("Getting a torrent file");

  my $link=$self->{ORIG_PFN};

  $link=~ s/^torrent/http/;

  my $interface=`/sbin/route -n | grep ^0.0`;
  chomp $interface;
  $interface=~ s/^.*\s(\S+)\s?$/$1/;
  $self->debug(1,"INTERFACE '$interface'");
  my $IP;
  my $line=`/sbin/ifconfig $interface`;
  
  $line=~ /inet addr:(\S+)/m and $IP=$1;
  if (!$IP){
    $IP=`hostname -i`;
    chomp $IP;
    $self->info("Can't get the IP, using $IP");
  }
  $self->info("Getting the file $link, using the IP $IP");
  
  my $args="-V --bt-external-ip=$IP --follow-torrent=mem";

  open (CMD, "aria2c $args --seed-time=0 $link|") or
    $self->info("Error getting the torrent file!!")  and return;
  my @out=<CMD>;
  close CMD;
  print "Got the file and @out\n";
  $self->info("And now we should start the seeder");
  


  system("aria2c $args --seed-time=10 --seed-ratio=0 $link  > /dev/null 2&>1 &"); 
  #"$PROGRAM" $ARGS --seed-time=10 --seed-ratio=0 "$@" &>/dev/null &
  $self->info("Returning");
  return $self->{LOCALFILE};
}

sub getSize {
  my $self = shift;
  $self->info("I'm not sure how to get the size of a torrent file...");
  return 1;
}
return 1;

