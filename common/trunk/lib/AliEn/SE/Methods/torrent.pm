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
  
  my $args="-V --bt-external-ip=$IP --follow-torrent=mem --timeout=300 --bt-stop-timeout=300";

  open (CMD, "aria2c $args --seed-time=0 $link -d $self->{LOCALFILE}_dir|") or
    $self->info("Error getting the torrent file!!")  and return;
  my @out=<CMD>;
  my $status=close CMD;
  print "Got the file and @out and $status\n";
  $status or
    $self->info("Error getting the file '$link'") and return;

  $self->info("Here we should check the signature");
  $self->info("And move the file");

  opendir (MYDIR, "$self->{LOCALFILE}_dir") or $self->info("Error opening the directory") and return;
  my @dirs=grep (! /.sha1$/, grep (!/^\./, readdir(MYDIR)));
  closedir(MYDIR);
  if ($dirs[0]) {
    opendir (MYDIR, "$self->{LOCALFILE}_dir/$dirs[0]") or 
      $self->info("Error opening the directory $self->{LOCALFILE}_dir/$dirs[0]") and return;
    my @dirs2=grep (! /.sha1$/, grep (!/^\./, readdir(MYDIR)));
    closedir(MYDIR);
    
    if ($dirs2[0]) {
      symlink("$self->{LOCALFILE}_dir/$dirs[0]/$dirs2[0]",$self->{LOCALFILE});
    }
  }
  $self->info("Finally, start the seeder");
  system("aria2c $args --seed-time=10 --seed-ratio=0 -d $self->{LOCALFILE}_dir  $link > /dev/null 2&>1 &"); 
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

