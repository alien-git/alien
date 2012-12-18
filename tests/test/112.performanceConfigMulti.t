use strict;
use Time::HiRes;
use AliEn::Config;


my $clients=1;
my $now=Time::HiRes::time();
my $start=$now+$clients;
my $j=0;
my $total=100;

while ($j++<$clients){
  my $id=fork();
  if (! $id) {
    my $time=Time::HiRes::time();
    my $sleep=$start-$time;
    print "$$ Sleeping $sleep \n";
    Time::HiRes::sleep ($sleep);
    getConfig($total, $$);
    exit;
  }
}
print "FATHER WAITS\n";
wait();
print "FATHER QUITS\n";
sub getConfig{
  my $total=shift;
  my $id=shift;
  my $i=$total;;
  my $before=Time::HiRes::time();
  while ($i--) {
    print "DOING $i\n";
    my $c=AliEn::Config->Reload({force=>1});

  }
  my $after=Time::HiRes::time();

  my $sec=$after-$before;
  my $div=$sec/$total;
  print "It took $sec seconds to do $total calls ( $div call/s)\n";
}
