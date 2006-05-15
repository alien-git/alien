use strict;
use Time::HiRes;
use AliEn::Config;

my $l=new AliEn::Logger;

#$l->debugOn();

my $total=100;
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
