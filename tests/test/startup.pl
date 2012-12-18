use strict;
use Apache::SOAP;
use AliEn::Logger;
use AliEn::Database::TaskQueue;

#my @services=("Broker::Job","Broker::Transfer","LBSG" #"Test2", "IS"
#);
my @services=("LBSG");

#following line is a mark for 300-prepareBankService.t. Please don't remove or modify 
#[TEST_MARK]
#$ENV{ALIEN_DATABASE_SSL}="adminssl";

my $l=AliEn::Logger->new();

$l->infoToSTDERR();

foreach my $s (@services) {
  print "Checking $s\n";
  my $name="AliEn::Service::$s";
  eval {
    eval "require $name" or die("Error requiring the module: $@");
    $name->new() or exit(-2);

  };
  if ($@) {
    print "NOPE!!\n $@\n";
        
    exit(-2);
  }

}

print "ok\n";

