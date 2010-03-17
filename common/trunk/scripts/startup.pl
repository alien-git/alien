use strict;
use Apache::SOAP;
use AliEn::Logger;
use AliEn::Database::TaskQueue;
#use Test::ServerDemo;
use Test::World;

#my @services=("Broker::Job","Broker::Transfer",
#);
my @services=qw( PackMan );

#following line is a mark for 300-prepareBankService.t. Please don't remove or modify 
#[TEST_MARK]
# Here come essential env vars 

$ENV{ALIEN_ROOT}="/home/alienmaster/alien.v2-17.84"; 
$ENV{ALIEN_HOME}="/home/alienmaster/.alien"; 
$ENV{ALIEN_ORGANISATION}="pcalice57"; 
$ENV{ALIEN_LDAP_DN}="pcalice57:8389/o=pcalice57,dc=cern,dc=ch"; 
#$ENV{ALIEN_DATABASE_SSL}="adminssl";

my $l=AliEn::Logger->new();

$l->infoToSTDERR();

foreach my $s (@services) {
  print "Checking $s\n";
  my $name="AliEn::Service::$s";
  eval {
    eval "require $name" or die("Error requiring the module: $@");
    my $serv=$name->new() ;
    $serv or exit(-2);
    # $serv->startListening();
    

  };
  if ($@) {
    print "NOPE!!\n $@\n";
        
    exit(-2);
  }

}

print "ok\n";

