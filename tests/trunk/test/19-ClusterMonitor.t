#!/bin/env alien-perl

use strict;
use Test;
BEGIN { plan tests => 1 }
use AliEn::Config;
{

eval "require AliEn::Service::ClusterMonitor" 
  or print "Error requiring the package\n $! $@\n" and exit(-2);

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

includeTest("14-se") or exit(-2);
my $config=new AliEn::Config or exit(-2);

my $key="name=testCE,ou=CE,ou=Services,$config->{FULLLDAPDN}";

addLdapEntry($key,["objectClass", ["AliEnCE"],
		   "name", "testCE",
		   "host", $config->{HOST},
		   "type", "FORK",
		   "maxjobs", 5,
		   "maxqueuedjobs", 5,
		  ]) or exit(-2);

startService("Monitor") or exit(-2);
print "YUHUUU\n";

my $command="alien proxy-destroy";
$< or $command="su - alienmaster -c '$command'";
system ($command);
ok(1);



}
