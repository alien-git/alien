use strict;
use AliEn::UI::Catalogue;

eval "require AliEn::Service::ClusterMonitor" 
  or print "Error requiring the package\n $! $@\n" and exit(-2);

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

includeTest("93-cpdir") or exit(-2);

my $c=AliEn::UI::Catalogue->new ({user=>'newuser'}) or exit(-2);
my $c2=AliEn::UI::Catalogue->new ({user=>'alienmaster'}) or exit(-2);

$c->execute("mkdir", "-p", 'rmDir') or exit(-1);

print "Checking if another user can remove the directory\n";
$c2->execute("rmdir", "-rf", "../../n/newuser/rmDir") and exit(-2);

print "Ok, let's delete the directory\n";
$c->execute("rmdir", "-rf", 'rmDir') or exit(-1);
print "ok\n";

