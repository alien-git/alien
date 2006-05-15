use strict;
use AliEn::UI::Catalogue::LCM;

my $c= AliEn::UI::Catalogue::LCM->new({user=>"newuser"}) or exit(-2);

$c->execute("rm", "httpFile");

print "Adding non existing http file (should fail)...\n";
$c->execute("add", "httpFile", "http://alien.cern.ch/blablablabla")
  and exit(-2);

print "Adding non existing http host (should fail)...\n";
$c->execute("add", "httpFile", "http://aliendasdas.cern.ch/blablablabla")
  and exit(-2);

print "Adding non existing http file (should work)...\n";
$c->execute("add", "httpFile",  "http://alien.cern.ch/index.html")
  or exit(-2);

