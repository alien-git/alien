use strict;
use AliEn::UI::Catalogue;

my $cat=AliEn::UI::Catalogue->new({user=>"newuser"}) or exit(-2);



$cat->execute("rmdir", "-rf", "findXML");
$cat->execute("mkdir", "-p", "findXML/") or exit(-2);
$cat->execute("touch", "findXML/myfile") or exit(-2);

print "Trying a normal find\n";

$cat->execute("find", "findXML", "myfile") or exit(-2);

print "Trying the find with xml\n";

$cat->execute("find", "-x", "file.xml", "findXML", "myfile") or exit(-2);

print "ok\n";

