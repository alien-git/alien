use strict;
use AliEn::UI::Catalogue::LCM;

my $cat=AliEn::UI::Catalogue::LCM->new({user=>'newuser'}) or exit(-2);

$cat->execute("rm", "-rf", "-silent", "moveFile", "moveFiledest");
$cat->execute("rmdir", "test", "gest");
print "Let's create an empty file\n";
$cat->execute("touch", "moveFile") or exit(-2);
print "Now, let's move it\n";
$cat->execute("mv", "moveFile", "moveFiledest") or exit(-2);
print "Let's check that the source doesn't exist\n";
$cat->execute("ls", "moveFile") and exit(-2);
print "And that the target exists\n";
$cat->execute("ls", "moveFiledest") or exit(-2);

print "Checking mv on directory(both source and target)\n";
$cat->execute("mkdir", "-p", "test/fest","gest");
$cat->execute("mv", "test/", "gest/");
$cat->execute("touch", "gest/test/fest/testFile.touch") or exit(-2);

