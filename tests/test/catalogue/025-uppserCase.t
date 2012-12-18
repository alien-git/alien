use strict;

use AliEn::UI::Catalogue;


my $cat=AliEn::UI::Catalogue->new ({user=>"newuser"}) or exit(-2);


$cat->execute("rmdir", "-silent", "-r", "test-upper");
$cat->execute("mkdir", "test-upper") or exit(-2);

$cat->execute("cd", "test-UPPER") and print "Error: I can move to the directory\n" and exit(-2);

$cat->execute("cd", "test-upper") or print "Error: I can't move to the directory!!\n" and exit(-2);


print "YUHUUU!\n";


