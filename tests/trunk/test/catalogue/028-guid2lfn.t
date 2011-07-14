use strict;

use AliEn::UI::Catalogue::LCM;
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;
includeTest("catalogue/003-add") or exit(-2);

my $cat = AliEn::UI::Catalogue::LCM->new({user => "newuser"}) or exit(-2);

#addFile($cat, "lfn2gui","This is just to check the guid...
#") or exit(-2);
$cat->execute("touch", "lfn2gui");
my $cat2 = AliEn::UI::Catalogue::LCM->new({role => 'admin'}) or exit(-2);
$cat2->execute("checkLFN");
$cat2->close();
my ($guid) = $cat->execute("lfn2guid", "lfn2gui") or exit(-2);
print "The guid is $guid\n";

my ($lfn) = $cat->execute("guid2lfn", $guid) or exit(-2);

print "The lfn is $lfn\n";

$lfn =~ /lfn2gui$/ or print "It is not the right lfn $lfn!!\n" and exit(-2);
print "ok\n";
