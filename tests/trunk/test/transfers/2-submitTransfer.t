use strict;

use AliEn::UI::Catalogue::LCM;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("catalogue/003-add") or exit(-2);

my $cat= AliEn::UI::Catalogue::LCM->new({user=>"newuser"})
  or exit(-2);
addFile($cat, "FTDfile.txt", "This file is going to be transfered with the FTD\n", "r") or exit(-2);
print "Let's do a transfer of the file FTDfile.txt\n";
my $seName=$cat->{CONFIG}->{SE_FULLNAME};
my ($done, $id)=$cat->execute("mirror", "FTDfile.txt", "${seName}2") or exit(-2);


print "YUHUUU. Transfer $id\n";
$cat->execute("listTransfer") or exit(-2);
my ($status)=$cat->execute("listTransfer", "-id", $id) or exit(-2);
print "Got the status of the transfer\n";
use Data::Dumper;
print Dumper ($status);
