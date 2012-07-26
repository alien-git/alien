use strict;

use AliEn::UI::Catalogue::LCM;
use Data::Dumper;

my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser"}) or exit(-2);
my $cat_adm=AliEn::UI::Catalogue::LCM->new({role=>"admin"}) or exit(-2);

$cat->execute("rm", "-rf temporary_masterSE");
$cat_adm->execute("checkLFN");
my $se=$cat->{CONFIG}->{SE_FULLNAME};
print "Checking the entries of $se\n";
my ($info)=$cat->execute("masterSE", $se);
$info or exit(-2);
print Dumper($info);

print "Using the -lfn option\n";
my ($list)=$cat->execute("masterSE", $se, "print -lfn");
$info or exit(-2);
print Dumper($info);

print "This looks good. Let's add one more file, and see if it gets updated\n";
$cat->execute("add", "temporary_masterSE", "/etc/hosts", $se) or exit(-2);
$cat_adm->execute("checkLFN");
my ($info2)=$cat->execute("masterSE", $se);
print Dumper($info2);

if ($info2->{referenced} != $info->{referenced}+1){
   print "The number of referenced files has not increased by on (from $info->{referenced} to $info2->{referenced}\n";
   exit(-2);
} 





print "OK\n";
