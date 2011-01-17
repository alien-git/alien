use strict;
use AliEn::UI::Catalogue::LCM;


my $cat=AliEn::UI::Catalogue::LCM->new({"role", "admin"});
$cat or exit (-1);

print "Let's register a file with a huge size\n";
my $file="myHugeFakeFile";
my $size=78103968171;


$cat->execute("rm", "-rf", $file);
$cat->execute("add", "-r", $file, "file://$cat->{CONFIG}->{HOST}//tmp/pcepalice11/log/SE_DATA/12/19431/../../../../../../dev/null", $size, "ffeed") or exit(-2);

print "The file is registered. Let's check the size\n";
my ($info)=$cat->execute("ls", "-z", $file, "-la")or exit(-2);

use Data::Dumper;
print Dumper($info);

${${$info}[0]}[0]->{size} eq $size or print "The size is ${${${$info}[0]}}[0]->{size}!! (instead of $size)\n" and exit(-2);
print "ok\n";


