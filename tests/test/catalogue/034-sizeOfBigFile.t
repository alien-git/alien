use strict;
use AliEn::UI::Catalogue::LCM;

my $config=new AliEn::Config;

my $cat=AliEn::UI::Catalogue::LCM->new({"role", "admin"});
$cat or exit (-1);

print "Let's register a file with a huge size\n";
my $file="myHugeFakeFile";
my $size=78103968171;


$cat->execute("rm", "-rf", $file);
system("cp","/etc/passwd","$config->{LOG_DIR}/SE_DATA/");
$cat->execute("add", "-r", $file, "file://$cat->{CONFIG}->{HOST}/$config->{LOG_DIR}/SE_DATA/passwd", $size, "ffeed") or exit(-2);

print "The file is registered. Let's check the size\n";
my ($info)=$cat->execute("ls", "-z", $file, "-la")or exit(-2);

use Data::Dumper;
print Dumper($info);

${${$info}[0]}[0]->{size} eq $size or print "The size is ${${${$info}[0]}}[0]->{size}!! (instead of $size)\n" and exit(-2);
print "ok\n";


