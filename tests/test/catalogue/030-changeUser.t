use strict;
use AliEn::UI::Catalogue::LCM;


my $cat=AliEn::UI::Catalogue::LCM->new({"role"=>"admin"}) or exit(-2);
print "Got the catalogue\n";

my ($dir)=$cat->execute("pwd");
$dir =~ s{/a/admin}{/n/newuser/changeUser};
$cat->execute("rmdir","-silent", $dir);
print "Changing user\n";
#$cat->execute("debug", 5);
$cat->execute("user", "-", "newuser") or exit(-2);

$cat->execute("cd") or exit(-2);
$cat->execute("mkdir", $dir) or exit(-2);
print "Directory $dir created :) Let's check the owner\n";


my ($owner)=$cat->execute("ls", '-ltd', $dir) or exit(-2);
print "Got\n";
use Data::Dumper;
print Dumper($owner);

$owner =~ /admin/ and print "The owner is admin :(\n" and exit(-2);
$owner =~ /newuser/ or print "The owner is not newuser :(\n" and exit(-2);

print "OK! :)\n";
