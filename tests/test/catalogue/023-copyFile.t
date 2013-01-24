use strict;

use AliEn::UI::Catalogue::LCM;

my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser"}) or exit(-2);

$cat->execute("rmdir", "-rf", "copyDirectory");
$cat->execute("mkdir", "copyDirectory") or exit(-2);

$cat->execute("cd", "copyDirectory") or exit(-2);

print "Trying to copy a file that doesn't exist\n";
$cat->execute("cp", "file1.txt", "file2.txt") and 
  print "WE COULD COPY A FILE THAT DOESN'T EXIST\n" and exit(-2);

print "YUHUU\n";

$cat->execute("add", "myfile", "$ENV{ALIEN_HOME}/Environment") or exit(-2);

$cat->execute("cp", "myfile", "file2.txt") or
  print "Error copying a file\n" and exit(-2);
print "OK\n";
