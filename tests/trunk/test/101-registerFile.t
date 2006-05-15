use strict;

use AliEn::UI::Catalogue;

my $cat=AliEn::UI::Catalogue->new({user=>"newuser"}) or exit(-2);

print "Let's try to register a file in a non-existent directory...";

my $file="/tmp/test_file.$<";
open (FILE, ">$file") or print "Error creating $file\n" and exit(-2);

print FILE "HELLO\n";
close FILE;

$cat->execute("register", "not_a_directory/myfile.".time, $file) and
  print "The registration worked!!!!! :(\n" and exit(-2);

print "ok\nLet's try to create a directory in a non_existent directory...";

$cat->execute("mkdir", "not_a_directory/mydir") and
  print "The registration worked!!!!! :(\n" and exit(-2);


unlink $file;
$cat->close();

print "ok\n";

