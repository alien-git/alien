use strict;


use AliEn::UI::Catalogue;


my $ui=AliEn::UI::Catalogue->new({user=>"newuser"}) or exit(-2);


my $dir="caseSen";
$ui->execute("rmdir", "-rf", $dir);
$ui->execute("mkdir", "-p", $dir) or exit(-2);
$ui->execute("touch", "$dir/file1",44) or exit(-2);
$ui->execute("touch", "$dir/FILE2",44) or exit(-2);

my @files=$ui->execute("find", $dir, "file")
  or exit(-2);

($#files ne "0") and print "We found several files!! (@files)\n" and exit;

print "FOUND @files\n";

@files=$ui->execute("find", $dir, "FILE")
  or exit(-2);

($#files ne "0") and print "We found several FILE!! (@files)\n" and exit;

print "FOUND @files\n";


$ui->close();

print "DONE!!\n";
