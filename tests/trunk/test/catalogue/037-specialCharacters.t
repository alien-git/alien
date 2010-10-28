use strict;


use AliEn::UI::Catalogue::LCM;
use Data::Dumper;


my $ui=AliEn::UI::Catalogue::LCM->new({role=>'newuser'}) 
  or exit(-2);


my @dirs=('0','1+1', '1%2',  '1@5', 
#'1$6'
);

$ui->execute("rmdir","-rf", 'special');
$ui->execute("mkdir", 'special') or exit(-2);

foreach my $dir (@dirs){
  print "Let's create a directory called '$dir'\n";
  $ui->execute("mkdir", "special/$dir") or print "NOPE" and exit(-2);
  print "Can I go into the directory?\n";
  $ui->execute("cd", "special/$dir") or print "NOPE" and exit(-2);
  $ui->execute('cd');
}

print "OK!\n";
