use strict;


use AliEn::UI::Catalogue::LCM;
use Test;


BEGIN { plan tests => 1 }

my  $cat=AliEn::UI::Catalogue::LCM->new({"user", "newuser",});
$cat or exit (-1);

my $file="/tmp/alien.$<.file.$$";
(-f $file) and unlink $file;

(-f $file) and print "Error: the file $file exists\n" and exit(-2);

$cat->execute("cd") or exit(-2);
$cat->execute("pwd") or exit(-2);
$cat->execute("rm","emptyFile");
$cat->execute("debug","Catalogue,Methods");
my @rets = $cat->execute("add","-r","emptyFile file://$cat->{CONFIG}->{HOST}:8092/$file, 0, ab");
$rets[0] and print "This is not supposed to work!!\n" and exit(-2);


ok(1);
