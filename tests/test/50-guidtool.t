use strict;
use AliEn::GUID;

my $file="$ENV{ALIEN_ROOT}/bin/guidtool";
print "Checking if the file guidtool exists...";

(-f $file) or print "Error $file doesn't exist!!\n" and exit(-1);
print "ok\nLet's create a new guid...";
system("$file -c") and print "Error creating a new guid\n" and exit(-2);

open (FILE, "rpm --dbpath $ENV{ALIEN_ROOT}/rpmdb/ -qf $ENV{ALIEN_ROOT}/bin/guidtool|") or exit(-2);
my @package=<FILE>;
close FILE or exit(-2);
chomp(@package);
print "'guidtool' belongs to @package\n";
grep (/AliEn-Client/, @package) or print "'guidtool' is not in the client rpm!!\n" and exit(-2);
print "ok\n";

exit(0);