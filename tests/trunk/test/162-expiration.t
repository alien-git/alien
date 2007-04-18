use strict;

use AliEn::UI::Catalogue::LCM;

$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM->new({"user"=>"newuser"}) or exit(-2);


print "Let's add three files\n";
$cat->execute("rmdir", "-rf", "expired", "-silent");
$cat->execute("mkdir", "expired") or exit(-2);
$cat->execute("touch", "expired/touch") or exit(-2);
addFile($cat, "expired/file", 'Hello world
') or exit(-2);


$cat->execute("setExpired",4, "expired/touch", "expired/file") or exit(-2);

print "Now, let's wait until the files have been removed\n";
sleep(60);

print "And now let's check if they are there";
$cat->execute('ls', 'expired/touch') and exit(-2);;
$cat->execute('ls', 'expired/file') and exit(-2);;
$cat->execute('ls', 'expired/touch.expired') or exit(-2);;
$cat->execute('ls', 'expired/file.expired') or exit(-2);;

print "ok\n";
