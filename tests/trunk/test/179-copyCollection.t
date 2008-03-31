use strict;



use AliEn::UI::Catalogue::LCM;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
includeTest("16-add") or exit(-2);

my $cat=AliEn::UI::Catalogue::LCM->new({"user", "newuser",})
  or exit (-1);

my $name="collections/moveCollection";
my ($c)=$cat->execute("mkdir", "-p", "collections") or exit(-2);
$cat->execute("rm", "-rf", $name, "$name.new");
$cat->execute("createCollection", $name) or exit(-2);
print "Let's move the collection\n";
$cat->execute("mv", $name, "$name.new") or exit(-2);

print "Is it still a collection?\n";

my ($type)=$cat->execute("type", "$name.new") or exit(-2);

$type =~ /^collection$/ or print "It is not a collection!!\n" and exit(-2);

print "ok\n";

