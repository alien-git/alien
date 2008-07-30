use strict;

use AliEn::UI::Catalogue::LCM::Computer;

my $id=shift;

$id or print "Error getting the id\n" and exit(-2);


my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);

my ($user)=$cat->execute("whoami");
my $vo=$cat->{CONFIG}->{ORG_NAME};
my $otherSE="${vo}::cern::testSE2";

my $procDir="/proc/$user/$id";
print "And the output is in $procDir\n";
my @where=$cat->execute("whereis", "-lr", "$procDir/job-output/stdout") or exit(-2);

print "The file is in @where\n";
grep( /^$otherSE$/i, @where) or print "The file is not in $otherSE!!\n" and exit(-2);

grep (/^${vo}::cern::testSE$/i, @where) and print "The file is not supposed to tbe in the standard SE!!\n" and exit(-2);

print "ok!!\n";

