use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

my $id=shift;
$id or print "Error getting the id\n" and exit(-2);
my $id2=shift;
$id2 or print "Error getting the id\n" and exit(-2);
my $id3=shift;
$id3 or print "Error getting the id\n" and exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);

my ($user)=$cat->execute("whoami");
my $vo=$cat->{CONFIG}->{ORG_NAME};
my $otherSE="${vo}::cern::testSE2";

my $procDir="/proc/$user/$id";
print "And the output is in $procDir\n";


my (@out)=$cat->execute("whereis","$procDir/job-output/file.out") 
  or exit(-2);

$out[2] or print "Error: the file is only in one SE\n" and exit(-2);
use Data::Dumper;
print Dumper(@out);

print "With the archives\n";
$procDir="/proc/$user/$id2";

(@out)=$cat->execute("whereis","$procDir/job-output/my_archive") 
  or exit(-2);

$out[2] or print "Error: the file is only in one SE\n" and exit(-2);


print "And now the localse\n";
$procDir="/proc/$user/$id3";

(@out)=$cat->execute("whereis","$procDir/job-output/my_archive") 
  or exit(-2);

$out[2] or print "Error: the file is only in one SE\n" and exit(-2);


print "ok\n";
