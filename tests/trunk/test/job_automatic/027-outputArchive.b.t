use strict;

use AliEn::UI::Catalogue::LCM::Computer;


my $id=shift;
$id or print "Error getting the id of the job\n" and exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my ($user)=$cat->execute("whoami") or exit(-2);

my $procDir="/proc/$user/$id";

$cat->execute("ls", "$procDir/job-output", "-l") or  exit(-2);
$cat->execute("get", "$procDir/job-output/my_archive") or exit(-2);
print "And checking that the file is not registered\n";

$cat->execute("ls", "$procDir/job-output/stdout") and exit(-2);
$cat->close();

print "ok\n";
