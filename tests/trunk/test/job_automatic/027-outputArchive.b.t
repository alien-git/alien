use strict;

use AliEn::UI::Catalogue::LCM::Computer;


my $id=shift;
$id or print "Error getting the id of the job\n" and exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);
my ($user)=$cat->execute("whoami") or exit(-2);

my $procDir="~/alien-job-$id";

$cat->execute("ls", "$procDir", "-l") or  exit(-2);
$cat->execute("get", "$procDir/my_archive") or exit(-2);
print "And checking that the file is not registered\n";

$cat->execute("ls", "$procDir/stdout") and exit(-2);
$cat->close();

print "ok\n";
